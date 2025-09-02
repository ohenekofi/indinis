// src/threading/thread_pool.cpp
#include "../../include/threading/thread_pool.h"
#include "../../include/debug_utils.h" // For LOG_... macros

namespace engine {
namespace threading {

ThreadPool::ThreadPool(size_t min_threads, size_t max_threads, const std::string& name)
    : min_threads_(min_threads),
      max_threads_(max_threads > min_threads ? max_threads : min_threads),
      name_(name)
{
    for (size_t i = 0; i < min_threads_; ++i) {
        workers_.emplace_back(&ThreadPool::workerLoop, this);
    }
    LOG_INFO("[{}] Created with {} to {} threads.", name_, min_threads_, max_threads_);
}

ThreadPool::~ThreadPool() {
    stop();
}

void ThreadPool::stop() {
    if (stop_.exchange(true)) {
        return; // Already stopping
    }
    LOG_INFO("[{}] Stopping...", name_);
    
    threads_to_stop_.store(0); // Cancel any pending scale-down requests
    condition_.notify_all();

    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();
    LOG_INFO("[{}] All threads joined. Stopped.", name_);
}

void ThreadPool::schedule(std::function<void()> task, TaskPriority priority) {
    if (stop_.load()) {
        LOG_WARN("[{}] scheduled a task after stop() was called. Task ignored.", name_);
        return;
    }
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        task_queues_[static_cast<int>(priority)].push(std::move(task));
    }
    condition_.notify_one();
}

void ThreadPool::workerLoop() {
    LOG_INFO("[{}] Worker thread {} started.", name_, std::this_thread::get_id());
    while (!stop_.load()) {
        std::optional<std::function<void()>> task;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            condition_.wait(lock, [this] {
                return stop_.load() || !allQueuesAreEmpty() || threads_to_stop_.load() > 0;
            });

            // Priority 1: Check if this thread needs to scale down
            if (threads_to_stop_.load() > 0) {
                threads_to_stop_--;
                LOG_INFO("[{}] Worker thread {} stopping for scale-down.", name_, std::this_thread::get_id());
                return; // Exit the loop
            }

            // Priority 2: Check for shutdown
            if (stop_.load()) {
                break;
            }

            // Priority 3: Fetch a task, scanning from HIGH to LOW priority
            for (int i = 0; i < 3; ++i) {
                if (!task_queues_[i].empty()) {
                    task = std::move(task_queues_[i].front());
                    task_queues_[i].pop();
                    break;
                }
            }
        } // Mutex is released here

        if (task) {
            active_threads_++;
            try {
                (*task)();
            } catch (const std::exception& e) {
                LOG_ERROR("[{}] Worker thread caught exception: {}", name_, e.what());
            } catch (...) {
                LOG_ERROR("[{}] Worker thread caught unknown exception.", name_);
            }
            active_threads_--;
        }
    }
    LOG_INFO("[{}] Worker thread {} stopped.", name_, std::this_thread::get_id());
}

void ThreadPool::scaleUp(size_t count) {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    for (size_t i = 0; i < count; ++i) {
        if (workers_.size() < max_threads_) {
            workers_.emplace_back(&ThreadPool::workerLoop, this);
            LOG_INFO("[{}] Scaled up. New thread count: {}", name_, workers_.size());
        } else {
            break;
        }
    }
}

void ThreadPool::scaleDown(size_t count) {
    // Only request to stop if it doesn't drop below min_threads_
    size_t current_count = getCurrentThreadCount();
    size_t effective_count = std::min(count, (current_count > min_threads_) ? (current_count - min_threads_) : 0);

    if (effective_count > 0) {
        threads_to_stop_.fetch_add(effective_count);
        condition_.notify_all(); // Wake up idle threads so they can terminate
        LOG_INFO("[{}] Requested scale-down of {} threads.", name_, effective_count);
    }
}

size_t ThreadPool::getQueueDepth() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    size_t total = 0;
    for (const auto& q : task_queues_) {
        total += q.size();
    }
    return total;
}

size_t ThreadPool::getActiveThreads() const {
    return active_threads_.load();
}

size_t ThreadPool::getCurrentThreadCount() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return workers_.size();
}

bool ThreadPool::allQueuesAreEmpty() const {
    // Assumes queue_mutex_ is held by caller
    for (const auto& q : task_queues_) {
        if (!q.empty()) {
            return false;
        }
    }
    return true;
}

} // namespace threading
} // namespace engine