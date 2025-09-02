//include/threading/thread_pool.h
#pragma once

#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <atomic>
#include <queue>
#include <string>
#include <array>
#include <optional>

namespace engine {
namespace threading {

class AdaptiveThreadPoolManager; // Forward-declare for friendship

// Defines the priority of a task. HIGH tasks are always executed before NORMAL/LOW.
enum class TaskPriority { HIGH = 0, NORMAL = 1, LOW = 2 };

/**
 * @class ThreadPool
 * @brief A dynamically sizable, priority-aware thread pool.
 *
 * This pool maintains separate queues for different task priorities and ensures
 * high-priority tasks are executed first. It supports dynamic scaling of its
 * worker threads, managed by an external AdaptiveThreadPoolManager.
 */
class ThreadPool {
    friend class AdaptiveThreadPoolManager;
public:
    ThreadPool(size_t min_threads, size_t max_threads, const std::string& name = "ThreadPool");
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    /**
     * @brief Schedules a task for execution.
     * @param task The function to execute.
     * @param priority The priority of the task.
     */
    void schedule(std::function<void()> task, TaskPriority priority);

    /**
     * @brief Gracefully stops the thread pool, waiting for all threads to join.
     */
    void stop();

    // --- Monitoring methods for the AdaptiveThreadPoolManager ---
    size_t getQueueDepth() const;
    size_t getActiveThreads() const;
    size_t getCurrentThreadCount() const;
    //const std::string name_;

    // --- Add public getters for N-API monitoring >>>
    const std::string& getName() const { return name_; }
    size_t getMinThreads() const { return min_threads_; }
    size_t getMaxThreads() const { return max_threads_; }

private:
    void workerLoop();
    bool allQueuesAreEmpty() const; // Helper for wait predicate

    void scaleUp(size_t count);
    void scaleDown(size_t count);

    const size_t min_threads_;
    const size_t max_threads_;
    const std::string name_;

    std::vector<std::thread> workers_;
    std::array<std::queue<std::function<void()>>, 3> task_queues_;
    
    mutable std::mutex queue_mutex_;
    std::condition_variable condition_;

    std::atomic<bool> stop_{false};
    std::atomic<size_t> threads_to_stop_{0};
    std::atomic<size_t> active_threads_{0};
};

} // namespace threading
} // namespace engine