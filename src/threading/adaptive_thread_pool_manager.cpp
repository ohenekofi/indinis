// src/threading/adaptive_thread_pool_manager.cpp
#include "../../include/threading/adaptive_thread_pool_manager.h"
#include "../../include/threading/thread_pool.h"
#include "../../include/threading/system_monitor.h"
#include "../../include/debug_utils.h"

namespace engine {
namespace threading {

AdaptiveThreadPoolManager::AdaptiveThreadPoolManager(ThreadPool* pool, SystemMonitor* monitor, const Config& config)
    : pool_(pool), monitor_(monitor), config_(config)
{
    if (!pool_ || !monitor_) {
        throw std::invalid_argument("ThreadPool and SystemMonitor pointers cannot be null.");
    }
}

AdaptiveThreadPoolManager::~AdaptiveThreadPoolManager() {
    stop();
}

void AdaptiveThreadPoolManager::start() {
    if (stop_.load()) return;
    manager_thread_ = std::thread(&AdaptiveThreadPoolManager::managerLoop, this);
}

void AdaptiveThreadPoolManager::stop() {
    if (stop_.exchange(true)) return;
    if (manager_thread_.joinable()) {
        manager_thread_.join();
    }
}

void AdaptiveThreadPoolManager::managerLoop() {
    LOG_INFO("[ThreadPoolManager] Adaptive manager thread {} started.", std::this_thread::get_id());
    
    while (!stop_.load()) {
        std::this_thread::sleep_for(config_.monitoring_interval);
        if (stop_.load()) break;

        try {
            double cpu_util = monitor_->getCpuUtilization();
            size_t queue_depth = pool_->getQueueDepth();
            size_t current_threads = pool_->getCurrentThreadCount();

            // --- Scale-Up Logic ---
            if (cpu_util < config_.scale_up_cpu_threshold &&
                queue_depth > (current_threads * config_.queue_depth_per_thread_threshold))
            {
                LOG_INFO("[ThreadPoolManager] Scaling UP. CPU: {:.2f}, Queue: {}, Threads: {}", cpu_util, queue_depth, current_threads);
                pool_->scaleUp(1);
                std::this_thread::sleep_for(config_.cooldown_period); // Hysteresis
                continue;
            }

            // --- Scale-Down Logic ---
            if (cpu_util < config_.scale_down_cpu_threshold &&
                queue_depth < config_.scale_down_queue_threshold)
            {
                LOG_INFO("[ThreadPoolManager] Scaling DOWN. CPU: {:.2f}, Queue: {}, Threads: {}", cpu_util, queue_depth, current_threads);
                pool_->scaleDown(1);
                std::this_thread::sleep_for(config_.cooldown_period); // Hysteresis
            }

        } catch (const std::exception& e) {
            LOG_ERROR("[ThreadPoolManager] Exception in manager loop: {}", e.what());
        }
    }
    LOG_INFO("[ThreadPoolManager] Adaptive manager thread {} stopped.", std::this_thread::get_id());
}

} // namespace threading
} // namespace engine

