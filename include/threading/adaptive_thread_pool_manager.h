// include/threading/adaptive_thread_pool_manager.h
#pragma once

#include <thread>
#include <atomic>
#include <chrono>
#include <memory>

namespace engine {
namespace threading {

// Forward-declare dependencies
class ThreadPool;
class SystemMonitor;

/**
 * @class AdaptiveThreadPoolManager
 * @brief Manages the dynamic sizing of a ThreadPool based on workload and system metrics.
 *
 * This manager runs in a background thread, periodically checking CPU utilization
 * and task queue depth to decide whether to add or remove worker threads from the
 * pool it manages.
 */
class AdaptiveThreadPoolManager {
public:
    struct Config {
        std::chrono::milliseconds monitoring_interval{1000};
        std::chrono::milliseconds cooldown_period{5000};
        size_t queue_depth_per_thread_threshold{10};
        double scale_up_cpu_threshold{0.85};
        double scale_down_cpu_threshold{0.40};
        size_t scale_down_queue_threshold{2};
    };

    AdaptiveThreadPoolManager(ThreadPool* pool, SystemMonitor* monitor, const Config& config = {});
    ~AdaptiveThreadPoolManager();

    AdaptiveThreadPoolManager(const AdaptiveThreadPoolManager&) = delete;
    AdaptiveThreadPoolManager& operator=(const AdaptiveThreadPoolManager&) = delete;

    void start();
    void stop();

private:
    void managerLoop();

    ThreadPool* pool_;
    SystemMonitor* monitor_;
    Config config_;

    std::thread manager_thread_;
    std::atomic<bool> stop_{false};
};

} // namespace threading
} // namespace engine