// src/wal/group_commit.cpp (Full Implementation)

#include "../../include/wal/group_commit.h"
#include "../../include/debug_utils.h"
#include <algorithm> // For std::min/max

GroupCommitManager::GroupCommitManager(WALMetrics* metrics)
    : metrics_(metrics) {
    if (!metrics_) {
        throw std::invalid_argument("WALMetrics pointer cannot be null for GroupCommitManager.");
    }
}

void GroupCommitManager::shutdown() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    shutdown_ = true;
    cv_.notify_all();
}

std::future<LSN> GroupCommitManager::submit(LogRecord record, WALPriority priority) {
    std::promise<LSN> promise;
    auto future = promise.get_future();
    int level = static_cast<int>(priority);

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (shutdown_) {
            promise.set_exception(std::make_exception_ptr(std::runtime_error("WAL is shutting down.")));
        } else {
            request_queues_[level].emplace_back(std::move(record), std::move(promise));
        }
    }

    // A high-priority write should wake up the commit leader immediately.
    if (priority == WALPriority::IMMEDIATE) {
        cv_.notify_one();
    }
    
    return future;
}

std::unique_ptr<CommitBatch> GroupCommitManager::getNextBatchToFlush() {
    std::unique_lock<std::mutex> lock(queue_mutex_);

    // Adjust batching parameters based on recent performance *before* waiting.
    adjustBatchingParameters();

    const auto timeout = std::chrono::microseconds(max_wait_microseconds_.load(std::memory_order_relaxed));

    // The waiting condition is now hierarchical.
    cv_.wait_for(lock, timeout, [this] {
        if (shutdown_) return true;
        // Wake up if ANY higher-priority queue has items, OR if the normal queue is full.
        return !request_queues_[static_cast<int>(WALPriority::IMMEDIATE)].empty() ||
               !request_queues_[static_cast<int>(WALPriority::HIGH)].empty() ||
               request_queues_[static_cast<int>(WALPriority::NORMAL)].size() >= target_batch_size_records_.load();
    });

    if (shutdown_ && request_queues_[0].empty() && request_queues_[1].empty() && request_queues_[2].empty()) {
        return nullptr; // Shutdown and empty, signal to stop.
    }

    auto batch = std::make_unique<CommitBatch>();
    
    // Drain queues, strictly respecting priority: IMMEDIATE > HIGH > NORMAL.
    for (int i = static_cast<int>(WALPriority::IMMEDIATE); i >= static_cast<int>(WALPriority::NORMAL); --i) {
        auto& queue = request_queues_[i];
        if (!queue.empty()) {
            std::move(queue.begin(), queue.end(), std::back_inserter(batch->requests));
            queue.clear();
        }
    }

    if (batch->requests.empty()) {
        return nullptr; // Woke up by timeout but nothing to do.
    }

    return batch;
}

void GroupCommitManager::adjustBatchingParameters() {
    // --- Configuration Constants ---
    constexpr uint64_t P99_LATENCY_TARGET_US = 1500;   // Target P99 latency of 1.5ms
    constexpr double THROUGHPUT_TARGET_RPS = 100000.0; // Target throughput of 100k records/sec
    constexpr int CYCLES_TO_BECOME_STABLE = 20;      // Requires 20 good cycles to switch to throughput mode

    // --- Collect Metrics ---
    uint64_t p99_latency = metrics_->getP99LatencyUs();
    double throughput_rps = metrics_->getThroughputRecordsPerSec();
    
    // getThroughputRecordsPerSec returns -1 if not enough time has passed
    if (throughput_rps < 0.0) {
        return; // Not enough time elapsed for a new throughput measurement, do not adjust.
    }

    // --- State Machine and Hysteresis Logic ---
    if (p99_latency > P99_LATENCY_TARGET_US && p99_latency > 0) {
        // Latency target missed. Immediately switch to latency-focused mode.
        if (current_state_ != TuningState::LATENCY_FOCUSED) {
            LOG_INFO("[WAL Adaptive] P99 Latency high ({}us > {}us). Switching to LATENCY_FOCUSED mode.", p99_latency, P99_LATENCY_TARGET_US);
            current_state_ = TuningState::LATENCY_FOCUSED;
        }
        stable_cycles_count_ = 0; // Reset stability counter
    } else {
        // Latency target met.
        stable_cycles_count_++;
        if (stable_cycles_count_ >= CYCLES_TO_BECOME_STABLE && current_state_ != TuningState::THROUGHPUT_FOCUSED) {
            LOG_INFO("[WAL Adaptive] P99 Latency stable for {} cycles. Switching to THROUGHPUT_FOCUSED mode.", stable_cycles_count_);
            current_state_ = TuningState::THROUGHPUT_FOCUSED;
        }
    }

    // --- Parameter Adjustment Logic ---
    uint32_t old_wait = max_wait_microseconds_.load();
    uint32_t old_size = target_batch_size_records_.load();

    if (current_state_ == TuningState::LATENCY_FOCUSED) {
        // We are in latency mode. Aggressively reduce batch size and wait time.
        // The adjustment is proportional to how much we missed the target.
        double error_ratio = static_cast<double>(p99_latency) / P99_LATENCY_TARGET_US;
        
        // Decrease wait time by a factor related to the error, but not too drastically.
        uint32_t new_wait = static_cast<uint32_t>(old_wait / (1.0 + (error_ratio - 1.0) * 0.5));
        max_wait_microseconds_.store(std::max(100u, new_wait)); // Floor at 100us

        // Decrease batch size similarly.
        uint32_t new_size = static_cast<uint32_t>(old_size / (1.0 + (error_ratio - 1.0) * 0.5));
        target_batch_size_records_.store(std::max(8u, new_size)); // Floor at 8 records

    } else { // THROUGHPUT_FOCUSED
        // Latency is good. Slowly and cautiously increase batch size and wait time to improve throughput.
        // We only increase if throughput is also below target, to avoid pointless increases.
        if (throughput_rps < THROUGHPUT_TARGET_RPS) {
            uint32_t new_wait = old_wait + 25; // Small, constant increase
            max_wait_microseconds_.store(std::min(10000u, new_wait)); // Cap at 10ms

            uint32_t new_size = old_size + 4; // Small, constant increase
            target_batch_size_records_.store(std::min(2048u, new_size)); // Cap at 2048 records
        }
    }
    
    // Log only if a change occurred
    uint32_t final_wait = max_wait_microseconds_.load();
    uint32_t final_size = target_batch_size_records_.load();
    if (final_wait != old_wait || final_size != old_size) {
         LOG_TRACE("[WAL Adaptive] State: {}. LatencyP99: {}us. Throughput: {:.0f} rps. BatchSize {}->{}. WaitTime {}->{}us.",
                  (current_state_ == TuningState::LATENCY_FOCUSED ? "LATENCY" : "THROUGHPUT"),
                  p99_latency, throughput_rps, old_size, final_size, old_wait, final_wait);
    }
}


size_t GroupCommitManager::getPendingRequestCount() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    size_t total = 0;
    for (const auto& queue : request_queues_) {
        total += queue.size();
    }
    return total;
}