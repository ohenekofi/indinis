// include/wal/group_commit.h

#pragma once

#include "../types.h"
#include "wal_metrics.h"
#include <vector>
#include <future>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <memory>

// --- START OF FIX ---
#include <deque>  // <<< ADDED: Provides the full definition for std::deque
#include <array>  // <<< ADDED: Provides the full definition for std::array
// --- END OF FIX ---

enum class WALHealth {
    HEALTHY,
    DEGRADED_PERFORMANCE,
    WRITE_STALLED,
    CRITICAL_ERROR
};

// Defines the urgency of a WAL write.
enum class WALPriority {
    NORMAL = 0,
    HIGH = 1,
    IMMEDIATE = 2
};

// A single request submitted by an application thread to the WAL.
struct CommitRequest {
    LogRecord record;
    std::promise<LSN> promise;

    CommitRequest() = default;
    
    CommitRequest(LogRecord rec, std::promise<LSN> prom)
        : record(std::move(rec)), promise(std::move(prom)) {}
};

// Represents a batch of records to be committed together.
struct CommitBatch {
    std::vector<CommitRequest> requests;
    std::chrono::steady_clock::time_point creation_time;

    CommitBatch() : creation_time(std::chrono::steady_clock::now()) {}
};

/**
 * @class GroupCommitManager
 * @brief Manages the batching of commit requests for the WAL.
 */
class GroupCommitManager {
public:
    GroupCommitManager(WALMetrics* metrics);
    ~GroupCommitManager() = default;

    std::future<LSN> submit(LogRecord record, WALPriority priority);
    std::unique_ptr<CommitBatch> getNextBatchToFlush();
    void shutdown();
    size_t getPendingRequestCount() const;

private:
    void adjustBatchingParameters();

    // This line will now compile correctly.
    std::array<std::deque<CommitRequest>, 3> request_queues_;
    mutable std::mutex queue_mutex_;
    std::condition_variable cv_;
    bool shutdown_ = false;

    WALMetrics* metrics_;
    std::atomic<uint32_t> target_batch_size_records_{64};
    std::atomic<uint32_t> max_wait_microseconds_{500};

    enum class TuningState {
        LATENCY_FOCUSED, // Prioritize meeting latency target
        THROUGHPUT_FOCUSED // Latency is good, try to increase throughput
    };
    TuningState current_state_{TuningState::LATENCY_FOCUSED};
    int stable_cycles_count_{0}; // Cycles with good performance
};