// include/wal/wal_metrics.h

#pragma once
#include <atomic>
#include <chrono>
#include <vector>
#include <cstdint>

/**
 * @struct WALMetricsSnapshot
 * @brief A plain data structure to hold a snapshot of WAL metrics.
 * This is returned to the user to avoid exposing atomic variables directly.
 */
struct WALMetricsSnapshot {
    uint64_t records_written = 0;
    uint64_t bytes_written = 0;
    uint64_t sync_operations = 0;
    uint64_t segment_rotations = 0;
    uint64_t p99_latency_us = 0;
    double throughput_rps = 0.0;
    uint64_t io_errors = 0;
};

class WALMetrics {
public:
    WALMetrics();

    void recordFlush(uint64_t records_in_batch, uint64_t bytes_in_batch, uint64_t flush_duration_us, bool was_synced);
    void recordSegmentRotation();
    void recordIOError();
    
    WALMetricsSnapshot getSnapshot();
    uint64_t getP99LatencyUs() const;
    double calculateThroughput();
    // Returns throughput in records per second over the last measurement period.
    double getThroughputRecordsPerSec();

private:


    // Latency Histogram (same as before)
    static constexpr size_t NUM_BUCKETS = 8;
    const std::vector<uint64_t> latency_buckets_us_;
    std::vector<std::atomic<uint64_t>> histogram_counts_;
    std::atomic<uint64_t> total_observations_{0};

    // Throughput measurement (same as before)
    std::atomic<uint64_t> records_since_last_check_{0};
    std::chrono::steady_clock::time_point last_throughput_check_time_;
    
    // --- NEW COUNTERS ---
    std::atomic<uint64_t> total_bytes_written_{0};
    std::atomic<uint64_t> total_sync_operations_{0};
    std::atomic<uint64_t> total_segment_rotations_{0};
    std::atomic<uint64_t> total_io_errors_{0};
};