// src/wal/wal_metrics.cpp

#include "../../include/wal/wal_metrics.h"
#include <algorithm> // For std::upper_bound

WALMetrics::WALMetrics()
    // Define the upper bounds for our latency buckets in microseconds
    : latency_buckets_us_({
          100,      // Bucket 0: <= 100 us
          250,      // Bucket 1: 101 - 250 us
          500,      // Bucket 2: 251 - 500 us
          1000,     // Bucket 3: 501 - 1000 us (1ms)
          2500,     // Bucket 4: 1001 - 2500 us
          5000,     // Bucket 5: 2501 - 5000 us (5ms)
          10000,    // Bucket 6: 5001 - 10000 us (10ms)
          -1ULL     // Bucket 7: > 10000 us (overflow)
      }),
      histogram_counts_(NUM_BUCKETS), // Vector of atomics
      last_throughput_check_time_(std::chrono::steady_clock::now()) 
{
    // Initialize all atomic counters in the histogram to zero.
    for(auto& count : histogram_counts_) {
        count.store(0, std::memory_order_relaxed);
    }
}

void WALMetrics::recordFlush(uint64_t records_in_batch, uint64_t bytes_in_batch, uint64_t flush_duration_us, bool was_synced) {
    // Atomically update counters. `relaxed` is fine as these are simple counters
    // without release/acquire semantics needed for other threads.
    records_since_last_check_.fetch_add(records_in_batch, std::memory_order_relaxed);
    total_bytes_written_.fetch_add(bytes_in_batch, std::memory_order_relaxed);
    
    if (was_synced) {
        total_sync_operations_.fetch_add(1, std::memory_order_relaxed);
    }

    // Find the correct bucket for the measured latency.
    auto it = std::upper_bound(latency_buckets_us_.begin(), latency_buckets_us_.end() - 1, flush_duration_us);
    size_t bucket_index = std::distance(latency_buckets_us_.begin(), it);

    // Atomically increment the count for that bucket.
    histogram_counts_[bucket_index].fetch_add(1, std::memory_order_relaxed);
    total_observations_.fetch_add(1, std::memory_order_relaxed);
}

void WALMetrics::recordSegmentRotation() {
    total_segment_rotations_.fetch_add(1, std::memory_order_relaxed);
}

void WALMetrics::recordIOError() {
    total_io_errors_.fetch_add(1, std::memory_order_relaxed);
}

WALMetricsSnapshot WALMetrics::getSnapshot() {
    WALMetricsSnapshot snapshot;
    // Note: records_written in the snapshot is the total count since the last throughput calculation.
    snapshot.records_written = records_since_last_check_.load(std::memory_order_relaxed);
    snapshot.bytes_written = total_bytes_written_.load(std::memory_order_relaxed);
    snapshot.sync_operations = total_sync_operations_.load(std::memory_order_relaxed);
    snapshot.segment_rotations = total_segment_rotations_.load(std::memory_order_relaxed);
    snapshot.io_errors = total_io_errors_.load(std::memory_order_relaxed);
    snapshot.p99_latency_us = getP99LatencyUs();
    snapshot.throughput_rps = calculateThroughput();
    return snapshot;
}

uint64_t WALMetrics::getP99LatencyUs() const {
    uint64_t total_count = total_observations_.load(std::memory_order_acquire);
    if (total_count < 100) { // Not enough data for a stable P99, return 0.
        return 0;
    }

    // Calculate the observation count that represents the 99th percentile.
    uint64_t p99_threshold_count = (total_count * 99) / 100;
    uint64_t cumulative_count = 0;

    for (size_t i = 0; i < NUM_BUCKETS; ++i) {
        cumulative_count += histogram_counts_[i].load(std::memory_order_relaxed);
        if (cumulative_count >= p99_threshold_count) {
            // We've found the bucket containing the 99th percentile.
            // Return the upper bound of this bucket as the P99 latency.
            return latency_buckets_us_[i];
        }
    }
    
    // Fallback in case of rounding issues, return the overflow bucket value.
    return latency_buckets_us_.back();
}

double WALMetrics::calculateThroughput() {
    auto now = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_throughput_check_time_).count();
    
    // To avoid noisy, volatile measurements, only calculate throughput if a
    // reasonable amount of time (e.g., 500ms) has passed.
    if (elapsed_ms < 500) {
        return -1.0; // Use a sentinel value to indicate "no new measurement available".
    }

    // Atomically read the number of records processed since the last check AND reset it to 0.
    // `exchange` provides the necessary atomic read-and-reset operation.
    uint64_t records = records_since_last_check_.exchange(0, std::memory_order_relaxed);
    
    // Update the timestamp for the next interval.
    last_throughput_check_time_ = now;
    
    // Calculate records per second.
    return static_cast<double>(records) * 1000.0 / static_cast<double>(elapsed_ms);
}

double WALMetrics::getThroughputRecordsPerSec() {
    auto now = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_throughput_check_time_).count();
    
    if (elapsed_ms < 500) { // Don't measure too frequently
        return -1.0; // Indicate no new measurement
    }

    uint64_t records = records_since_last_check_.exchange(0, std::memory_order_relaxed);
    last_throughput_check_time_ = now;
    
    return static_cast<double>(records) * 1000.0 / static_cast<double>(elapsed_ms);
}