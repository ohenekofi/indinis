// include/lsm/fifo_compaction_strategy.h
#pragma once

#include "compaction_strategy.h"
#include <chrono>
#include <atomic>
#include <string>
#include <sstream>

class LSMTree; // Forward-declare

namespace engine {
namespace lsm {

/**
 * @struct FIFOCompactionMetricsSnapshot
 * @brief A plain data structure holding a snapshot of FIFO compaction metrics.
 */
struct FIFOCompactionMetricsSnapshot {
    uint64_t expired_files_found = 0;
    uint64_t total_files_deleted = 0;
    uint64_t total_bytes_deleted = 0;
    double average_selection_time_us = 0.0;
};

/**
 * @struct FIFOCompactionMetrics
 * @brief Tracks FIFO compaction performance metrics in a thread-safe manner.
 */
struct FIFOCompactionMetrics {
    std::atomic<uint64_t> expired_files_found{0};
    std::atomic<uint64_t> total_files_deleted{0};
    std::atomic<uint64_t> total_bytes_deleted{0};
    std::atomic<uint64_t> total_selection_time_us{0};
    std::atomic<uint64_t> selection_calls{0};

    void record_selection_time(std::chrono::microseconds duration) {
        total_selection_time_us.fetch_add(duration.count(), std::memory_order_relaxed);
        selection_calls.fetch_add(1, std::memory_order_relaxed);
    }
    
    double average_selection_time_us() const {
        uint64_t calls = selection_calls.load(std::memory_order_relaxed);
        return calls > 0 ? static_cast<double>(total_selection_time_us.load(std::memory_order_relaxed)) / calls : 0.0;
    }
};

/**
 * @struct FIFOCompactionConfig
 * @brief Configuration parameters for the FIFO compaction strategy.
 */
struct FIFOCompactionConfig {
    // The age after which an entire SSTable is considered expired and can be deleted.
    std::chrono::seconds ttl_seconds{0};

    // The maximum number of SSTables to delete in a single compaction job.
    // Prevents a single job from holding locks for too long if many files expire at once.
    size_t max_files_to_delete_in_one_go = 10;

    bool is_valid() const {
        // A TTL of zero or less means FIFO is effectively disabled.
        return max_files_to_delete_in_one_go > 0;
    }
    
    std::string to_string() const {
        std::ostringstream oss;
        oss << "FIFOConfig{ttl=" << ttl_seconds.count() << "s"
            << ", max_delete_batch=" << max_files_to_delete_in_one_go << "}";
        return oss.str();
    }
};

/**
 * @class FIFOCompactionStrategy
 * @brief Implements FIFO (First-In, First-Out) compaction for TTL-based data.
 *
 * This strategy is for cache-like workloads where old data should be discarded entirely.
 * It does not merge data. Instead, it identifies SSTables whose creation timestamp
 * is older than a configured Time-To-Live (TTL) and schedules them for deletion.
 */
class FIFOCompactionStrategy : public CompactionStrategy {
public:
    explicit FIFOCompactionStrategy(const FIFOCompactionConfig& config = FIFOCompactionConfig{});
    ~FIFOCompactionStrategy();

    std::optional<CompactionJobForIndinis> SelectCompaction(
        const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
        int max_level
    ) const override;
        
    FIFOCompactionMetricsSnapshot get_metrics() const;
    const FIFOCompactionConfig& get_config() const { return config_; }

private:
    const FIFOCompactionConfig config_;
    mutable FIFOCompactionMetrics metrics_;
    const std::chrono::steady_clock::time_point creation_time_;
};

} // namespace lsm
} // namespace engine