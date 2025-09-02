// include/lsm/leveled_compaction_strategy.h
#pragma once

#include "compaction_strategy.h"
#include <memory>
#include <atomic>
#include <chrono>

class LSMTree; // Forward-declare

namespace engine {
namespace lsm {

/**
 * @struct CompactionMetricsSnapshot
 * @brief A plain data structure holding a snapshot of compaction metrics.
 * This struct is copyable and safe to pass around.
 */
struct CompactionMetricsSnapshot {
    uint64_t l0_compactions_selected = 0;
    uint64_t level_compactions_selected = 0;
    uint64_t compactions_skipped_not_worthwhile = 0;
    uint64_t empty_levels_encountered = 0;
    double average_selection_time_us = 0.0;
};

/**
 * @struct CompactionMetrics
 * @brief Tracks compaction performance and behavior metrics in a thread-safe manner.
 */
struct CompactionMetrics {
    std::atomic<uint64_t> l0_compactions_selected{0};
    std::atomic<uint64_t> level_compactions_selected{0};
    std::atomic<uint64_t> compactions_skipped_not_worthwhile{0};
    std::atomic<uint64_t> empty_levels_encountered{0};
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
 * @struct LeveledCompactionConfig
 * @brief Configuration parameters for the leveled compaction strategy.
 */
struct LeveledCompactionConfig {
    size_t l0_compaction_trigger = 4;
    size_t l0_slowdown_trigger = 8;
    size_t l0_stop_trigger = 12;
    size_t sstable_target_size = 64 * 1024 * 1024; // 64MB
    double level_size_multiplier = 10.0;
    size_t max_compaction_bytes = 256 * 1024 * 1024; // 256MB
    size_t max_files_per_compaction = 10;
    
    bool is_valid() const {
        return l0_compaction_trigger > 0 && 
               l0_compaction_trigger <= l0_slowdown_trigger &&
               l0_slowdown_trigger <= l0_stop_trigger &&
               sstable_target_size > 0 &&
               level_size_multiplier > 1.0 &&
               max_compaction_bytes >= sstable_target_size;
    }
};

/**
 * @enum CompactionPriority
 * @brief Priority levels used internally for selecting the most urgent compaction job.
 */
enum class CompactionPriority { LOW = 0, MEDIUM = 1, HIGH = 2, URGENT = 3 };

/**
 * @class LeveledCompactionStrategy
 * @brief A production-ready implementation of the classic leveled compaction algorithm.
 */
class LeveledCompactionStrategy : public CompactionStrategy {
public:
    explicit LeveledCompactionStrategy(
        LSMTree* lsm_tree_ptr,
        const LeveledCompactionConfig& config = LeveledCompactionConfig{}
    );
    
    ~LeveledCompactionStrategy();

    std::optional<CompactionJobForIndinis> SelectCompaction(
        const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
        int max_level
    ) const override;

     /**
     * @brief Atomically captures a snapshot of the current compaction metrics.
     * @return A CompactionMetricsSnapshot object containing the current values.
     */
    CompactionMetricsSnapshot get_metrics() const;
    const LeveledCompactionConfig& get_config() const { return config_; }
    
    bool should_slowdown_writes(size_t l0_file_count) const {
        return l0_file_count >= config_.l0_slowdown_trigger;
    }
    
    bool should_stop_writes(size_t l0_file_count) const {
        return l0_file_count >= config_.l0_stop_trigger;
    }

private:
    uint64_t calculate_dynamic_target_for_level(
        int level_idx,
        const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot
    ) const;
    
    std::optional<CompactionJobForIndinis> try_select_l0_compaction(
        const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot
    ) const;

    std::optional<CompactionJobForIndinis> try_select_level_compaction(
        const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
        int max_level
    ) const;
    
    std::optional<CompactionJobForIndinis> select_compaction_candidate_for_level(
        const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot, 
        int level_idx
    ) const;
    
    CompactionPriority calculate_compaction_priority(
        int level_idx, 
        uint64_t current_size, 
        uint64_t target_size
    ) const;
    
    bool validate_levels_snapshot(
        const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
        int max_level
    ) const;
    
    bool is_within_resource_limits(
        const std::vector<std::shared_ptr<SSTableMetadata>>& source_files,
        const std::vector<std::shared_ptr<SSTableMetadata>>& target_files
    ) const;

    double calculate_score(const SSTableMetadata& sstable) const;
    bool is_worthwhile(
        const std::vector<std::shared_ptr<SSTableMetadata>>& source_files,
        const std::vector<std::shared_ptr<SSTableMetadata>>& target_files
    ) const;

    LSMTree* lsm_tree_ptr_;
    const LeveledCompactionConfig config_;
    mutable CompactionMetrics metrics_;
    const std::chrono::steady_clock::time_point creation_time_;
};

} // namespace lsm
} // namespace engine