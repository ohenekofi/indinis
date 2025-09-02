// include/lsm/universal_compaction_strategy.h
#pragma once

#include "compaction_strategy.h"
#include <memory>
#include <atomic>
#include <chrono>
#include <vector>
#include <string>
#include <sstream> // For config to_string()

class LSMTree; // Forward-declare to avoid including the full lsm_tree.h here

namespace engine {
namespace lsm {

/**
 * @struct UniversalCompactionMetricsSnapshot
 * @brief A plain data structure holding a snapshot of universal compaction metrics.
 * This struct is copyable and safe to pass around for monitoring.
 */
struct UniversalCompactionMetricsSnapshot {
    uint64_t l0_compactions_selected = 0;
    uint64_t size_tiered_compactions_selected = 0;
    uint64_t compactions_skipped_size_ratio = 0;
    uint64_t compactions_skipped_resource_limits = 0;
    uint64_t total_files_compacted = 0;
    uint64_t total_bytes_compacted = 0;
    double average_selection_time_us = 0.0;
    double average_compaction_size_mb = 0.0;
};


/**
 * @struct UniversalCompactionMetrics
 * @brief Tracks universal compaction performance and behavior metrics in a thread-safe manner.
 */
struct UniversalCompactionMetrics {
    std::atomic<uint64_t> l0_compactions_selected{0};
    std::atomic<uint64_t> size_tiered_compactions_selected{0};
    std::atomic<uint64_t> compactions_skipped_size_ratio{0};
    std::atomic<uint64_t> compactions_skipped_resource_limits{0};
    std::atomic<uint64_t> total_files_compacted{0};
    std::atomic<uint64_t> total_bytes_compacted{0};
    std::atomic<uint64_t> total_selection_time_us{0};
    std::atomic<uint64_t> selection_calls{0};
    std::atomic<uint64_t> runs_evaluated{0};
    
    void record_selection_time(std::chrono::microseconds duration) {
        total_selection_time_us.fetch_add(duration.count(), std::memory_order_relaxed);
        selection_calls.fetch_add(1, std::memory_order_relaxed);
    }
    
    void record_compaction(size_t file_count, uint64_t bytes) {
        total_files_compacted.fetch_add(file_count, std::memory_order_relaxed);
        total_bytes_compacted.fetch_add(bytes, std::memory_order_relaxed);
    }
    
    double average_selection_time_us() const {
        uint64_t calls = selection_calls.load(std::memory_order_relaxed);
        return calls > 0 ? static_cast<double>(total_selection_time_us.load(std::memory_order_relaxed)) / calls : 0.0;
    }
    
    double average_compaction_size_mb() const {
        uint64_t compactions = size_tiered_compactions_selected.load(std::memory_order_relaxed) + 
                               l0_compactions_selected.load(std::memory_order_relaxed);
        return compactions > 0 ? static_cast<double>(total_bytes_compacted.load(std::memory_order_relaxed)) / (1024 * 1024 * compactions) : 0.0;
    }
};

/**
 * @enum CompactionTrigger
 * @brief Describes why a universal compaction job was selected. Used for metrics and logging.
 */
enum class CompactionTrigger {
    L0_COUNT_TRIGGER,
    SIZE_RATIO_TRIGGER,
    SPACE_AMPLIFICATION
};

/**
 * @struct UniversalCompactionConfig
 * @brief Configuration parameters for the Universal (Size-Tiered) compaction strategy.
 */
struct UniversalCompactionConfig {
    size_t size_ratio_percent = 120;
    size_t min_merge_width = 2;
    size_t max_merge_width = 10;
    size_t l0_compaction_trigger = 4;
    size_t l0_slowdown_trigger = 8;
    size_t l0_stop_trigger = 12;
    size_t max_compaction_bytes = 1024ULL * 1024 * 1024; // 1GB
    size_t max_files_per_compaction = 20;
    double max_space_amplification = 1.5; // Stricter default than 2.0
    bool sort_runs_by_age = true;

    bool is_valid() const {
        return size_ratio_percent >= 100 &&
               min_merge_width >= 2 &&
               max_merge_width >= min_merge_width &&
               l0_compaction_trigger > 0 &&
               l0_compaction_trigger <= l0_slowdown_trigger &&
               l0_slowdown_trigger <= l0_stop_trigger &&
               max_space_amplification >= 1.0;
    }
    
    std::string to_string() const {
        std::ostringstream oss;
        oss << "UniversalConfig{ratio=" << size_ratio_percent << "%"
            << ", merge_width=" << min_merge_width << "-" << max_merge_width
            << ", l0_trigger=" << l0_compaction_trigger
            << ", space_amp_max=" << max_space_amplification << "}";
        return oss.str();
    }
};

/**
 * @struct CompactionCandidate
 * @brief Internal helper struct to represent and score a potential compaction job.
 */
struct CompactionCandidate {
    int level_idx;
    std::vector<std::shared_ptr<SSTableMetadata>> source_files;
    std::vector<std::shared_ptr<SSTableMetadata>> target_files;
    CompactionTrigger trigger_type;
    uint64_t total_size_bytes;
    double priority_score;
    
    // Sorts by score (lower is better/higher priority)
    bool operator<(const CompactionCandidate& other) const {
        return priority_score < other.priority_score;
    }
};

/**
 * @class UniversalCompactionStrategy
 * @brief A production-ready Universal/Size-Tiered compaction implementation.
 */
class UniversalCompactionStrategy : public CompactionStrategy {
public:
    explicit UniversalCompactionStrategy(
        LSMTree* lsm_tree_ptr, 
        const UniversalCompactionConfig& config = UniversalCompactionConfig{}
    );
    
    ~UniversalCompactionStrategy();

    std::optional<CompactionJobForIndinis> SelectCompaction(
        const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
        int max_level
    ) const override;

    UniversalCompactionMetricsSnapshot get_metrics() const;
    const UniversalCompactionConfig& get_config() const { return config_; }
    
    bool should_slowdown_writes(size_t l0_file_count) const {
        return l0_file_count >= config_.l0_slowdown_trigger;
    }
    
    bool should_stop_writes(size_t l0_file_count) const {
        return l0_file_count >= config_.l0_stop_trigger;
    }

private:
    std::optional<CompactionJobForIndinis> try_select_l0_compaction(
        const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot
    ) const;

    std::optional<CompactionJobForIndinis> try_select_size_tiered_compaction(
        const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
        int max_level
    ) const;
    
    std::vector<CompactionCandidate> find_compaction_candidates(
        const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
        int max_level
    ) const;
    
    std::vector<CompactionCandidate> evaluate_compaction_triggers(
        const std::vector<std::shared_ptr<SSTableMetadata>>& level_files,
        int level_idx,
        const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot
    ) const;
    
    double calculate_priority_score(const CompactionCandidate& candidate) const;
    bool is_within_resource_limits(const CompactionCandidate& candidate) const;
    
    std::vector<std::shared_ptr<SSTableMetadata>> sort_files_by_age(
        const std::vector<std::shared_ptr<SSTableMetadata>>& files
    ) const;
    
    bool validate_levels_snapshot(
        const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
        int max_level
    ) const;
    
    uint64_t calculate_total_size(
        const std::vector<std::shared_ptr<SSTableMetadata>>& files
    ) const;

    double calculate_space_amplification(
        const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot
    ) const;
    
    static std::string generate_instance_id();

    LSMTree* lsm_tree_ptr_;
    const UniversalCompactionConfig config_;
    mutable UniversalCompactionMetrics metrics_;
    const std::chrono::steady_clock::time_point creation_time_;
    const std::string instance_id_;
};

} // namespace lsm
} // namespace engine