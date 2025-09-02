// src/lsm/leveled_compaction_strategy.cpp
#include "../../include/lsm/leveled_compaction_strategy.h"
#include "../../include/lsm_tree.h"
#include "../../include/debug_utils.h" // For LOG_INFO, LOG_WARN, etc.
#include "../../include/storage_error/storage_error.h" // For throwing structured errors
#include "../../include/storage_error/error_codes.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <sstream>
#include <chrono>

namespace engine {
namespace lsm {

LeveledCompactionStrategy::LeveledCompactionStrategy(
    LSMTree* lsm_tree_ptr,
    const LeveledCompactionConfig& config
) : lsm_tree_ptr_(lsm_tree_ptr),
    config_(config),
    creation_time_(std::chrono::steady_clock::now())
{
    if (!lsm_tree_ptr_) {
        // Use the storage_error library for consistent error objects
        throw storage::StorageError(storage::ErrorCode::INVALID_CONFIGURATION,
            "LeveledCompactionStrategy: LSMTree pointer cannot be null.");
    }
    
    if (!config_.is_valid()) {
        std::ostringstream oss;
        oss << "Invalid LeveledCompactionConfig: "
            << "l0_trigger=" << config_.l0_compaction_trigger
            << ", l0_slowdown=" << config_.l0_slowdown_trigger
            << ", l0_stop=" << config_.l0_stop_trigger
            << ", target_size=" << config_.sstable_target_size
            << ", multiplier=" << config_.level_size_multiplier;
        throw storage::StorageError(storage::ErrorCode::INVALID_CONFIGURATION, oss.str());
    }
    
    // Use the engine's standard logging utility
    LOG_INFO("[LeveledCompactionStrategy] Created with config: L0_trigger={}, target_size={}MB, multiplier={:.1f}",
             config_.l0_compaction_trigger, config_.sstable_target_size / (1024*1024), config_.level_size_multiplier);
}

uint64_t LeveledCompactionStrategy::calculate_dynamic_target_for_level(
    int level_idx,
    const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot
) const {
    if (level_idx <= 0) {
        // L0 does not have a size target; its trigger is based on file count.
        return 0;
    }
    if (level_idx == 1) {
        // The target for L1 is always the fixed base size.
        return config_.sstable_target_size;
    }

    // For L2+, calculate the target dynamically.
    uint64_t target_size = config_.sstable_target_size;
    for (int i = 2; i <= level_idx; ++i) {
        target_size = static_cast<uint64_t>(target_size * config_.level_size_multiplier);
    }
    return target_size;

    // --- ADVANCED DYNAMIC LOGIC (For future consideration) ---
    /*
    // Find the deepest level with data.
    int max_level_with_data = 0;
    for (int i = levels_snapshot.size() - 1; i >= 0; --i) {
        if (!levels_snapshot[i].empty()) {
            max_level_with_data = i;
            break;
        }
    }

    if (level_idx >= max_level_with_data) {
        // The last level with data has no size limit.
        return std::numeric_limits<uint64_t>::max();
    }

    // Calculate the total size of all deeper levels.
    uint64_t deeper_levels_size = 0;
    for (int i = level_idx + 1; i < levels_snapshot.size(); ++i) {
        deeper_levels_size += lsm_tree_ptr_->calculate_level_size_bytes(levels_snapshot[i]);
    }
    
    // The target size for the current level is a fraction of the total size of all deeper levels.
    // Ensure we don't go below the base target size.
    uint64_t dynamic_target = static_cast<uint64_t>(deeper_levels_size / config_.level_size_multiplier);
    return std::max(config_.sstable_target_size, dynamic_target);
    */
}

double LeveledCompactionStrategy::calculate_score(const SSTableMetadata& sstable) const {
    // Score is higher for more desirable candidates.

    // 1. Tombstone Score (Highest Weight)
    // A file that is 50% tombstones gets a very high score.
    double tombstone_ratio = (sstable.total_entries > 0)
        ? static_cast<double>(sstable.tombstone_entries) / sstable.total_entries
        : 0.0;
    double tombstone_score = tombstone_ratio * 100.0; // Weight: 100

    // 2. Age Score (Lower Weight)
    // Older files (smaller ID) get a higher score.
    uint64_t current_max_id = lsm_tree_ptr_->getNextSSTableFileId();
    double age_score = 0.0;
    if (current_max_id > sstable.sstable_id && sstable.sstable_id > 0) {
        age_score = static_cast<double>(current_max_id - sstable.sstable_id) / 
                    static_cast<double>(current_max_id) * 10.0; // Weight: 10
    }
    
    double final_score = tombstone_score + age_score;
    LOG_TRACE("  [LeveledScore] SSTable {}: TombstoneRatio={:.2f} (Score={:.2f}), AgeScore={:.2f}. FinalScore={:.2f}",
              sstable.sstable_id, tombstone_ratio, tombstone_score, age_score, final_score);
              
    return final_score;
}

bool LeveledCompactionStrategy::is_worthwhile(
    const std::vector<std::shared_ptr<SSTableMetadata>>& source_files,
    const std::vector<std::shared_ptr<SSTableMetadata>>& target_files
) const {
    if (source_files.empty()) {
        return false;
    }
    
    // Rule 1: Don't compact a single, small, healthy source file if it doesn't overlap with anything.
    if (source_files.size() == 1 && target_files.empty()) {
        const auto& single_source = source_files[0];
        
        // Use the strategy's own config and scoring method.
        if (single_source->size_bytes < (config_.sstable_target_size / 2) && 
            calculate_score(*single_source) < 0.5) // A low heuristic score threshold
        {
            LOG_TRACE("    [is_worthwhile] Single source SSTable {} is too small/healthy to compact alone (Score: {:.2f}).",
                      single_source->sstable_id, calculate_score(*single_source));
            return false;
        }
    }
    
    // Rule 2: Avoid tiny compactions. The total data processed should be significant.
    uint64_t total_bytes_to_process = 0;
    for (const auto& table : source_files) total_bytes_to_process += table->size_bytes;
    for (const auto& table : target_files) total_bytes_to_process += table->size_bytes;
    
    // Minimum threshold is a fraction of the target SSTable size.
    const uint64_t MIN_COMPACTION_BYTES = config_.sstable_target_size / 4;
    if (total_bytes_to_process < MIN_COMPACTION_BYTES) {
        LOG_TRACE("    [is_worthwhile] Total bytes to process ({}B) is less than minimum worthwhile ({}B). Skipping.",
                  total_bytes_to_process, MIN_COMPACTION_BYTES);
        return false;
    }
    
    return true;
}



LeveledCompactionStrategy::~LeveledCompactionStrategy() {
    auto lifetime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - creation_time_
    );
    
    LOG_INFO("[LeveledCompactionStrategy] Destroyed after {}s. Final metrics: L0_jobs={}, Level_jobs={}, Avg_select_time={:.2f}us",
             lifetime.count(),
             metrics_.l0_compactions_selected.load(),
             metrics_.level_compactions_selected.load(),
             metrics_.average_selection_time_us());
}

std::optional<CompactionJobForIndinis> LeveledCompactionStrategy::SelectCompaction(
    const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
    int max_level
) const {
    auto start_time = std::chrono::steady_clock::now();
    
    if (max_level < 0 || !validate_levels_snapshot(levels_snapshot, max_level)) {
        // Throw a specific, detailed error for invalid input
        throw storage::StorageError(storage::ErrorCode::INVALID_DATA_FORMAT,
            "LeveledCompactionStrategy::SelectCompaction received invalid levels snapshot or max_level.");
    }
    
    std::optional<CompactionJobForIndinis> result;
    
    try {
        if (auto l0_job = try_select_l0_compaction(levels_snapshot); l0_job.has_value()) {
            metrics_.l0_compactions_selected.fetch_add(1);
            result = std::move(l0_job);
        } else if (auto ln_job = try_select_level_compaction(levels_snapshot, max_level); ln_job.has_value()) {
            metrics_.level_compactions_selected.fetch_add(1);
            result = std::move(ln_job);
        }
    }
    catch (const storage::StorageError& e) {
        LOG_ERROR("[LeveledCompactionStrategy] SelectCompaction failed with StorageError: {}", e.toString());
        throw; // Re-throw structured error
    }
    catch (const std::exception& e) {
        LOG_ERROR("[LeveledCompactionStrategy] SelectCompaction failed with std::exception: {}", e.what());
        // Wrap standard exception in our error type for consistency
        throw storage::StorageError(storage::ErrorCode::LSM_COMPACTION_FAILED, "Compaction selection failed.")
            .withDetails(e.what());
    }
    
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    metrics_.record_selection_time(duration);
    
    return result;
}

CompactionMetricsSnapshot LeveledCompactionStrategy::get_metrics() const {
    CompactionMetricsSnapshot snapshot;
    
    // Atomically load each value from the internal metrics struct into the snapshot.
    // std::memory_order_relaxed is sufficient here as we are just reading stats
    // and don't need to synchronize memory with other operations.
    snapshot.l0_compactions_selected = metrics_.l0_compactions_selected.load(std::memory_order_relaxed);
    snapshot.level_compactions_selected = metrics_.level_compactions_selected.load(std::memory_order_relaxed);
    snapshot.compactions_skipped_not_worthwhile = metrics_.compactions_skipped_not_worthwhile.load(std::memory_order_relaxed);
    snapshot.empty_levels_encountered = metrics_.empty_levels_encountered.load(std::memory_order_relaxed);
    snapshot.average_selection_time_us = metrics_.average_selection_time_us();
    
    return snapshot;
}


std::optional<CompactionJobForIndinis> LeveledCompactionStrategy::try_select_l0_compaction(
    const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot
) const {
    if (levels_snapshot.empty() || levels_snapshot[0].size() < config_.l0_compaction_trigger) {
        return std::nullopt;
    }
    
    std::vector<std::shared_ptr<SSTableMetadata>> l0_tables = levels_snapshot[0];
    auto key_range = lsm_tree_ptr_->compute_key_range(l0_tables);
    auto overlapping_l1 = lsm_tree_ptr_->find_overlapping_sstables(
        levels_snapshot, 1, key_range.first, key_range.second
    );
    
    if (!is_within_resource_limits(l0_tables, overlapping_l1)) {
        LOG_WARN("[LeveledCompactionStrategy] L0 compaction skipped due to resource limits. L0 files: {}, L1 files: {}",
                 l0_tables.size(), overlapping_l1.size());
        return std::nullopt;
    }
    
    LOG_INFO("[LeveledCompactionStrategy] Selected L0 compaction: {} L0 files, {} overlapping L1 files.",
             l0_tables.size(), overlapping_l1.size());
    
    return CompactionJobForIndinis(0, std::move(l0_tables), std::move(overlapping_l1));
}

std::optional<CompactionJobForIndinis> LeveledCompactionStrategy::try_select_level_compaction(
    const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
    int max_level
) const {
    struct LevelCandidate {
        int level_idx;
        CompactionPriority priority;
        double size_ratio; // Use double for more precise sorting
    };
    std::vector<LevelCandidate> candidates;
    
    for (int level_idx = 1; level_idx < max_level - 1; ++level_idx) {
        if (static_cast<size_t>(level_idx) >= levels_snapshot.size() || levels_snapshot[level_idx].empty()) {
            metrics_.empty_levels_encountered.fetch_add(1);
            continue;
        }
        
        uint64_t current_size = lsm_tree_ptr_->calculate_level_size_bytes(levels_snapshot[level_idx]);
        uint64_t target_size = calculate_dynamic_target_for_level(level_idx, levels_snapshot);
        
        if (current_size > target_size) {
            candidates.push_back({
                level_idx,
                calculate_compaction_priority(level_idx, current_size, target_size),
                static_cast<double>(current_size) / std::max(target_size, static_cast<uint64_t>(1))
            });
        }
    }
    
    if (candidates.empty()) return std::nullopt;
    
    std::sort(candidates.begin(), candidates.end(), [](const auto& a, const auto& b) {
        if (a.priority != b.priority) return static_cast<int>(a.priority) > static_cast<int>(b.priority);
        return a.size_ratio > b.size_ratio;
    });
    
    for (const auto& candidate : candidates) {
        if (auto job = select_compaction_candidate_for_level(levels_snapshot, candidate.level_idx); job) {
            return job;
        }
    }
    
    return std::nullopt;
}

std::optional<CompactionJobForIndinis> LeveledCompactionStrategy::select_compaction_candidate_for_level(
    const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot, 
    int level_idx
) const {
    const auto& level_tables = levels_snapshot[level_idx];
    
    // Find the SSTable in this level with the highest score.
    auto best_candidate_it = std::max_element(
        level_tables.begin(), level_tables.end(),
        [this](const auto& a, const auto& b) {
            return this->calculate_score(*a) < this->calculate_score(*b);
        }
    );
    if (best_candidate_it == level_tables.end()) return std::nullopt;

    const auto& selected_sstable = *best_candidate_it;
    std::vector<std::shared_ptr<SSTableMetadata>> ln_to_compact = { selected_sstable };
    
    auto overlapping_files = lsm_tree_ptr_->find_overlapping_sstables(
        levels_snapshot, level_idx + 1, selected_sstable->min_key, selected_sstable->max_key
    );
    
    if (!is_within_resource_limits(ln_to_compact, overlapping_files)) {
        LOG_INFO("[LeveledCompactionStrategy] L{} compaction on SSTable {} skipped due to resource limits.", level_idx, selected_sstable->sstable_id);
        return std::nullopt;
    }

    // ---  Call the local `is_worthwhile` method ---
    if (!is_worthwhile(ln_to_compact, overlapping_files)) {
        metrics_.compactions_skipped_not_worthwhile.fetch_add(1);
        LOG_INFO("[LeveledCompactionStrategy] L{} compaction on SSTable {} skipped as not worthwhile.", level_idx, selected_sstable->sstable_id);
        return std::nullopt;
    }
    
    LOG_INFO("[LeveledCompactionStrategy] Selected L{} compaction: Source SSTable ID {}, {} overlapping L+1 files.",
             level_idx, selected_sstable->sstable_id, overlapping_files.size());
    
    return CompactionJobForIndinis(level_idx, std::move(ln_to_compact), std::move(overlapping_files));
}

CompactionPriority LeveledCompactionStrategy::calculate_compaction_priority(
    int level_idx, uint64_t current_size, uint64_t target_size
) const {
    // A target_size of 0 indicates an unconfigured or invalid state, which should be treated as urgent.
    if (target_size == 0) return CompactionPriority::URGENT;
    
    double ratio = static_cast<double>(current_size) / target_size;

    // L0 compactions are always high priority because they directly impact write stalls.
    // The actual trigger (file count) is handled before this scoring is even needed.
    if (level_idx == 0) return CompactionPriority::HIGH;
    
    // For deeper levels, the priority escalates as the size imbalance grows.
    if (ratio > 4.0) return CompactionPriority::URGENT;
    if (ratio > 2.0) return CompactionPriority::HIGH;
    if (ratio > 1.5) return CompactionPriority::MEDIUM;
    
    return CompactionPriority::LOW;
}

bool LeveledCompactionStrategy::validate_levels_snapshot(
    const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
    int max_level
) const {
    // Logic is sound. No changes needed.
    if (levels_snapshot.empty() && max_level > 0) return false;
    for (const auto& level : levels_snapshot) {
        for (const auto& sstable : level) {
            if (!sstable) {
                LOG_WARN("[LeveledCompactionStrategy] Validation failed: Found null SSTable metadata in snapshot.");
                return false;
            }
        }
    }
    return true;
}

bool LeveledCompactionStrategy::is_within_resource_limits(
    const std::vector<std::shared_ptr<SSTableMetadata>>& source_files,
    const std::vector<std::shared_ptr<SSTableMetadata>>& target_files
) const {
    // The member is `size_bytes`, not `file_size`.
    if (source_files.size() + target_files.size() > config_.max_files_per_compaction) {
        return false;
    }
    
    uint64_t total_bytes = 0;
    for (const auto& file : source_files) {
        total_bytes += file->size_bytes; // <<< FIX: Use size_bytes
    }
    for (const auto& file : target_files) {
        total_bytes += file->size_bytes; // <<< FIX: Use size_bytes
    }

    return total_bytes <= config_.max_compaction_bytes;
}

} // namespace lsm
} // namespace engine