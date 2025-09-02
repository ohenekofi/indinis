//src/lsm/universal_compaction_strategy.cpp
#include "../../include/lsm/universal_compaction_strategy.h"
#include "../../include/lsm_tree.h"
#include "../../include/debug_utils.h"
#include "../../include/storage_error/storage_error.h"
#include "../../include/storage_error/error_codes.h"

#include <algorithm>
#include <numeric>
#include <sstream>
#include <random>
#include <iomanip>

namespace engine {
namespace lsm {

UniversalCompactionStrategy::UniversalCompactionStrategy(
    LSMTree* lsm_tree_ptr, 
    const UniversalCompactionConfig& config
) : lsm_tree_ptr_(lsm_tree_ptr),
    config_(config),
    creation_time_(std::chrono::steady_clock::now()),
    instance_id_(generate_instance_id())
{
    if (!lsm_tree_ptr_) {
        throw storage::StorageError(storage::ErrorCode::INVALID_CONFIGURATION, "UniversalCompactionStrategy: LSMTree pointer cannot be null");
    }
    if (!config_.is_valid()) {
        throw storage::StorageError(storage::ErrorCode::INVALID_CONFIGURATION, "UniversalCompactionStrategy: Invalid configuration - " + config_.to_string());
    }
    LOG_INFO("[UniversalStrategy ", instance_id_, "] Created with config: ", config_.to_string());
}

UniversalCompactionStrategy::~UniversalCompactionStrategy() {
    auto lifetime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - creation_time_
    );
    LOG_INFO("[UniversalStrategy ", instance_id_, "] Destroyed after ", lifetime.count(), "s. Final metrics: ",
             "L0_jobs=", metrics_.l0_compactions_selected.load(),
             ", SizeTiered_jobs=", metrics_.size_tiered_compactions_selected.load(),
             ", Avg_size=", std::fixed, std::setprecision(1), metrics_.average_compaction_size_mb(), "MB",
             ", Avg_time=", std::fixed, std::setprecision(1), metrics_.average_selection_time_us(), "us");
}

std::optional<CompactionJobForIndinis> UniversalCompactionStrategy::SelectCompaction(
    const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
    int max_level
) const {
    auto start_time = std::chrono::steady_clock::now();
    if (max_level < 0 || !validate_levels_snapshot(levels_snapshot, max_level)) {
        throw storage::StorageError(storage::ErrorCode::INVALID_DATA_FORMAT, "UniversalCompactionStrategy::SelectCompaction received invalid levels snapshot or max_level.");
    }
    std::optional<CompactionJobForIndinis> result;
    try {
        if (auto l0_job = try_select_l0_compaction(levels_snapshot); l0_job) {
            metrics_.l0_compactions_selected.fetch_add(1);
            result = std::move(l0_job);
        } else if (auto st_job = try_select_size_tiered_compaction(levels_snapshot, max_level); st_job) {
            metrics_.size_tiered_compactions_selected.fetch_add(1);
            result = std::move(st_job);
        }
        if (result) {
            uint64_t total_bytes = calculate_total_size(result->sstables_L) + calculate_total_size(result->sstables_L_plus_1);
            metrics_.record_compaction(result->sstables_L.size() + result->sstables_L_plus_1.size(), total_bytes);
        }
    }
    catch (const storage::StorageError& e) {
        LOG_ERROR("[UniversalStrategy {}] SelectCompaction failed: {}", instance_id_, e.toString());
        throw;
    }
    catch (const std::exception& e) {
        LOG_ERROR("[UniversalStrategy {}] SelectCompaction failed: {}", instance_id_, e.what());
        throw storage::StorageError(storage::ErrorCode::LSM_COMPACTION_FAILED, "Universal compaction selection failed.").withDetails(e.what());
    }
    metrics_.record_selection_time(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time));
    return result;
}

std::optional<CompactionJobForIndinis> UniversalCompactionStrategy::try_select_l0_compaction(
    const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot
) const {
    if (levels_snapshot.empty() || levels_snapshot[0].size() < config_.l0_compaction_trigger) {
        return std::nullopt;
    }
    std::vector<std::shared_ptr<SSTableMetadata>> l0_tables = levels_snapshot[0];
    auto key_range = lsm_tree_ptr_->compute_key_range(l0_tables);
    auto overlapping_l1 = lsm_tree_ptr_->find_overlapping_sstables(levels_snapshot, 1, key_range.first, key_range.second);
    CompactionCandidate candidate{
        0, std::move(l0_tables), std::move(overlapping_l1),
        CompactionTrigger::L0_COUNT_TRIGGER, 0, 0.0
    };
    candidate.total_size_bytes = calculate_total_size(candidate.source_files) + calculate_total_size(candidate.target_files);
    if (!is_within_resource_limits(candidate)) {
        LOG_WARN("[UniversalStrategy] L0 compaction skipped due to resource limits. Files: {}, Bytes: {}MB",
                 candidate.source_files.size() + candidate.target_files.size(), candidate.total_size_bytes / (1024*1024));
        metrics_.compactions_skipped_resource_limits.fetch_add(1);
        return std::nullopt;
    }
    return CompactionJobForIndinis(0, std::move(candidate.source_files), std::move(candidate.target_files));
}

std::optional<CompactionJobForIndinis> UniversalCompactionStrategy::try_select_size_tiered_compaction(
    const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
    int max_level
) const {
    auto candidates = find_compaction_candidates(levels_snapshot, max_level);
    if (candidates.empty()) return std::nullopt;
    const auto& best_candidate = *std::min_element(candidates.begin(), candidates.end());
    LOG_INFO("[UniversalStrategy] Selected size-tiered L{} compaction. Trigger: {}, Score: {:.2f}, Files: {}, Size: {}MB",
             best_candidate.level_idx, static_cast<int>(best_candidate.trigger_type),
             best_candidate.priority_score, best_candidate.source_files.size(),
             best_candidate.total_size_bytes / (1024*1024));
    return CompactionJobForIndinis(
        best_candidate.level_idx, 
        std::move(best_candidate.source_files),
        std::move(best_candidate.target_files)
    );
}

std::vector<CompactionCandidate> UniversalCompactionStrategy::find_compaction_candidates(
    const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
    int max_level
) const {
    std::vector<CompactionCandidate> candidates;
    for (int level_idx = 1; level_idx < max_level; ++level_idx) {
        if (static_cast<size_t>(level_idx) >= levels_snapshot.size() ||
            levels_snapshot[level_idx].size() < config_.min_merge_width) {
            continue;
        }
        auto sorted_files = sort_files_by_age(levels_snapshot[level_idx]);
        auto level_candidates = evaluate_compaction_triggers(sorted_files, level_idx, levels_snapshot);
        candidates.insert(candidates.end(), std::make_move_iterator(level_candidates.begin()), std::make_move_iterator(level_candidates.end()));
    }
    metrics_.runs_evaluated.fetch_add(candidates.size());
    return candidates;
}

std::vector<CompactionCandidate> UniversalCompactionStrategy::evaluate_compaction_triggers(
    const std::vector<std::shared_ptr<SSTableMetadata>>& level_files,
    int level_idx,
    const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot
) const {
    std::vector<CompactionCandidate> candidates;
    for (size_t start_idx = 0; start_idx < level_files.size(); ++start_idx) {
        if (level_files.size() - start_idx < config_.min_merge_width) {
            break;
        }
        for (size_t len = config_.min_merge_width; len <= config_.max_merge_width && (start_idx + len) <= level_files.size(); ++len) {
            std::vector<std::shared_ptr<SSTableMetadata>> run(level_files.begin() + start_idx, level_files.begin() + start_idx + len);
            uint64_t run_size = calculate_total_size(run);
            bool compact = false;
            CompactionTrigger trigger = CompactionTrigger::SIZE_RATIO_TRIGGER;
            if (start_idx + len < level_files.size()) {
                uint64_t next_size = level_files[start_idx + len]->size_bytes;
                if (next_size > 0 && (static_cast<double>(run_size * 100) / next_size) < config_.size_ratio_percent) {
                    compact = true;
                }
            } else if (calculate_space_amplification(levels_snapshot) > config_.max_space_amplification) {
                compact = true;
                trigger = CompactionTrigger::SPACE_AMPLIFICATION;
            }
            if (compact) {
                auto key_range = lsm_tree_ptr_->compute_key_range(run);
                auto overlapping = lsm_tree_ptr_->find_overlapping_sstables(levels_snapshot, level_idx + 1, key_range.first, key_range.second);
                uint64_t total_job_size = run_size + calculate_total_size(overlapping);
                CompactionCandidate cand{
                    level_idx, std::move(run), std::move(overlapping), trigger,
                    total_job_size, 0.0
                };
                cand.priority_score = calculate_priority_score(cand);
                if (is_within_resource_limits(cand)) {
                    candidates.push_back(std::move(cand));
                } else {
                    metrics_.compactions_skipped_resource_limits.fetch_add(1);
                }
            } else {
                metrics_.compactions_skipped_size_ratio.fetch_add(1);
            }
        }
    }
    return candidates;
}

double UniversalCompactionStrategy::calculate_priority_score(const CompactionCandidate& candidate) const {
    double score = 100.0;
    score -= (lsm_tree_ptr_->MAX_LEVELS - candidate.level_idx) * 10.0;
    switch (candidate.trigger_type) {
        case CompactionTrigger::SPACE_AMPLIFICATION: score -= 50.0; break;
        case CompactionTrigger::L0_COUNT_TRIGGER: score -= 40.0; break;
        case CompactionTrigger::SIZE_RATIO_TRIGGER: score -= 20.0; break;
    }
    score -= static_cast<double>(candidate.source_files.size());
    score += static_cast<double>(candidate.total_size_bytes) / (1024.0 * 1024.0 * 10.0);
    return score;
}

bool UniversalCompactionStrategy::is_within_resource_limits(const CompactionCandidate& candidate) const {
    if (candidate.source_files.size() + candidate.target_files.size() > config_.max_files_per_compaction) {
        return false;
    }
    if (candidate.total_size_bytes > config_.max_compaction_bytes) {
        return false;
    }
    return true;
}

std::vector<std::shared_ptr<SSTableMetadata>> UniversalCompactionStrategy::sort_files_by_age(
    const std::vector<std::shared_ptr<SSTableMetadata>>& files) const {
    if (!config_.sort_runs_by_age) return files;
    auto sorted = files;
    std::sort(sorted.begin(), sorted.end(),
        [](const auto& a, const auto& b) { return a->sstable_id > b->sstable_id; });
    return sorted;
}

bool UniversalCompactionStrategy::validate_levels_snapshot(
    const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
    int max_level
) const {
    if (static_cast<size_t>(max_level) > levels_snapshot.size()) {
        LOG_WARN("[UniversalStrategy] Validation failed: max_level ({}) is greater than snapshot size ({}).", max_level, levels_snapshot.size());
        return false;
    }
    for (const auto& level : levels_snapshot) {
        for (const auto& sstable : level) {
            if (!sstable) {
                LOG_WARN("[UniversalStrategy] Validation failed: Found null SSTable metadata pointer in snapshot.");
                return false;
            }
        }
    }
    return true;
}

uint64_t UniversalCompactionStrategy::calculate_total_size(
    const std::vector<std::shared_ptr<SSTableMetadata>>& files) const {
    return std::accumulate(files.begin(), files.end(), 0ULL,
        [](uint64_t sum, const auto& file) { return sum + file->size_bytes; });
}

double UniversalCompactionStrategy::calculate_space_amplification(
    const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot
) const {
    if (levels_snapshot.empty()) return 1.0;
    uint64_t total_size = 0;
    uint64_t last_level_size = 0;
    for (size_t i = 0; i < levels_snapshot.size(); ++i) {
        uint64_t level_size = calculate_total_size(levels_snapshot[i]);
        total_size += level_size;
        if (!levels_snapshot[i].empty()) {
            last_level_size = level_size;
        }
    }
    if (last_level_size == 0) return 1.0;
    return static_cast<double>(total_size) / last_level_size;
}

std::string UniversalCompactionStrategy::generate_instance_id() {
    static std::atomic<uint64_t> instance_counter{0};
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    std::ostringstream oss;
    oss << "UCS-" << now_ms << "-" << instance_counter.fetch_add(1, std::memory_order_relaxed);
    return oss.str();
}

UniversalCompactionMetricsSnapshot UniversalCompactionStrategy::get_metrics() const {
    UniversalCompactionMetricsSnapshot snapshot;
    snapshot.l0_compactions_selected = metrics_.l0_compactions_selected.load(std::memory_order_relaxed);
    snapshot.size_tiered_compactions_selected = metrics_.size_tiered_compactions_selected.load(std::memory_order_relaxed);
    snapshot.compactions_skipped_size_ratio = metrics_.compactions_skipped_size_ratio.load(std::memory_order_relaxed);
    snapshot.compactions_skipped_resource_limits = metrics_.compactions_skipped_resource_limits.load(std::memory_order_relaxed);
    snapshot.total_files_compacted = metrics_.total_files_compacted.load(std::memory_order_relaxed);
    snapshot.total_bytes_compacted = metrics_.total_bytes_compacted.load(std::memory_order_relaxed);
    snapshot.average_selection_time_us = metrics_.average_selection_time_us();
    snapshot.average_compaction_size_mb = metrics_.average_compaction_size_mb();
    return snapshot;
}

} // namespace lsm
} // namespace engine