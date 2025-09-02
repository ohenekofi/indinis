// src/lsm/hybrid_compaction_strategy.cpp
#include "../../include/lsm/hybrid_compaction_strategy.h"
#include "../../include/lsm_tree.h"
#include "../../include/debug_utils.h"
#include "../../include/storage_error/storage_error.h"
#include "../../include/storage_error/error_codes.h"

namespace engine {
namespace lsm {

HybridCompactionStrategy::HybridCompactionStrategy(LSMTree* lsm_tree_ptr, const HybridCompactionConfig& config)
    : lsm_tree_ptr_(lsm_tree_ptr), config_(config)
{
    if (!lsm_tree_ptr_) {
        throw storage::StorageError(storage::ErrorCode::INVALID_CONFIGURATION, "HybridCompactionStrategy: LSMTree pointer cannot be null.");
    }
    if (!config_.is_valid()) {
        throw storage::StorageError(storage::ErrorCode::INVALID_CONFIGURATION, "HybridCompactionStrategy: Invalid configuration provided.");
    }

    // Instantiate the underlying strategies this hybrid strategy will use.
    leveled_strategy_ = std::make_unique<LeveledCompactionStrategy>(lsm_tree_ptr, config_.leveled_config);
    universal_strategy_ = std::make_unique<UniversalCompactionStrategy>(lsm_tree_ptr, config_.universal_config);

    LOG_INFO("[HybridStrategy] Created. Leveled strategy will be used for levels < {}.", config_.leveled_to_universal_threshold);
}

HybridCompactionStrategy::~HybridCompactionStrategy() {
    LOG_INFO("[HybridStrategy] Destroyed.");
}

std::optional<CompactionJobForIndinis> HybridCompactionStrategy::SelectCompaction(
    const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
    int max_level
) const {
    // Hybrid Rule 1: L0 compaction is always handled by the Leveled strategy
    // because its trigger is based on file count, which is critical for write stalls.
    if (levels_snapshot[0].size() >= config_.leveled_config.l0_compaction_trigger) {
        LOG_TRACE("[HybridStrategy] L0 trigger met, delegating to Leveled strategy.");
        // We can call the full SelectCompaction here, as Leveled will prioritize L0.
        return leveled_strategy_->SelectCompaction(levels_snapshot, max_level);
    }
    
    // Hybrid Rule 2: For deeper levels, check against the threshold.
    // To find the best candidate, we must check all levels and pick the one with the highest priority.
    // For simplicity, we will scan from lower levels to higher levels and pick the first valid job.
    // A more advanced implementation would gather candidates from all levels and score them.
    for (int level_idx = 1; level_idx < max_level -1; ++level_idx) {
        
        // This is a simplified selection loop. The strategies themselves should be
        // modified to have a `SelectCompactionForLevel(level_idx)` method for a true
        // priority-based selection. For now, we delegate the full selection and let
        // the strategy decide.

        if (static_cast<size_t>(level_idx) < config_.leveled_to_universal_threshold) {
            // Use Leveled strategy for this level
            LOG_TRACE("[HybridStrategy] Checking L{} with Leveled strategy.", level_idx);
            // We need to temporarily create a "snapshot" for just this level to check
            // NOTE: This is a simplification. A better way is to refactor Leveled/Universal
            // to have a "SelectForLevel" method.
            if (auto job = leveled_strategy_->SelectCompaction(levels_snapshot, max_level); job && job->level_L == level_idx) {
                return job;
            }
        } else {
            // Use Universal strategy for this level
            LOG_TRACE("[HybridStrategy] Checking L{} with Universal strategy.", level_idx);
            if (auto job = universal_strategy_->SelectCompaction(levels_snapshot, max_level); job && job->level_L == level_idx) {
                return job;
            }
        }
    }
    
    return std::nullopt;
}

} // namespace lsm
} // namespace engine