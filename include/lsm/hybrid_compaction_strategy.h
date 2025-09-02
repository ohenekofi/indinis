//include/lsm/hybrid_compaction_strategy.h
#pragma once

#include "compaction_strategy.h"
#include "leveled_compaction_strategy.h"
#include "universal_compaction_strategy.h"
#include <memory>

class LSMTree; // Forward-declare

namespace engine {
namespace lsm {

/**
 * @struct HybridCompactionConfig
 * @brief Configuration for the Hybrid compaction strategy.
 */
struct HybridCompactionConfig {
    // The configuration for the underlying Leveled strategy.
    LeveledCompactionConfig leveled_config;

    // The configuration for the underlying Universal strategy.
    UniversalCompactionConfig universal_config;

    // Rule: Levels below this threshold (e.g., 0, 1, 2) will use the Leveled strategy.
    // Levels at or above this threshold will use the Universal strategy.
    size_t leveled_to_universal_threshold = 3; // L0, L1, L2 are Leveled. L3+ are Universal.

    bool is_valid() const {
        return leveled_config.is_valid() && universal_config.is_valid() && leveled_to_universal_threshold > 0;
    }
};

/**
 * @class HybridCompactionStrategy
 * @brief A meta-strategy that combines multiple compaction algorithms.
 *
 * This strategy delegates compaction selection to different underlying strategies
 * based on a set of rules, typically the level of the data. This allows for
 * fine-tuning the trade-offs between read, write, and space amplification across
 * the data lifecycle (hot vs. cold data).
 */
class HybridCompactionStrategy : public CompactionStrategy {
public:
    explicit HybridCompactionStrategy(LSMTree* lsm_tree_ptr, const HybridCompactionConfig& config);
    
    ~HybridCompactionStrategy() override;

    std::optional<CompactionJobForIndinis> SelectCompaction(
        const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
        int max_level
    ) const override;

private:
    LSMTree* lsm_tree_ptr_;
    const HybridCompactionConfig config_;
    
    // Owns instances of the strategies it delegates to.
    std::unique_ptr<LeveledCompactionStrategy> leveled_strategy_;
    std::unique_ptr<UniversalCompactionStrategy> universal_strategy_;
};

} // namespace lsm
} // namespace engine