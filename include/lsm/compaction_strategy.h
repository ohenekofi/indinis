// include/lsm/compaction_strategy.h
#pragma once

#include <vector>
#include <memory>
#include <optional>
#include "sstable_meta.h" // For SSTableMetadata
#include "compaction_job.h"

namespace engine {
namespace lsm {

// Forward-declare to avoid circular dependencies if structs were moved
// struct SSTableMetadata;
// struct CompactionJobForIndinis; // Now included via lsm_tree.h

/**
 * @brief An enum to identify the configured compaction strategy.
 * This is used by factory functions to create the correct strategy instance.
 */
enum class CompactionStrategyType {
    LEVELED,    // Default strategy, similar to LevelDB/RocksDB. Optimizes for read performance.
    UNIVERSAL,  // Also known as Size-Tiered. Optimizes for write throughput.
    FIFO,       // Discards old data based on TTL. For caches or ephemeral data.
    HYBRID      // <<< FIX: Add the HYBRID option to the enum >>>
};

/**
 * @class CompactionStrategy
 * @brief An abstract base class defining the interface for compaction selection algorithms.
 *
 * This class uses the Strategy Pattern to decouple the LSM-Tree from the specific
 * logic of how compaction candidates are chosen. Concrete implementations will
 * contain the rules for Leveled, Universal, FIFO, or other compaction types.
 */
class CompactionStrategy {
public:
    virtual ~CompactionStrategy() = default;

    /**
     * @brief Selects the next compaction job based on the current state of the LSM-Tree levels.
     *
     * This is the core method of the strategy. It inspects the provided snapshot of
     * SSTable metadata and applies its specific rules to determine if a compaction
     * is needed and, if so, which files should be included.
     *
     * @param levels_snapshot A const reference to a vector of vectors, representing the
     *        SSTables at each level of the tree. This is an immutable snapshot.
     * @param max_level The maximum level allowed in the LSM-Tree.
     * @return An optional containing a CompactionJobForIndinis if a compaction should
     *         be scheduled. Returns std::nullopt if no compaction is needed at this time.
     */
    virtual std::optional<CompactionJobForIndinis> SelectCompaction(
        const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
        int max_level
    ) const = 0;
};

} // namespace lsm
} // namespace engine