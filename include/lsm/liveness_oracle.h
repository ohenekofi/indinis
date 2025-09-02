// include/lsm/liveness_oracle.h
#pragma once

#include "../include/bloom_filter.h"
#include <vector>
#include <memory>
#include <string>
#include <shared_mutex>
#include <unordered_map>
#include <cstdint>

namespace engine {
namespace lsm {

/**
 * @class LivenessOracle
 * @brief A thread-safe service that provides fast, probabilistic checks for key liveness.
 *
 * This component is updated by the LSM-Tree during its compactions and is read by the
 * ColumnarStore during its compactions to avoid expensive point-reads to the LSM-Tree.
 */
class LivenessOracle {
public:
    LivenessOracle() = default;

    /**
     * @brief Updates the oracle with a new set of liveness filters from an LSM compaction.
     * @param sstable_id The ID of the SSTable the filter corresponds to.
     * @param filter A shared_ptr to the Bloom filter containing all live keys from that SSTable.
     */
    void addLivenessFilter(uint64_t sstable_id, std::shared_ptr<BloomFilter> filter);
    
    /**
     * @brief Removes the liveness filter for an SSTable that is being deleted.
     * @param sstable_id The ID of the SSTable whose filter should be removed.
     */
    void removeLivenessFilter(uint64_t sstable_id);
    
    /**
     * @brief Checks if a single key might be live in any of the registered SSTables.
     * @return false if the key is definitively not live; true if it *might* be live.
     */
    bool mightBeLive(const std::string& key) const;

    /**
     * @brief Performs a batch check for a vector of keys.
     * @return A vector of booleans, where each element corresponds to a key in the input vector.
     */
    std::vector<bool> batchMightBeLive(const std::vector<std::string>& keys) const;

private:
    // This map holds the source of truth for liveness.
    std::unordered_map<uint64_t, std::shared_ptr<BloomFilter>> liveness_filters_;
    
    // A mutex to protect the structure of the map itself during additions/removals.
    mutable std::shared_mutex mutex_;
};

} // namespace lsm
} // namespace engine