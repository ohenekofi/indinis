// src/lsm/liveness_oracle.cpp
#include "../../include/lsm/liveness_oracle.h"
#include <future>

namespace engine {
namespace lsm {

void LivenessOracle::addLivenessFilter(uint64_t sstable_id, std::shared_ptr<BloomFilter> filter) {
    std::unique_lock lock(mutex_);
    liveness_filters_[sstable_id] = std::move(filter);
}

void LivenessOracle::removeLivenessFilter(uint64_t sstable_id) {
    std::unique_lock lock(mutex_);
    liveness_filters_.erase(sstable_id);
}

bool LivenessOracle::mightBeLive(const std::string& key) const {
    std::shared_lock lock(mutex_);
    // If any of the filters report a potential match, the key might be live.
    for (const auto& pair : liveness_filters_) {
        if (pair.second && pair.second->mightContain(key)) {
            return true;
        }
    }
    // If no filter reports a match, the key is definitively not live.
    return false;
}

std::vector<bool> LivenessOracle::batchMightBeLive(const std::vector<std::string>& keys) const {
    if (keys.empty()) {
        return {};
    }

    std::vector<bool> results(keys.size(), false);
    std::vector<std::shared_ptr<BloomFilter>> filters_snapshot;

    // 1. Take a snapshot of the current filters under a shared lock.
    // This is a fast operation that minimizes lock hold time.
    {
        std::shared_lock lock(mutex_);
        filters_snapshot.reserve(liveness_filters_.size());
        for (const auto& pair : liveness_filters_) {
            filters_snapshot.push_back(pair.second);
        }
    }

    if (filters_snapshot.empty()) {
        // No filters means we have no information; assume all keys might be live.
        std::fill(results.begin(), results.end(), true);
        return results;
    }

    // 2. Process the batch check in parallel.
    // This is an example of a simple parallelization that could be enhanced
    // with a proper thread pool for very large batches.
    std::vector<std::future<void>> futures;
    size_t num_threads = std::min(static_cast<size_t>(std::thread::hardware_concurrency()), keys.size());
    size_t chunk_size = (keys.size() + num_threads - 1) / num_threads;

    for (size_t i = 0; i < num_threads; ++i) {
        futures.push_back(std::async(std::launch::async, [&, i] {
            size_t start = i * chunk_size;
            size_t end = std::min(start + chunk_size, keys.size());
            for (size_t j = start; j < end; ++j) {
                // Check the key against all filters in the snapshot.
                for (const auto& filter : filters_snapshot) {
                    if (filter && filter->mightContain(keys[j])) {
                        results[j] = true;
                        break; // Found a potential match, no need to check other filters for this key.
                    }
                }
            }
        }));
    }

    for (auto& f : futures) {
        f.get(); // Wait for all chunks to complete.
    }

    return results;
}

} // namespace lsm
} // namespace engine