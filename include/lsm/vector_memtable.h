// include/lsm/vector_memtable.h
#pragma once

#include "memtable_rep.h"
#include <vector>
#include <atomic>
#include <mutex>
#include <shared_mutex>

namespace engine {
namespace lsm {

class Arena; // Forward-declare

/**
 * @class VectorMemTable
 * @brief A MemTableRep implementation using a std::vector, optimized for sequential writes.
 *
 * This memtable achieves very high write throughput for workloads where keys are inserted
 * in strictly ascending order (e.g., time-series data, monotonically increasing IDs).
 * Writes are simple appends. Reads require a binary search, which is efficient only if the
 * data remains sorted. Out-of-order writes will flag the vector as unsorted, triggering a
 * potentially expensive sort operation on the next read to maintain correctness.
 */
class VectorMemTable : public MemTableRep {
public:
    explicit VectorMemTable(Arena& arena);
    ~VectorMemTable() override = default;

    VectorMemTable(const VectorMemTable&) = delete;
    VectorMemTable& operator=(const VectorMemTable&) = delete;

    // --- MemTableRep Interface Implementation ---
    void Add(const std::string& key, const std::string& value, uint64_t seq) override;
    void Delete(const std::string& key, uint64_t seq) override;
    bool Get(const std::string& key, std::string& value, uint64_t& seq) const override;
    std::unique_ptr<MemTableIterator> NewIterator() const override;
    size_t ApproximateMemoryUsage() const override;
    size_t Count() const override;
    bool Empty() const override;
    std::string GetName() const override { return "Vector"; }
    MemTableType GetType() const override { return MemTableType::VECTOR; }

private:
    class Iterator; // Forward-declare nested iterator

    //void EnsureSorted() const;

    // Although this implementation doesn't heavily use the arena for node-based
    // allocations like a SkipList, it's kept for interface consistency and potential
    // future use for storing string data contiguously.
    Arena& arena_;
    
    mutable std::vector<Entry> entries_;
    mutable std::shared_mutex mutex_; // Protects both `entries_` and `sorted_` flag
    // mutable std::atomic<bool> sorted_{true};
};

} // namespace lsm
} // namespace engine