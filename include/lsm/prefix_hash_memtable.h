// include/lsm/prefix_hash_memtable.h
#pragma once

#include "memtable_rep.h"
#include <memory> // For std::unique_ptr

namespace engine {
namespace lsm {

class Arena; // Forward-declare

/**
 * @class PrefixHashMemTable
 * @brief A MemTableRep combining O(1) point lookups and efficient prefix scans.
 *
 * This implementation uses two internal data structures:
 * 1. A std::unordered_map for fast Get() operations.
 * 2. A std::map (acting as a simple Trie/ordered index) for prefix-based iteration.
 *
 * This is suitable for workloads with a mix of random reads and prefix scans.
 */
class PrefixHashMemTable : public MemTableRep {
public:
    explicit PrefixHashMemTable(Arena& arena);
    ~PrefixHashMemTable() override; // Must define destructor in .cpp due to PImpl

    PrefixHashMemTable(const PrefixHashMemTable&) = delete;
    PrefixHashMemTable& operator=(const PrefixHashMemTable&) = delete;

    // --- MemTableRep Interface Implementation ---
    void Add(const std::string& key, const std::string& value, uint64_t seq) override;
    void Delete(const std::string& key, uint64_t seq) override;
    bool Get(const std::string& key, std::string& value, uint64_t& seq) const override;
    std::unique_ptr<MemTableIterator> NewIterator() const override;
    size_t ApproximateMemoryUsage() const override;
    size_t Count() const override;
    bool Empty() const override;
    std::string GetName() const override { return "PrefixHashTable"; }
    MemTableType GetType() const override { return MemTableType::PREFIX_HASH_TABLE; }

private:
    // PImpl Idiom: Hides implementation details (unordered_map, map) from the header,
    // reducing dependencies and compile times.
    struct PImpl;
    std::unique_ptr<PImpl> pimpl_;
    Arena& arena_;
};

} // namespace lsm
} // namespace engine