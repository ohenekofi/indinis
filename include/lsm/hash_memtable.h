// include/lsm/hash_memtable.h
#pragma once

#include "memtable_rep.h"
#include <unordered_map>
#include <mutex>

namespace engine {
namespace lsm {

class Arena; // Forward-declare

/**
 * @class HashMemTable
 * @brief A MemTableRep implementation using a std::unordered_map for O(1) average access time.
 *
 * This memtable is optimized for workloads dominated by random `Get`, `Add`, and `Delete`
 * operations. It provides the fastest possible point lookups.
 *
 * The significant trade-off is that it does not support ordered iteration. Calling `NewIterator`
 * will throw an exception, as a hash map does not maintain key order. Consequently, a
 * `PartitionedMemTable` using this type cannot be flushed to a sorted SSTable and is
 * typically only suitable for specialized, in-memory-only use cases or different storage backends.
 */
class HashMemTable : public MemTableRep {
public:
    explicit HashMemTable(Arena& arena);
    ~HashMemTable() override = default;

    HashMemTable(const HashMemTable&) = delete;
    HashMemTable& operator=(const HashMemTable&) = delete;

    // --- MemTableRep Interface Implementation ---
    void Add(const std::string& key, const std::string& value, uint64_t seq) override;
    void Delete(const std::string& key, uint64_t seq) override;
    bool Get(const std::string& key, std::string& value, uint64_t& seq) const override;
    std::unique_ptr<MemTableIterator> NewIterator() const override;
    size_t ApproximateMemoryUsage() const override;
    size_t Count() const override;
    bool Empty() const override;
    std::string GetName() const override { return "HashTable"; }
    MemTableType GetType() const override { return MemTableType::HASH_TABLE; }

private:
    // This implementation uses a simple Entry struct. For MVCC, a hash table
    // could map to a small vector or list of versions if needed, but for the
    // current interface, we only store the latest version (by sequence number).
    std::unordered_map<std::string, Entry> map_;
    mutable std::mutex mutex_; // A simple mutex is sufficient for protecting the hash map.
    Arena& arena_;
};

} // namespace lsm
} // namespace engine