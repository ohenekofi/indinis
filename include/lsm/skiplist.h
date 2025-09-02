// include/lsm/skiplist.h
#pragma once

#include "arena.h"
#include "memtable_rep.h" // The abstract interface we are implementing
#include <atomic>
#include <string>
#include <random>

namespace engine {
namespace lsm {

/**
 * @class SkipList
 * @brief A highly concurrent, SkipList-based implementation of the MemTableRep interface.
 *
 * This class provides lock-free reads and uses atomic Compare-And-Swap (CAS)
 * for insertions, allowing for high-throughput concurrent writes. It is designed to be
 * used as the underlying key-value store within a Partition of a PartitionedMemTable.
 */
class SkipList : public MemTableRep {
private:
    struct Node; // Forward-declare the internal node structure

public:
    using Key = std::string_view;

    /**
     * @struct Value
     * @brief The payload stored for each key in the SkipList.
     *
     * This simple struct holds the serialized value, the sequence number of the operation,
     * and a flag indicating if the entry is a tombstone (a logical deletion).
     */
    struct Value {
        std::string value_str;
        uint64_t sequence;
        bool is_tombstone;
    };

    explicit SkipList(Arena& arena);
    ~SkipList() override = default;

    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    // --- MemTableRep Interface Implementation ---
    void Add(const std::string& key, const std::string& value, uint64_t seq) override;
    void Delete(const std::string& key, uint64_t seq) override;
    bool Get(const std::string& key, std::string& value, uint64_t& seq) const override;
    std::unique_ptr<MemTableIterator> NewIterator() const override;
    size_t ApproximateMemoryUsage() const override;
    size_t Count() const override;
    bool Empty() const override;
    std::string GetName() const override { return "SkipList"; }
    MemTableType GetType() const override { return MemTableType::SKIP_LIST; }

    // --- Iterator for scanning the SkipList (implements MemTableIterator) ---
    class Iterator : public MemTableIterator {
    public:
        explicit Iterator(const SkipList* list);
        bool Valid() const override;
        void Next() override;
        void Prev() override;
        void SeekToFirst() override;
        void SeekToLast() override;
        void Seek(const std::string& key) override;
        Entry GetEntry() const override;
    private:
        const SkipList* list_;
        Node* node_;
    };

private:
    // Internal helper for both Add and Delete
    void Upsert(const std::string& key, const std::string& value, uint64_t seq, bool is_tombstone);

    enum { kMaxHeight = 12 };

    Node* NewNode(const Key& key, Value* value, int height);
    int RandomHeight();
    bool KeyIsAfterNode(const Key& key, Node* n) const;
    Node* FindGreaterOrEqual(const Key& key, Node** prev) const;
    Node* FindLessThan(const Key& key) const;
    Node* FindLast() const;

    Arena& arena_;
    Node* const head_;
    std::atomic<int> max_height_;
    std::atomic<size_t> count_{0}; // Atomic counter for the number of entries
    
    std::mt19937 rnd_generator_;
    std::uniform_int_distribution<int> rnd_dist_;
};

} // namespace lsm
} // namespace engine