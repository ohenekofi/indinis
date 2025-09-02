#pragma once

#include "memtable_rep.h" // The abstract interface we are implementing and wrapping
#include <memory>         // For std::unique_ptr

namespace engine {
namespace lsm {

/**
 * @class CompressedMemTableIterator
 * @brief An iterator that wraps another MemTableIterator and decompresses values on the fly.
 *
 * When a client requests an entry, this iterator retrieves the compressed entry from the
 * underlying iterator and transparently decompresses its value before returning it.
 */
class CompressedMemTableIterator : public MemTableIterator {
public:
    explicit CompressedMemTableIterator(std::unique_ptr<MemTableIterator> underlying_iterator);

    // MemTableIterator interface implementation
    bool Valid() const override;
    void Next() override;
    void Prev() override;
    void SeekToFirst() override;
    void SeekToLast() override;
    void Seek(const std::string& key) override;
    Entry GetEntry() const override;

private:
    std::unique_ptr<MemTableIterator> underlying_iterator_;
};


/**
 * @class CompressedMemTable
 * @brief A decorator that adds transparent value compression to an underlying MemTableRep.
 *
 * This class wraps another MemTableRep (e.g., a SkipList) and compresses values
 * (using LZ4 for speed) on `Add` operations and decompresses them on `Get` operations. It is
 * useful for reducing the in-memory footprint of the memtable, especially for workloads
 * with large, compressible (e.g., JSON, text) values, at the cost of increased CPU usage.
 *
 * Tomstones (deletions) are passed through without modification.
 */
class CompressedMemTable : public MemTableRep {
public:
    /**
     * @brief Constructs a CompressedMemTable.
     * @param underlying_memtable A unique_ptr to the MemTableRep instance that this
     *        class will wrap and take ownership of.
     */
    explicit CompressedMemTable(std::unique_ptr<MemTableRep> underlying_memtable);
    ~CompressedMemTable() override = default;

    CompressedMemTable(const CompressedMemTable&) = delete;
    CompressedMemTable& operator=(const CompressedMemTable&) = delete;

    // --- MemTableRep Interface Implementation ---
    void Add(const std::string& key, const std::string& value, uint64_t seq) override;
    void Delete(const std::string& key, uint64_t seq) override;
    bool Get(const std::string& key, std::string& value, uint64_t& seq) const override;
    std::unique_ptr<MemTableIterator> NewIterator() const override;
    
    // Pass-through methods that delegate directly to the underlying implementation.
    size_t ApproximateMemoryUsage() const override;
    size_t Count() const override;
    bool Empty() const override;
    std::string GetName() const override;
    MemTableType GetType() const override { return MemTableType::COMPRESSED_SKIP_LIST; }

private:
    std::unique_ptr<MemTableRep> underlying_memtable_;
};

} // namespace lsm
} // namespace engine