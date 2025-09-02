// include/lsm/memtable_rep.h
#pragma once
#include "memtable_types.h"

#include <string>
#include <memory>

// Forward-declare to avoid circular dependencies if needed later.
// For now, no forward declarations are strictly necessary.

namespace engine {
namespace lsm {

/**
 * @struct Entry
 * @brief Represents a single key-value pair with its sequence number, as returned by an iterator.
 */
struct Entry {
    std::string key;
    std::string value;
    uint64_t sequence_number;
};

/**
 * @class MemTableIterator
 * @brief An abstract iterator interface for scanning the contents of a MemTableRep.
 */
class MemTableIterator {
public:
    virtual ~MemTableIterator() = default;

    virtual bool Valid() const = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0; // Can be a no-op if not supported
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0; // Can be a no-op if not supported
    virtual void Seek(const std::string& key) = 0;
    virtual Entry GetEntry() const = 0;
};

/**
 * @class MemTableRep
 * @brief An abstract representation (interface) for an in-memory key-value store.
 *
 * This pure virtual class defines the contract that all memtable implementations
 * (e.g., SkipList, HashTable, Vector) must follow to be used within a Partition.
 */
class MemTableRep {
public:
    virtual ~MemTableRep() = default;
    
    // --- Add a virtual method to get the type ---
    virtual MemTableType GetType() const = 0;

    /**
     * @brief Adds or updates a key-value pair.
     * @param key The key.
     * @param value The value.
     * @param seq The sequence number for this operation.
     */
    virtual void Add(const std::string& key, const std::string& value, uint64_t seq) = 0;

    /**
     * @brief Marks a key as deleted by adding a tombstone.
     * @param key The key to delete.
     * @param seq The sequence number for this operation.
     */
    virtual void Delete(const std::string& key, uint64_t seq) = 0;

    /**
     * @brief Retrieves the value for a key.
     * @param key The key to look up.
     * @param value Output parameter for the found value.
     * @param seq Output parameter for the sequence number.
     * @return True if the key was found and is not a tombstone, false otherwise.
     */
    virtual bool Get(const std::string& key, std::string& value, uint64_t& seq) const = 0;

    /**
     * @brief Creates a new iterator for this memtable.
     * @return A unique_ptr to a new MemTableIterator instance.
     */
    virtual std::unique_ptr<MemTableIterator> NewIterator() const = 0;

    /**
     * @brief Returns the approximate memory usage of the memtable in bytes.
     */
    virtual size_t ApproximateMemoryUsage() const = 0;
    
    /**
     * @brief Returns the number of entries in the memtable.
     */
    virtual size_t Count() const = 0;

    /**
     * @brief Returns true if the memtable contains no entries.
     */
    virtual bool Empty() const = 0;

    /**
     * @brief Returns the name of the implementation for debugging.
     */
    virtual std::string GetName() const { return "MemTableRep"; }

};

} // namespace lsm
} // namespace engine