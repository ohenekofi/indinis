// src/lsm/hash_memtable.cpp

#include "../../include/lsm/hash_memtable.h"
#include <stdexcept>

namespace engine {
namespace lsm {

HashMemTable::HashMemTable(Arena& arena) : arena_(arena) {}

void HashMemTable::Add(const std::string& key, const std::string& value, uint64_t seq) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = map_.find(key);
    if (it != map_.end()) {
        // Entry exists. Overwrite only if the new sequence number is higher.
        if (seq > it->second.sequence_number) {
            it->second.value = value;
            it->second.sequence_number = seq;
        }
    } else {
        // New entry.
        map_.emplace(key, Entry{key, value, seq});
    }
}

void HashMemTable::Delete(const std::string& key, uint64_t seq) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = map_.find(key);
    if (it != map_.end()) {
        // Entry exists. Overwrite with a tombstone if the sequence number is higher.
        if (seq > it->second.sequence_number) {
            it->second.value.clear(); // Empty value string signifies a tombstone.
            it->second.sequence_number = seq;
        }
    } else {
        // New entry, which is a tombstone.
        map_.emplace(key, Entry{key, "", seq});
    }
}

bool HashMemTable::Get(const std::string& key, std::string& value, uint64_t& seq) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = map_.find(key);
    if (it != map_.end()) {
        const Entry& entry = it->second;
        // Check if it's a tombstone (empty value).
        if (!entry.value.empty()) {
            value = entry.value;
            seq = entry.sequence_number;
            return true;
        }
    }
    return false;
}

std::unique_ptr<MemTableIterator> HashMemTable::NewIterator() const {
    // A hash map is inherently unordered. Creating a sorted iterator would require
    // copying all data into a vector and sorting it, which is extremely inefficient
    // and defeats the purpose of this memtable type.
    // Therefore, we explicitly state that this operation is not supported.
    throw std::runtime_error("Ordered iteration is not supported by HashMemTable.");
}

size_t HashMemTable::ApproximateMemoryUsage() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t total_size = sizeof(*this);
    // This is a rough estimate; std::unordered_map has significant internal overhead.
    total_size += map_.bucket_count() * (sizeof(void*) + sizeof(size_t));
    for (const auto& pair : map_) {
        total_size += pair.first.capacity();
        total_size += pair.second.key.capacity() + pair.second.value.capacity();
    }
    return total_size;
}

size_t HashMemTable::Count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_.size();
}

bool HashMemTable::Empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_.empty();
}

} // namespace lsm
} // namespace engine