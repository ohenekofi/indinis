// src/lsm/vector_memtable.cpp

#include "../../include/lsm/vector_memtable.h"
#include <algorithm> // For std::sort, std::lower_bound

namespace engine {
namespace lsm {

/**
 * @class VectorMemTable::Iterator
 * @brief An iterator for the VectorMemTable that conforms to the MemTableIterator interface.
 */
class VectorMemTable::Iterator : public MemTableIterator {
public:
    explicit Iterator(const VectorMemTable* memtable) : memtable_(memtable) {
        // Ensure the vector is sorted before creating an iterator on it.
       // memtable_->EnsureSorted();
        // The iterator starts by pointing to the beginning of the sorted vector.
        current_it_ = memtable_->entries_.begin();
    }

    bool Valid() const override {
        return current_it_ != memtable_->entries_.end();
    }

    void Next() override {
        if (Valid()) {
            ++current_it_;
        }
    }

    void Prev() override {
        if (current_it_ != memtable_->entries_.begin()) {
            --current_it_;
        }
    }

    void SeekToFirst() override {
        current_it_ = memtable_->entries_.begin();
    }

    void SeekToLast() override {
        if (!memtable_->entries_.empty()) {
            current_it_ = memtable_->entries_.end() - 1;
        } else {
            current_it_ = memtable_->entries_.end();
        }
    }

    void Seek(const std::string& key) override {
        // Use std::lower_bound, which is efficient on a sorted vector.
        current_it_ = std::lower_bound(memtable_->entries_.begin(), memtable_->entries_.end(), key,
            [](const Entry& entry, const std::string& k) {
                return entry.key < k;
            });
    }

    Entry GetEntry() const override {
        if (Valid()) {
            return *current_it_;
        }
        // Return an empty entry if not valid, though client should check Valid() first.
        return {};
    }

private:
    const VectorMemTable* memtable_;
    std::vector<Entry>::const_iterator current_it_;
};

// --- VectorMemTable Method Implementations ---

VectorMemTable::VectorMemTable(Arena& arena) : arena_(arena) {}

void VectorMemTable::Add(const std::string& key, const std::string& value, uint64_t seq) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    Entry new_entry{key, value, seq};
    
    // Find the correct sorted position to insert the new entry.
    auto it = std::lower_bound(entries_.begin(), entries_.end(), new_entry, 
        [](const Entry& a, const Entry& b) {
            return a.key < b.key;
        });
    
    entries_.insert(it, new_entry);
}

void VectorMemTable::Delete(const std::string& key, uint64_t seq) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    Entry tombstone{key, "", seq};

    auto it = std::lower_bound(entries_.begin(), entries_.end(), tombstone,
        [](const Entry& a, const Entry& b) {
            return a.key < b.key;
        });

    entries_.insert(it, tombstone);
}

bool VectorMemTable::Get(const std::string& key, std::string& value, uint64_t& seq) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    // O(log N) search to find the first potential entry for the key.
    auto it = std::lower_bound(entries_.begin(), entries_.end(), key,
        [](const Entry& entry, const std::string& k) {
            return entry.key < k;
        });

    if (it == entries_.end() || it->key != key) {
        return false; // Key not found.
    }

    // Now scan forward from this point to find the latest version (highest sequence number).
    // This scan is very short, only covering duplicate keys.
    uint64_t max_seq = 0;
    const Entry* latest_entry = nullptr;
    
    while (it != entries_.end() && it->key == key) {
        if (it->sequence_number > max_seq) {
            max_seq = it->sequence_number;
            latest_entry = &(*it);
        }
        ++it;
    }

    if (latest_entry && !latest_entry->value.empty()) { // Check for tombstone
        value = latest_entry->value;
        seq = latest_entry->sequence_number;
        return true;
    }
    
    return false;
}

std::unique_ptr<MemTableIterator> VectorMemTable::NewIterator() const {
    return std::make_unique<Iterator>(this);
}

size_t VectorMemTable::ApproximateMemoryUsage() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    size_t total_size = sizeof(*this) + (entries_.capacity() * sizeof(Entry));
    for (const auto& entry : entries_) {
        total_size += entry.key.capacity() + entry.value.capacity();
    }
    return total_size;
}

size_t VectorMemTable::Count() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return entries_.size();
}

bool VectorMemTable::Empty() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return entries_.empty();
}

/*
void VectorMemTable::EnsureSorted() const {
    if (!sorted_.load(std::memory_order_acquire)) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        // Double-check after acquiring exclusive lock
        if (!sorted_.load()) {
            std::sort(entries_.begin(), entries_.end(), [](const Entry& a, const Entry& b) {
                if (a.key != b.key) {
                    return a.key < b.key;
                }
                // For identical keys, sort by sequence number descending to keep newest first.
                return a.sequence_number > b.sequence_number;
            });
            sorted_.store(true, std::memory_order_release);
        }
    }
}
*/
} // namespace lsm
} // namespace engine