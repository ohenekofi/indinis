// src/lsm/prefix_hash_memtable.cpp
#include "../../include/lsm/prefix_hash_memtable.h"
#include "../../include/lsm/arena.h"

#include <unordered_map>
#include <map>
#include <mutex>

namespace engine {
namespace lsm {

// --- Iterator for PrefixHashMemTable ---
class PrefixHashIterator : public MemTableIterator {
public:
    explicit PrefixHashIterator(const std::map<std::string, Entry>* sorted_map)
        : map_(sorted_map), it_(map_->begin()) {}

    bool Valid() const override { return it_ != map_->end(); }
    void Next() override { if (Valid()) ++it_; }
    void Prev() override { if (it_ != map_->begin()) --it_; }
    void SeekToFirst() override { it_ = map_->begin(); }
    void SeekToLast() override { if (!map_->empty()) it_ = --map_->end(); else it_ = map_->end(); }
    void Seek(const std::string& key) override { it_ = map_->lower_bound(key); }
    Entry GetEntry() const override { return Valid() ? it_->second : Entry{}; }

private:
    const std::map<std::string, Entry>* map_;
    std::map<std::string, Entry>::const_iterator it_;
};

// --- PImpl Definition ---
struct PrefixHashMemTable::PImpl {
    std::unordered_map<std::string, Entry> point_lookup_map;
    std::map<std::string, Entry> prefix_lookup_map;
    mutable std::mutex mutex;
};

// --- PrefixHashMemTable Method Implementations ---
PrefixHashMemTable::PrefixHashMemTable(Arena& arena)
    : pimpl_(std::make_unique<PImpl>()), arena_(arena) {}

PrefixHashMemTable::~PrefixHashMemTable() = default;

void PrefixHashMemTable::Add(const std::string& key, const std::string& value, uint64_t seq) {
    std::lock_guard<std::mutex> lock(pimpl_->mutex);
    Entry new_entry{key, value, seq};
    pimpl_->point_lookup_map[key] = new_entry;
    pimpl_->prefix_lookup_map[key] = new_entry;
}

void PrefixHashMemTable::Delete(const std::string& key, uint64_t seq) {
    std::lock_guard<std::mutex> lock(pimpl_->mutex);
    Entry tombstone{key, "", seq};
    pimpl_->point_lookup_map[key] = tombstone;
    pimpl_->prefix_lookup_map[key] = tombstone;
}

bool PrefixHashMemTable::Get(const std::string& key, std::string& value, uint64_t& seq) const {
    std::lock_guard<std::mutex> lock(pimpl_->mutex);
    auto it = pimpl_->point_lookup_map.find(key);
    if (it != pimpl_->point_lookup_map.end()) {
        const Entry& entry = it->second;
        if (!entry.value.empty()) { // Not a tombstone
            value = entry.value;
            seq = entry.sequence_number;
            return true;
        }
    }
    return false;
}

std::unique_ptr<MemTableIterator> PrefixHashMemTable::NewIterator() const {
    std::lock_guard<std::mutex> lock(pimpl_->mutex);
    return std::make_unique<PrefixHashIterator>(&pimpl_->prefix_lookup_map);
}

size_t PrefixHashMemTable::ApproximateMemoryUsage() const {
    std::lock_guard<std::mutex> lock(pimpl_->mutex);
    return sizeof(*this) + arena_.MemoryUsage();
}

size_t PrefixHashMemTable::Count() const {
    std::lock_guard<std::mutex> lock(pimpl_->mutex);
    return pimpl_->point_lookup_map.size();
}

bool PrefixHashMemTable::Empty() const {
    std::lock_guard<std::mutex> lock(pimpl_->mutex);
    return pimpl_->point_lookup_map.empty();
}

} // namespace lsm
} // namespace engine