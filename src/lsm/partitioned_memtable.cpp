// src/lsm/partitioned_memtable.cpp

#include "../../include/lsm/partitioned_memtable.h"
#include "../../include/lsm/compressed_memtable.h"
#include "../../include/lsm/skiplist.h"
#include "../../include/lsm/vector_memtable.h"
#include "../../include/lsm/hash_memtable.h"
#include "../../include/debug_utils.h"
#include "../../include/lsm/prefix_hash_memtable.h"
#include "../../include/lsm/rcu_memtable_adapter.h"
#include "../../include/indinis.h"
#include <algorithm>
#include <stdexcept>
#include <chrono>
#include <numeric> // For std::accumulate
#include <cmath>   // For std::sqrt


namespace engine {
namespace lsm {

/**
 * @brief A factory function that creates an instance of a specific MemTableRep
 *        implementation based on the provided type.
 *
 * This function decouples the PartitionedMemTable from the concrete memtable
 * implementations, allowing new types to be added easily.
 *
 * @param type The enum value specifying which memtable to create.
 * @param arena A reference to the Arena allocator that the memtable will use.
 * @return A std::unique_ptr to the newly created MemTableRep.
 * @throws std::invalid_argument if an unsupported MemTableType is provided.
 */
static std::unique_ptr<MemTableRep> CreateMemTableRep(MemTableType type, Arena& arena) {
    switch (type) {
        case MemTableType::SKIP_LIST:
            return std::make_unique<SkipList>(arena);

        case MemTableType::VECTOR:
            return std::make_unique<VectorMemTable>(arena);
        
        case MemTableType::HASH_TABLE:
            return std::make_unique<HashMemTable>(arena);
        
        case MemTableType::PREFIX_HASH_TABLE:
            return std::make_unique<PrefixHashMemTable>(arena);

        case MemTableType::COMPRESSED_SKIP_LIST: {
            auto underlying_skiplist = std::make_unique<SkipList>(arena);
            return std::make_unique<CompressedMemTable>(std::move(underlying_skiplist));
        }

        case MemTableType::RCU: {
            // The RCUMemTableRepAdapter takes an Arena in its constructor to conform
            // to this factory's signature, even though the underlying RCUMemTable
            // manages its own memory without the Arena.
            return std::make_unique<RCUMemTableRepAdapter>(arena);
        }

        default:
            throw std::invalid_argument("Unsupported MemTableType specified for Partition.");
    }
}

// PartitionStats Impl
PartitionStats::PartitionStats(const PartitionStats& other) {
    read_count.store(other.read_count.load());
    write_count.store(other.write_count.load());
    delete_count.store(other.delete_count.load());
    miss_count.store(other.miss_count.load());
    current_size.store(other.current_size.load());
    total_read_time_ns.store(other.total_read_time_ns.load());
    total_write_time_ns.store(other.total_write_time_ns.load());
}

PartitionStats& PartitionStats::operator=(const PartitionStats& other) {
    if (this != &other) {
        read_count.store(other.read_count.load());
        write_count.store(other.write_count.load());
        delete_count.store(other.delete_count.load());
        miss_count.store(other.miss_count.load());
        current_size.store(other.current_size.load());
        total_read_time_ns.store(other.total_read_time_ns.load());
        total_write_time_ns.store(other.total_write_time_ns.load());
    }
    return *this;
}

// Partition Impl
Partition::Partition(size_t id, MemTableType type)
    : partition_id_(id),
      arena_(std::make_unique<Arena>())
{
    // This call now correctly dispatches to the right `make_unique`.
    memtable_ = CreateMemTableRep(type, *arena_);
}

Partition::~Partition() = default;

void Partition::Add(const std::string& k, const std::string& v, uint64_t s) {
    if (IsFrozen()) return;
    auto t = std::chrono::steady_clock::now();
    std::unique_lock<std::shared_mutex> l(partition_mutex_);
    memtable_->Add(k, v, s);
    auto d = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t);
    stats_.total_write_time_ns.fetch_add(d.count());
    stats_.write_count.fetch_add(1);
    stats_.current_size.fetch_add(k.size() + v.size());
}

void Partition::Delete(const std::string& k, uint64_t s) {
    if (IsFrozen()) return;
    auto t = std::chrono::steady_clock::now();
    std::unique_lock<std::shared_mutex> l(partition_mutex_);
    memtable_->Delete(k, s);
    auto d = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t);
    stats_.total_write_time_ns.fetch_add(d.count());
    stats_.delete_count.fetch_add(1);
    stats_.current_size.fetch_add(k.size());
}

const std::vector<std::unique_ptr<Partition>>& PartitionedMemTable::getPartitions() const {
    return partitions_;
}

MemTableRep* Partition::getMemTableRep() const {
    return memtable_.get();
}

bool Partition::Get(const std::string& k, std::string& v, uint64_t& s) const {
    auto t = std::chrono::steady_clock::now();
    std::shared_lock<std::shared_mutex> l(partition_mutex_);
    bool f = memtable_->Get(k, v, s);
    auto d = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t);
    stats_.total_read_time_ns.fetch_add(d.count());
    if (f) stats_.read_count.fetch_add(1);
    else stats_.miss_count.fetch_add(1);
    return f;
}

std::unique_ptr<MemTableIterator> Partition::NewIterator() const {
    // std::shared_lock<std::shared_mutex> l(partition_mutex_);
    return memtable_->NewIterator();
}

size_t Partition::ApproximateMemoryUsage() const {
    std::shared_lock<std::shared_mutex> l(partition_mutex_);
    return memtable_->ApproximateMemoryUsage();
}

size_t Partition::Count() const {
    std::shared_lock<std::shared_mutex> l(partition_mutex_);
    return memtable_->Count();
}

bool Partition::Empty() const {
    std::shared_lock<std::shared_mutex> l(partition_mutex_);
    return memtable_->Empty();
}

void Partition::Freeze() {
    frozen_.store(true);
}

bool Partition::IsFrozen() const {
    return frozen_.load();
}

PartitionStats Partition::GetStats() const {
    return stats_;
}

// PartitionedIterator Impl
PartitionedIterator::PartitionedIterator(const std::vector<std::unique_ptr<Partition>>& p) : partitions_(p) {
    iter_states_.reserve(p.size());
    for (const auto& part : p) {
        iter_states_.push_back({part->NewIterator(), false});
    }
}

void PartitionedIterator::Heapify() {
    merge_heap_.clear();
    for (size_t i = 0; i < iter_states_.size(); ++i)
        if (iter_states_[i].has_data)
            merge_heap_.push_back(i);
    auto comp = [&](size_t a, size_t b) {
        return iter_states_[a].iterator->GetEntry().key > iter_states_[b].iterator->GetEntry().key;
    };
    std::make_heap(merge_heap_.begin(), merge_heap_.end(), comp);
}

bool PartitionedIterator::Valid() const {
    return !merge_heap_.empty();
}

void PartitionedIterator::Next() {
    if (!Valid()) return;
    auto comp = [&](size_t a, size_t b) {
        return iter_states_[a].iterator->GetEntry().key > iter_states_[b].iterator->GetEntry().key;
    };
    std::pop_heap(merge_heap_.begin(), merge_heap_.end(), comp);
    size_t min_idx = merge_heap_.back();
    merge_heap_.pop_back();
    iter_states_[min_idx].iterator->Next();
    iter_states_[min_idx].has_data = iter_states_[min_idx].iterator->Valid();
    if (iter_states_[min_idx].has_data) {
        merge_heap_.push_back(min_idx);
        std::push_heap(merge_heap_.begin(), merge_heap_.end(), comp);
    }
}

void PartitionedIterator::SeekToFirst() {
    for (auto& s : iter_states_) {
        s.iterator->SeekToFirst();
        s.has_data = s.iterator->Valid();
    }
    Heapify();
}

void PartitionedIterator::Seek(const std::string& k) {
    for (auto& s : iter_states_) {
        s.iterator->Seek(k);
        s.has_data = s.iterator->Valid();
    }
    Heapify();
}

Entry PartitionedIterator::GetEntry() const {
    if (!Valid()) throw std::runtime_error("Invalid iterator");
    return iter_states_[merge_heap_.front()].iterator->GetEntry();
}

void PartitionedIterator::Prev() {
    throw std::runtime_error("Not implemented");
}

void PartitionedIterator::SeekToLast() {
    throw std::runtime_error("Not implemented");
}

// HashPartitionSelector Impl
HashPartitionSelector::HashPartitionSelector(uint64_t seed) : seed_(seed) {}

size_t HashPartitionSelector::SelectPartition(const std::string& key, size_t count) const {
    return (hasher_(key) ^ seed_) % count;
}

// RangePartitionSelector Impl
RangePartitionSelector::RangePartitionSelector(const std::vector<std::string>& b) : boundaries_(b) {
    std::sort(boundaries_.begin(), boundaries_.end());
}

size_t RangePartitionSelector::SelectPartition(const std::string& key, size_t count) const {
    auto it = std::upper_bound(boundaries_.begin(), boundaries_.end(), key);
    return std::min(static_cast<size_t>(std::distance(boundaries_.begin(), it)), count - 1);
}

// PartitionedMemTable Impl
PartitionedMemTable::PartitionedMemTable(const Config& config)
    : partition_count_(config.partition_count),
      partition_memtable_type_(config.partition_type),
      stats_interval_(config.stats_interval),
      imbalance_threshold_(config.imbalance_threshold) {
    if (partition_count_ < config::MIN_PARTITION_COUNT || partition_count_ > config::MAX_PARTITION_COUNT)
        throw std::invalid_argument("Partition count out of range");
    if (!config.range_boundaries.empty())
        partition_selector_ = std::make_unique<RangePartitionSelector>(config.range_boundaries);
    else
        partition_selector_ = std::make_unique<HashPartitionSelector>(config.hash_seed);
    partitions_.reserve(partition_count_);
    for (size_t i = 0; i < partition_count_; ++i)
        partitions_.push_back(std::make_unique<Partition>(i, partition_memtable_type_));
    if (stats_interval_.count() > 0)
        stats_thread_ = std::thread(&PartitionedMemTable::StatsThreadFunction, this);
}

PartitionedMemTable::PartitionedMemTable(const Config& config, PartitionedMemTable& source)
    : partition_count_(config.partition_count),
      partition_memtable_type_(config.partition_type),
      stats_interval_(config.stats_interval),
      imbalance_threshold_(config.imbalance_threshold) {
    if (!config.range_boundaries.empty())
        partition_selector_ = std::make_unique<RangePartitionSelector>(config.range_boundaries);
    else
        partition_selector_ = std::make_unique<HashPartitionSelector>(config.hash_seed);
    partitions_.reserve(partition_count_);
    for (size_t i = 0; i < partition_count_; ++i)
        partitions_.push_back(std::make_unique<Partition>(i, partition_memtable_type_));
    auto source_iter = source.NewIterator();
    for (source_iter->SeekToFirst(); source_iter->Valid(); source_iter->Next()) {
        Entry entry = source_iter->GetEntry();
        if (entry.value.empty())
            this->Delete(entry.key, entry.sequence_number);
        else
            this->Add(entry.key, entry.value, entry.sequence_number);
    }
}

PartitionedMemTable::~PartitionedMemTable() {
    if (stats_interval_.count() > 0) {
        shutdown_requested_.store(true);
        stats_cv_.notify_one();
        if (stats_thread_.joinable())
            stats_thread_.join();
    }
}

void PartitionedMemTable::Add(const std::string& k, const std::string& v, uint64_t s) {
    partitions_[SelectPartitionForKey(k)]->Add(k, v, s);
}

void PartitionedMemTable::Delete(const std::string& k, uint64_t s) {
    partitions_[SelectPartitionForKey(k)]->Delete(k, s);
}

bool PartitionedMemTable::Get(const std::string& k, std::string& v, uint64_t& s) const {
    return partitions_[SelectPartitionForKey(k)]->Get(k, v, s);
}

void Partition::BeginTxnRead(::Transaction* tx_ptr) {
    if (memtable_ && memtable_->GetType() == MemTableType::RCU) {
        auto* adapter = static_cast<RCUMemTableRepAdapter*>(memtable_.get());
        adapter->BeginTxnRead(tx_ptr);
    }
}

void Partition::EndTxnRead(::Transaction* tx_ptr) {
    if (memtable_ && memtable_->GetType() == MemTableType::RCU) {
        auto* adapter = static_cast<RCUMemTableRepAdapter*>(memtable_.get());
        adapter->EndTxnRead(tx_ptr);
    }
}

void PartitionedMemTable::BeginTxnRead(::Transaction* tx_ptr) {
    for (const auto& partition : partitions_) {
        partition->BeginTxnRead(tx_ptr);
    }
}

void PartitionedMemTable::EndTxnRead(::Transaction* tx_ptr) {
    for (const auto& partition : partitions_) {
        partition->EndTxnRead(tx_ptr);
    }
}

std::unique_ptr<MemTableIterator> PartitionedMemTable::NewIterator() const {
    return std::make_unique<PartitionedIterator>(partitions_);
}

size_t PartitionedMemTable::ApproximateMemoryUsage() const {
    size_t t = 0;
    for (const auto& p : partitions_)
        t += p->ApproximateMemoryUsage();
    return t;
}

size_t PartitionedMemTable::Count() const {
    size_t t = 0;
    for (const auto& p : partitions_)
        t += p->Count();
    return t;
}

bool PartitionedMemTable::Empty() const {
    for (const auto& p : partitions_)
        if (!p->Empty())
            return false;
    return true;
}

void PartitionedMemTable::FreezeAll() {
    for (auto& p : partitions_)
        p->Freeze();
}

GlobalStats PartitionedMemTable::GetGlobalStats() const {
    std::lock_guard<std::mutex> l(stats_mutex_);
    return global_stats_;
}

bool PartitionedMemTable::IsImbalanced() const {
    return is_imbalanced_.load();
}

std::vector<size_t> PartitionedMemTable::GetPartitionSizes() const {
    std::vector<size_t> s;
    s.reserve(partition_count_);
    for (const auto& p : partitions_)
        s.push_back(p->ApproximateMemoryUsage());
    return s;
}

double PartitionedMemTable::GetImbalanceMetric() const {
    auto sizes = GetPartitionSizes();
    if (sizes.empty()) return 0.0;
    double sum = std::accumulate(sizes.begin(), sizes.end(), 0.0);
    double mean = sum / sizes.size();
    if (mean == 0) return 0.0;
    double sq_sum = std::inner_product(sizes.begin(), sizes.end(), sizes.begin(), 0.0);
    double stddev = std::sqrt(sq_sum / sizes.size() - mean * mean);
    return stddev / mean;
}

void PartitionedMemTable::UpdateGlobalStats() {
    GlobalStats ns;
    ns.partition_count = partition_count_;
    uint64_t tr = 0, tw = 0;
    for (const auto& p : partitions_) {
        auto ps = p->GetStats();
        ns.total_reads += ps.read_count;
        ns.total_writes += ps.write_count;
        ns.total_deletes += ps.delete_count;
        ns.total_misses += ps.miss_count;
        ns.total_size_bytes += ps.current_size;
        tr += ps.read_count + ps.miss_count;
        tw += ps.write_count + ps.delete_count;
        ns.avg_read_latency += std::chrono::nanoseconds(ps.total_read_time_ns);
        ns.avg_write_latency += std::chrono::nanoseconds(ps.total_write_time_ns);
    }
    if (tr > 0)
        ns.avg_read_latency /= tr;
    if (tw > 0)
        ns.avg_write_latency /= tw;
    ns.load_balance_factor = GetImbalanceMetric();
    is_imbalanced_.store(ns.load_balance_factor > imbalance_threshold_);
    std::lock_guard<std::mutex> l(stats_mutex_);
    global_stats_ = ns;
}

void PartitionedMemTable::StatsThreadFunction() {
    while (!shutdown_requested_.load()) {
        std::unique_lock<std::mutex> l(stats_thread_mutex_);
        if (stats_cv_.wait_for(l, stats_interval_, [this] { return shutdown_requested_.load(); }))
            break;
        UpdateGlobalStats();
    }
}

size_t PartitionedMemTable::SelectPartitionForKey(const std::string& k) const {
    return partition_selector_->SelectPartition(k, partition_count_);
}

} // namespace lsm
} // namespace engine