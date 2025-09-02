// include/partitioned_memtable.h
#pragma once

#include "memtable_rep.h"
#include "memtable_types.h" 
#include "arena.h" // Required for the std::unique_ptr<Arena> member


#include <atomic>
#include <memory>
#include <vector>
#include <string>
#include <mutex>
#include <shared_mutex>
#include <functional>
#include <thread>
#include <condition_variable>
#include <chrono>

class Transaction; 

namespace engine {
namespace config {
    static constexpr size_t DEFAULT_PARTITION_COUNT = 16;
    static constexpr size_t MAX_PARTITION_COUNT = 1024;
    static constexpr size_t MIN_PARTITION_COUNT = 2;
    static constexpr size_t CACHE_LINE_SIZE = 64;
}


namespace lsm {

// enum class MemTableType { SKIP_LIST, HASH_TABLE, VECTOR ,  COMPRESSED_SKIP_LIST, PREFIX_HASH_TABLE,   RCU };

class PartitionSelector {
public:
    virtual ~PartitionSelector() = default;
    virtual size_t SelectPartition(const std::string& key, size_t partition_count) const = 0;
    virtual std::string GetName() const = 0;
};

class HashPartitionSelector : public PartitionSelector {
public:
    explicit HashPartitionSelector(uint64_t seed = 0);
    size_t SelectPartition(const std::string& key, size_t partition_count) const override;
    std::string GetName() const override { return "Hash"; }
private:
    mutable std::hash<std::string> hasher_;
    uint64_t seed_;
};

class RangePartitionSelector : public PartitionSelector {
public:
    explicit RangePartitionSelector(const std::vector<std::string>& boundaries);
    size_t SelectPartition(const std::string& key, size_t partition_count) const override;
    std::string GetName() const override { return "Range"; }
private:
    std::vector<std::string> boundaries_;
};

struct alignas(config::CACHE_LINE_SIZE) PartitionStats {
    std::atomic<uint64_t> read_count{0};
    std::atomic<uint64_t> write_count{0};
    std::atomic<uint64_t> delete_count{0};
    std::atomic<uint64_t> miss_count{0};
    std::atomic<size_t> current_size{0};
    std::atomic<uint64_t> total_read_time_ns{0};
    std::atomic<uint64_t> total_write_time_ns{0};
    PartitionStats() = default;
    PartitionStats(const PartitionStats& other);
    PartitionStats& operator=(const PartitionStats& other);
    PartitionStats(PartitionStats&&) = default;
    PartitionStats& operator=(PartitionStats&&) = default;
};

struct GlobalStats {
    uint64_t total_reads{0}, total_writes{0}, total_deletes{0}, total_misses{0};
    size_t total_size_bytes{0}, partition_count{0};
    std::chrono::nanoseconds avg_read_latency{0}, avg_write_latency{0};
    double load_balance_factor{0.0};
};

class Partition {
public:
    Partition(size_t id, MemTableType type);
    ~Partition();
    Partition(const Partition&) = delete;
    Partition& operator=(const Partition&) = delete;
    void Add(const std::string& key, const std::string& value, uint64_t seq);
    void Delete(const std::string& key, uint64_t seq);
    bool Get(const std::string& key, std::string& value, uint64_t& seq) const;
    std::unique_ptr<MemTableIterator> NewIterator() const;
    size_t ApproximateMemoryUsage() const;
    size_t Count() const;
    bool Empty() const;
    void Freeze();
    bool IsFrozen() const;
    PartitionStats GetStats() const;
    MemTableRep* getMemTableRep() const;

    void BeginTxnRead(::Transaction* tx_ptr);
    void EndTxnRead(::Transaction* tx_ptr);
private:
    std::unique_ptr<Arena> arena_;
    std::unique_ptr<MemTableRep> memtable_;
    mutable std::shared_mutex partition_mutex_;
    size_t partition_id_;
    std::atomic<bool> frozen_{false};
    mutable PartitionStats stats_;
};

class PartitionedIterator : public MemTableIterator {
public:
    explicit PartitionedIterator(const std::vector<std::unique_ptr<Partition>>& partitions);
    bool Valid() const override;
    void Next() override;
    void Prev() override;
    void SeekToFirst() override;
    void SeekToLast() override;
    void Seek(const std::string& key) override;
    Entry GetEntry() const override;
private:
    struct PartitionIteratorState {
        std::unique_ptr<MemTableIterator> iterator;
        bool has_data;
    };
    const std::vector<std::unique_ptr<Partition>>& partitions_;
    std::vector<PartitionIteratorState> iter_states_;
    std::vector<size_t> merge_heap_;
    void Heapify();
};

class PartitionedMemTable {
public:
    struct Config {
        size_t partition_count = config::DEFAULT_PARTITION_COUNT;
        MemTableType partition_type = MemTableType::SKIP_LIST;
        uint64_t hash_seed = 0;
        std::chrono::milliseconds stats_interval = std::chrono::milliseconds(5000);
        double imbalance_threshold = 0.75;
        size_t rebalance_frequency = 0;
        size_t sample_size = 256;
        std::vector<std::string> range_boundaries;
    };
    void BeginTxnRead(::Transaction* tx_ptr);
    void EndTxnRead(::Transaction* tx_ptr);
    explicit PartitionedMemTable(const Config& config = Config{});
    PartitionedMemTable(const Config& config, PartitionedMemTable& source_memtable);
    ~PartitionedMemTable();
    PartitionedMemTable(const PartitionedMemTable&) = delete;
    PartitionedMemTable& operator=(const PartitionedMemTable&) = delete;
    
    void Add(const std::string& key, const std::string& value, uint64_t seq);
    void Delete(const std::string& key, uint64_t seq);
    bool Get(const std::string& key, std::string& value, uint64_t& seq) const;
    std::unique_ptr<MemTableIterator> NewIterator() const;
    size_t ApproximateMemoryUsage() const;
    size_t Count() const;
    bool Empty() const;
    void FreezeAll();
    GlobalStats GetGlobalStats() const;
    bool IsImbalanced() const;
    const std::vector<std::unique_ptr<Partition>>& getPartitions() const;


private:
    size_t SelectPartitionForKey(const std::string& key) const;
    void StatsThreadFunction();
    void UpdateGlobalStats();
    std::vector<size_t> GetPartitionSizes() const;
    double GetImbalanceMetric() const;
    
    std::vector<std::unique_ptr<Partition>> partitions_;
    std::unique_ptr<PartitionSelector> partition_selector_;
    const size_t partition_count_;
    const MemTableType partition_memtable_type_;
    const std::chrono::milliseconds stats_interval_;
    const double imbalance_threshold_;
    
    std::atomic<bool> is_imbalanced_{false};
    mutable std::mutex stats_mutex_;
    GlobalStats global_stats_;
    std::thread stats_thread_;
    std::atomic<bool> shutdown_requested_{false};
    std::condition_variable stats_cv_;
    std::mutex stats_thread_mutex_;
    // std::unique_ptr<MemTableRep> memtable_;
};

} // namespace lsm
} // namespace engine