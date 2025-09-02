// include/lsm/memtable.h
#pragma once

#include "memtable_rep.h" // Inherits from the core MemTableRep interface

#include <atomic>
#include <memory>
#include <vector>
#include <string>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <functional>
#include <chrono>
#include <condition_variable>
#include <cassert>
#include <cstdint>
#include <tuple>

class Transaction;

namespace lsm {
namespace memtable {

// Forward declarations for classes within this header
class RCUMemTable;
class RCUIterator;
class RCUNode;
class RCUReclamationManager; // Forward declare this as well


// RCUConfig definition
struct RCUConfig {
    std::chrono::milliseconds grace_period{100};
    size_t max_deferred_reclaims{1000};
    size_t memory_threshold{64 * 1024 * 1024};
    bool enable_statistics{true};
};

// RCUStatisticsSnapshot definition
struct RCUStatisticsSnapshot {
    uint64_t total_reads = 0;
    uint64_t total_writes = 0;
    uint64_t total_deletes = 0;
    uint64_t memory_reclamations = 0;
    size_t active_readers = 0;
    size_t pending_reclamations = 0;
    size_t current_memory_usage = 0;
    size_t peak_memory_usage = 0;
    uint64_t avg_read_latency_ns = 0;
    uint64_t avg_write_latency_ns = 0;
    uint64_t max_grace_period_ns = 0;
};

// RCUStatistics definition
struct RCUStatistics {
    std::atomic<uint64_t> total_reads{0};
    std::atomic<uint64_t> total_writes{0};
    std::atomic<uint64_t> total_deletes{0};
    std::atomic<uint64_t> memory_reclamations{0};
    std::atomic<size_t> active_readers{0};
    std::atomic<size_t> pending_reclamations{0};
    std::atomic<size_t> current_memory_usage{0};
    std::atomic<size_t> peak_memory_usage{0};
    std::atomic<uint64_t> total_read_time_ns{0};
    std::atomic<uint64_t> total_write_time_ns{0};
    std::atomic<uint64_t> max_grace_period_ns{0};
    
    void reset() noexcept;
    std::string to_string() const;
};

// RCUEntry definition
struct RCUEntry {
    std::string key;
    std::string value;
    uint64_t sequence_number;
    bool deleted;
    
    RCUEntry() = default;
    RCUEntry(std::string k, std::string v, uint64_t seq, bool del = false)
        : key(std::move(k)), value(std::move(v)), sequence_number(seq), deleted(del) {}
    
    size_t memory_footprint() const noexcept {
        return sizeof(*this) + key.capacity() + value.capacity();
    }
};

// RCUNode definition
class RCUNode {
public:
    using Ptr = std::shared_ptr<RCUNode>;
private:
    RCUEntry entry_;
    mutable std::atomic<uint64_t> access_count_{0};
public:
    explicit RCUNode(RCUEntry entry) : entry_(std::move(entry)) {}
    RCUNode(const RCUNode&) = delete;
    RCUNode& operator=(const RCUNode&) = delete;
    const RCUEntry& entry() const noexcept { return entry_; }
    const std::string& key() const noexcept { return entry_.key; }
    const std::string& value() const noexcept { return entry_.value; }
    uint64_t sequence_number() const noexcept { return entry_.sequence_number; }
    bool is_deleted() const noexcept { return entry_.deleted; }
    uint64_t access_count() const noexcept { return access_count_.load(std::memory_order_acquire); }
    size_t memory_footprint() const noexcept { return sizeof(*this) + entry_.memory_footprint(); }
};

// RCUReadGuard definition
class RCUReadGuard {
private:
    RCUMemTable* memtable_;
    std::thread::id reader_id_;
    bool active_;
    friend class RCUMemTable;
public:
    explicit RCUReadGuard(RCUMemTable* memtable);
    ~RCUReadGuard() noexcept;
    RCUReadGuard(const RCUReadGuard&) = delete;
    RCUReadGuard& operator=(const RCUReadGuard&) = delete;
    RCUReadGuard(RCUReadGuard&& other) noexcept;
    RCUReadGuard& operator=(RCUReadGuard&& other) noexcept;
};

// RCUIterator definition
class RCUIterator : public ::engine::lsm::MemTableIterator {
private:
    using NodeMap = std::map<std::string, RCUNode::Ptr>;
    std::shared_ptr<const NodeMap> snapshot_;
    NodeMap::const_iterator current_;
    RCUReadGuard read_guard_;
    mutable std::vector<RCUNode::Ptr> pinned_nodes_;
    void pin_current_node() const;
public:
    explicit RCUIterator(std::shared_ptr<const NodeMap> snapshot, RCUMemTable* memtable);
    ~RCUIterator() override;
    bool Valid() const override;
    void Next() override;
    void Prev() override;
    void SeekToFirst() override;
    void SeekToLast() override;
    void Seek(const std::string& key) override;
    ::engine::lsm::Entry GetEntry() const override;
};

/**
 * @brief Deferred reclamation manager for RCU
 */
class RCUReclamationManager {
private:
    struct DeferredNode {
        RCUNode::Ptr node;
        uint64_t grace_period_id;
        DeferredNode(RCUNode::Ptr n, uint64_t gp_id) : node(std::move(n)), grace_period_id(gp_id) {}
    };

    RCUMemTable* memtable_ptr_;
    mutable std::shared_mutex deferred_mutex_;
    std::vector<DeferredNode> deferred_nodes_;
    std::atomic<uint64_t> current_grace_period_{0};
    std::atomic<uint64_t> completed_grace_period_{0};
    std::unique_ptr<std::thread> reclamation_thread_;
    std::atomic<bool> shutdown_requested_{false};
    std::condition_variable_any reclamation_cv_;
    const RCUConfig& config_;
    RCUStatistics& stats_;
    
public:
    explicit RCUReclamationManager(const RCUConfig& config, RCUStatistics& stats, RCUMemTable* memtable);
    ~RCUReclamationManager() noexcept;
    RCUReclamationManager(const RCUReclamationManager&) = delete;
    RCUReclamationManager& operator=(const RCUReclamationManager&) = delete;
    void defer_reclamation(RCUNode::Ptr node);
    void advance_grace_period() noexcept;
    void force_reclamation();
    void shutdown() noexcept;
    size_t pending_reclamations() const;
    size_t total_memory_deferred() const;
    std::atomic<bool> debug_pause_reclamation_{false};

private:
    void reclamation_worker();
    void try_reclaim_nodes();
};

/**
 * @brief Production-ready RCU (Read-Copy-Update) MemTable implementation
 */
class RCUMemTable : public ::engine::lsm::MemTableRep {
    friend class RCUReclamationManager;
public:
    using NodeMap = std::map<std::string, RCUNode::Ptr>;
    using SharedNodeMap = std::shared_ptr<const NodeMap>;

    void BeginTxnRead(uintptr_t tx_id);
    void EndTxnRead(uintptr_t tx_id);

    bool has_active_readers() const {
        std::shared_lock<std::shared_mutex> lock(reader_mutex_);
        return !active_readers_.empty();
    }
    
    RCUReclamationManager* getReclamationManagerForDebug() {
        return reclamation_manager_.get();
    }
    
private:
    SharedNodeMap data_;
    mutable std::mutex writer_mutex_;
    mutable std::shared_mutex reader_mutex_;
    mutable std::map<std::thread::id, std::chrono::steady_clock::time_point> active_readers_;
    
    std::unique_ptr<RCUReclamationManager> reclamation_manager_;
    mutable std::map<uintptr_t, std::chrono::steady_clock::time_point> active_txn_readers_;

    RCUConfig config_;
    mutable RCUStatistics stats_;
    
    std::atomic<bool> shutdown_requested_{false};
    ::engine::lsm::MemTableType GetType() const override;
    
    friend class RCUReadGuard;
    friend class RCUIterator;
    
public:
    // --- START OF FIX ---
    // Constructor DECLARATION
    explicit RCUMemTable(RCUConfig config = RCUConfig{});

    // The implementation logic that was here is MOVED to the .cpp file.
    // The class body should only contain declarations.
    // --- END OF FIX ---
    
    ~RCUMemTable() override;
    
    RCUMemTable(const RCUMemTable&) = delete;
    RCUMemTable& operator=(const RCUMemTable&) = delete;
    
    void Add(const std::string& key, const std::string& value, uint64_t seq) override;
    void Delete(const std::string& key, uint64_t seq) override;
    bool Get(const std::string& key, std::string& value, uint64_t& seq) const override;
    std::unique_ptr<::engine::lsm::MemTableIterator> NewIterator() const override;
    
    size_t ApproximateMemoryUsage() const override;
    size_t Count() const override;
    bool Empty() const override;
    std::string GetName() const override { return "RCUMemTable"; }
    
    RCUStatisticsSnapshot get_statistics() const;
    bool shutdown(std::chrono::milliseconds timeout = std::chrono::milliseconds{10000});
    SharedNodeMap get_snapshot() const;
    
private:
    void register_reader(std::thread::id reader_id) const;
    void unregister_reader(std::thread::id reader_id) const;
    
    SharedNodeMap copy_and_modify(const std::string& key, RCUNode::Ptr new_node);
    
    void validate_key(const std::string& key) const;
    void validate_not_shutdown() const;
    
    static constexpr size_t MAX_KEY_SIZE = 65536;
};

} // namespace memtable
} // namespace lsm