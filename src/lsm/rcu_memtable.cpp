// src/lsm/rcu_memtable.cpp
#include "../../include/lsm/memtable.h"
#include "../../include/debug_utils.h"

namespace lsm {
namespace memtable {

// Helper to get a printable thread ID
static std::string get_printable_tid() {
    std::stringstream ss;
    ss << std::this_thread::get_id();
    return ss.str();
}

// --- RCUReadGuard Implementation ---
RCUReadGuard::RCUReadGuard(RCUMemTable* memtable) : memtable_(memtable), active_(false) {
    if (memtable_) {
        reader_id_ = std::this_thread::get_id();
        memtable_->register_reader(reader_id_);
        active_ = true;
        LOG_TRACE("[RCUReadGuard CONSTRUCT] Acquired read lock for Thread ID: {}", get_printable_tid());

    }
}

RCUReadGuard::~RCUReadGuard() noexcept {
    if (active_ && memtable_) {
        LOG_TRACE("[RCUReadGuard DESTRUCT] Releasing read lock for Thread ID: {}", get_printable_tid());
        memtable_->unregister_reader(reader_id_);
    }
}

RCUReadGuard::RCUReadGuard(RCUReadGuard&& other) noexcept
    : memtable_(other.memtable_), reader_id_(other.reader_id_), active_(other.active_) {
    other.active_ = false;
    other.memtable_ = nullptr;
}

RCUReadGuard& RCUReadGuard::operator=(RCUReadGuard&& other) noexcept {
    if (this != &other) {
        if (active_ && memtable_) {
            memtable_->unregister_reader(reader_id_);
        }
        memtable_ = other.memtable_;
        reader_id_ = other.reader_id_;
        active_ = other.active_;
        other.active_ = false;
        other.memtable_ = nullptr;
    }
    return *this;
}


// --- RCUIterator Implementation ---
RCUIterator::RCUIterator(std::shared_ptr<const NodeMap> snapshot, RCUMemTable* memtable)
    : snapshot_(std::move(snapshot)), read_guard_(memtable) {
    current_ = snapshot_->cbegin();
    pin_current_node();
}

RCUIterator::~RCUIterator() {
    pinned_nodes_.clear();
}

bool RCUIterator::Valid() const {
    return current_ != snapshot_->cend();
}

void RCUIterator::Next() {
    if (Valid()) {
        ++current_;
        pin_current_node();
    }
}

void RCUIterator::Prev() {
    if (current_ != snapshot_->cbegin()) {
        --current_;
        pin_current_node();
    }
}
void RCUIterator::SeekToFirst() { 
    current_ = snapshot_->cbegin();
    pin_current_node(); 
}

void RCUIterator::SeekToLast() { 
    if (!snapshot_->empty()) {
        current_ = --snapshot_->cend();
    } else {
        current_ = snapshot_->cend();
    }
    pin_current_node(); 
}

void RCUIterator::Seek(const std::string& key) { 
    current_ = snapshot_->lower_bound(key);
    pin_current_node();
}

// === FIX: Correctly implement GetEntry with type translation ===
::engine::lsm::Entry RCUIterator::GetEntry() const {
    if (!Valid()) {
        return {}; // Return default-constructed Entry
    }

    const auto& rcu_node = current_->second;
    const auto& rcu_entry = rcu_node->entry();
    
    // Translate from the internal RCUEntry to the public ::engine::lsm::Entry
    return { rcu_entry.key, rcu_entry.value, rcu_entry.sequence_number };
}

void RCUIterator::pin_current_node() const {
    if (Valid()) {
        // Pinning the node by holding a shared_ptr to it ensures it won't be
        // reclaimed while the iterator is potentially referencing its data.
        pinned_nodes_.push_back(current_->second);
    }
}

// --- RCUMemTable Implementation ---
void RCUMemTable::BeginTxnRead(uintptr_t tx_id) {
    std::unique_lock<std::shared_mutex> lock(reader_mutex_);
    active_txn_readers_[tx_id] = std::chrono::steady_clock::now();
    size_t new_count = active_readers_.size() + active_txn_readers_.size();
    stats_.active_readers.store(new_count, std::memory_order_relaxed);
    LOG_INFO("[RCUMemTable] Long-lived transaction reader registered. Txn Ptr ID: {}. Total active readers: {}",
             tx_id, new_count);
}

void RCUMemTable::EndTxnRead(uintptr_t tx_id) {
    std::unique_lock<std::shared_mutex> lock(reader_mutex_);
    active_txn_readers_.erase(tx_id);
    size_t new_count = active_readers_.size() + active_txn_readers_.size();
    stats_.active_readers.store(new_count, std::memory_order_relaxed);
    LOG_INFO("[RCUMemTable] Long-lived transaction reader unregistered. Txn Ptr ID: {}. Total active readers: {}",
             tx_id, new_count);
}

RCUMemTable::RCUMemTable(RCUConfig config)
    : config_(std::move(config)) {
    // This is where the executable code belongs.
    std::atomic_store(&data_, std::make_shared<const NodeMap>());
    reclamation_manager_ = std::make_unique<RCUReclamationManager>(config_, stats_, this);
}

RCUMemTable::~RCUMemTable() {
    shutdown();
}

bool RCUMemTable::shutdown(std::chrono::milliseconds timeout) {
    shutdown_requested_.store(true, std::memory_order_release);
    if (reclamation_manager_) {
        reclamation_manager_->shutdown();
    }
    // A more robust implementation might wait for active readers/writers to finish.
    return true;
}

RCUMemTable::SharedNodeMap RCUMemTable::get_snapshot() const {
    return std::atomic_load(&data_);
}

void RCUMemTable::Add(const std::string& key, const std::string& value, uint64_t seq) {
    validate_not_shutdown();
    validate_key(key);
    
    auto start_time = std::chrono::high_resolution_clock::now();
    std::unique_lock<std::mutex> lock(writer_mutex_);
    RCUNode::Ptr new_node = std::make_shared<RCUNode>(RCUEntry{key, value, seq, false});
    SharedNodeMap new_map = copy_and_modify(key, new_node);
    std::atomic_store(&data_, new_map);
    reclamation_manager_->advance_grace_period();
    auto end_time = std::chrono::high_resolution_clock::now();
    
    stats_.total_write_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    stats_.total_writes.fetch_add(1, std::memory_order_relaxed);
}

void RCUMemTable::Delete(const std::string& key, uint64_t seq) {
    validate_not_shutdown();
    validate_key(key);

    std::unique_lock<std::mutex> lock(writer_mutex_);
    RCUNode::Ptr tombstone = std::make_shared<RCUNode>(RCUEntry{key, "", seq, true});
    SharedNodeMap new_map = copy_and_modify(key, tombstone);
    // === FIX: Use the standard atomic free function to store the shared_ptr ===
    std::atomic_store(&data_, new_map);
    reclamation_manager_->advance_grace_period();
    stats_.total_deletes.fetch_add(1, std::memory_order_relaxed);
}

bool RCUMemTable::Get(const std::string& key, std::string& value, uint64_t& seq) const {
    validate_not_shutdown();
    
    auto start_time = std::chrono::high_resolution_clock::now();
    RCUReadGuard guard(const_cast<RCUMemTable*>(this));
    SharedNodeMap current_map = get_snapshot();
    
    bool found = false;
    auto it = current_map->find(key);
    if (it != current_map->end()) {
        const RCUNode::Ptr& node = it->second;
        if (!node->is_deleted()) {
            value = node->value();
            seq = node->sequence_number();
            found = true;
        }
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    stats_.total_read_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    stats_.total_reads.fetch_add(1, std::memory_order_relaxed);

    return found;
}

std::unique_ptr<::engine::lsm::MemTableIterator> RCUMemTable::NewIterator() const {
    return std::make_unique<RCUIterator>(get_snapshot(), const_cast<RCUMemTable*>(this));
}

size_t RCUMemTable::ApproximateMemoryUsage() const {
    return stats_.current_memory_usage.load(std::memory_order_relaxed);
}

size_t RCUMemTable::Count() const {
    return get_snapshot()->size();
}

bool RCUMemTable::Empty() const {
    return get_snapshot()->empty();
}

// --- Private Methods ---

RCUMemTable::SharedNodeMap RCUMemTable::copy_and_modify(const std::string& key, RCUNode::Ptr new_node) {
    SharedNodeMap old_map = get_snapshot();
    
    // --- START OF FIX ---
    // 1. Find the old node in the ORIGINAL map snapshot first.
    auto old_it = old_map->find(key);
    size_t mem_removed = 0;

    if (old_it != old_map->end()) {
        // 2. If it exists, get its details and defer IT for reclamation.
        mem_removed = old_it->second->memory_footprint();
        reclamation_manager_->defer_reclamation(old_it->second); // <<< CORRECT: Deferring pointer from the OLD map
    }

    // 3. Now, create the copy of the map.
    auto new_map_ptr = std::make_shared<NodeMap>(*old_map);
    // --- END OF FIX ---
    
    // 4. Modify the copy. This will either insert a new key or overwrite the old one.
    (*new_map_ptr)[key] = new_node;

    size_t mem_added = new_node->memory_footprint();
    // Ensure subtraction doesn't underflow if mem_removed > mem_added
    if (mem_added >= mem_removed) {
        stats_.current_memory_usage += (mem_added - mem_removed);
    } else {
        stats_.current_memory_usage -= (mem_removed - mem_added);
    }

    if (stats_.current_memory_usage > stats_.peak_memory_usage) {
        stats_.peak_memory_usage = stats_.current_memory_usage.load();
    }
    
    return new_map_ptr;
}

::engine::lsm::MemTableType RCUMemTable::GetType() const {
    return ::engine::lsm::MemTableType::RCU;
}

void RCUMemTable::register_reader(std::thread::id reader_id) const {
    std::unique_lock<std::shared_mutex> lock(reader_mutex_);
    active_readers_[reader_id] = std::chrono::steady_clock::now();
    // --- HEAVY DEBUGGING ---
    size_t new_count = active_readers_.size();
    stats_.active_readers.store(new_count, std::memory_order_relaxed);
    LOG_INFO("[RCUMemTable] Reader registered for Thread ID: {}. Total active readers: {}", get_printable_tid(), new_count);
}

void RCUMemTable::unregister_reader(std::thread::id reader_id) const {
    std::unique_lock<std::shared_mutex> lock(reader_mutex_);
    active_readers_.erase(reader_id);
    // --- HEAVY DEBUGGING ---
    size_t new_count = active_readers_.size();
    stats_.active_readers.store(new_count, std::memory_order_relaxed);
    LOG_INFO("[RCUMemTable] Reader unregistered for Thread ID: {}. Total active readers: {}", get_printable_tid(), new_count);
}

void RCUMemTable::validate_key(const std::string& key) const {
    if (key.empty() || key.size() > MAX_KEY_SIZE) {
        throw std::invalid_argument("Invalid key size");
    }
}

RCUStatisticsSnapshot RCUMemTable::get_statistics() const {
    RCUStatisticsSnapshot snapshot;
    
    uint64_t total_reads_val = stats_.total_reads.load(std::memory_order_relaxed);
    uint64_t total_writes_val = stats_.total_writes.load(std::memory_order_relaxed);
    uint64_t total_deletes_val = stats_.total_deletes.load(std::memory_order_relaxed);

    snapshot.total_reads = total_reads_val;
    snapshot.total_writes = total_writes_val;
    snapshot.total_deletes = total_deletes_val;
    snapshot.memory_reclamations = stats_.memory_reclamations.load(std::memory_order_relaxed);
    snapshot.active_readers = stats_.active_readers.load(std::memory_order_relaxed);
    snapshot.pending_reclamations = stats_.pending_reclamations.load(std::memory_order_relaxed);
    snapshot.current_memory_usage = stats_.current_memory_usage.load(std::memory_order_relaxed);
    snapshot.peak_memory_usage = stats_.peak_memory_usage.load(std::memory_order_relaxed);
    
    // Calculate averages safely, avoiding division by zero
    uint64_t total_read_ops = total_reads_val;
    if (total_read_ops > 0) {
        snapshot.avg_read_latency_ns = stats_.total_read_time_ns.load(std::memory_order_relaxed) / total_read_ops;
    }
    
    uint64_t total_write_ops = total_writes_val + total_deletes_val;
    if (total_write_ops > 0) {
        snapshot.avg_write_latency_ns = stats_.total_write_time_ns.load(std::memory_order_relaxed) / total_write_ops;
    }

    snapshot.max_grace_period_ns = stats_.max_grace_period_ns.load(std::memory_order_relaxed);
    
    return snapshot;
}

void RCUMemTable::validate_not_shutdown() const {
    if (shutdown_requested_.load(std::memory_order_acquire)) {
        throw std::runtime_error("RCUMemTable is shutting down");
    }
}

} // namespace memtable
} // namespace lsm