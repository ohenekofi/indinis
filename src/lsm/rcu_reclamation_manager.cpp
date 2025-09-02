//src/lsm/rcu_reclamation_manager.cpp
#include "../../include/lsm/memtable.h" // Your new header file
#include "../../include/debug_utils.h"
#include <thread>

namespace lsm {
namespace memtable {

// Helper to get a printable thread ID
static std::string get_printable_tid_reclaimer() {
    std::stringstream ss;
    ss << std::this_thread::get_id();
    return ss.str();
}

// --- RCUStatistics Method Implementations ---
void RCUStatistics::reset() noexcept {
    total_reads.store(0);
    total_writes.store(0);
    total_deletes.store(0);
    memory_reclamations.store(0);
    pending_reclamations.store(0);
    active_readers.store(0);
    // peak_memory_usage is a high-water mark, not reset.
    
    // === FIX: Use the new member names ===
    total_read_time_ns.store(0);
    total_write_time_ns.store(0);
    max_grace_period_ns.store(0);
}

std::string RCUStatistics::to_string() const {
    return "RCUStats{reads=" + std::to_string(total_reads.load()) +
           ", writes=" + std::to_string(total_writes.load()) +
           ", mem_usage=" + std::to_string(current_memory_usage.load()) + "}";
}

// --- RCUReclamationManager Method Implementations ---
RCUReclamationManager::RCUReclamationManager(const RCUConfig& config, RCUStatistics& stats, RCUMemTable* memtable)
    : config_(config), stats_(stats), memtable_ptr_(memtable) {
    if (!memtable_ptr_) {
        throw std::invalid_argument("RCUReclamationManager requires a valid RCUMemTable pointer.");
    }
    reclamation_thread_ = std::make_unique<std::thread>(&RCUReclamationManager::reclamation_worker, this);
    LOG_INFO("[RCUReclaimer] Background reclamation thread started.");
}

RCUReclamationManager::~RCUReclamationManager() noexcept {
    shutdown();
    if (reclamation_thread_ && reclamation_thread_->joinable()) {
        reclamation_thread_->join();
    }
}

void RCUReclamationManager::shutdown() noexcept {
    shutdown_requested_.store(true, std::memory_order_release);
    reclamation_cv_.notify_one();
}

void RCUReclamationManager::defer_reclamation(RCUNode::Ptr node) {
    if (!node) return;
    std::unique_lock<std::shared_mutex> lock(deferred_mutex_);
    uint64_t gp_id = current_grace_period_.load(std::memory_order_relaxed);
    deferred_nodes_.emplace_back(std::move(node), gp_id);
    size_t new_pending_count = stats_.pending_reclamations.fetch_add(1, std::memory_order_relaxed) + 1;

    // --- HEAVY DEBUGGING ---
    LOG_INFO("[RCUReclaimer DEFER] Node for key '{}' deferred in Grace Period {}. Total pending: {}",
             node->key(), gp_id, new_pending_count);
}

void RCUReclamationManager::advance_grace_period() noexcept {
    uint64_t new_gp = current_grace_period_.fetch_add(1, std::memory_order_relaxed) + 1;
    // --- HEAVY DEBUGGING ---
    LOG_TRACE("[RCUReclaimer ADVANCE_GP] Grace period advanced to {}.", new_gp);
    reclamation_cv_.notify_one();
}


void RCUReclamationManager::force_reclamation() {
    std::unique_lock<std::shared_mutex> lock(deferred_mutex_);
    // In a forced scenario, we advance the completed period to the current one
    completed_grace_period_.store(current_grace_period_.load());
    try_reclaim_nodes();
}

size_t RCUReclamationManager::pending_reclamations() const {
    std::shared_lock<std::shared_mutex> lock(deferred_mutex_);
    return deferred_nodes_.size();
}

size_t RCUReclamationManager::total_memory_deferred() const {
    std::shared_lock<std::shared_mutex> lock(deferred_mutex_);
    size_t total = 0;
    for (const auto& dn : deferred_nodes_) {
        total += dn.node->memory_footprint();
    }
    return total;
}

void RCUReclamationManager::reclamation_worker() {
    LOG_INFO("[RCUReclaimer WORKER] Worker thread {} started.", get_printable_tid_reclaimer());
    while (!shutdown_requested_.load()) {
        {
            std::shared_lock<std::shared_mutex> lock(deferred_mutex_);
            LOG_TRACE("[RCUReclaimer WORKER] Going to sleep/wait. Current GP: {}, Completed GP: {}.",
                      current_grace_period_.load(), completed_grace_period_.load());
            
            reclamation_cv_.wait_for(lock, config_.grace_period, [this]{
                return shutdown_requested_.load() || 
                       (completed_grace_period_.load() < current_grace_period_.load());
            });
        }

        if (shutdown_requested_.load()) break;
        
        LOG_INFO("[RCUReclaimer WORKER] Woke up. Current GP: {}, Completed GP: {}.",
                 current_grace_period_.load(), completed_grace_period_.load());

        // This is the core RCU guarantee: A grace period cannot be considered
        // "completed" if there are any readers still active that may have started
        // before or during that grace period. This simple check is a conservative
        // but safe way to enforce this.
        if (memtable_ptr_->has_active_readers()) {
            LOG_INFO("[RCUReclaimer WORKER] Readers are active. Deferring reclamation cycle to respect RCU grace period.");
            continue; // Go back to the top of the while loop and wait again.
        }

        uint64_t current_gp = current_grace_period_.load();
        uint64_t completed_gp = completed_grace_period_.load();
        
        if (completed_gp < current_gp) {
            LOG_INFO("[RCUReclaimer WORKER] Grace period has passed AND no readers are active. Updating completed GP to {} and attempting reclamation.", current_gp);
            completed_grace_period_.store(current_gp);
            try_reclaim_nodes();
        } else {
            LOG_TRACE("[RCUReclaimer WORKER] Woke up, but no new grace period has passed. Going back to sleep.");
        }
    }
    LOG_INFO("[RCUReclaimer WORKER] Worker thread {} stopped.", get_printable_tid_reclaimer());
}

void RCUReclamationManager::try_reclaim_nodes() {
    // This function must be called only when the caller holds an exclusive (unique) lock on deferred_mutex_.
    // This is guaranteed by its sole caller, reclamation_worker(), which takes the lock.

    size_t reclaimed_count = 0;
    // Get the most recently completed grace period ID. Any node deferred in a period
    // less than or equal to this is now safe to be deleted.
    uint64_t completed_gp = completed_grace_period_.load(std::memory_order_acquire);
    
    LOG_INFO("[RCUReclaimer TRY_RECLAIM] Starting cleanup. Completed GP is {}. Total nodes to check: {}.",
             completed_gp, deferred_nodes_.size());

    // Use the standard C++ idiom for erasing elements from a vector while iterating.
    auto it = deferred_nodes_.begin();
    while (it != deferred_nodes_.end()) {
        LOG_TRACE("  [TRY_RECLAIM] Checking node for key '{}' with GP ID {}.", it->node->key(), it->grace_period_id);
        
        // --- THE CRITICAL FIX ---
        // The condition must be less than or equal to (<=).
        // This ensures that nodes deferred in the grace period that just completed are
        // eligible for reclamation. A common bug is to use '<', which would fail to
        // reclaim the nodes from the most recently completed period.
        if (it->grace_period_id <= completed_gp) {
            
            LOG_TRACE("    -> Node is eligible for reclamation (GP ID {} <= Completed GP {}). Reclaiming.", it->grace_period_id, completed_gp);
            
            // Update memory usage statistics before the node's memory is released.
            stats_.current_memory_usage -= it->node->memory_footprint();
            
            // std::vector::erase(iterator) invalidates the iterator but returns a new,
            // valid iterator pointing to the next element in the vector.
            // Assigning this back to 'it' correctly continues the loop.
            it = deferred_nodes_.erase(it);
            reclaimed_count++;
        } else {
            // The node's grace period ID is newer than the last completed one.
            // It is not yet safe to reclaim. Skip it and advance the iterator.
            LOG_TRACE("    -> Node is NOT eligible (GP ID {} > Completed GP {}). Skipping.", it->grace_period_id, completed_gp);
            ++it;
        }
    }

    // Update final statistics after the loop completes.
    if (reclaimed_count > 0) {
        stats_.memory_reclamations.fetch_add(reclaimed_count, std::memory_order_relaxed);
        size_t new_pending_count = stats_.pending_reclamations.fetch_sub(reclaimed_count, std::memory_order_relaxed) - reclaimed_count;
        LOG_INFO("[RCUReclaimer TRY_RECLAIM] Reclaimed {} nodes. Total pending now: {}.", reclaimed_count, new_pending_count);
    } else {
        LOG_TRACE("[RCUReclaimer TRY_RECLAIM] No nodes were eligible for reclamation in this cycle.");
    }
}

} // namespace memtable
} // namespace lsm