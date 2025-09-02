//// src/lsm/write_buffer_manager.cpp
#include "../../include/lsm/write_buffer_manager.h"
#include "../../include/lsm/interfaces.h" // Now depends on the interface, not LSMTree
#include "../../include/debug_utils.h"

#include <algorithm> // For std::sort in list (though list::sort is a member)

namespace engine {
namespace lsm {

// Constructor and other methods (GetBufferSize, GetMemoryUsage, Shutdown) remain the same.
WriteBufferManager::WriteBufferManager(size_t buffer_size) : buffer_size_(buffer_size) {
    LOG_INFO("[WriteBufferManager] Initialized with a global memory limit of {} MB.", buffer_size / (1024 * 1024));
}

size_t WriteBufferManager::GetBufferSize() const {
    return buffer_size_;
}

size_t WriteBufferManager::GetMemoryUsage() const {
    return memory_usage_.load(std::memory_order_relaxed);
}

void WriteBufferManager::Shutdown() {
    std::unique_lock<std::mutex> lock(mtx_);
    shutdown_ = true;
    cv_.notify_all();
    LOG_INFO("[WriteBufferManager] Shutdown signaled. All waiting threads notified.");
}

void WriteBufferManager::ReserveMemory(size_t memtable_size) {
    std::unique_lock<std::mutex> lock(mtx_);
    
    cv_.wait(lock, [this, memtable_size] {
        return shutdown_ || (GetMemoryUsage() + memtable_size <= GetBufferSize());
    });
    if (shutdown_) return;
    
    size_t new_total = memory_usage_.fetch_add(memtable_size, std::memory_order_relaxed) + memtable_size;

    // === FIX: Add usage tracking for rate calculation ===
    auto now = std::chrono::steady_clock::now();
    recent_memory_usage_.emplace_back(now, new_total);
    // Prune old entries
    while (!recent_memory_usage_.empty() && (now - recent_memory_usage_.front().first > kRateCalculationWindow)) {
        recent_memory_usage_.pop_front();
    }
    // =====================================================

    LOG_TRACE("[WriteBufferManager] Reserved {} bytes. Total usage: {} / {} bytes.",
              memtable_size, new_total, GetBufferSize());
}

size_t WriteBufferManager::getImmutableMemtablesCount() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return immutable_memtables_.size();
}


void WriteBufferManager::FreeMemory(size_t memtable_size) {
    memory_usage_.fetch_sub(memtable_size, std::memory_order_relaxed);
    LOG_TRACE("[WriteBufferManager] Freed {} bytes. Total usage: {} / {} bytes.",
              memtable_size, GetMemoryUsage(), GetBufferSize());
    cv_.notify_all();
}

/**
 * @brief Registers an immutable memtable from an IFlushable component as a candidate for flushing.
 *
 * This is called by an LSMTree when its active memtable becomes full. The manager adds
 * it to a global list of flush candidates and immediately checks if this new addition
 * necessitates a proactive flush due to memory pressure.
 *
 * @param owner A pointer to the IFlushable component (e.g., the LSMTree instance).
 * @param memtable_size The size of the memtable being registered.
 */
void WriteBufferManager::RegisterImmutable(IFlushable* owner, size_t memtable_size) {
    std::unique_lock<std::mutex> lock(mtx_);
    
    // ===  Use emplace_back for explicit construction ===
    immutable_memtables_.emplace_back(owner, memtable_size, std::chrono::steady_clock::now());
    
    LOG_TRACE("[WriteBufferManager] Registered immutable memtable ({} bytes) from owner {:p}. Total candidates: {}",
              memtable_size, static_cast<void*>(owner), immutable_memtables_.size());

    TriggerFlushIfNeeded();
} 

/**
 * @brief Unregisters an immutable memtable that a component is about to flush.
 *
 * Called by an LSMTree's flush worker *before* it begins flushing a memtable
 * it picked from its own queue. This removes the candidate from the global manager's
 * list, preventing the manager from redundantly trying to trigger a flush on the same memtable.
 *
 * @param owner A pointer to the IFlushable component.
 * @param memtable_size The size of the memtable being unregistered.
 */
void WriteBufferManager::UnregisterImmutable(IFlushable* owner, size_t memtable_size) {
    std::unique_lock<std::mutex> lock(mtx_);

    for (auto it = immutable_memtables_.begin(); it != immutable_memtables_.end(); ++it) {
        // This comparison now works because both `it->owner` and `owner` are of type `IFlushable*`
        if (it->owner == owner && it->memtable_size == memtable_size) {
            immutable_memtables_.erase(it);
            LOG_TRACE("[WriteBufferManager] Unregistered immutable memtable ({} bytes) from owner {:p}. Total candidates: {}",
                      memtable_size, static_cast<void*>(owner), immutable_memtables_.size());
            return;
        }
    }
    LOG_WARN("[WriteBufferManager] Attempted to unregister a memtable (owner {:p}, size {}) that was not found.",
             static_cast<void*>(owner), memtable_size);
}

void WriteBufferManager::TriggerFlushIfNeeded() {
    // This method must be called with mtx_ already locked.
    if (immutable_memtables_.empty()) return;

    bool trigger = false;
    std::string trigger_reason;

    // Condition 1: Absolute threshold
    const size_t absolute_threshold = buffer_size_ * 3 / 4;
    if (GetMemoryUsage() > absolute_threshold) {
        trigger = true;
        trigger_reason = "absolute threshold (" + std::to_string(GetMemoryUsage()) + " > " + std::to_string(absolute_threshold) + ")";
    }

    // === FIX: Add predictive trigger logic ===
    if (!trigger && recent_memory_usage_.size() > 1) {
        auto& first = recent_memory_usage_.front();
        auto& last = recent_memory_usage_.back();
        
        auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(last.first - first.first).count();
        if (duration_ns > 0) {
            size_t bytes_added = (last.second > first.second) ? (last.second - first.second) : 0;
            double rate_bps = static_cast<double>(bytes_added) * 1e9 / duration_ns;
            
            // Only trigger if rate is significant to avoid flushing on tiny, fast writes
            if (rate_bps > 1024 * 1024) { // e.g., > 1MB/s
                size_t remaining_bytes = (buffer_size_ > GetMemoryUsage()) ? (buffer_size_ - GetMemoryUsage()) : 0;
                double time_to_full_s = static_cast<double>(remaining_bytes) / rate_bps;
                
                if (time_to_full_s < kTimeToFullThreshold.count()) {
                    trigger = true;
                    std::ostringstream reason_ss;
                    reason_ss << std::fixed << std::setprecision(2) 
                              << "predictive (Rate: " << (rate_bps / (1024*1024))
                              << " MB/s, Est. time to full: " << time_to_full_s << "s)";
                    trigger_reason = reason_ss.str();
                }
            }
        }
    }
    // ========================================

    if (trigger) {
        immutable_memtables_.sort(); 
        FlushCandidate candidate_to_flush = immutable_memtables_.front();
        immutable_memtables_.pop_front();

        IFlushable* owner = candidate_to_flush.owner;
        LOG_INFO("[WriteBufferManager] Triggering flush of oldest memtable. Reason: {}", trigger_reason);
        owner->TriggerFlush();
    }
}


} // namespace lsm
} // namespace engine