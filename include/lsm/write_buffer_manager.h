// include/lsm/write_buffer_manager.h
#pragma once
#include "interfaces.h"

#include <atomic>
#include <cstdint>
#include <list>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <utility>

class LSMTree;
namespace engine {
namespace lsm {

class Arena; // Forward-declare Arena


// A lightweight struct to track flushable memtables
struct FlushCandidate {
    IFlushable* owner;
    size_t memtable_size;
    std::chrono::steady_clock::time_point creation_time;

    // === : Add an explicit constructor ===
    FlushCandidate(IFlushable* o, size_t size, std::chrono::steady_clock::time_point time)
        : owner(o), memtable_size(size), creation_time(std::move(time)) {}
    // =======================================

    bool operator<(const FlushCandidate& other) const {
        return creation_time < other.creation_time;
    }
};

/**
 * @class WriteBufferManager
 * @brief Manages the total memory usage of all memtables across the entire database engine.
 *
 * This class provides a centralized mechanism to enforce a global memory limit for
 * all active and immutable memtables combined. It prevents out-of-memory errors in
 * scenarios with many concurrent stores or bursty write workloads.
 *
 * LSMTree instances register their memtables' memory usage with this manager and
 * must request permission before allocating a new active memtable. If the global
 * limit is reached, write threads will be blocked until memory is freed by a
 * background flush operation.
 */
class WriteBufferManager {
public:
    /**
     * @param buffer_size The total memory limit in bytes for all memtables.
     */
    explicit WriteBufferManager(size_t buffer_size);

    WriteBufferManager(const WriteBufferManager&) = delete;
    WriteBufferManager& operator=(const WriteBufferManager&) = delete;

    /**
     * @brief Reserves memory for a new memtable. This is a blocking call.
     * If the total memory usage is at or near the limit, this function will
     * wait until enough memory is freed by a flush operation.
     * @param memtable_size The size of the memtable to be allocated.
     */
    void ReserveMemory(size_t memtable_size);

    /**
     * @brief Frees the memory associated with a memtable that has been flushed.
     * This is called by a flush worker after an immutable memtable has been successfully
     * written to disk. It signals waiting threads that memory is now available.
     * @param memtable_size The size of the flushed memtable.
     */
    void FreeMemory(size_t memtable_size);

        /**
     * @brief Registers an immutable memtable as a candidate for flushing.
     * Called by an LSMTree when its active memtable becomes immutable.
     * @param owner A pointer to the LSMTree that owns this memtable.
     * @param memtable_size The size of the memtable being registered.
     */
    void RegisterImmutable(IFlushable* owner, size_t memtable_size);

    /**
     * @brief Unregisters an immutable memtable.
     * Called by an LSMTree's flush worker *before* it begins flushing a memtable
     * it picked from its own queue.
     * @param owner A pointer to the LSMTree that owns this memtable.
     * @param memtable_size The size of the memtable being unregistered.
     */
    void UnregisterImmutable(IFlushable* owner, size_t memtable_size);

    /**
     * @brief Gets the total memory limit for the write buffers.
     */
    size_t GetBufferSize() const;

    /**
     * @brief Gets the current total memory usage of all tracked memtables.
     */
    size_t GetMemoryUsage() const;
    
    /**
     * @brief Signals all waiting threads to unblock during shutdown.
     */
    void Shutdown();

    size_t getImmutableMemtablesCount() const;

private:
    const size_t buffer_size_;
    std::atomic<size_t> memory_usage_{0};
    void TriggerFlushIfNeeded();

    // List of all immutable memtables across all LSMTree instances
    std::list<FlushCandidate> immutable_memtables_;
    
    mutable std::mutex mtx_;
    std::condition_variable cv_;
    bool shutdown_ = false;

    // For rate-based prediction
    std::deque<std::pair<std::chrono::steady_clock::time_point, size_t>> recent_memory_usage_;
    static constexpr std::chrono::seconds kRateCalculationWindow{5};
    static constexpr std::chrono::seconds kTimeToFullThreshold{2};
};

} // namespace lsm
} // namespace engine