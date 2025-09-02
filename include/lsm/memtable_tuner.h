// include/lsm/memtable_tuner.h
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>

namespace engine {
namespace lsm {

/**
 * @class MemTableTuner
 * @brief A component for adaptively sizing the LSM-Tree's memtable.
 *
 * This class monitors the recent write throughput (bytes per second) and adjusts the
 * target memtable size. The goal is to create a feedback loop that balances two competing needs:
 * 1.  **High Throughput**: Larger memtables can absorb more writes before a stall is needed
 *     for flushing, which is good for bursty write loads.
 * 2.  **Memory Usage & Flush Frequency**: Smaller memtables consume less RAM and are flushed
 *     more frequently, leading to smaller, faster-to-compact L0 SSTables.
 *
 * The tuner aims for a target flush interval (e.g., flush every 30 seconds under sustained load)
 * and calculates the required memtable size to achieve this based on the observed write rate.
 */
class MemTableTuner {
public:
    MemTableTuner(size_t initial_target_size, size_t min_size, size_t max_size);

    /**
     * @brief Notifies the tuner that a certain number of bytes have been written.
     * This method is called by the LSMTree on every `put` operation.
     * @param bytes_written The number of bytes for the new record.
     */
    void RecordWrite(size_t bytes_written);

    /**
     * @brief Retrieves the current target memtable size.
     * This method also internally triggers an adjustment calculation if enough time has passed.
     * @return The current recommended target size in bytes for the memtable.
     */
    size_t GetTargetMemTableSize();

    double getSmoothedWriteRateBps() const;
   

private:
    void AdjustTargetSize();

    // --- Configuration ---
    const size_t min_target_size_;
    const size_t max_target_size_;
    static constexpr std::chrono::seconds kTargetFlushInterval{30};
    static constexpr std::chrono::seconds kAdjustmentInterval{5};
    static constexpr double kSmoothingFactor = 0.2;
    //double smoothed_write_rate_bps_ = 0.0; 

    // --- State for Throughput Calculation ---
    std::atomic<size_t> bytes_since_last_adjustment_{0};
    std::chrono::steady_clock::time_point last_adjustment_time_;
    double smoothed_write_rate_bps_ = 0.0; 

    // --- Current Target Size ---
    std::atomic<size_t> current_target_size_bytes_;
    mutable std::mutex adjustment_mutex_; // Protects the adjustment logic
};

} // namespace lsm
} // namespace engine