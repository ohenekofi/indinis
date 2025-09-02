// src/lsm/memtable_tuner.cpp
#include "../../include/lsm/memtable_tuner.h"
#include "../../include/debug_utils.h" // For LOG_TRACE/LOG_INFO
#include <algorithm> // For std::clamp

namespace engine {
namespace lsm {

MemTableTuner::MemTableTuner(size_t initial_target_size, size_t min_size, size_t max_size)
    : min_target_size_(min_size),
      max_target_size_(max_size),
      last_adjustment_time_(std::chrono::steady_clock::now()),
      current_target_size_bytes_(initial_target_size)
{
    LOG_INFO("[MemTableTuner] Initialized. Initial Target: {}B, Min: {}B, Max: {}B",
             initial_target_size, min_size, max_size);
}

void MemTableTuner::RecordWrite(size_t bytes_written) {
    bytes_since_last_adjustment_.fetch_add(bytes_written, std::memory_order_relaxed);
}

size_t MemTableTuner::GetTargetMemTableSize() {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_adjustment_time_);

    // Check if it's time to recalculate the target size.
    if (elapsed >= kAdjustmentInterval) {
        // Use a mutex to ensure only one thread performs the adjustment calculation.
        std::lock_guard<std::mutex> lock(adjustment_mutex_);
        // Double-check the condition after acquiring the lock to prevent a race.
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last_adjustment_time_) >= kAdjustmentInterval) {
            AdjustTargetSize();
            last_adjustment_time_ = now;
        }
    }

    return current_target_size_bytes_.load(std::memory_order_relaxed);
}

double MemTableTuner::getSmoothedWriteRateBps() const {
    // Reading a double is not atomic, but it's only read for stats, and writes
    // are protected by the mutex, so this is safe enough for debug/monitoring.
    return smoothed_write_rate_bps_;
}

void MemTableTuner::AdjustTargetSize() {
    auto now = std::chrono::steady_clock::now();
    auto elapsed_nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now - last_adjustment_time_).count();
    
    if (elapsed_nanos == 0) return;
    
    size_t bytes_in_period = bytes_since_last_adjustment_.exchange(0, std::memory_order_relaxed);

    // === FIX: Use SMA for rate calculation ===
    double seconds_elapsed = static_cast<double>(elapsed_nanos) / 1e9;
    double current_rate_bps = (seconds_elapsed > 0) ? (static_cast<double>(bytes_in_period) / seconds_elapsed) : 0.0;

    // Update the Smoothed Moving Average (SMA).
    if (smoothed_write_rate_bps_ == 0.0) {
        smoothed_write_rate_bps_ = current_rate_bps; // Initialize on first run
    } else {
        smoothed_write_rate_bps_ = (kSmoothingFactor * current_rate_bps) + ((1.0 - kSmoothingFactor) * smoothed_write_rate_bps_);
    }

    // Calculate the new target size based on the SMOOTHED rate.
    size_t new_target_size = static_cast<size_t>(smoothed_write_rate_bps_ * kTargetFlushInterval.count());
    // ==========================================

    new_target_size = std::clamp(new_target_size, min_target_size_, max_target_size_);

    size_t old_target_size = current_target_size_bytes_.load();
    current_target_size_bytes_.store(new_target_size, std::memory_order_relaxed);

    if (new_target_size != old_target_size) {
        LOG_INFO("[MemTableTuner] Memtable size adjusted. Smoothed Rate: {:.2f} MB/s. New Target: {} MB",
                 smoothed_write_rate_bps_ / (1024 * 1024), new_target_size / (1024 * 1024));
    }
}


} // namespace lsm
} // namespace engine