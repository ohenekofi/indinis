// include/wal/segment_manager.h
#pragma once

#include "wal_segment.h"
#include <thread>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <atomic>
#include <string>
#include <functional>

/**
 * @class SegmentManager
 * @brief Manages the lifecycle of WAL segment files in the background.
 *
 * This class is responsible for predictively pre-allocating new, empty WAL
 * segments to eliminate latency spikes during log rotation. It runs in its
 * own dedicated thread.
 */
class SegmentManager {
public:
    SegmentManager(const std::string& wal_directory, const std::string& file_prefix, uint32_t start_segment_id);
    ~SegmentManager();

    // Disallow copy/move
    SegmentManager(const SegmentManager&) = delete;
    SegmentManager& operator=(const SegmentManager&) = delete;

    /**
     * @brief Retrieves a pre-allocated, ready-to-use segment from the pool.
     * If the pool is empty, this will block until the manager can create one.
     * @return A unique_ptr to a ready WALSegment.
     */
    std::unique_ptr<WALSegment> getReadySegment(LSN first_lsn_for_segment);

    /**
     * @brief Starts the background management thread.
     */
    void start();

    /**
     * @brief Stops the background management thread.
     */
    void stop();

private:
    void managerThreadLoop();
    void preallocateSegmentsIfNeeded();

    std::string wal_directory_;
    std::string file_prefix_;
    std::atomic<uint32_t> next_segment_id_to_create_;

    // --- Segment Queues ---
    std::deque<std::unique_ptr<WALSegment>> ready_segments_;
    mutable std::mutex queue_mutex_;
    std::condition_variable ready_segment_cv_;

    // --- Thread Management ---
    std::unique_ptr<std::thread> manager_thread_;
    std::atomic<bool> stop_requested_{false};

    // --- Configuration ---
    static constexpr size_t READY_QUEUE_TARGET_SIZE = 2;
};