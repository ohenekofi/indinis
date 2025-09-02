// src/wal/segment_manager.cpp

#include "../../include/wal/segment_manager.h"
#include "../../include/debug_utils.h"
#include "../../include/storage_error/storage_error.h" // For custom errors
#include <sstream>
#include <iomanip>

SegmentManager::SegmentManager(const std::string& wal_directory, const std::string& file_prefix, uint32_t start_segment_id)
    : wal_directory_(wal_directory),
      file_prefix_(file_prefix),
      next_segment_id_to_create_(start_segment_id) {}

SegmentManager::~SegmentManager() {
    stop(); // Ensure thread is stopped and joined.
}

void SegmentManager::start() {
    if (manager_thread_) {
        LOG_WARN("[SegmentManager] Start called, but manager thread already running.");
        return;
    }
    LOG_INFO("[SegmentManager] Starting background thread...");
    stop_requested_.store(false);
    manager_thread_ = std::make_unique<std::thread>(&SegmentManager::managerThreadLoop, this);
}

void SegmentManager::stop() {
    if (!stop_requested_.exchange(true)) {
        LOG_INFO("[SegmentManager] Stopping background thread...");
        ready_segment_cv_.notify_all(); // Wake up the thread if it's waiting
        if (manager_thread_ && manager_thread_->joinable()) {
            manager_thread_->join();
        }
        manager_thread_.reset();
        LOG_INFO("[SegmentManager] Background thread stopped.");
    }
}

void SegmentManager::managerThreadLoop() {
    LOG_INFO("[SegmentManager Thread] Started.");
    
    // Initial sleep to allow the rest of the system to stabilize on startup.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    while (!stop_requested_.load()) {
        try {
            // --- Phase 1: Pre-allocation Logic ---
            preallocateSegmentsIfNeeded();

        } catch (const storage::StorageError& e) {
            LOG_ERROR("[SegmentManager Thread] A storage error occurred during pre-allocation: {}. Backing off for 10 seconds.", e.toDetailedString());
            // An I/O error occurred (e.g., disk full). We should stop trying to allocate
            // for a while to avoid spamming logs and burning CPU. The parent WAL manager
            // will be marked as unhealthy by this failure.
            std::this_thread::sleep_for(std::chrono::seconds(10));
            continue; // Continue the loop to try again later.
        } catch (const std::exception& e) {
            LOG_ERROR("[SegmentManager Thread] An unexpected exception occurred: {}. Backing off.", e.what());
            std::this_thread::sleep_for(std::chrono::seconds(10));
            continue;
        }

        // --- Phase 2: Adaptive Wait Logic ---
        std::unique_lock<std::mutex> lock(queue_mutex_);
        
        // Determine the wait duration. If the queue is full, we can sleep for longer.
        // If it's not full, we use a shorter timeout to be more responsive.
        auto wait_duration = (ready_segments_.size() >= READY_QUEUE_TARGET_SIZE)
                                 ? std::chrono::seconds(5)   // Queue is full, long sleep.
                                 : std::chrono::milliseconds(200); // Queue needs filling, short sleep.

        // Wait on the condition variable. We can be woken up either by the timeout
        // or by a notification from getReadySegment() when it consumes a segment.
        ready_segment_cv_.wait_for(lock, wait_duration, [this] {
            // Predicate to handle spurious wakeups: wake up if stop is requested
            // or if the queue has dropped below our target size.
            return stop_requested_.load() || ready_segments_.size() < READY_QUEUE_TARGET_SIZE;
        });
    }
    LOG_INFO("[SegmentManager Thread] Stopped.");
}

void SegmentManager::preallocateSegmentsIfNeeded() {
    // This loop is separate to allow locking the mutex once for the whole check-and-fill operation.
    std::lock_guard<std::mutex> lock(queue_mutex_);

    while (ready_segments_.size() < READY_QUEUE_TARGET_SIZE && !stop_requested_.load()) {
        uint32_t new_id = next_segment_id_to_create_.fetch_add(1);
        
        std::ostringstream oss;
        oss << wal_directory_ << "/" << file_prefix_ << "_" 
            << std::setfill('0') << std::setw(9) << new_id << ".log";
        std::string new_path = oss.str();
        
        LOG_INFO("[SegmentManager] Pre-allocating new WAL segment: {}", new_path);
        
        auto new_segment = std::make_unique<WALSegment>(new_path, new_id, 0 /* Placeholder LSN */);
        
        // The `open()` call performs the actual file creation I/O.
        if (!new_segment->open(true /* create_new */)) {
            LOG_ERROR("[SegmentManager] Failed to create and open pre-allocated segment file: {}", new_path);
            next_segment_id_to_create_.fetch_sub(1); // Roll back the ID on failure.
            
            // Throw a specific, detailed error. This will be caught by the main loop.
            throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Failed to preallocate WAL segment file.")
                .withFilePath(new_path)
                .withSuggestedAction("Check disk space, permissions, and for open file handle limits.");
        }
        
        ready_segments_.push_back(std::move(new_segment));
        
        // Notify any single thread that might be blocked in getReadySegment() waiting for this.
        ready_segment_cv_.notify_one();
    }
}

std::unique_ptr<WALSegment> SegmentManager::getReadySegment(LSN first_lsn_for_segment) {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    
    // Wait until a segment is available in the ready queue.
    ready_segment_cv_.wait(lock, [this] { return !ready_segments_.empty() || stop_requested_.load(); });
    
    if (stop_requested_.load()) {
        LOG_WARN("[SegmentManager] getReadySegment called while shutting down.");
        return nullptr;
    }

    // A segment is ready. Pop it from the queue.
    std::unique_ptr<WALSegment> segment = std::move(ready_segments_.front());
    ready_segments_.pop_front();
    
    // Wake up the manager thread in case this dropped the queue below the target size.
    ready_segment_cv_.notify_one();
    
    // Now that we know the first LSN, update the segment's in-memory header and write it to disk.
    segment->header_.first_lsn_in_segment = first_lsn_for_segment;
    if (!segment->writeHeader()) {
        LOG_ERROR("[SegmentManager] Failed to update header with first LSN for segment {}", segment->getFilePath());
        // This is a critical failure. The segment is unusable.
        return nullptr;
    }
    
    LOG_INFO("[SegmentManager] Provided ready segment {} for LSN {}. {} segments remain in ready queue.",
             segment->getFilePath(), first_lsn_for_segment, ready_segments_.size());
             
    return segment;
}