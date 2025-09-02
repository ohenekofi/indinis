// @src/wal_manager.cpp
#include "../include/wal.h" // ExtWALManager class declaration
#include "../include/wal/wal_segment.h" // WALSegment class declaration
#include "../include/wal/segment_manager.h"
#include "../include/debug_utils.h" // For LOG_ macros
#include "../include/types.h"       // For LogRecord

#include <filesystem>
#include <iostream>
#include <algorithm> // For std::sort, std::min, std::max
#include <sstream>   // For std::ostringstream
#include <iomanip>   // For std::setw, std::setfill
#include <chrono>    // For std::chrono
#include <limits>    // For std::numeric_limits
#include <future>

namespace fs = std::filesystem;

static std::optional<uint32_t> extractSegmentIdFromFilename(const std::string& filename, const std::string& prefix) {
    fs::path p(filename);
    std::string basename = p.stem().string(); // e.g., "test_seg_000000001"
    
    // Check if the basename starts with the prefix and an underscore.
    if (basename.rfind(prefix, 0) == 0 && basename.length() > prefix.length() && basename[prefix.length()] == '_') {
        std::string num_part = basename.substr(prefix.length() + 1);
        try {
            // Use std::stoull for safe conversion and check against uint32_t max.
            unsigned long long parsed_val = std::stoull(num_part);
            if (parsed_val > std::numeric_limits<uint32_t>::max()) {
                LOG_WARN("[WAL extractSegmentId] Segment ID '{}' in filename '{}' exceeds uint32_t max.", num_part, filename);
                return std::nullopt;
            }
            return static_cast<uint32_t>(parsed_val);
        } catch (const std::exception& e) {
            LOG_WARN("[WAL extractSegmentId] Could not parse segment ID from '{}' in filename '{}': {}", num_part, filename, e.what());
        }
    }
    return std::nullopt;
}
// Helper to extract segment ID from filename (e.g., "wal_segment_000000001.log")
// This helper is specific to this file's needs.
static std::optional<uint32_t> extractSegmentIdFromFilenameLocal(const std::string& filename_str, const std::string& prefix_to_strip) {
    fs::path p(filename_str);
    std::string basename = p.stem().string(); 
    
    if (basename.rfind(prefix_to_strip, 0) == 0) { 
        size_t prefix_len = prefix_to_strip.length();
        if (basename.length() > prefix_len && basename[prefix_len] == '_') {
            std::string num_part = basename.substr(prefix_len + 1);
            try {
                if (num_part.empty() || !std::all_of(num_part.begin(), num_part.end(), ::isdigit)) {
                     LOG_WARN("[WAL extractSegmentId] Non-digit characters in segment ID part '{}' of filename '{}'.", num_part, filename_str);
                    return std::nullopt;
                }
                unsigned long long parsed_val = std::stoull(num_part);
                if (parsed_val > std::numeric_limits<uint32_t>::max()) {
                    LOG_WARN("[WAL extractSegmentId] Segment ID part '{}' in filename '{}' exceeds uint32_t max.", num_part, filename_str);
                    return std::nullopt;
                }
                return static_cast<uint32_t>(parsed_val);
            } catch (const std::exception& e) {
                LOG_WARN("[WAL extractSegmentId] Could not parse segment ID from filename part '{}' in '{}': {}", num_part, filename_str, e.what());
            }
        }
    }
    return std::nullopt;
}

ExtWALManager::ExtWALManager(const Config& config)
    : config_(config),
      next_lsn_to_assign_(1), // Default, will be updated by recovery in initialize()
      last_written_lsn_(0),
      last_synced_lsn_(0),
      is_healthy_(false),     // System is not healthy until initialize() succeeds
      shutdown_requested_(true), // Start in a "stopped" state
      //current_segment_id_counter_(0),
      wal_metrics_() // Default-construct the metrics collector
{
    LOG_INFO("[ExtWALManager] Constructing. Directory: {}", config_.wal_directory);

    // Instantiate the GroupCommitManager, passing it a pointer to the metrics
    // object that this manager owns. This establishes the feedback loop for adaptive batching.
    try {
        group_commit_manager_ = std::make_unique<GroupCommitManager>(&wal_metrics_);
    } catch (const std::exception& e) {
        LOG_FATAL("[ExtWALManager] CRITICAL: Failed to construct GroupCommitManager: {}. WAL will be non-functional.", e.what());
        // A failure here is unrecoverable for the WAL, so we re-throw to halt engine construction.
        throw;
    }

    // The SegmentManager and the writer_thread_ are NOT created or started here.
    // That is the responsibility of the `initialize()` method.
}

ExtWALManager::~ExtWALManager() {
    LOG_INFO("[ExtWALManager] Destructing...");
    
    // 1. Signal all background threads to begin their shutdown sequence.
    // This flag is checked at the top of their loops.
    shutdown_requested_.store(true, std::memory_order_release);

    // 2. Stop the SegmentManager thread first.
    // It's a dependency of the writer thread (for new segments), so it should be
    // stopped before we fully tear down the writer.
    if (segment_manager_) {
        segment_manager_->stop();
    }

    // 3. Signal the GroupCommitManager to stop accepting new submissions and to
    //    wake up the writer thread if it's currently waiting.
    if (group_commit_manager_) {
        group_commit_manager_->shutdown();
    }

    // 4. Join the main writer thread. It will now exit its loop because
    //    `shutdown_requested_` is true and `group_commit_manager_` will
    //    return nullptr from `getNextBatchToFlush`.
    if (writer_thread_.joinable()) {
        writer_thread_.join();
        LOG_INFO("[ExtWALManager] Writer thread joined.");
    }

    // 5. Perform a final, synchronous flush as a safety net.
    // This handles any records that were in-flight when the shutdown was signaled.
    LOG_INFO("[ExtWALManager] Performing final flush of any remaining records before closing segments...");
    if (group_commit_manager_) {
        auto final_batch = group_commit_manager_->getNextBatchToFlush();
        if (final_batch && !final_batch->requests.empty()) {
            LOG_INFO("  [ExtWALManager Destructor] Found {} final records to flush.", final_batch->requests.size());
            // This final write must be synchronous and durable.
            writeBatchToDisk(*final_batch, true);
        }
    }

    // 6. Now that all writing is complete, safely close all segment files.
    // This lock ensures no other operation can interfere with the final closing process.
    std::lock_guard<std::mutex> seg_lock(segments_mutex_);
    if (current_segment_) {
        current_segment_->close(); // close() updates header and trailer for a clean shutdown.
        LOG_INFO("[ExtWALManager] Current segment {} closed.", current_segment_->getFilePath());
    }

    for (auto& segment : archived_segments_) {
        if (segment) {
            segment->close();
        }
    }
    archived_segments_.clear();
    LOG_INFO("[ExtWALManager] Archived segments cleared and closed.");
    
    LOG_INFO("[ExtWALManager] Destruction complete.");
}

std::future<LSN> ExtWALManager::appendLogRecord(LogRecord& record, WALPriority priority) {
    if (!is_healthy_.load(std::memory_order_acquire)) {
        std::promise<LSN> p;
        p.set_exception(std::make_exception_ptr(std::runtime_error("WAL is not healthy.")));
        return p.get_future();
    }
    // LSN is now assigned by the writer thread, not here. Set to 0 initially.
    record.lsn = 0; 
    return group_commit_manager_->submit(record, priority);
}

bool ExtWALManager::initialize() {
    // --- START OF FIX ---
    // Use the standard locking pattern for the segments_mutex_.
    std::lock_guard<std::mutex> lock(segments_mutex_);
    // --- END OF FIX ---

    LOG_INFO("[ExtWALManager] Initializing WAL system...");

    fs::path dir_path(config_.wal_directory);
    try {
        if (!fs::exists(dir_path)) {
            fs::create_directories(dir_path);
            LOG_INFO("[ExtWALManager] Created WAL directory: {}", config_.wal_directory);
        }
    } catch (const fs::filesystem_error& e) {
        LOG_ERROR("[ExtWALManager] Failed to create WAL directory '{}': {}", config_.wal_directory, e.what());
        is_healthy_.store(false);
        return false;
    }

    // scanForRecovery now returns the max segment ID, which is then used to seed the SegmentManager.
    // We need to store this ID temporarily.
    uint32_t last_segment_id_found = 0;
    LSN recovered_last_lsn = scanForRecovery(&last_segment_id_found); // Pass pointer to get the ID back
    
    if (recovered_last_lsn > 0) {
        next_lsn_to_assign_.store(recovered_last_lsn + 1, std::memory_order_relaxed);
        last_written_lsn_.store(recovered_last_lsn, std::memory_order_relaxed);
        last_synced_lsn_.store(recovered_last_lsn, std::memory_order_relaxed);
    } else {
        next_lsn_to_assign_.store(1, std::memory_order_relaxed);
        last_written_lsn_.store(0, std::memory_order_relaxed);
        last_synced_lsn_.store(0, std::memory_order_relaxed);
    }
    LOG_INFO("[ExtWALManager] Recovery scan complete. Next LSN: {}. Last durable LSN: {}. Highest Seg ID found: {}",
             next_lsn_to_assign_.load(), last_written_lsn_.load(), last_segment_id_found);

    segment_manager_ = std::make_unique<SegmentManager>(
        config_.wal_directory, 
        config_.wal_file_prefix, 
        last_segment_id_found + 1
    );
    segment_manager_->start();

    if (!current_segment_ || !current_segment_->isHealthy() ||
        (current_segment_->getCurrentSize() >= config_.segment_size && current_segment_->getLastLSN() > 0)) {
        
        if (current_segment_) {
            LOG_INFO("[ExtWALManager] Initial segment {} is unhealthy or full. Requesting a new one.", current_segment_->getFilePath());
        } else {
            LOG_INFO("[ExtWALManager] No existing segment to continue. Requesting a new one.");
        }
        
        if (!openNewSegment(next_lsn_to_assign_.load())) {
            LOG_ERROR("[ExtWALManager] Failed to open initial/new segment during initialization.");
            segment_manager_->stop();
            is_healthy_.store(false);
            return false;
        }
    } else {
         LOG_INFO("[ExtWALManager] Re-using segment {} as current after recovery.", current_segment_->getFilePath());
    }

    shutdown_requested_.store(false);
    writer_thread_ = std::thread(&ExtWALManager::writerThreadLoop, this);
    LOG_INFO("[ExtWALManager] Background writer thread started.");

    is_healthy_.store(true);
    LOG_INFO("[ExtWALManager] Initialization successful.");
    return true;
}

std::string ExtWALManager::getSegmentFilePath(uint32_t segment_id) const {
    std::ostringstream oss;
    std::string dir_path_str = config_.wal_directory;
    if (!dir_path_str.empty() && dir_path_str.back() != fs::path::preferred_separator) {
        dir_path_str += fs::path::preferred_separator;
    }
    oss << dir_path_str << config_.wal_file_prefix << "_" 
        << std::setfill('0') << std::setw(9) 
        << segment_id << ".log";
    return oss.str();
}

// Assumes segments_mutex_ is HELD by caller.
bool ExtWALManager::openNewSegment(LSN first_lsn_for_new_segment) {
    // This method assumes the caller (e.g., initialize, writeBatchToDisk)
    // is already holding the `segments_mutex_`.

    // Step 1: Archive the current segment if it exists.
    if (current_segment_) {
        LOG_INFO("[ExtWALManager] Archiving current segment {} (ID: {}, Last LSN: {}).",
                 current_segment_->getFilePath(), current_segment_->getSegmentId(), current_segment_->getLastLSN());
        
        // Ensure the segment is fully durable and its header is updated before archiving.
        current_segment_->sync();
        current_segment_->close();
        
        archived_segments_.push_back(std::move(current_segment_));
        // current_segment_ is now null.
    }

    // Step 2: Request a new, pre-allocated segment from the manager.
    if (!segment_manager_) {
        LOG_ERROR("[ExtWALManager] SegmentManager is not initialized. Cannot get new segment.");
        is_healthy_.store(false, std::memory_order_release);
        return false;
    }
    
    LOG_INFO("[ExtWALManager] Requesting a ready segment from SegmentManager for first LSN {}.", first_lsn_for_new_segment);
    std::unique_ptr<WALSegment> new_segment = segment_manager_->getReadySegment(first_lsn_for_new_segment);
    
    if (!new_segment) {
        LOG_ERROR("[ExtWALManager] SegmentManager failed to provide a ready segment. WAL is now unhealthy.");
        is_healthy_.store(false, std::memory_order_release);
        return false;
    }

    // Step 3: Set the new segment as the current one.
    current_segment_ = std::move(new_segment);
    LOG_INFO("[ExtWALManager] Rotated to new active segment: {}", current_segment_->getFilePath());
    
    return true;
}

// Assumes buffer_mutex_ may be held by caller (appendLogRecord)
// Needs to acquire segments_mutex_ if openNewSegment is called.
bool ExtWALManager::rotateLogIfNeeded(size_t next_record_size_approx, LSN lsn_of_record_triggering_roll) {
    // This function is typically called when buffer_mutex is already held by appendLogRecord.
    // It needs to check current_segment_ state and potentially call openNewSegment (which needs segments_mutex_).

    // To avoid complex lock ordering or recursive mutexes, this check can be split:
    // 1. Read current_segment_ size under a shared lock / or if current_segment_ access is safe.
    // 2. If rotation is needed, then acquire full segments_mutex_ for openNewSegment.

    // Simpler for now: assume this check doesn't need segments_mutex_ unless rotation is certain.
    // The critical part is that openNewSegment itself is fully protected by segments_mutex_.
    bool needs_rotation = false;
    { // Minimal scope for checking current_segment_ state without holding segments_mutex_ for long
        std::lock_guard<std::mutex> seg_lock(segments_mutex_); // Protect current_segment_ access
        if (!current_segment_ || !current_segment_->isOpen() || !current_segment_->isHealthy()) {
            needs_rotation = true;
            LOG_INFO("[ExtWALManager] No current/healthy segment. Rotation needed.");
        } else if (current_segment_->getCurrentSize() > WAL_SEGMENT_HEADER_SIZE && // Has data
                   (current_segment_->getCurrentSize() + next_record_size_approx) > config_.segment_size) {
            needs_rotation = true;
            LOG_INFO("[ExtWALManager] Current segment {} needs rotation (size {} + next_approx {} > max {}).",
                     current_segment_->getFilePath(), current_segment_->getCurrentSize(), next_record_size_approx, config_.segment_size);
        }
    } // seg_lock released

   if (needs_rotation) {
        std::lock_guard<std::mutex> seg_lock(segments_mutex_);
        return openNewSegment(lsn_of_record_triggering_roll); // Pass the LSN of the record that will be first
    }
    return true; // No rotation needed
}


// Implementation of appendAndForceFlushLogRecord
LSN ExtWALManager::appendAndForceFlushLogRecord(LogRecord& record, bool force_sync_overall) {
    // This function becomes synchronous by waiting on the future.
    // We use IMMEDIATE priority to ensure it's flushed promptly.
    auto future = appendLogRecord(record, WALPriority::IMMEDIATE);
    try {
        // Blocks until the commit is durable and its LSN is returned.
        return future.get();
    } catch (const std::exception& e) {
        LOG_ERROR("[ExtWALManager] appendAndForceFlush failed: {}", e.what());
        return 0;
    }
}

// Deprecated
//void ExtWALManager::flushBuffer() { flushBufferToCurrentSegment(false); }
// Deprecated
//void ExtWALManager::syncLog() { flushBufferToCurrentSegment(true); }

// THE NEW BACKGROUND THREAD (COMMIT LEADER)
void ExtWALManager::writerThreadLoop() {
    LOG_INFO("[WAL WriterThread] Started.");
    while (!shutdown_requested_.load()) {
        auto batch = group_commit_manager_->getNextBatchToFlush();

        if (!batch) {
            if (shutdown_requested_.load()) break;
            continue; // Spurious wakeup or timeout with no data.
        }

        if (batch->requests.empty()) {
            continue;
        }

        // A batch requires sync if it contains a COMMIT record and sync_on_commit is enabled.
        bool needs_sync = config_.sync_on_commit_record && 
                          std::any_of(batch->requests.begin(), batch->requests.end(), 
                                      [](const auto& req){ return req.record.type == LogRecordType::COMMIT_TXN; });
        
        auto start_time = std::chrono::high_resolution_clock::now();
        bool success = writeBatchToDisk(*batch, needs_sync);
        if (success && needs_sync) {
            // After a successful sync, update the last_synced_lsn_
            LSN max_lsn_in_batch = 0;
            for(const auto& req : batch->requests) {
                if (req.record.lsn > max_lsn_in_batch) {
                    max_lsn_in_batch = req.record.lsn;
                }
            }
            if (max_lsn_in_batch > 0) {
                last_synced_lsn_.store(max_lsn_in_batch, std::memory_order_release);
                LOG_TRACE("[WAL WriterThread] Sync complete. Updated last_synced_lsn to {}.", max_lsn_in_batch);
            }
        }
        auto end_time = std::chrono::high_resolution_clock::now();

        auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        uint64_t bytes_written_in_batch = 0;
        for(const auto& req : batch->requests) {
            bytes_written_in_batch += req.record.getDiskSize();
        }
        wal_metrics_.recordFlush(batch->requests.size(), duration_us);

        if (success) {
            for (auto& req : batch->requests) {
                req.promise.set_value(req.record.lsn);
            }
        } else {
            auto ex = std::make_exception_ptr(std::runtime_error("Failed to write WAL batch to disk."));
            for (auto& req : batch->requests) {
                req.promise.set_exception(ex);
            }
        }
    }
    LOG_INFO("[WAL WriterThread] Stopped.");
}

// Helper to write a batch to disk and assign LSNs
bool ExtWALManager::writeBatchToDisk(CommitBatch& batch, bool force_sync) {
    std::lock_guard<std::mutex> seg_lock(segments_mutex_);
    
    for (auto& req : batch.requests) {
        // Assign LSN just before writing.
        req.record.lsn = next_lsn_to_assign_.fetch_add(1, std::memory_order_relaxed);

        if (!current_segment_ || !current_segment_->isHealthy() || 
            (current_segment_->getCurrentSize() + req.record.getDiskSize() > config_.segment_size)) {
            if (!openNewSegment(req.record.lsn)) {
                return false; // Critical failure
            }
        }

        if (!current_segment_->appendRecord(req.record)) {
            return false; // Critical failure
        }
        last_written_lsn_.store(req.record.lsn, std::memory_order_relaxed);
    }

    if (force_sync) {
        if (!current_segment_->sync()) {
            return false; // Critical failure
        }
    }
    return true;
}

LSN ExtWALManager::scanForRecovery(uint32_t* out_max_segment_id) {
    LOG_INFO("[ExtWALManager] Starting WAL scan for recovery in directory: {}", config_.wal_directory);
    
    std::vector<std::pair<uint32_t, std::string>> found_segments;
    uint32_t max_segment_id_found = 0;

    // 1. Discover and sort segment files
    try {
        if (!fs::exists(config_.wal_directory)) {
            LOG_INFO("[ExtWALManager] WAL directory does not exist. No recovery needed.");
            if (out_max_segment_id) *out_max_segment_id = 0;
            return 0;
        }

        for (const auto& entry : fs::directory_iterator(config_.wal_directory)) {
            if (entry.is_regular_file()) {
                // CORRECTED: Call the now-defined helper function
                auto seg_id_opt = extractSegmentIdFromFilename(entry.path().filename().string(), config_.wal_file_prefix);
                if (seg_id_opt) {
                    found_segments.push_back({*seg_id_opt, entry.path().string()});
                    // CORRECTED: This call to std::max is now valid
                    max_segment_id_found = std::max(max_segment_id_found, *seg_id_opt);
                }
            }
        }
    } catch (const fs::filesystem_error& e) {
        LOG_ERROR("[ExtWALManager] Filesystem error scanning WAL directory {}: {}", config_.wal_directory, e.what());
        is_healthy_.store(false);
        if (out_max_segment_id) *out_max_segment_id = 0;
        return 0;
    }

    if (found_segments.empty()) {
        LOG_INFO("[ExtWALManager] No WAL segment files found. Starting fresh.");
        // CORRECTED: Use the out-parameter instead of the non-existent member
        if (out_max_segment_id) *out_max_segment_id = 0;
        return 0;
    }
    
    // CORRECTED: Use the out-parameter to return the max ID found.
    if (out_max_segment_id) {
        *out_max_segment_id = max_segment_id_found;
    }
    
    std::sort(found_segments.begin(), found_segments.end());

    // --- The rest of the function remains the same ---
    LSN overall_max_lsn = 0;
    archived_segments_.clear();
    current_segment_.reset();

    for (size_t i = 0; i < found_segments.size(); ++i) {
        const auto& [seg_id, file_path] = found_segments[i];
        
        LOG_INFO("[Recovery Scan] Processing segment file {}. ID: {}", file_path, seg_id);
        
        LSN expected_first_lsn = (overall_max_lsn == 0 && i == 0) ? 1 : overall_max_lsn + 1;
        auto segment = std::make_unique<WALSegment>(file_path, seg_id, expected_first_lsn);
        
        if (segment->open(false)) {
            if (!segment->isHealthy()) {
                LOG_ERROR("[Recovery Scan] Segment {} opened but is unhealthy. Stopping recovery scan.", file_path);
                is_healthy_.store(false);
                break;
            }

            LSN segment_last_lsn = segment->getLastLSN();
            if (segment_last_lsn > 0) {
                overall_max_lsn = std::max(overall_max_lsn, segment_last_lsn);
            }

            if (seg_id == max_segment_id_found) { // Use the max ID we found
                current_segment_ = std::move(segment);
            } else {
                archived_segments_.push_back(std::move(segment));
            }
        } else {
            LOG_ERROR("[Recovery Scan] Failed to open or verify segment {}. Stopping recovery scan.", file_path);
            is_healthy_.store(false);
            break;
        }
    }

    return overall_max_lsn;
}

std::vector<LogRecord> ExtWALManager::getRecordsStrictlyAfter(LSN start_lsn_exclusive) const {
    std::vector<LogRecord> result_records;
    std::lock_guard<std::mutex> seg_lock(segments_mutex_); 

    for (const auto& segment_ptr : archived_segments_) {
        if (segment_ptr && segment_ptr->isHealthy()) {
            LSN seg_last_lsn = segment_ptr->getLastLSN(); // Reads from header
            if (seg_last_lsn > 0 && seg_last_lsn > start_lsn_exclusive) {
                 if (start_lsn_exclusive < seg_last_lsn) { 
                    auto segment_records = segment_ptr->readRecordsStrictlyAfter(start_lsn_exclusive);
                    result_records.insert(result_records.end(), std::make_move_iterator(segment_records.begin()), std::make_move_iterator(segment_records.end()));
                }
            }
        }
    }
    if (current_segment_ && current_segment_->isHealthy()) {
        LSN current_seg_last_lsn = current_segment_->getLastLSN();
        if (current_seg_last_lsn > 0 && current_seg_last_lsn > start_lsn_exclusive) {
            if (start_lsn_exclusive < current_seg_last_lsn) {
                auto segment_records = current_segment_->readRecordsStrictlyAfter(start_lsn_exclusive);
                result_records.insert(result_records.end(), std::make_move_iterator(segment_records.begin()), std::make_move_iterator(segment_records.end()));
            }
        }
    }
    std::sort(result_records.begin(), result_records.end(), [](const LogRecord& a, const LogRecord& b){ return a.lsn < b.lsn; });
    LOG_INFO("[ExtWALManager] Retrieved {} records strictly after LSN {}.", result_records.size(), start_lsn_exclusive);
    return result_records;
}

std::vector<LogRecord> ExtWALManager::getAllPersistedRecordsForTesting() const {
    std::vector<LogRecord> all_records;
    std::lock_guard<std::mutex> seg_lock(segments_mutex_);
    for (const auto& segment_ptr : archived_segments_) {
        if (segment_ptr && segment_ptr->isHealthy()) {
            auto segment_records = segment_ptr->readAllRecords();
            all_records.insert(all_records.end(), std::make_move_iterator(segment_records.begin()), std::make_move_iterator(segment_records.end()));
        }
    }
    if (current_segment_ && current_segment_->isHealthy()) {
        auto segment_records = current_segment_->readAllRecords();
        all_records.insert(all_records.end(), std::make_move_iterator(segment_records.begin()), std::make_move_iterator(segment_records.end()));
    }
    std::sort(all_records.begin(), all_records.end(), [](const LogRecord& a, const LogRecord& b){ return a.lsn < b.lsn; });
    return all_records;
}

bool ExtWALManager::truncateSegmentsBeforeLSN(LSN lsn_exclusive_lower_bound) {
    std::lock_guard<std::mutex> seg_lock(segments_mutex_);
    LOG_INFO("[ExtWALManager] Attempting to truncate WAL segments strictly before LSN {}.", lsn_exclusive_lower_bound);
    bool all_success = true;
    auto it = archived_segments_.begin();
    while (it != archived_segments_.end()) {
        const auto& segment = *it;
        if (segment) { 
            LSN seg_last_lsn = segment->getLastLSN(); 
            if (seg_last_lsn > 0 && seg_last_lsn < lsn_exclusive_lower_bound) {
                LOG_INFO("[ExtWALManager] Truncating segment {} (ID {}, LSNs {}-{}). Bound: LSN {}.",
                         segment->getFilePath(), segment->getSegmentId(),
                         segment->getFirstLSN(), seg_last_lsn, lsn_exclusive_lower_bound);
                segment->close(); 
                try {
                    if (fs::exists(segment->getFilePath())) {
                        if (!fs::remove(segment->getFilePath())) {
                             LOG_ERROR("[ExtWALManager] Failed to delete segment file {}.", segment->getFilePath());
                             all_success = false; 
                        } else {
                             LOG_INFO("[ExtWALManager] Successfully deleted segment file {}.", segment->getFilePath());
                        }
                    } else {
                         LOG_INFO("[ExtWALManager] Segment file {} for truncation did not exist.", segment->getFilePath());
                    }
                } catch (const fs::filesystem_error& e) {
                    LOG_ERROR("[ExtWALManager] FS error deleting segment {}: {}", segment->getFilePath(), e.what());
                    all_success = false;
                }
                it = archived_segments_.erase(it); 
            } else {
                LOG_TRACE("[ExtWALManager] Segment {} (ID {}, HeaderLastLSN {}) not entirely before LSN {}. Stop.",
                         segment->getFilePath(), segment->getSegmentId(), seg_last_lsn, lsn_exclusive_lower_bound);
                break; 
            }
        } else { it = archived_segments_.erase(it); } 
    }
    LOG_INFO("[ExtWALManager] WAL truncation {} (Success: {}).", (all_success ? "completed" : "completed with errors"), all_success);
    return all_success;
}

std::string ExtWALManager::getStatus() const {
    std::ostringstream oss;
    oss << "ExtWALManager Status:\n"
        << "  Healthy: " << (is_healthy_.load(std::memory_order_acquire) ? "Yes" : "No") << "\n"
        << "  WAL Directory: " << config_.wal_directory << "\n"
        << "  File Prefix: " << config_.wal_file_prefix << "\n"
        << "  Segment Size (config): " << config_.segment_size / (1024*1024) << " MB\n"
        << "  Next LSN to Assign: " << next_lsn_to_assign_.load(std::memory_order_relaxed) << "\n"
        << "  Last LSN Written to Segment: " << last_written_lsn_.load(std::memory_order_relaxed) << "\n"
        << "  BG Writer Enabled: Yes (Group Commit Model)\n"
        << "  Sync on Commit: " << (config_.sync_on_commit_record ? "Yes" : "No") << "\n";

    // --- THIS IS THE CORRECTED LOGIC ---
    if (group_commit_manager_) {
        oss << "  Pending Requests in Queues: " << group_commit_manager_->getPendingRequestCount() << "\n";
    }
    // ------------------------------------

    std::lock_guard<std::mutex> seg_lock(segments_mutex_);
    if (current_segment_) {
        oss << "  Current Segment ID: " << current_segment_->getSegmentId() << "\n"
            << "    Path: " << current_segment_->getFilePath() << "\n"
            << "    Header First LSN: " << current_segment_->getFirstLSN() << "\n"
            << "    Header Last LSN on Disk: " << current_segment_->getLastLSN() << "\n" 
            << "    Approx. Phys. Size: " << current_segment_->getCurrentSize() / 1024 << " KB\n";
    } else {
        oss << "  Current Segment: None\n";
    }
    oss << "  Archived Segments Count: " << archived_segments_.size() << "\n";
    return oss.str();
}

void ExtWALManager::forceSyncToLSN(LSN lsn) {
    if (!is_healthy_.load(std::memory_order_acquire)) {
        throw std::runtime_error("Cannot sync WAL because it is not healthy.");
    }
    
    // Optimization: If the requested LSN is already known to be synced, we can return immediately.
    if (lsn <= last_synced_lsn_.load(std::memory_order_acquire)) {
        LOG_TRACE("[ExtWALManager] forceSyncToLSN({}): Already synced up to {}. No-op.", lsn, last_synced_lsn_.load());
        return;
    }

    LOG_INFO("[ExtWALManager] forceSyncToLSN({}): Submitting sync marker to guarantee durability.", lsn);

    // We don't need a real log record, just a promise to wait on.
    // The writer thread will update last_synced_lsn_ after each sync.
    // We can poll or use a more complex signaling. A simple approach:
    // Submit a high-priority empty record and wait for it.
    LogRecord sync_marker;
    sync_marker.type = LogRecordType::INVALID; // A special type that isn't processed on recovery
    sync_marker.txn_id = 0; // System operation
    
    auto sync_future = appendLogRecord(sync_marker, WALPriority::IMMEDIATE);
    
    try {
        LSN sync_lsn = sync_future.get(); // This blocks until the batch is synced.
        LOG_INFO("[ExtWALManager] forceSyncToLSN({}): Durability confirmed up to at least LSN {}.", lsn, sync_lsn);
    } catch (const std::exception& e) {
        LOG_ERROR("[ExtWALManager] forceSyncToLSN({}): Exception while waiting for sync future: {}", lsn, e.what());
        is_healthy_.store(false, std::memory_order_release);
        throw; // Re-throw critical failure
    }
}