// src/wal/wal_shard.cpp

#include "../../include/wal/wal_shard.h"
#include "../../include/debug_utils.h"
#include "../../include/storage_error/storage_error.h"
#include "../../include/storage_error/error_codes.h"
#include <algorithm> // For std::any_of
#include <filesystem> // <<< FIX: Include the filesystem header

// --- START OF FIX ---
namespace fs = std::filesystem; // <<< FIX: Add the namespace alias
// --- END OF FIX ---

static std::optional<uint32_t> extractSegmentIdFromFilename(const std::string& filename, const std::string& prefix) {
    // ... (implementation from previous step)
    fs::path p(filename);
    std::string basename = p.stem().string();
    if (basename.rfind(prefix, 0) == 0 && basename.length() > prefix.length() && basename[prefix.length()] == '_') {
        std::string num_part = basename.substr(prefix.length() + 1);
        try {
            unsigned long long parsed_val = std::stoull(num_part);
            if (parsed_val > std::numeric_limits<uint32_t>::max()) return std::nullopt;
            return static_cast<uint32_t>(parsed_val);
        } catch (...) {}
    }
    return std::nullopt;
}

WALShard::WALShard(uint32_t shard_id,
                 const std::string& wal_directory,
                 const std::string& file_prefix,
                 uint64_t segment_size,
                 bool sync_on_commit,
                 std::atomic<LSN>& global_lsn_counter,
                 std::atomic<WALHealth>& parent_health_status) // <<< ADDED
    : shard_id_(shard_id),
      wal_directory_(wal_directory),
      file_prefix_(file_prefix),
      segment_size_(segment_size),
      sync_on_commit_record_(sync_on_commit),
      global_lsn_counter_(global_lsn_counter),
      parent_health_status_(parent_health_status), // <<< ADDED
      metrics_()
{
    LOG_INFO("[WALShard {}] Constructed.", shard_id_);
    group_commit_manager_ = std::make_unique<GroupCommitManager>(&metrics_);
}

WALShard::~WALShard() {
    stop_requested_.store(true);
    if (group_commit_manager_) {
        group_commit_manager_->shutdown();
    }
    if (writer_thread_.joinable()) {
        writer_thread_.join();
    }
    if (segment_manager_) {
        segment_manager_->stop();
    }
    LOG_INFO("[WALShard {}] Destructed.", shard_id_);
}

bool WALShard::initializeAndStart() {
    LOG_INFO("[WALShard {}] Initializing and starting...", shard_id_);
    
    // Perform a full recovery scan for this shard's files.
    scanForRecovery();
    
    // The scan populates current_segment_ etc. Now we can start the threads.
    stop_requested_.store(false);
    writer_thread_ = std::thread(&WALShard::writerThreadLoop, this);
    
    LOG_INFO("[WALShard {}] Initialized and started successfully.", shard_id_);
    return true;
}

LSN WALShard::scanForRecovery() {
    LOG_INFO("[WALShard {}] Starting recovery scan.", shard_id_);
    std::vector<std::pair<uint32_t, std::string>> found_segments;
    uint32_t max_segment_id = 0;

    try {
        if (fs::exists(wal_directory_)) {
            for (const auto& entry : fs::directory_iterator(wal_directory_)) {
                if (entry.is_regular_file()) {
                    const std::string filename = entry.path().filename().string();
                    if (filename.rfind(file_prefix_, 0) == 0) { // Only check this shard's files
                        auto seg_id_opt = extractSegmentIdFromFilename(filename, file_prefix_);
                        if (seg_id_opt) {
                            found_segments.push_back({*seg_id_opt, entry.path().string()});
                            max_segment_id = std::max(max_segment_id, *seg_id_opt);
                        }
                    }
                }
            }
        }
    } catch (const fs::filesystem_error& e) {
        LOG_ERROR("[WALShard {}] Filesystem error during recovery scan: {}", shard_id_, e.what());
        parent_health_status_.store(WALHealth::CRITICAL_ERROR);
        return 0;
    }

    // After discovering files, create and start the SegmentManager
    segment_manager_ = std::make_unique<SegmentManager>(wal_directory_, file_prefix_, max_segment_id + 1);
    segment_manager_->start();

    if (found_segments.empty()) {
        LOG_INFO("[WALShard {}] No segments found, starting fresh.", shard_id_);
        return 0;
    }

    std::sort(found_segments.begin(), found_segments.end());
    LSN max_lsn_in_shard = 0;

    for (size_t i = 0; i < found_segments.size(); ++i) {
        const auto& [seg_id, file_path] = found_segments[i];
        auto segment = std::make_unique<WALSegment>(file_path, seg_id, max_lsn_in_shard + 1);
        if (segment->open(false) && segment->isHealthy()) {
            max_lsn_in_shard = std::max(max_lsn_in_shard, segment->getLastLSN());
            if (i == found_segments.size() - 1) { // Last segment is the current one
                current_segment_ = std::move(segment);
            } else {
                archived_segments_.push_back(std::move(segment));
            }
        } else {
            LOG_ERROR("[WALShard {}] Failed to open/verify segment {}. Halting shard recovery.", shard_id_, file_path);
            parent_health_status_.store(WALHealth::CRITICAL_ERROR);
            break; // Stop processing on corruption
        }
    }
    return max_lsn_in_shard;
}

std::future<LSN> WALShard::submitRecord(LogRecord& record, WALPriority priority) {
    return group_commit_manager_->submit(record, priority);
}

void WALShard::writerThreadLoop() {
    LOG_INFO("[WALShard {} WriterThread] Started.", shard_id_);
    while (!stop_requested_.load()) {
        auto batch = group_commit_manager_->getNextBatchToFlush();
        if (!batch || batch->requests.empty()) {
            if (stop_requested_.load()) break;
            continue;
        }

        bool needs_sync = sync_on_commit_record_ &&
                          std::any_of(batch->requests.begin(), batch->requests.end(), 
                                      [](const auto& req){ return req.record.type == LogRecordType::COMMIT_TXN; });

        if (writeBatchToDisk(*batch, needs_sync)) {
            for (auto& req : batch->requests) {
                req.promise.set_value(req.record.lsn);
            }
        } else {
            // --- FIX: Use storage::StorageError ---
            auto ex = std::make_exception_ptr(
                storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "WALShard failed to write batch to disk.")
                    .withContext("shard_id", std::to_string(shard_id_))
            );
            // --- END OF FIX ---
            for (auto& req : batch->requests) {
                req.promise.set_exception(ex);
            }
        }
    }
    LOG_INFO("[WALShard {} WriterThread] Stopped.", shard_id_);
}

bool WALShard::writeBatchToDisk(CommitBatch& batch, bool force_sync) {
    std::lock_guard<std::mutex> seg_lock(segments_mutex_);
    
    size_t batch_size = batch.requests.size();
    LSN base_lsn = global_lsn_counter_.fetch_add(batch_size, std::memory_order_relaxed);
    for (size_t i = 0; i < batch_size; ++i) {
        batch.requests[i].record.lsn = base_lsn + i;
    }

    bool rotation_occurred = false;
    for (const auto& req : batch.requests) {
        if (!current_segment_ || !current_segment_->isHealthy() ||
            (current_segment_->getCurrentSize() + req.record.getDiskSize() > segment_size_)) {
            
            if (current_segment_) {
                current_segment_->sync();
                current_segment_->close();
                archived_segments_.push_back(std::move(current_segment_));
            }
            
            current_segment_ = segment_manager_->getReadySegment(req.record.lsn);
            if (!current_segment_) {
                metrics_.recordIOError(); // Record failure to get segment
                return false;
            }
            rotation_occurred = true; // Flag that a rotation happened
        }

        if (!current_segment_->appendRecord(req.record)) {
            LOG_ERROR("[WALShard {}] Failed to append LSN {} to segment.", shard_id_, req.record.lsn);
            parent_health_status_.store(WALHealth::CRITICAL_ERROR); // <<< PROPAGATE HEALTH
            return false;
        }
    }

    if (rotation_occurred) {
        metrics_.recordSegmentRotation(); // Record the rotation event once per batch if it happened
    }

    if (force_sync && current_segment_) {
        if (!current_segment_->sync()) {
            metrics_.recordIOError(); // Record sync failure
            return false;
        }
    }
    return true;
}

WALMetricsSnapshot WALShard::getMetricsSnapshot() const {
    // const_cast is safe here because the public getSnapshot() is const
    // and the underlying metrics use atomics, which are thread-safe for reads.
    return const_cast<WALMetrics&>(metrics_).getSnapshot();
}

std::vector<LogRecord> WALShard::recoverRecords() const {
    std::vector<LogRecord> all_records;
    std::lock_guard<std::mutex> seg_lock(segments_mutex_);
    for(const auto& segment : archived_segments_) {
        auto records = segment->readAllRecords();
        all_records.insert(all_records.end(), std::make_move_iterator(records.begin()), std::make_move_iterator(records.end()));
    }
    if (current_segment_) {
        auto records = current_segment_->readAllRecords();
        all_records.insert(all_records.end(), std::make_move_iterator(records.begin()), std::make_move_iterator(records.end()));
    }
    return all_records;
}

void WALShard::truncateSegmentsBefore(LSN checkpoint_lsn) {
    std::lock_guard<std::mutex> seg_lock(segments_mutex_);
    std::deque<std::unique_ptr<WALSegment>> keepers;
    for (auto& segment : archived_segments_) {
        if (segment->getLastLSN() >= checkpoint_lsn) {
            keepers.push_back(std::move(segment));
        } else {
            LOG_INFO("[WALShard {}] Truncating segment {} (last LSN {} < checkpoint LSN {})", 
                     shard_id_, segment->getFilePath(), segment->getLastLSN(), checkpoint_lsn);
            segment->close();
            try {
                // --- FIX: Use fs::remove and catch fs::filesystem_error ---
                if (fs::exists(segment->getFilePath())) {
                    fs::remove(segment->getFilePath());
                }
            } catch(const fs::filesystem_error& e) {
                LOG_ERROR("[WALShard {}] Filesystem error removing truncated segment file {}: {}", 
                          shard_id_, segment->getFilePath(), e.what());
                // Don't stop truncation for other files, just log the error.
            }
            // --- END OF FIX ---
        }
    }
    archived_segments_ = std::move(keepers);
}