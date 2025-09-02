// include/wal/wal_shard.h
#pragma once

#include "group_commit.h"
#include "segment_manager.h"
#include "wal_metrics.h"
#include "../types.h"
#include <atomic>
#include <memory>
#include <string>

/**
 * @class WALShard
 * @brief Represents a single, independent, single-threaded WAL stream.
 *
 * In a sharded WAL architecture, multiple instances of WALShard run in parallel,
 * each handling a subset of the total transaction load. This allows the WAL system
 * to scale beyond the limits of a single CPU core and disk.
 */
class WALShard {
public:
    WALShard(uint32_t shard_id,
             const std::string& wal_directory,
             const std::string& file_prefix,
             uint64_t segment_size,
             bool sync_on_commit,
             std::atomic<LSN>& global_lsn_counter,
             std::atomic<WALHealth>& parent_health_status);

    ~WALShard();

    // Disallow copy/move
    WALShard(const WALShard&) = delete;
    WALShard& operator=(const WALShard&) = delete;

    /**
     * @brief Starts the shard's background threads (writer and segment manager).
     * @return true on success, false on failure.
     */
    bool initializeAndStart();

    /**
     * @brief Submits a log record to this specific shard for processing.
     * @param record The log record to write.
     * @param priority The priority of the write.
     * @return A future that will be fulfilled with the globally unique LSN.
     */
    std::future<LSN> submitRecord(LogRecord& record, WALPriority priority);

    /**
     * @brief Reads all records from this shard's persisted segments for recovery.
     * @return A vector of all log records found in this shard's files.
     */
    std::vector<LogRecord> recoverRecords() const;
    
    /**
     * @brief Truncates segments in this shard that are older than the given LSN.
     */
    void truncateSegmentsBefore(LSN checkpoint_lsn);
    WALMetricsSnapshot getMetricsSnapshot() const;
    void setHealthStatus(WALHealth status); 

private:
    void writerThreadLoop();
    bool writeBatchToDisk(CommitBatch& batch, bool force_sync);

    const uint32_t shard_id_;
    const std::string wal_directory_;
    const std::string file_prefix_;
    const uint64_t segment_size_;
    const bool sync_on_commit_record_;

    // Reference to the global LSN counter owned by the parent manager.
    std::atomic<LSN>& global_lsn_counter_;
    std::atomic<WALHealth>& parent_health_status_;

    // Each shard has its own set of managers and threads.
    WALMetrics metrics_;
    std::unique_ptr<GroupCommitManager> group_commit_manager_;
    std::unique_ptr<SegmentManager> segment_manager_;
    
    // Per-shard segment state
    std::unique_ptr<WALSegment> current_segment_;
    std::deque<std::unique_ptr<WALSegment>> archived_segments_;
    mutable std::mutex segments_mutex_;
    
    std::thread writer_thread_;
    std::atomic<bool> stop_requested_{false};
    LSN scanForRecovery();
    //WALMetrics metrics_;
};