// @include/wal.h
#pragma once

#include "types.h"       
#include "debug_utils.h" 
#include "wal/lock_free_ring_buffer.h"
#include "wal/group_commit.h" 
#include "wal/wal_metrics.h"
#include "wal/segment_manager.h"
#include <string>
#include <vector>
#include <memory>    
#include <mutex>     
#include <atomic>    
#include <condition_variable> 
#include <thread>    
#include <optional>  
#include <deque>     
#include <future>

class WALSegment;

struct ExtWALManagerConfig {
    std::string wal_directory = "./wal_data";
    std::string wal_file_prefix = "wal_segment";
    uint64_t segment_size = 64 * 1024 * 1024; 
    bool background_flush_enabled = true;
    std::atomic<uint32_t> flush_interval_ms{100}; 
    bool sync_on_commit_record = true; 

    // Default constructor
    ExtWALManagerConfig() = default;

    // *** ADD EXPLICIT COPY CONSTRUCTOR ***
    ExtWALManagerConfig(const ExtWALManagerConfig& other)
        : wal_directory(other.wal_directory),
          wal_file_prefix(other.wal_file_prefix),
          segment_size(other.segment_size),
          background_flush_enabled(other.background_flush_enabled),
          flush_interval_ms(other.flush_interval_ms.load(std::memory_order_relaxed)), // Load from other's atomic
          sync_on_commit_record(other.sync_on_commit_record)
    {}

    // *** ADD EXPLICIT COPY ASSIGNMENT OPERATOR (Good Practice) ***
    ExtWALManagerConfig& operator=(const ExtWALManagerConfig& other) {
        if (this == &other) {
            return *this;
        }
        wal_directory = other.wal_directory;
        wal_file_prefix = other.wal_file_prefix;
        segment_size = other.segment_size;
        background_flush_enabled = other.background_flush_enabled;
        flush_interval_ms.store(other.flush_interval_ms.load(std::memory_order_relaxed), std::memory_order_relaxed); // Store to this atomic
        sync_on_commit_record = other.sync_on_commit_record;
        return *this;
    }

    // Move constructor and move assignment operator can be defaulted if no special logic needed
    ExtWALManagerConfig(ExtWALManagerConfig&& other) noexcept = default;
    ExtWALManagerConfig& operator=(ExtWALManagerConfig&& other) noexcept = default;
};


class ExtWALManager {
public:
    using Config = ExtWALManagerConfig; // This now refers to the updated struct

    explicit ExtWALManager(const Config& config);
    ~ExtWALManager();

    // ... (rest of the ExtWALManager class declaration remains the same) ...
    ExtWALManager(const ExtWALManager&) = delete;
    ExtWALManager& operator=(const ExtWALManager&) = delete;

    bool initialize();
    // LSN appendLogRecord(LogRecord& record); //DEPRECATED
    std::future<LSN> appendLogRecord(LogRecord& record, WALPriority priority);
    LSN appendAndForceFlushLogRecord(LogRecord& record, bool force_sync_overall = false);
    //void syncLog();
    LSN getLastAssignedLSN() const { return next_lsn_to_assign_.load(std::memory_order_relaxed) -1; }
    LSN getLastWrittenLSNToSegment() const { return last_written_lsn_.load(std::memory_order_relaxed); }
    std::vector<LogRecord> getRecordsStrictlyAfter(LSN start_lsn_exclusive) const;
    std::vector<LogRecord> getAllPersistedRecordsForTesting() const;
    bool truncateSegmentsBeforeLSN(LSN lsn_exclusive_lower_bound);
    bool isHealthy() const { return is_healthy_.load(std::memory_order_acquire); }
    std::string getStatus() const;
    void forceSyncToLSN(LSN lsn);

private:
    std::string getSegmentFilePath(uint32_t segment_id) const;
    bool rotateLogIfNeeded(size_t next_record_size_approx, LSN lsn_of_record_triggering_roll);
    bool openNewSegment(LSN first_record_lsn_for_this_segment);
    LSN scanForRecovery(uint32_t* out_max_segment_id);
    // void backgroundFlushWorker(); deprecated 
    bool flushBufferToCurrentSegment(bool force_sync_segment);

    Config config_; // This will now use the copy constructor when ExtWALManager is initialized
    std::atomic<LSN> next_lsn_to_assign_;
    std::atomic<LSN> last_synced_lsn_{0}; 
    std::atomic<LSN> last_written_lsn_;
    std::atomic<bool> is_healthy_;
    std::atomic<bool> shutdown_requested_;
    std::unique_ptr<WALSegment> current_segment_;
    std::deque<std::unique_ptr<WALSegment>> archived_segments_;
    std::unique_ptr<SegmentManager> segment_manager_;
    // uint32_t current_segment_id_counter_; // deprecated
    mutable std::mutex segments_mutex_;
    // std::vector<LogRecord> record_buffer_; //deprecated
    // mutable std::mutex buffer_mutex_;//deprecated
    // LockFreeRingBuffer ring_buffer_; //DEPRECATED
    std::unique_ptr<GroupCommitManager> group_commit_manager_;
    std::atomic<bool> has_unflushed_data_;
    // std::thread background_flush_thread_;
    // std::condition_variable flush_cv_;
    // std::mutex flush_thread_mutex_;
    void writerThreadLoop(); // <<< MODIFIED ROLE
    bool writeBatchToDisk(CommitBatch& batch, bool force_sync);

    WALMetrics wal_metrics_; // <<< NOW OWNS THE METRICS OBJECT
    std::thread writer_thread_; // <<< RENAMED from background_flush_thread_
    std::condition_variable cv_for_writer_; // <<< RENAMED from flush_cv_
    std::mutex writer_thread_mutex_; // <<< RENAMED from flush_thread_mutex_

};