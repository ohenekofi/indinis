// src/wal/sharded_wal_manager.cpp

#include "../../include/wal/sharded_wal_manager.h"
#include "../../include/wal.h" // For ExtWALManagerConfig
#include "../../include/debug_utils.h"
#include <functional> // For std::hash
#include <algorithm> // For std::sort

ShardedWALManager::ShardedWALManager(const ExtWALManagerConfig& config, size_t num_shards)
    : config_(config), num_shards_(num_shards > 0 ? num_shards : 1) {
    LOG_INFO("[ShardedWALManager] Constructing with {} shards.", num_shards_);
    shards_.reserve(num_shards_);
    for (size_t i = 0; i < num_shards_; ++i) {
        std::string shard_prefix = config_.wal_file_prefix + "_shard_" + std::to_string(i);
        shards_.push_back(std::make_unique<WALShard>(
            i,
            config_.wal_directory,
            shard_prefix,
            config_.segment_size,
            config_.sync_on_commit_record,
            global_lsn_counter_,
            current_health_ 
        ));
    }
}

ShardedWALManager::~ShardedWALManager() {
    LOG_INFO("[ShardedWALManager] Destructing... Shutting down {} shards.", shards_.size());
    // The unique_ptr's destruction of each WALShard will trigger its destructor,
    // which in turn will stop its threads and close its segments.
    // We can make this more explicit if needed, but this is generally sufficient.
    shards_.clear(); // Explicitly trigger destructors.
    LOG_INFO("[ShardedWALManager] All shards destructed.");
}

bool ShardedWALManager::initialize() {
    LOG_INFO("[ShardedWALManager] Initializing...");
    
    std::vector<LogRecord> recovered_records;
    LSN last_recovered_lsn = recoverAllShards(recovered_records);
    
    if (last_recovered_lsn > 0) {
        global_lsn_counter_.store(last_recovered_lsn + 1);
    } else {
        global_lsn_counter_.store(1);
    }

    for (auto& shard : shards_) {
        if (!shard->initializeAndStart()) {
            LOG_FATAL("[ShardedWALManager] Failed to initialize shard. WAL system failed to start.");
            current_health_.store(WALHealth::CRITICAL_ERROR); // Explicitly set failure state
            return false;
        }
    }

    // --- START OF FIX ---
    // Set the health status using the correct enum and member.
    current_health_.store(WALHealth::HEALTHY);
    LOG_INFO("[ShardedWALManager] All shards initialized. System Health: HEALTHY.");
    return true; // Directly return true on success.
    // --- END OF FIX ---
}

bool ShardedWALManager::isHealthy() const {
    // The public `isHealthy` is now just a convenient check on the detailed health status.
    return current_health_.load(std::memory_order_acquire) == WALHealth::HEALTHY;
}

WALHealth ShardedWALManager::getHealthStatus() const {
    return current_health_.load(std::memory_order_acquire);
}

WALMetricsSnapshot ShardedWALManager::getAggregatedMetrics() const {
    WALMetricsSnapshot aggregated_snapshot;
    
    for (const auto& shard : shards_) {
        WALMetricsSnapshot shard_snapshot = shard->getMetricsSnapshot();
        
        aggregated_snapshot.records_written += shard_snapshot.records_written;
        aggregated_snapshot.bytes_written += shard_snapshot.bytes_written;
        aggregated_snapshot.sync_operations += shard_snapshot.sync_operations;
        aggregated_snapshot.segment_rotations += shard_snapshot.segment_rotations;
        aggregated_snapshot.io_errors += shard_snapshot.io_errors;
        
        // For latency and throughput, we should average or take the max. Max is often more insightful for P99.
        aggregated_snapshot.p99_latency_us = std::max(aggregated_snapshot.p99_latency_us, shard_snapshot.p99_latency_us);
        aggregated_snapshot.throughput_rps += shard_snapshot.throughput_rps;
    }
    
    return aggregated_snapshot;
}

std::vector<LogRecord> ShardedWALManager::getRecordsStrictlyAfter(LSN start_lsn_exclusive) {
    std::vector<LogRecord> combined_records;
    LOG_INFO("[ShardedWALManager] Getting records strictly after LSN {} from all shards.", start_lsn_exclusive);

    // This operation is for recovery or debugging and can be less optimized.
    // A simple approach is to get ALL records from each shard and then filter.
    // A more optimized approach (not implemented here) would be for each shard
    // to filter by LSN first.
    for (const auto& shard : shards_) {
        auto shard_records = shard->recoverRecords(); // Gets all records from this shard
        for (const auto& rec : shard_records) {
            if (rec.lsn > start_lsn_exclusive) {
                combined_records.push_back(rec);
            }
        }
    }

    // CRITICAL: Sort the combined records by LSN to ensure global chronological order.
    std::sort(combined_records.begin(), combined_records.end(), [](const LogRecord& a, const LogRecord& b){
        return a.lsn < b.lsn;
    });
    
    LOG_INFO("[ShardedWALManager] Found a total of {} records after LSN {} across all shards.", combined_records.size(), start_lsn_exclusive);
    return combined_records;
}


std::future<LSN> ShardedWALManager::appendLogRecord(LogRecord& record, WALPriority priority) {
    if (!isHealthy()) {
        std::promise<LSN> p;
        p.set_exception(std::make_exception_ptr(std::runtime_error("Sharded WAL is not healthy.")));
        return p.get_future();
    }
    
    // Use the transaction ID to select the shard.
    size_t shard_idx = std::hash<TxnId>{}(record.txn_id) % num_shards_;
    return shards_[shard_idx]->submitRecord(record, priority);
}

LSN ShardedWALManager::appendAndForceFlushLogRecord(LogRecord& record) {
    // This is a synchronous, blocking call.
    auto future = appendLogRecord(record, WALPriority::IMMEDIATE);
    try {
        return future.get();
    } catch(const std::exception& e) {
        LOG_ERROR("[ShardedWALManager] appendAndForceFlushLogRecord failed: {}", e.what());
        return 0;
    }
}

void ShardedWALManager::forceSyncToLSN(LSN lsn) {
    // This is a complex operation in a sharded system. To guarantee durability up to
    // a specific LSN, we need to ensure the shard that *will write* that LSN syncs.
    // A simpler, brute-force approach is to send a sync marker to *all* shards.
    LOG_INFO("[ShardedWALManager] forceSyncToLSN({}) called. Submitting sync markers to all shards.", lsn);
    std::vector<std::future<LSN>> sync_futures;
    for (auto& shard : shards_) {
        LogRecord sync_marker;
        sync_marker.type = LogRecordType::INVALID;
        sync_marker.txn_id = 0; // System operation
        sync_futures.push_back(shard->submitRecord(sync_marker, WALPriority::IMMEDIATE));
    }
    
    // Wait for all shards to complete their sync.
    for (auto& f : sync_futures) {
        try {
            f.get();
        } catch (...) { /* ignore exceptions from individual shards */ }
    }
    LOG_INFO("[ShardedWALManager] forceSyncToLSN({}) completed.", lsn);
}

LSN ShardedWALManager::recoverAllShards(std::vector<LogRecord>& out_all_records) {
    LOG_INFO("[ShardedWALManager] Recovering all shards...");
    out_all_records.clear();
    
    for (const auto& shard : shards_) {
        auto shard_records = shard->recoverRecords();
        out_all_records.insert(out_all_records.end(), 
                               std::make_move_iterator(shard_records.begin()), 
                               std::make_move_iterator(shard_records.end()));
    }

    if (out_all_records.empty()) {
        LOG_INFO("[ShardedWALManager] No records found across any shards during recovery.");
        return 0;
    }
    
    std::sort(out_all_records.begin(), out_all_records.end(), [](const LogRecord& a, const LogRecord& b){
        return a.lsn < b.lsn;
    });

    LSN max_lsn = out_all_records.back().lsn;
    LOG_INFO("[ShardedWALManager] Recovery scan found {} total records. Max LSN is {}.", out_all_records.size(), max_lsn);
    return max_lsn;
}

bool ShardedWALManager::truncateSegmentsBeforeLSN(LSN checkpoint_lsn) {
    LOG_INFO("[ShardedWALManager] Truncating segments before LSN {} across all shards.", checkpoint_lsn);
    for (auto& shard : shards_) {
        shard->truncateSegmentsBefore(checkpoint_lsn);
    }
    return true; // Simplified success metric
}