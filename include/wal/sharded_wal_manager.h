// include/wal/sharded_wal_manager.h
#pragma once

#include "wal_shard.h"
#include "wal_metrics.h" 
#include "../types.h"
#include <vector>
#include <memory>
#include <atomic>

// Re-using the same config struct for simplicity.
struct ExtWALManagerConfig;


/**
 * @class ShardedWALManager
 * @brief The top-level, multi-threaded WAL manager.
 *
 * This class orchestrates multiple parallel WALShard instances to achieve high
 * write throughput. It is responsible for initializing all shards, providing a
 * unified API for submitting log records, and coordinating global LSN assignment.
 */
class ShardedWALManager {
public:
    explicit ShardedWALManager(const ExtWALManagerConfig& config, size_t num_shards);
    ~ShardedWALManager();

    // Disallow copy/move
    ShardedWALManager(const ShardedWALManager&) = delete;
    ShardedWALManager& operator=(const ShardedWALManager&) = delete;

    bool isHealthy() const;
    std::vector<LogRecord> getRecordsStrictlyAfter(LSN start_lsn_exclusive);

    WALHealth getHealthStatus() const;
    WALMetricsSnapshot getAggregatedMetrics() const;

    /**
     * @brief Initializes all shards, performs recovery, and starts background threads.
     * @return true on success, false on failure.
     */
    bool initialize();

    /**
     * @brief Submits a log record to the appropriate shard based on its transaction ID.
     * @param record The log record to write.
     * @param priority The priority of the write.
     * @return A future that will be fulfilled with the globally unique LSN.
     */
    std::future<LSN> appendLogRecord(LogRecord& record, WALPriority priority);

    /**
     * @brief Performs a synchronous, durable write of a log record.
     * @param record The log record to write.
     * @return The LSN of the durable record, or 0 on failure.
     */
    LSN appendAndForceFlushLogRecord(LogRecord& record);
    
    /**
     * @brief Ensures all records up to a given LSN are durable across all shards.
     * @param lsn The LSN to sync up to.
     */
    void forceSyncToLSN(LSN lsn);
    
    /**
     * @brief Truncates old segments across all shards based on a checkpoint LSN.
     */
    bool truncateSegmentsBeforeLSN(LSN checkpoint_lsn);

    // Observability methods would go here (e.g., getStatus, getMetrics)
    LSN recoverAllShards(std::vector<LogRecord>& out_all_records); // Modified to return records


private:

    const ExtWALManagerConfig& config_;
    const size_t num_shards_;

    std::vector<std::unique_ptr<WALShard>> shards_;
    std::atomic<LSN> global_lsn_counter_{1};
    //std::atomic<bool> is_healthy_{false};
    std::atomic<WALHealth> current_health_{WALHealth::CRITICAL_ERROR}; // Start in a non-healthy state

};