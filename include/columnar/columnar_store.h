// include/columnar/columnar_store.h
#pragma once

#include "columnar_file.h"
#include "columnar_buffer.h"
#include "store_schema_manager.h"
#include "column_types.h"
#include "../../include/types.h"
#include "../../include/wal/lock_free_ring_buffer.h"
#include "columnar_compaction_coordinator.h"
#include <mutex> 

#include <string>
#include <vector>
#include <memory>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <queue>


class StorageEngine;

namespace engine {

// Forward-declare StorageEngine to avoid including the full header,
// preventing circular dependencies. The .cpp file will include the full definition.
//class StorageEngine;

namespace columnar {

class ColumnarCompactionManager;
struct ColumnarCompactionJob;

//  --- Payload for the lock-free ingestion queue
struct ColumnarWriteRequest {
    std::string primary_key;
    std::unordered_map<uint32_t, ValueType> flattened_record;
};

struct ColumnarStoreStats {
    size_t ingestion_queue_depth = 0;
    size_t active_buffer_count = 0;
    size_t total_rows_in_active_buffers = 0;
    size_t total_bytes_in_active_buffers = 0;
    size_t immutable_buffer_count = 0;
    size_t total_rows_in_immutable_buffers = 0;
    size_t total_bytes_in_immutable_buffers = 0;
    std::vector<size_t> files_per_level;
};

/**
 * @class ColumnarStore
 * @brief A tiered, column-oriented storage engine.
 *
 * Architecturally cloned from LSMTree, this class manages data in a columnar format
 * suitable for analytical queries. It uses in-memory ColumnarBuffers that are flushed
 * to on-disk, immutable ColumnarFiles. It orchestrates background threads for
 * flushing and tiered compaction to maintain performance and organize data.
 */
class ColumnarStore {
    // Grant friendship to the compaction manager so it can call execute_compaction_job.
    friend class ColumnarCompactionManager;
    friend class ColumnarCompactionJobCoordinator; 

public:
    ColumnarStoreStats getStats() const;
    /**
     * @struct ColumnarFileMetadata
     * @brief In-memory metadata for an on-disk ColumnarFile.
     * Cloned from LSMTree::SSTable, this holds information needed for compaction
     * selection and query planning without keeping the entire file open.
     */
/*   
    struct ColumnarFileMetadata {
        std::string filename;
        uint64_t file_id;
        int level;
        size_t size_bytes;
        uint64_t row_count;
        uint32_t schema_version;
        std::string min_primary_key;
        std::string max_primary_key;
        TxnId min_txn_id;
        TxnId max_txn_id;
        std::unique_ptr<BloomFilter> pk_bloom_filter;
        std::unordered_map<uint32_t, std::pair<ValueType, ValueType>> column_stats; // column_id -> {min, max}
    };
*/ 
    // --- Constructor & Destructor ---

    /**
     * @brief Constructs the ColumnarStore.
     * Note: This is part one of a two-phase initialization. The object is not fully
     * functional until setStorageEngine() is called.
     * @param store_path The logical path of the store (e.g., 'users').
     * @param data_dir The physical directory path where this store's files will be saved.
     * @param write_buffer_max_size Max size in bytes for the in-memory write buffer.
     * @param compression_type The compression algorithm to use for column chunks.
     * @param compression_level The level for the chosen compression algorithm.
     * @param encryption_is_active Whether to encrypt data on disk.
     * @param encryption_scheme The specific AES scheme to use if active.
     * @param dek The Data Encryption Key to use for encryption.
     */
    ColumnarStore(
        const std::string& store_path,
        const std::string& data_dir,
        StoreSchemaManager* schema_manager,
        size_t write_buffer_max_size,
        CompressionType compression_type,
        int compression_level,
        bool encryption_is_active,
        EncryptionScheme encryption_scheme,
        const std::vector<unsigned char>& dek
    );
    ~ColumnarStore();

    // Prevent copying and assignment.
    ColumnarStore(const ColumnarStore&) = delete;
    ColumnarStore& operator=(const ColumnarStore&) = delete;

    // --- Public API for StorageEngine ---

    /**
     * @brief Completes the two-phase initialization by linking back to the parent engine.
     * This is also when background threads are started.
     */
    void setStorageEngine(::StorageEngine* engine);

    /**
     * @brief Ingests a new or updated record into the shadow store.
     * The record is flattened and added to the active write buffer.
     */
    void shadowInsert(const Record& record);
    /*std::vector<Record> executeAggregationQuery(
        const std::vector<std::string>& group_by_fields,
        // const AggregationPlan& aggregations, // This would be a more robust struct
        const std::vector<FilterCondition>& filters
    );*/

    std::vector<Record> executeAggregationQuery(const AggregationPlan &plan, const std::vector<FilterCondition> &filters);

    /**
     * @brief Records a deletion.
     * In this model, deletions are handled during compaction by checking the primary store.
     */
    void shadowDelete(const std::string& primary_key);

    /**
     * @brief Executes an analytical query against the columnar data.
     * @param filters A vector of conditions to apply.
     * @param limit The maximum number of records to return.
     * @return A vector of reconstructed Records matching the query.
     */
    //std::vector<Record> executeQuery(const std::vector<FilterCondition>& filters, size_t limit);
    std::vector<Record> executeQuery(const std::vector<FilterCondition> &filters, const std::optional<AggregationPlan> &aggregation_plan, size_t limit);
    //  --- Public method for the coordinator to atomically update levels
    void atomicallyUpdateLevels(
        const std::vector<std::shared_ptr<ColumnarFileMetadata>>& files_to_delete_L,
        const std::vector<std::shared_ptr<ColumnarFileMetadata>>& files_to_delete_L_plus_1,
        const std::vector<std::shared_ptr<ColumnarFileMetadata>>& files_to_add
    );
    //  --- Public method for the coordinator to notify scheduler
    void notifyScheduler();
    void stopBackgroundThreads();
    void startBackgroundThreads();
    
    // --- Public method for direct file ingestion
    void ingestFile(const std::string& file_path, bool move_file);

private:
    // --- Lifecycle Methods (Cloned from LSMTree) ---
    void startCompactionAndFlushThreads();
    void stopCompactionAndFlushThreads();
    void loadFileMetadata();
    void backgroundFlushWorkerLoop();
    void schedulerThreadLoop();
    void checkAndScheduleCompactions();
    void ingestionWorkerLoop(); 
    static std::pair<std::string, std::string> compute_key_range(
        const std::vector<std::shared_ptr<ColumnarFileMetadata>>& files
    );
    // --- Core Logic (Refactored from LSMTree) ---
    std::optional<std::shared_ptr<ColumnarFileMetadata>> flushWriteBufferToFile(
        std::shared_ptr<ColumnarBuffer> buffer_to_flush
    );
    std::optional<ColumnarCompactionJob> selectCompactionCandidate();
    bool executeCompactionJob(
        int level_L,
        std::vector<std::shared_ptr<ColumnarFileMetadata>> files_L,
        std::vector<std::shared_ptr<ColumnarFileMetadata>> files_L_plus_1
    );
    //  --- This contains the actual merge logic, executed by worker threads
    std::vector<std::shared_ptr<ColumnarFileMetadata>> executeSubCompactionTask(
        const SubCompactionTask& task
    );
    StoreSchemaManager* schema_manager_; 
    // --- Helper Methods ---
    std::optional<ColumnSchema> getStoreSchema(uint32_t version);
    std::optional<ColumnSchema> getLatestStoreSchema() const;
    std::unordered_map<uint32_t, ValueType> flattenRecord(const Record& record, const ColumnSchema& schema);
    std::string generateFilePath(int level, uint64_t id) const;

    std::vector<std::shared_ptr<ColumnarFileMetadata>> find_overlapping_files(int target_level, const std::string &min_key, const std::string &max_key);

    // --- Member Variables (Cloned and adapted from LSMTree) ---
    StorageEngine* storage_engine_ptr_;  // **FIXED**: Removed :: prefix to use engine::StorageEngine
    std::string store_path_;
    std::string data_dir_;

    //std::shared_ptr<ColumnarBuffer> active_write_buffer_;
    //std::vector<std::shared_ptr<ColumnarBuffer>> immutable_buffers_;
    static constexpr size_t NUM_WRITE_BUFFER_SHARDS = 16;
    std::vector<std::shared_ptr<ColumnarBuffer>> active_write_buffers_;
    mutable std::vector<std::unique_ptr<std::mutex>> active_buffer_mutexes_;

    
    std::vector<std::shared_ptr<ColumnarBuffer>> immutable_buffers_;
    std::vector<std::vector<std::shared_ptr<ColumnarFileMetadata>>> levels_;
    
    std::unique_ptr<ColumnarCompactionManager> compaction_manager_;
    std::atomic<uint64_t> next_columnar_file_id_{1};
    std::optional<std::shared_ptr<ColumnarFileMetadata>> flushBuffersToSingleFile(std::vector<std::shared_ptr<ColumnarBuffer>> buffers_to_flush);
    // --- Configuration (mirrors LSMTree) ---
    size_t write_buffer_max_size_;
    //  --- Lock-free queue for ingestion
    std::unique_ptr<LockFreeRingBuffer<ColumnarWriteRequest>> ingestion_queue_;
    std::thread ingestion_thread_;
    std::atomic<bool> ingestion_worker_shutdown_{false};
    
    bool encryption_is_active_;
    EncryptionScheme default_encryption_scheme_;
    std::vector<unsigned char> database_dek_;
    CompressionType compression_type_;
    int compression_level_;
    
    // --- Threading (mirrors LSMTree) ---
    mutable std::shared_mutex buffer_mutex_;
    mutable std::mutex levels_metadata_mutex_;
    std::atomic<bool> scheduler_running_{false};
    std::thread scheduler_thread_;
    mutable std::condition_variable scheduler_cv_;
    mutable std::mutex scheduler_mutex_;

    std::vector<std::thread> flush_worker_threads_;
    std::atomic<bool> flush_workers_shutdown_{false};
    mutable std::condition_variable immutable_buffer_ready_cv_;
    mutable std::condition_variable immutable_slot_available_cv_;
    mutable std::mutex immutable_cv_mutex_;

    static constexpr size_t L0_COMPACTION_TRIGGER = 4; // Compact L0 when it has this many files.
    static constexpr double LEVEL_SIZE_MULTIPLIER = 10.0; // L1 is X times bigger than L0, L2 is X times bigger than L1, etc.
    static constexpr size_t SSTABLE_TARGET_SIZE_BASE = 64 * 1024 * 1024; // Base size for L1 target in bytes (64MB)

    mutable std::unordered_map<uint32_t, ColumnSchema> schema_cache_;
    mutable std::shared_mutex schema_cache_mutex_;
};

// --- Compaction Orchestration (cloned from LSMTree) ---
struct ColumnarCompactionJob {
    int level_L;
    std::vector<std::shared_ptr<ColumnarFileMetadata>> files_L;
    std::vector<std::shared_ptr<ColumnarFileMetadata>> files_L_plus_1;
};

class ColumnarCompactionManager {
public:
    ColumnarCompactionManager(ColumnarStore* store_ptr, size_t num_threads);
    ~ColumnarCompactionManager();
    void scheduleJob(ColumnarCompactionJob job);

private:
    void workerThreadLoop();
    ColumnarStore* store_ptr_;
    std::queue<ColumnarCompactionJob> job_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::vector<std::thread> workers_;
    std::atomic<bool> shutdown_{false};
};

} // namespace columnar
} // namespace engine