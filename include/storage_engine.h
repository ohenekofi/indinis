// @include/storage_engine.h
#pragma once

#include "types.h" // Includes Record, TxnId, CheckpointInfo, CheckpointMetadata, EncryptionScheme etc.
#include "wal.h"   // For ExtWALManagerConfig (used in constructor default)
#include "cache.h" // <<< ADD: Include the cache library header
#include "columnar/store_schema_manager.h" 
#include "columnar/column_types.h"
#include "columnar/schema_validator.h"
#include "lsm/write_buffer_manager.h"
#include "lsm/partitioned_memtable.h"
#include "threading/thread_pool.h" 
#include "threading/system_monitor.h" 
#include "lsm/compaction_strategy.h" 
#include "lsm/leveled_compaction_strategy.h"
#include "lsm/universal_compaction_strategy.h" 
#include "lsm/fifo_compaction_strategy.h"
#include "lsm/hybrid_compaction_strategy.h" 
#include "lsm/compaction_filter.h"
#include "lsm/liveness_oracle.h"

#include <string>
#include <vector>
#include <shared_mutex>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <memory>
#include <optional>
#include <filesystem>
#include <condition_variable>
#include <deque>
#include <set>

// Forward declarations
class LSMTree;
class IndexManager;
class Transaction;
class SimpleDiskManager;
class SimpleBufferPoolManager;
// class ExtWALManager; //deprecated 
class ShardedWALManager; 

// Structure to be serialized as cache value
struct CachedLsmEntry {
    TxnId commit_txn_id = 0;
    bool is_tombstone = false;
    std::string serialized_value_type; // Result of LogRecord::serializeValueTypeBinary

    std::string serialize() const;
    bool deserialize(const std::string& data);
};

namespace engine {
namespace columnar {
    class StoreSchemaManager;
    class ColumnarStore;
    class QueryRouter;
}
}

namespace engine {
namespace threading { class TokenBucketRateLimiter; }
}

class StorageEngine {
private:
    std::string data_dir_;
    std::unique_ptr<SimpleDiskManager> disk_manager_;
    std::unique_ptr<SimpleBufferPoolManager> bpm_;
    // std::unique_ptr<LSMTree> storage_;
    std::unique_ptr<IndexManager> index_manager_;
    std::unique_ptr<ShardedWALManager> wal_manager_;
    // std::unique_ptr<ExtWALManager> wal_manager_; //deprecated
    mutable std::shared_mutex encryption_state_mutex_; // Protects encryption state (KEK, DEK, etc.)
    std::shared_ptr<engine::lsm::WriteBufferManager> write_buffer_manager_;

    engine::lsm::CompactionStrategyType default_compaction_strategy_type_;
    engine::lsm::LeveledCompactionConfig default_leveled_config_;
    std::unordered_map<std::string, std::unique_ptr<LSMTree>> lsm_stores_;
    engine::lsm::UniversalCompactionConfig default_universal_config_;
    engine::lsm::FIFOCompactionConfig default_fifo_config_; 
    engine::lsm::HybridCompactionConfig default_hybrid_config_;

    std::shared_ptr<engine::threading::TokenBucketRateLimiter> compaction_rate_limiter_;

    /**
     * @brief A read-write mutex to protect the lsm_stores_ map.
     * A shared_mutex is used to allow multiple threads to read from the map
     * concurrently (fast path), while ensuring that the creation of a new
     * store instance is an exclusive, thread-safe operation (slow path).
     */
    mutable std::shared_mutex lsm_stores_mutex_;
    std::atomic<bool> force_checkpoint_request_{false};
    std::atomic<TxnId> next_txn_id_;
    std::unordered_map<TxnId, std::shared_ptr<Transaction>> active_transactions_;
    mutable std::mutex txn_mutex_; // Made mutable to allow locking in const getOldestActiveTxnId
    std::atomic<TxnId> oldest_active_txn_id_;

    std::atomic<uint64_t> next_checkpoint_id_{1};
    std::atomic<bool> checkpoint_in_progress_{false};
    std::chrono::seconds checkpoint_interval_;
    std::thread checkpoint_thread_;
    std::mutex checkpoint_control_mutex_;
    std::condition_variable checkpoint_cv_;
    std::atomic<bool> shutdown_initiated_{false};

    std::string checkpoint_metadata_filepath_;
    CheckpointMetadata last_successful_checkpoint_metadata_;

    std::unique_ptr<engine::threading::SystemMonitor> system_monitor_;
    std::shared_ptr<engine::threading::ThreadPool> shared_compaction_pool_;
    std::unique_ptr<engine::threading::AdaptiveThreadPoolManager> adaptive_pool_manager_;
    
    //std::unordered_map<std::string, std::unique_ptr<LSMTree>> lsm_stores_;
    // mutable std::shared_mutex lsm_stores_mutex_;

    std::deque<CheckpointInfo> checkpoint_history_;
    mutable std::mutex checkpoint_history_mutex_; // Made mutable
    static const size_t MAX_CHECKPOINT_HISTORY = 10;

    std::unique_ptr<engine::columnar::StoreSchemaManager> schema_manager_;
    std::unique_ptr<engine::columnar::QueryRouter> query_router_;
    // Map of store_path to its columnar engine instance
    std::unordered_map<std::string, std::unique_ptr<engine::columnar::ColumnarStore>> columnar_stores_;
    mutable std::shared_mutex columnar_stores_mutex_; 

    std::shared_ptr<engine::lsm::CompactionFilter> default_compaction_filter_;

    // --- Encryption Related Members ---
    bool encryption_active_ = false;
    EncryptionScheme current_encryption_scheme_ = EncryptionScheme::NONE; // Scheme to use for NEW data
    int kdf_iterations_ = 0; // KDF iterations used for current DEK (0 means default)
    std::vector<unsigned char> database_dek_; // Holds the derived Data Encryption Key
    std::vector<unsigned char> database_global_salt_; // Salt used for DEK derivation

    std::string dekinfo_filepath_;              // NEW
    std::vector<unsigned char> database_kek_; // NEW: Holds the KEK (derived from password)

    void initializeEncryptionState(
        const std::optional<std::string>& encryption_password,
        EncryptionScheme scheme_to_use_for_data, // This is for page/block data
        int kdf_iterations_for_kek_param);       // This is for KEK derivation

    bool loadOrCreateDEKInfo(
        const std::vector<unsigned char>& kek_candidate, // Derived KEK
        int kdf_iterations_used_for_kek,                 // Iterations used for this KEK
        EncryptionScheme scheme_for_data_encryption,     // Scheme to use for data pages
        std::vector<unsigned char>& out_dek);          // Populated with the final DEK
    bool loadExistingDEKInfo(const std::vector<unsigned char>& kek_candidate, 
                           int kdf_iterations_used_for_kek, 
                           std::vector<unsigned char>& out_dek);
    bool validateDEKFileHeader(const WrappedDEKInfo& header_from_file, 
                             int kdf_iterations_used_for_kek);
    bool verifyKEKValidation(const WrappedDEKInfo& header_from_file, 
                           const std::vector<unsigned char>& kek_candidate);
    bool readAndUnwrapDEK(std::fstream& dek_info_file, 
                        const WrappedDEKInfo& header_from_file, 
                        const std::vector<unsigned char>& kek_candidate, 
                        std::vector<unsigned char>& out_dek);
    bool createNewDEKInfo(const std::vector<unsigned char>& kek_candidate, 
                        int kdf_iterations_used_for_kek, 
                        std::vector<unsigned char>& out_dek);
    bool encryptAndStoreKEKValidation(WrappedDEKInfo& header, 
                                    const std::vector<unsigned char>& kek_candidate);
    bool generateAndWrapDEK(const std::vector<unsigned char>& kek_candidate, 
                          std::vector<unsigned char>& out_dek, 
                          std::vector<unsigned char>& raw_wrapped_dek_package);
    bool writeDEKInfoFile(const WrappedDEKInfo& header, 
                        const std::vector<unsigned char>& raw_wrapped_dek_package);


    // --- Private Helper Method Declarations ---
    void updateOldestActiveTxnId();
    void performRecovery();

    TxnId analysisPass(const std::vector<LogRecord>& records_to_replay,
                       std::set<TxnId>& committed_txns,
                       std::set<TxnId>& aborted_txns,
                       std::map<TxnId, std::vector<LogRecord>>& pending_data_ops);


    void redoPass(const std::set<TxnId>& committed_txns,
                  const std::map<TxnId, std::vector<LogRecord>>& pending_data_ops);


    void finalizeRecovery(TxnId max_txn_id_seen);

    // --- End of Recovery Helpers ---
   // engine::columnar::ColumnarStore* getOrCreateColumnarStore(const std::string& store_path);

    void loadOrGenerateGlobalSalt();
    void securelyClearPassword(std::string& password_buffer);

    // Checkpointing methods
    void checkpointThreadLoop();
    
    bool tryPerformCheckpoint();

    LSN beginCheckpoint(uint64_t& checkpoint_id, std::set<TxnId>& active_txns_at_begin);
    
    void flushMemoryToDisk();

    void finishCheckpoint(uint64_t checkpoint_id, LSN begin_lsn, const std::set<TxnId>& active_txns_at_begin);

    // --- End of Checkpoint Helpers ---
    void loadLastCheckpointMetadata();
    void persistCheckpointMetadata(const CheckpointMetadata& meta);
    std::set<TxnId> getActiveTransactionIdsForCheckpoint();

    // ---  Cache Members ---
    bool cache_enabled_ = false;
    std::unique_ptr<cache::Cache> data_cache_;
    // ---  NEW Cache Members ---
    std::pair<const FilterCondition*, IndexDefinition> selectIndexForQuery(
        const std::string& storePath,
        const std::vector<FilterCondition>& filters,
        const std::optional<std::pair<std::string, IndexSortOrder>>& sortBy // Add this
    );

    size_t sstable_block_size_bytes_;
    CompressionType sstable_compression_type_;
    int sstable_compression_level_;

    std::vector<Record> queryLsm(
        TxnId reader_txn_id,
        const std::string& storePath,
        const std::vector<FilterCondition>& filters,
        const std::optional<std::pair<std::string, IndexSortOrder>>& sortBy,
        size_t limit
    );

    std::unique_ptr<engine::lsm::LivenessOracle> liveness_oracle_;
    std::vector<ValueType> extractCursorValues(
        const Record& doc,
        const std::vector<OrderByClause>& orderBy
    );
    

public:
    // Constructor declaration with new encryption parameters
    StorageEngine(
        const std::string& data_dir,
        std::chrono::seconds checkpoint_interval,
        const ExtWALManagerConfig& wal_config,
        size_t sstable_block_size_bytes,
        CompressionType sstable_compression,
        int sstable_compression_level,
        const std::optional<std::string>& encryption_password,
        EncryptionScheme data_encryption_scheme_to_use,
        int kdf_iterations_for_kek_param,
        bool enable_data_cache,
        const cache::CacheConfig& cache_config,
        engine::lsm::MemTableType default_memtable_type,
        size_t min_compaction_threads,
        size_t max_compaction_threads,
        engine::lsm::CompactionStrategyType default_compaction_strategy,
        const engine::lsm::LeveledCompactionConfig& leveled_config,
        const engine::lsm::UniversalCompactionConfig& universal_config, 
        const engine::lsm::FIFOCompactionConfig& fifo_config,
        const engine::lsm::HybridCompactionConfig& hybrid_config,
        std::shared_ptr<engine::lsm::CompactionFilter> default_compaction_filter = nullptr

    );

    ~StorageEngine();

    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;
    StorageEngine(StorageEngine&&) = delete;
    StorageEngine& operator=(StorageEngine&&) = delete;
    SimpleDiskManager* getDiskManager() { return disk_manager_.get(); }

    void init(); // Potentially for deferred initialization
    engine::columnar::StoreSchemaManager* getSchemaManager();

     bool changeMasterPassword( // NOT static
        const std::string& old_password,
        const std::string& new_password,
        int new_kdf_iterations);

    // Transaction management
    std::shared_ptr<Transaction> beginTransaction();
    //commitTransaction
    bool commitTransaction(TxnId txn_id);
    bool validateTransactionForCommit(TxnId txn_id, std::shared_ptr<Transaction>& txn_ptr);
    void prepareWALRecords(TxnId txn_id, std::shared_ptr<Transaction> txn_ptr, std::vector<LogRecord>& wal_records_for_txn);
    bool writeToWAL(TxnId txn_id, std::shared_ptr<Transaction> txn_ptr, std::vector<LogRecord>& wal_records_for_txn, LSN& local_commit_lsn);
    void applyToMemoryStructures(TxnId txn_id, std::shared_ptr<Transaction> txn_ptr);
    void processAtomicUpdates(TxnId txn_id, std::shared_ptr<Transaction> txn_ptr);
    void applyWritesetToStores(TxnId txn_id, std::shared_ptr<Transaction> txn_ptr);
    void finalizeTransactionState(TxnId txn_id);
    void handleWALFailure(TxnId txn_id, std::shared_ptr<Transaction> txn_ptr);
    void handleMemoryApplyFailure(TxnId txn_id, std::shared_ptr<Transaction> txn_ptr, LSN local_commit_lsn);
    //end of committtransation
    bool ingestExternalFile(
        const std::string& store_path,
        const std::string& external_file_path,
        bool move_file = true
    );
    void abortTransaction(TxnId txn_id);
    TxnId getOldestActiveTxnId() const;
    bool registerStoreSchema(const engine::columnar::ColumnSchema& schema);
    engine::lsm::MemTableType default_memtable_type_;
    // Data access methods (used by Transaction)
    std::optional<Record> get(const std::string& key, TxnId txn_id);
    std::vector<Record> getPrefix(
        const std::string& prefix, 
        TxnId txn_id, 
        size_t limit = 0,
        const std::string& start_key_exclusive = "" // NEW
    );

    bool ingestColumnarFile(
        const std::string& store_path,
        const std::string& external_file_path,
        bool move_file = true
    );
    PaginatedQueryResult<Record> paginatedQuery(
        TxnId reader_txn_id,
        const std::string& storePath,
        const std::vector<FilterCondition>& filters,
        const std::vector<OrderByClause>& orderBy,
        size_t limit,
        const std::optional<std::vector<ValueType>>& startCursor,
        bool startExclusive,
        const std::optional<std::vector<ValueType>>& endCursor,
        bool endExclusive
    );

    std::shared_ptr<engine::threading::ThreadPool> getSharedCompactionPool() const;

    // Getters for internal components (mostly for testing or advanced use)
    LSMTree* getOrCreateLsmStore(const std::string& store_path);
    LSMTree* getStorage(const std::string& store_path);
    IndexManager* getIndexManager() { return index_manager_.get(); }
    SimpleBufferPoolManager* getBufferPoolManager() { return bpm_.get(); }
    // ExtWALManager* getWalManager() { return wal_manager_.get(); } //deprecated
    ShardedWALManager* getWalManager() { return wal_manager_.get(); }

    engine::columnar::ColumnarStore* getOrCreateColumnarStore(const std::string& store_path);

    // Public Checkpoint Control
    bool forceCheckpoint();
    std::vector<CheckpointInfo> getCheckpointHistory() const; // Made const

    // Encryption context getters (for BPM, LSMTree, etc. if they need direct access post-init)
    bool isEncryptionActive() const;
    EncryptionScheme getActiveEncryptionScheme() const ;
    int getKdfIterations() const ;
    const std::vector<unsigned char>& getDatabaseDEK() const ;
    const std::vector<unsigned char>& getDatabaseGlobalSalt() const ;
    void commitBatch(const std::vector<BatchOperation>& operations); // New public method
    
    // ---  Cache Stats Method ---
    std::optional<cache::CacheStats> getCacheStats() const;
    // ---  NEW Cache Stats Method ---
    /**
     * @brief Executes a query on behalf of a transaction.
     *
     * This method orchestrates the query by selecting an appropriate index,
     * scanning the B-Tree for candidate primary keys, fetching the full documents
     * from the LSM-Tree, and applying any necessary post-filters.
     *
     * @param reader_txn_id The transaction ID of the calling transaction, used for snapshot visibility.
     * @param storePath The collection path being queried.
     * @param filters The filter conditions for the query.
     * @param sortBy the filter condition for index sorting
     * @param limit The maximum number of documents to return.
     * @return A vector of matching Record objects.
     */
    std::vector<Record> query(TxnId reader_txn_id, const std::string &storePath, const std::vector<FilterCondition> &filters, const std::optional<std::pair<std::string, IndexSortOrder>> &sortBy, const std::optional<engine::columnar::AggregationPlan> &aggPlan, size_t limit);
    LSMTree* getLsmStore(const std::string& store_path);
    struct MemTableTunerStats {
        size_t current_target_size_bytes;
        double smoothed_write_rate_bps;
    };
    std::optional<MemTableTunerStats> getMemTableTunerStatsForStore(const std::string& store_path);

    struct WriteBufferManagerStats {
        size_t current_memory_usage_bytes;
        size_t buffer_size_bytes;
        size_t immutable_memtables_count; // This info comes from WBM
    };
    std::optional<WriteBufferManagerStats> getWriteBufferManagerStats() const;
    std::atomic<uint64_t> flush_count_;

    engine::lsm::LivenessOracle* getLivenessOracle() { return liveness_oracle_.get(); }

    engine::columnar::StoreSchemaManager* getSchemaManager() const { 
        return schema_manager_.get(); 
    }
};

