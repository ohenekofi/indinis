// include/lsm_tree.h
#pragma once

#include "lsm/interfaces.h"
#include "lsm/compaction_job.h"
#include "lsm/sstable_meta.h"
#include "lsm/version.h"
#include "types.h"
#include "bloom_filter.h"
#include "debug_utils.h"
#include "lsm/arena.h"
#include "lsm/compaction_strategy.h" 
#include "lsm/skiplist.h"
#include "lsm/memtable_tuner.h"
#include "lsm/write_buffer_manager.h"
#include "lsm/partitioned_memtable.h"
#include "threading/thread_pool.h"
#include "lsm/leveled_compaction_strategy.h"
#include "lsm/universal_compaction_strategy.h"
#include "lsm/fifo_compaction_strategy.h"
#include "lsm/hybrid_compaction_strategy.h"
#include "threading/rate_limiter.h" 
#include "lsm/compaction_filter.h" 


#include <string>
#include <vector>
#include <map>
#include <set>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <shared_mutex>
#include <memory>
#include <optional>
#include <filesystem>
#include <fstream>
#include <limits>
#include <thread>
#include <cstdint>
#include <queue>
#include <condition_variable>
#include <algorithm>
#include <cmath>

// Forward declarations
class SSTableReader;
class StorageEngine;
class CompactionManagerForIndinisLSM;
struct CompactionJobForIndinis;
class Transaction;

namespace engine::lsm {
    class SSTableBuilder; // Forward-declare the builder
    // ...
}

namespace engine {
namespace lsm {
    class VersionSet;
    struct LeveledCompactionConfig;
    struct SubCompactionTask; 
}
}

class LSMTree : public engine::lsm::IFlushable {
    friend class SSTableReader;
    friend class CompactionManagerForIndinisLSM;
    friend class LeveledCompactionStrategy; 
    friend class UniversalCompactionStrategy;
    friend class FIFOCompactionStrategy;

public:

 // Public Structs for Stats
    struct PartitionStatsSnapshot {
        uint64_t partition_id;
        uint64_t read_count;
        uint64_t write_count;
        size_t current_size_bytes;
    };

    struct MemTableStatsSnapshot {
        size_t total_size_bytes;
        size_t record_count;
        std::vector<PartitionStatsSnapshot> partition_stats;
    };

    struct VersionStatsSnapshot {
        uint64_t current_version_id;
        int live_versions_count;
        std::vector<size_t> sstables_per_level;
    };

    // Constructor
    LSMTree(
        const engine::lsm::PartitionedMemTable::Config& config,
        const std::string& data_dir_param,
        size_t memtable_max_size_param,
        size_t sstable_target_uncompressed_block_size_param,
        CompressionType sstable_compression_type_param,
        int sstable_compression_level_param,
        bool overall_encryption_enabled_passed,
        EncryptionScheme scheme_for_new_blocks_passed,
        const std::vector<unsigned char>& dek_passed,
        const std::vector<unsigned char>& global_salt_passed,
        size_t num_flush_threads,
        std::shared_ptr<engine::threading::ThreadPool> compaction_pool,
        std::shared_ptr<engine::lsm::WriteBufferManager> write_buffer_manager,
        std::shared_ptr<engine::threading::TokenBucketRateLimiter> rate_limiter,
        engine::lsm::CompactionStrategyType strategy_type ,
        const engine::lsm::LeveledCompactionConfig& leveled_config,
        const engine::lsm::UniversalCompactionConfig& universal_config,
        const engine::lsm::FIFOCompactionConfig& fifo_config,
        const engine::lsm::HybridCompactionConfig& hybrid_config,
        std::shared_ptr<engine::lsm::CompactionFilter> compaction_filter // <-- Add new parameter


    );

    // Destructor
    ~LSMTree();
    
    // Public API
    void setCompactionFilter(std::shared_ptr<engine::lsm::CompactionFilter> filter);
    void put(const std::string& key, const Record& record, TxnId oldest_active_txn_id);
    std::optional<Record> get(const std::string& key, TxnId txn_id);
    std::vector<Record> getPrefix(const std::string& prefix, TxnId txn_id, size_t limit = 0, const std::string& start_key_exclusive = "");
    void remove(const std::string& key, TxnId txn_id);
    void setStorageEngine(StorageEngine* engine);
    StorageEngine* getStorageEngine() const { return storage_engine_ptr_; }
    void BeginTxnRead(Transaction* tx_ptr);
    void EndTxnRead(Transaction* tx_ptr);

    void ingestFile(
        std::shared_ptr<engine::lsm::SSTableMetadata> file_metadata,
        const std::string& file_path,
        bool move_file
    );

    // Management & Lifecycle
    void startCompactionAndFlushThreads();
    void stopCompactionAndFlushThreads();
    void loadSSTablesMetadata();
    void flushActiveMemTable(bool synchronous = false);

    // Private Structs
    struct MemTable {
        engine::lsm::Arena arena_;
        engine::lsm::SkipList list_;
        std::atomic<size_t> estimated_size_bytes_ = 0;
        std::atomic<size_t> num_records_ = 0;
        std::atomic<TxnId> min_txn_id;
        std::atomic<TxnId> max_txn_id;

        MemTable()
            : arena_(),
            list_(arena_),
            min_txn_id(std::numeric_limits<TxnId>::max()),
            max_txn_id(0)
        {}

        // Explicitly delete copy constructor and copy assignment
        MemTable(const MemTable&) = delete;
        MemTable& operator=(const MemTable&) = delete;

        // Allow move constructor and move assignment if needed
        MemTable(MemTable&&) = default;
        MemTable& operator=(MemTable&&) = default;

        size_t ApproximateMemoryUsage() const {
            return arena_.MemoryUsage();
        }
    };

    // Compaction and SSTable Processing
    bool execute_compaction_job(
        int level_L,
        std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> sstables_L,
        std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> sstables_L_plus_1
    );

    std::vector<uint8_t> readAndProcessSSTableBlock(
        std::ifstream& file_stream,
        const SSTableBlockIndexEntry& block_meta,
        const std::string& sstable_filename_for_context
    ) const;

    void TriggerFlush() override;

    // Public Stats Getters
    MemTableStatsSnapshot getActiveMemTableStats() const;
    uint64_t getFlushCount() const;
    engine::lsm::MemTableTuner* getTuner() const;
    VersionStatsSnapshot getVersionStats() const;

    // Serialization and Utility Methods
    static std::string GetMemTableLogInfo(const std::shared_ptr<MemTable>& mt_ptr, const  char* context_label);
    static void serializeRecord(std::ostream& stream, const Record& record);
    static std::optional<Record> deserializeRecord(std::ifstream& file);
    static std::optional<Record> deserializeRecordFromBuffer(const uint8_t*& current_ptr, const uint8_t* end_ptr);
    static std::optional<std::string> deserializeKey(std::ifstream& file);
    static bool skipRecordValueAndMeta(std::ifstream& file);
    std::shared_ptr<engine::lsm::PartitionedMemTable> getActivePartitionedMemTable() const;
    std::shared_ptr<engine::lsm::PartitionedMemTable> RebalanceAndSwapActiveMemTable(
        std::shared_ptr<engine::lsm::PartitionedMemTable> memtable_to_rebalance
    );

    struct MergeHeapEntry {
        Record record;
        size_t reader_index;
        bool operator>(const MergeHeapEntry& other) const {
            if (record.key != other.record.key) {
                return record.key > other.record.key;
            }
            return record.commit_txn_id < other.record.commit_txn_id;
        }
    };


    std::pair<std::string, std::string> compute_key_range(
        const std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>& tables
    ) const;

    std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> find_overlapping_sstables(
        const std::vector<std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>>& levels_snapshot,
        int target_level,
        const std::string& min_key,
        const std::string& max_key
    );

    std::shared_ptr<engine::lsm::SSTableMetadata> validateAndLoadExternalSSTable(
        const std::string& filepath
    ) const;

    uint64_t calculate_level_size_bytes(const std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>& level_tables) const;
    // uint64_t calculate_target_size_for_level(int level_idx) const;
    
    // double calculate_compaction_score(const engine::lsm::SSTableMetadata& sstable) const;
    // bool is_compaction_worthwhile(const std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>& source_tables, const std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>& target_tables) const;

    static constexpr size_t L0_SSTABLE_COUNT_TRIGGER = 4;
    static constexpr size_t SSTABLE_TARGET_MAX_SIZE_BYTES = 32 * 1024 * 1024;
    static constexpr double LEVEL_SIZE_MULTIPLIER = 10.0;

    static constexpr int MAX_LEVELS = 7;

    /**
     * @brief Atomically applies a set of metadata changes (new/deleted SSTables)
     *        to the LSM-Tree's version set.
     * @param edit The VersionEdit describing the changes to apply.
     */
    void applyAndPersistVersionEdit(const engine::lsm::VersionEdit& edit);
    void notifyScheduler();
    uint64_t getNextSSTableFileId() const;

private:


    struct CompactionState {
        int level_L;
        int target_output_level;
        TxnId effective_gc_txn_id;
        std::vector<std::unique_ptr<SSTableReader>> readers;
        std::priority_queue<MergeHeapEntry, std::vector<MergeHeapEntry>, std::greater<MergeHeapEntry>> min_heap;
        std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> new_output_files_metadata;
        std::shared_ptr<engine::lsm::SSTableMetadata> current_output_sstable_meta;
        std::string temp_output_filename;
        std::ofstream output_file_stream;
        std::unique_ptr<engine::lsm::SSTableBuilder> builder;
        uint64_t current_sstable_uncompressed_size = 0;
        uint32_t records_in_current_block = 0;
        std::vector<uint8_t> current_block_buffer;
        std::string first_key_in_current_block;
        std::map<std::string, SSTableBlockIndexEntry> l1_block_index_temp;
        std::unique_ptr<BloomFilter> current_filter_partition;
        std::string first_key_in_current_filter_partition;
        int blocks_in_current_filter_partition = 0;
    };
/*
    void flushFilterPartition(
        std::ofstream& output_stream,
        std::shared_ptr<engine::lsm::SSTableMetadata>& sstable_meta,
        std::unique_ptr<BloomFilter>& current_partition,
        std::string& first_key_in_partition,
        int& blocks_in_partition
    );*/
    struct SSTableFileInfo {
        std::string full_path;
        int level;
        uint64_t sstable_id;
    };

    struct PrefixScanState {
        const std::string& prefix;
        TxnId reader_txn_id;
        size_t limit;
        const std::string& start_key_exclusive;
        std::map<std::string, Record> results_map;
        std::set<std::string> deleted_keys_tracker;

        bool isLimitReached() const {
            return limit > 0 && results_map.size() >= limit;
        }
    };

    struct LookupResult {
        std::optional<Record> record;
        bool found_definitive_tombstone = false;
    };

    std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> execute_sub_compaction(
        const engine::lsm::SubCompactionTask& task,
        int target_level,
        TxnId oldest_active_txn
    );

    // Private Members
    std::shared_ptr<engine::lsm::CompactionFilter> compaction_filter_;
    std::unique_ptr<engine::lsm::VersionSet> versions_;
    std::shared_ptr<engine::lsm::PartitionedMemTable> active_memtable_;
    std::vector<std::shared_ptr<engine::lsm::PartitionedMemTable>> immutable_memtables_;
    std::unique_ptr<CompactionManagerForIndinisLSM> compaction_manager_;
    std::shared_ptr<engine::lsm::WriteBufferManager> write_buffer_manager_;
    std::unique_ptr<engine::lsm::MemTableTuner> memtable_tuner_;
    StorageEngine* storage_engine_ptr_ = nullptr;
    std::atomic<uint64_t> next_sstable_file_id_{1};
    std::atomic<size_t> flush_count_{0};
    const engine::lsm::PartitionedMemTable::Config memtable_config_;

    std::unique_ptr<engine::lsm::CompactionStrategy> compaction_strategy_;

    std::string data_dir_;
    size_t memtable_max_size_;
    bool sstable_encryption_is_active_ = false;
    EncryptionScheme sstable_default_encryption_scheme_ = EncryptionScheme::NONE;
    std::vector<unsigned char> database_dek_;
    std::vector<unsigned char> database_global_salt_;
    size_t sstable_target_uncompressed_block_size_;
    CompressionType sstable_compression_type_;
    int sstable_compression_level_;
    static constexpr double DEFAULT_BLOOM_FILTER_FPR = 0.01;

    mutable std::shared_mutex memtable_data_mutex_; // Renamed from memtable_mutex_
    mutable std::mutex memtable_cv_mutex_;  // New mutex for condition variables
    std::atomic<bool> scheduler_running_{false};
    std::thread scheduler_thread_;
    std::condition_variable scheduler_cv_;
    mutable std::mutex scheduler_mutex_;


    // static constexpr int MAX_LEVELS = 7;
    static constexpr size_t DEFAULT_COMPACTION_WORKER_THREADS = 2;
    std::chrono::seconds scheduler_interval_ = std::chrono::seconds(5);

    static constexpr size_t DEFAULT_FLUSH_WORKER_THREADS = 1;
    static constexpr size_t MAX_IMMUTABLE_MEMTABLES = 2;
    std::vector<std::thread> flush_worker_threads_;
    std::atomic<bool> flush_workers_shutdown_{false};
    std::condition_variable immutable_memtable_ready_cv_;
    std::condition_variable immutable_slot_available_cv_;

    // Private Methods
    // std::optional<CompactionJobForIndinis> select_compaction_candidate();
    // std::optional<CompactionJobForIndinis> try_select_l0_compaction(const std::vector<std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>>& levels_snapshot);
    // std::optional<CompactionJobForIndinis> try_select_level_compaction(const std::vector<std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>>& levels_snapshot);
    // std::optional<CompactionJobForIndinis> select_compaction_candidate_for_level(
       // const std::vector<std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>>& levels_snapshot, 
       // int level_idx
   // );
    
    void scanSSTableForPrefix(
        const std::shared_ptr<engine::lsm::SSTableMetadata>& sstable_meta,
        PrefixScanState& state // Pass state by reference to be modified
    ) const;

    std::shared_ptr<engine::threading::TokenBucketRateLimiter> compaction_rate_limiter_;
    std::optional<Record> searchSSTables(const std::string& key, TxnId reader_txn_id);
    std::optional<Record> searchLevel0(const std::vector<std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>>& levels_snapshot, const std::string& key, TxnId reader_txn_id);
    std::optional<Record> searchLevelN(const std::vector<std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>>& levels_snapshot, int level_idx, const std::string& key, TxnId reader_txn_id);
    std::optional<Record> findRecordInSSTable(const std::shared_ptr<engine::lsm::SSTableMetadata>& sstable_meta, const std::string& key, TxnId reader_txn_id);

    std::shared_ptr<engine::lsm::SSTableMetadata> createSSTableMetadataForFlush(const std::shared_ptr<engine::lsm::PartitionedMemTable>& memtable_to_flush, int target_level);
    void writeMemTableToTempSSTable(const std::shared_ptr<engine::lsm::PartitionedMemTable>& memtable_to_flush, std::shared_ptr<engine::lsm::SSTableMetadata>& sstable_metadata, const std::string& temp_filename);
    void finalizeSSTableFlush(std::shared_ptr<engine::lsm::SSTableMetadata>& sstable_metadata, const std::string& temp_filename);
    //void flushActiveMemTable(bool synchronous = false);
    /*
    void writeIndexAndFooter(
        std::ofstream& output_file,
        std::shared_ptr<engine::lsm::SSTableMetadata>& table_metadata, // Note: non-const to update size
        const std::map<std::string, SSTableBlockIndexEntry>& l1_block_index
    );*/
    std::vector<SSTableFileInfo> discoverSSTableFiles(uint64_t& max_sstable_id_found) const;
    std::shared_ptr<engine::lsm::SSTableMetadata> loadSingleSSTableMetadata(const SSTableFileInfo& file_info) const;
    void organizeLoadedSSTables(std::map<int, std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>>& temp_levels_from_disk) const;

    void setupCompactionState(
        CompactionState& state, // Note: Non-const reference
        const std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>& sstables_L,
        const std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>& sstables_L_plus_1
    );    void processCompactionMergeLoop(CompactionState& state);
    void writeCompactedRecord(CompactionState& state, const Record& record_to_write);
    void startNewCompactionOutputFile(CompactionState& state);
    void finalizeCurrentCompactionOutputFile(CompactionState& state);
    void finalizeCompactionJob(
        const CompactionState& state, // Note: Const reference is correct here
        const std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>& sstables_L,
        const std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>& sstables_L_plus_1
    );
    std::vector<uint8_t> readBlockHeaderAndPayload(
        std::ifstream& file_stream,
        const SSTableBlockIndexEntry& block_meta,
        SSTableDataBlockHeader& out_header,
        const std::string& sstable_filename_for_context
    ) const;

    std::vector<uint8_t> decryptBlockData(
        const SSTableDataBlockHeader& block_header,
        std::vector<uint8_t>&& encrypted_payload
    ) const;

    std::vector<uint8_t> decompressBlockData(
        const SSTableDataBlockHeader& block_header,
        std::vector<uint8_t>&& compressed_payload
    ) const;

    std::string generateSSTableFilename(int level, uint64_t id);
    /*
    void writeSSTableDataBlock(
        std::ofstream& file_stream,
        const std::vector<uint8_t>& uncompressed_block_content,
        uint32_t num_records_in_block_param,
        SSTableBlockIndexEntry& out_block_index_entry,
        const std::string& sstable_filename_for_context
    );
*/
    bool flushImmutableMemTableToFile(std::shared_ptr<engine::lsm::PartitionedMemTable> memtable_to_flush);
    void background_flush_worker_loop();
    TxnId getOldestActiveTxnId();
    std::vector<Record> processKeyVersionsAndGC(
        const std::vector<Record>& versions_for_key,
        TxnId effective_oldest_active_txn_id_for_gc,
        const std::string& key
    );

    void scheduler_thread_loop();
    void check_and_schedule_compactions();

    void scanMemTableForPrefix(const MemTable& memtable, const std::string& prefix, TxnId reader_txn_id, size_t limit, const std::string& start_key_exclusive, std::map<std::string, Record>& results, std::set<std::string>& deleted_keys);
    size_t estimateRecordSize(const std::string& key, const Record& record) const;
    void waitForImmutableSlot();
    void addToActiveMemTable(const std::string& key, const Record& record, size_t estimated_size, TxnId oldest_active_txn_id);
    void notifyFlushWorker();
    LookupResult searchMemTables(const std::string& key, TxnId reader_txn_id);
    std::shared_ptr<engine::lsm::PartitionedMemTable> swapActiveMemTable();
    void scanMemTablesForPrefix(PrefixScanState& state);
    void scanSSTablesForPrefix(PrefixScanState& state);
};

/*
struct CompactionJobForIndinis {
    int level_L;
    std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> sstables_L;
    std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> sstables_L_plus_1;

    CompactionJobForIndinis(
        int l,
        std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> s_l,
        std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> s_l_plus_1
    )
        : level_L(l),
          sstables_L(std::move(s_l)),
          sstables_L_plus_1(std::move(s_l_plus_1)) {}
};
*/
