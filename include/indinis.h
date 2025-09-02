// @include/indinis.h
#pragma once

#pragma once

#include "types.h"      // Includes Record, TxnId, IndexDef, etc.
// #include "lsm_tree.h"   // Not needed directly by Transaction
// #include "btree.h"      // Not needed directly by Transaction
// #include "index_manager.h" // Not needed directly by Transaction
// #include "storage_engine.h" // REMOVE this include
#include "columnar/column_types.h"

#include <string>
#include <vector>
#include <unordered_map>
// #include <mutex>          // Not needed by Transaction directly
// #include <atomic>         // Not needed by Transaction directly
#include <memory>           // For shared_ptr (used by StorageEngine return type)
#include <optional>
// #include <filesystem>     // Not needed by Transaction directly
// #include <utility>        // Not needed by Transaction directly
// #include <thread>         // Not needed by Transaction directly

// Forward declarations (LSMTree is now included)
class Transaction;
class StorageEngine;
// class LSMTree; // No longer needed
// class BTree;
class IndexManager; // <<< ADD Forward Declaration

// NOTE: IndexSortOrder, IndexField, IndexDefinition, TxnId, ValueType, Record, VersionedRecord
// are now defined in types.h
enum class TransactionPhase {
    ACTIVE,             // Normal operational state
    WAL_COMMIT_WRITTEN, // COMMIT_TXN record written to WAL buffer, maybe not flushed
    WAL_COMMIT_DURABLE, // COMMIT_TXN record is durable on disk
    APPLYING_TO_MEMORY, // Changes are being applied to LSM/Indexes
    FULLY_APPLIED,      // Changes applied to LSM/Indexes, can be removed from active list
    ABORTED,
    COMMITTED_FINAL     // Legacy state, might be replaced by FULLY_APPLIED
};


// Transaction class for MVCC
class Transaction {
private:
    TxnId id_;
    StorageEngine& engine_;
    std::unordered_map<std::string, Record> write_set_;
    //bool committed_ = false;
    //bool aborted_ = false;
    TransactionPhase phase_ = TransactionPhase::ACTIVE; // <<< NEW
    mutable std::mutex phase_mutex_; // <<< NEW: Protects concurrent access to phase_ if needed, though likely set serially by StorageEngine
    LSN first_lsn_ = 0;
    LSN commit_lsn_ = 0; // <<< NEW: Store the LSN of the COMMIT_TXN record
    std::unordered_map<std::string, std::unordered_map<std::string, AtomicUpdate>> update_set_;

    std::vector<Record> mergePrefixResults(
        const std::vector<Record>& engine_results,
        const std::string& prefix,
        size_t limit);

public:
    Transaction(TxnId id, LSN begin_lsn, StorageEngine& engine); 
    LSN getFirstLsn() const { return first_lsn_; }
    ~Transaction();

    LSN getCommitLsn() const { std::lock_guard<std::mutex> lock(phase_mutex_); return commit_lsn_; } // <<< NEW
    void setCommitLsn(LSN lsn) { std::lock_guard<std::mutex> lock(phase_mutex_); commit_lsn_ = lsn; } // <<< NEW

    TxnId getId() const { return id_; }
       TransactionPhase getPhase() const { 
        std::lock_guard<std::mutex> lock(phase_mutex_); 
        return phase_; 
    }
    void setPhase(TransactionPhase new_phase) { 
        std::lock_guard<std::mutex> lock(phase_mutex_); 
        phase_ = new_phase; 
    }

    bool isCommitted() const { std::lock_guard<std::mutex> lock(phase_mutex_); return phase_ == TransactionPhase::COMMITTED_FINAL || phase_ == TransactionPhase::FULLY_APPLIED; }
    bool isAborted() const { std::lock_guard<std::mutex> lock(phase_mutex_); return phase_ == TransactionPhase::ABORTED; }
    bool canModify() const { std::lock_guard<std::mutex> lock(phase_mutex_); return phase_ == TransactionPhase::ACTIVE; }


    bool put(const std::string& key, const ValueType& value, bool overwrite = false);
    std::optional<Record> get(const std::string& key);
    std::vector<Record> getPrefix(const std::string& prefix, size_t limit = 0);
    bool remove(const std::string& key);
    void update(const std::string& key, const std::unordered_map<std::string, AtomicUpdate>& operations);

   void force_abort_internal() { 
        std::lock_guard<std::mutex> lock(phase_mutex_);
        phase_ = TransactionPhase::ABORTED;
        write_set_.clear(); // Keep clearing write_set on abort
    }

    /**
     * @brief Executes a structured query against the database.
     * @param storePath The collection path to query.
     * @param filters A vector of filter conditions to apply.
     * @param sortBy Optional. The field and direction to sort the results by.
     * @param limit The maximum number of documents to return (0 for no limit).
     * @return A vector of matching Record objects.
     */

    std::vector<Record> query(
        const std::string& storePath,
        const std::vector<FilterCondition>& filters,
        const std::optional<std::pair<std::string, IndexSortOrder>>& sortBy,
        const std::optional<engine::columnar::AggregationPlan>& aggPlan, // <<< NEW
        size_t limit
    );
    //bool commit();
    //void abort();

    PaginatedQueryResult<Record> paginatedQuery(
        const std::string& storePath,
        const std::vector<FilterCondition>& filters,
        const std::vector<OrderByClause>& orderBy,
        size_t limit,
        // It needs all cursor types to be passed down
        const std::optional<std::vector<ValueType>>& startCursor,
        bool startExclusive,
        const std::optional<std::vector<ValueType>>& endCursor,
        bool endExclusive
    );

    friend class StorageEngine;
    friend class TransactionWrapper;
};

