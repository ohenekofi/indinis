// @src/indinis_impl.cpp
#include "../include/indinis.h" // This now includes types.h and lsm_tree.h
#include "../include/index_manager.h" 
#include "../include/storage_engine.h" 
#include "../include/debug_utils.h"     // For LOG_ macros

#include <iostream>
#include <algorithm>
#include <system_error> 
#include <cerrno>     
#include <cstring>    
#include <set>        
#include <map>        
#include <vector>     
#include <string>     
#include <sstream>    
#include <stdexcept>  
#include <cstdint>     
#include <limits>      


// Only include filesystem if needed for path operations not covered by other includes.
// #include <filesystem> 
// namespace fs = std::filesystem; // Alias for convenience if used

// --- Helper Functions (Internal to this file) ---
namespace { 

    // countPathSegments: Counts non-empty segments in a path separated by '/'.
    size_t countPathSegments(const std::string& path) {
        std::string path_to_split = path;
        if (!path_to_split.empty() && path_to_split.back() == '/') {
            path_to_split.pop_back();
        }
        if (path_to_split.empty()) {
            return 0;
        }
        size_t count = 0;
        std::stringstream ss(path_to_split);
        std::string segment;
        while (std::getline(ss, segment, '/')) {
            if (!segment.empty()) {
                count++;
            }
        }
        if (count == 0 && !path_to_split.empty()) {
            count = 1;
        }
        return count;
    }

} // end anonymous namespace


// --- Transaction Method Implementations ---

Transaction::Transaction(TxnId id, LSN begin_lsn, StorageEngine& engine)
    : id_(id), 
      first_lsn_(begin_lsn), 
      engine_(engine),
      phase_(TransactionPhase::ACTIVE), // Initialize phase
      commit_lsn_(0) // Initialize commit LSN
      // committed_ and aborted_ booleans are removed
{
    LOG_TRACE("[Transaction Ctor] Created TxnID {}, BeginLSN {}, Phase: ACTIVE", id_, first_lsn_);
}

Transaction::~Transaction() {
    // Get phase under lock for safety, though destructor implies exclusive access
    TransactionPhase current_phase;
    {
        std::lock_guard<std::mutex> lock(phase_mutex_);
        current_phase = phase_;
    }

    if (current_phase != TransactionPhase::FULLY_APPLIED &&
        current_phase != TransactionPhase::COMMITTED_FINAL && // Should ideally be FULLY_APPLIED
        current_phase != TransactionPhase::ABORTED) {
        LOG_WARN("[Transaction Dtor] TxnID {} destroyed in phase {}. Forcing abort via engine.", 
                 id_, static_cast<int>(current_phase));
        try {
            // Call engine's abort. It's crucial this doesn't deadlock or throw unhandled exceptions
            // if the engine itself is also being destructed.
            // If engine_ is already invalid (e.g. StorageEngine dtor ran), this could be problematic.
            // A robust shutdown sequence in StorageEngine should handle aborting active txns.
            if (current_phase < TransactionPhase::WAL_COMMIT_DURABLE) { // Only if not durably committed
                 engine_.abortTransaction(id_); // This will try to log ABORT to WAL
            } else {
                // If WAL is durably committed but not fully applied (e.g. APPLYING_TO_MEMORY),
                // it's a complex state. Recovery will handle it. Forcing an "abort" here is misleading.
                // The Transaction object is going away, but the effects are durable or will be recovered.
                LOG_INFO("[Transaction Dtor] TxnID {} in post-WAL-commit phase {}. Data is durable or will be recovered.", 
                         id_, static_cast<int>(current_phase));
            }
        } catch (const std::exception& e) {
            LOG_ERROR("[Transaction Dtor] Exception during implicit abort for TxnID {}: {}", id_, e.what());
        } catch (...) {
            LOG_ERROR("[Transaction Dtor] Unknown exception during implicit abort for TxnID {}.", id_);
        }
    } else {
        LOG_TRACE("[Transaction Dtor] TxnID {} destroyed in completed phase {}.", id_, static_cast<int>(current_phase));
    }
}

void Transaction::update(const std::string& key, const std::unordered_map<std::string, AtomicUpdate>& operations) {
    if (!canModify()) {
        // Or throw an exception
        LOG_WARN("[Transaction Update] TxnID {}: Attempt to use transaction in non-active phase {}.",
                 id_, static_cast<int>(getPhase()));
        return;
    }
    // Merge the new operations with any existing ones for the same key.
    update_set_[key].insert(operations.begin(), operations.end());
    LOG_TRACE("[Transaction Update] TxnID {}: Added {} atomic op(s) for key '{}' to updateset.", 
              id_, operations.size(), format_key_for_print(key));
}

bool Transaction::put(const std::string& key, const ValueType& value, bool overwrite) {
    if (!canModify()) { 
        LOG_WARN("[Transaction Put] TxnID {}: Attempt to use transaction in non-active phase {}.", 
                 id_, static_cast<int>(this->getPhase()));
       // return false; 
       throw std::runtime_error("Transaction is not active and cannot be modified.");
    }

    // --- NEW: Overwrite Check Logic ---
    if (!overwrite) {
        auto ws_it = write_set_.find(key);
        if (ws_it != write_set_.end() && !ws_it->second.deleted) {
            // *** THE FIX: Throw a specific, structured error ***
            throw storage::StorageError(storage::ErrorCode::DUPLICATE_KEY,
                "Failed to create document: A document with this ID already exists in the current transaction's writeset.")
                .withContext("key", key);
        }

        std::optional<Record> existing_record = engine_.get(key, id_);
        if (existing_record.has_value()) {
            // *** THE FIX: Throw a specific, structured error ***
            throw storage::StorageError(storage::ErrorCode::DUPLICATE_KEY,
                "Failed to create document: A document with this ID already exists.")
                .withContext("key", key);
        }
    }
    // --- END NEW ---

    Record record{
        key,
        value,
        id_, 
        0,   
        std::chrono::system_clock::now(),
        false 
    };
    write_set_[key] = record;
    LOG_TRACE("[Transaction Put] TxnID {}: Key '{}' added/updated in writeset (overwrite: {}).", id_, format_key_for_print(key), overwrite);
    return true;
}

std::optional<Record> Transaction::get(const std::string& key) {
    LOG_TRACE("[Transaction GET] START. TxnID: {}, Key: '{}'", id_, format_key_for_print(key));
    if (isAborted()) {
        LOG_TRACE("  [Transaction GET] TxnID {}: Aborted. Returning std::nullopt.", id_);
        return std::nullopt;
    }

    auto it = write_set_.find(key);
    if (it != write_set_.end()) {
        const Record& rec_in_ws = it->second;
        LOG_TRACE("  [Transaction GET] TxnID {}: Key '{}' found in writeset. Deleted: {}, ValueType index: {}",
                  id_, format_key_for_print(key), rec_in_ws.deleted, rec_in_ws.value.index());
        return rec_in_ws.deleted ? std::nullopt : std::optional<Record>(rec_in_ws);
    }

    LOG_TRACE("  [Transaction GET] TxnID {}: Key '{}' not in writeset. Querying engine.", id_, format_key_for_print(key));
    std::optional<Record> engine_record_opt = engine_.get(key, id_); // StorageEngine::get logs extensively
    
    if (engine_record_opt) {
        LOG_TRACE("  [Transaction GET] TxnID {}: Engine returned a record for key '{}'. Deleted: {}, ValueType index: {}",
                  id_, format_key_for_print(key), engine_record_opt->deleted, engine_record_opt->value.index());
    } else {
        LOG_TRACE("  [Transaction GET] TxnID {}: Engine returned std::nullopt for key '{}'.", id_, format_key_for_print(key));
    }
    LOG_TRACE("[Transaction GET] END. TxnID: {}. Returning record_has_value: {}", id_, engine_record_opt.has_value());
    return engine_record_opt;
}


std::vector<Record> Transaction::mergePrefixResults(
    const std::vector<Record>& engine_results,
    const std::string& prefix,
    size_t limit)
{
    // (Implementation of mergePrefixResults remains unchanged from previous version,
    // as it primarily deals with logic of merging writeset with engine results
    // based on prefix and segment counts. It doesn't directly involve transaction phase logic.)
    // ... (keep existing mergePrefixResults logic)
    std::map<std::string, Record> merged_map; 
    std::set<std::string> deleted_in_txn;

    const size_t prefix_segments = countPathSegments(prefix);
    const size_t expected_key_segments = prefix_segments + 1;

    for (const auto& [ws_key, ws_record] : write_set_) {
        if (ws_key.size() >= prefix.size() && ws_key.compare(0, prefix.size(), prefix) == 0) {
            if (countPathSegments(ws_key) == expected_key_segments) {
                 if (ws_record.deleted) {
                     deleted_in_txn.insert(ws_key);
                     merged_map.erase(ws_key); 
                 } else {
                     merged_map[ws_key] = ws_record;
                     deleted_in_txn.erase(ws_key); 
                 }
            }
        }
    }
    for (const auto& engine_record : engine_results) {
         if (merged_map.find(engine_record.key) == merged_map.end() &&
             deleted_in_txn.find(engine_record.key) == deleted_in_txn.end())
         {
              merged_map[engine_record.key] = engine_record;
         }
    }
    std::vector<Record> final_results;
    final_results.reserve(merged_map.size());
    for (auto const& [map_key, map_val] : merged_map) { 
        if (limit > 0 && final_results.size() >= limit) {
             break; 
        }
        final_results.push_back(map_val);
    }
    return final_results;
}

std::vector<Record> Transaction::getPrefix(const std::string& prefix, size_t limit) {
    if (isAborted()) {
        LOG_TRACE("[Transaction GetPrefix] TxnID {}: Transaction aborted, getPrefix for '{}' returns empty.", id_, prefix);
        return {}; 
    }

    std::string collectionPrefix = prefix;
    if (collectionPrefix.empty() || collectionPrefix.back() != '/') {
        collectionPrefix += '/';
    }
    LOG_TRACE("[Transaction GetPrefix] TxnID {}: Prefix='{}', Limit={}. Effective collectionPrefix='{}'", 
              id_, prefix, limit, collectionPrefix);

    // ===  ===
    // The N-API wrapper layer is responsible for catching exceptions.
    std::vector<Record> engine_results = engine_.getPrefix(collectionPrefix, id_, 0); 
    return mergePrefixResults(engine_results, collectionPrefix, limit);
    // =========================================
}

bool Transaction::remove(const std::string& key) {
    if (!canModify()) { 
        // CORRECTED: Use this->getPhase() or just getPhase() for the member function call
        LOG_WARN("[Transaction Remove] TxnID {}: Attempt to use transaction in non-active phase {}.",
                 id_, static_cast<int>(this->getPhase())); // or just getPhase()
        return false;
    }

    std::optional<Record> current_visible_record;
    auto it_ws = write_set_.find(key);
    if (it_ws != write_set_.end()) {
        if (!it_ws->second.deleted) {
            current_visible_record = it_ws->second;
        } else {
            LOG_TRACE("[Transaction Remove] TxnID {}: Key '{}' already marked deleted in writeset.", id_, format_key_for_print(key));
            return false; 
        }
    } else {
        current_visible_record = engine_.get(key, id_); 
    }

    if (!current_visible_record) {
        LOG_TRACE("[Transaction Remove] TxnID {}: Key '{}' not found (or already deleted in snapshot). Cannot remove.", id_, format_key_for_print(key));
        return false; 
    }

    Record tombstone{
        key,
        current_visible_record->value, 
        id_,
        0, 
        std::chrono::system_clock::now(),
        true 
    };
    write_set_[key] = tombstone;
    LOG_TRACE("[Transaction Remove] TxnID {}: Key '{}' marked for deletion in writeset.", id_, format_key_for_print(key));
    return true;
}
// commit() and abort() are called by StorageEngine, which manages phase transitions.
// The Transaction object's own commit/abort methods would mostly just update its phase
// if they were to be public and callable, but the real work is in StorageEngine.
// The `force_abort_internal` is used by StorageEngine.

std::vector<Record> Transaction::query(
    const std::string& storePath, 
    const std::vector<FilterCondition>& filters, 
    const std::optional<std::pair<std::string, IndexSortOrder>>& sortBy,
    const std::optional<engine::columnar::AggregationPlan>& aggPlan, // <<< NEW
    size_t limit) 
{
    if (isAborted()) {
        LOG_WARN("[Transaction Query] TxnID {}: Attempt to query on an aborted transaction.", id_);
        return {}; // Return empty vector if transaction is aborted
    }

    LOG_TRACE("[Transaction Query] TxnID {}: Delegating query for store '{}' to StorageEngine.", id_, storePath);
    
    // The transaction's role is to pass its context (its own ID for snapshot reads) and the query
    // parameters to the engine. The engine handles the complex parts like index selection and data fetching.
    // The call to engine_.query() now includes the sortBy parameter.
    return engine_.query(id_, storePath, filters, sortBy, aggPlan, limit);
}

PaginatedQueryResult<Record> Transaction::paginatedQuery(
    const std::string& storePath,
    const std::vector<FilterCondition>& filters,
    const std::vector<OrderByClause>& orderBy,
    size_t limit,
    const std::optional<std::vector<ValueType>>& startCursor,
    bool startExclusive,
    const std::optional<std::vector<ValueType>>& endCursor,
    bool endExclusive)
{
    // The transaction's primary role is to provide its ID (this->id_)
    // to the engine for MVCC.
    return engine_.paginatedQuery(
        this->id_, // Pass the transaction's own ID for snapshot isolation
        storePath,
        filters,
        orderBy,
        limit,
        startCursor,
        startExclusive,
        endCursor,
        endExclusive
    );
}