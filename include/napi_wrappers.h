// @include/napi_wrappers.h
#ifndef NAPI_WRAPPERS_H
#define NAPI_WRAPPERS_H

#include <napi.h>
#include <memory>
#include <string>
#include <vector>

// Include the header that defines OrderByClause, FilterCondition, ValueType, etc.
#include "types.h"

// Forward declarations of the main C++ classes
class StorageEngine;
class Transaction;
class TransactionWrapper;

// --- IndinisWrapper ---
// Main engine wrapper class exposed to JS as 'Indinis'
class IndinisWrapper : public Napi::ObjectWrap<IndinisWrapper> {
public:
    static Napi::FunctionReference transactionWrapperConstructor; 
    static Napi::Object Init(Napi::Env env, Napi::Object exports); 
    IndinisWrapper(const Napi::CallbackInfo& info); 
    ~IndinisWrapper(); 

    StorageEngine* GetInternal();
    std::shared_ptr<StorageEngine> GetInternalSharedPtr();

    // NAPI Methods
    Napi::Value BeginTransaction(const Napi::CallbackInfo& info); 
    Napi::Value CreateIndex(const Napi::CallbackInfo& info);      
    Napi::Value DeleteIndex(const Napi::CallbackInfo& info);      
    Napi::Value ListIndexes(const Napi::CallbackInfo& info);      
    Napi::Value Close(const Napi::CallbackInfo& info);            
    Napi::Value DebugScanBTree(const Napi::CallbackInfo& info); 
    Napi::Value ForceCheckpoint(const Napi::CallbackInfo& info);
    Napi::Value GetCheckpointHistory(const Napi::CallbackInfo& info);
    Napi::Value ChangeDatabasePassword(const Napi::CallbackInfo& info);
    Napi::Value GetCacheStats(const Napi::CallbackInfo& info);
    Napi::Value DebugVerifyFreeList(const Napi::CallbackInfo& info);
    Napi::Value DebugAnalyzeFragmentation(const Napi::CallbackInfo& info);
    Napi::Value DebugDefragment(const Napi::CallbackInfo& info);
    Napi::Value DebugGetDatabaseStats(const Napi::CallbackInfo& info);
    Napi::Value RegisterStoreSchema(const Napi::CallbackInfo& info);
    Napi::Value DebugGetLsmStoreStats(const Napi::CallbackInfo& info);
    Napi::Value DebugGetMemTableTunerStats(const Napi::CallbackInfo& info);
    Napi::Value DebugGetWriteBufferManagerStats(const Napi::CallbackInfo& info);
    Napi::Value DebugGetLsmFlushCount(const Napi::CallbackInfo& info);
    Napi::Value DebugGetLsmVersionStats(const Napi::CallbackInfo& info);
    Napi::Value DebugGetLsmRcuStats(const Napi::CallbackInfo& info);
    Napi::Value DebugPauseRcuReclamation(const Napi::CallbackInfo& info);
    Napi::Value DebugResumeRcuReclamation(const Napi::CallbackInfo& info);
    Napi::Value DebugGetThreadPoolStats(const Napi::CallbackInfo& info);
    Napi::Value IngestExternalFile(const Napi::CallbackInfo& info);
    Napi::Value DebugGetSSTableBuilder(const Napi::CallbackInfo& info);
    Napi::Value DebugSerializeValue(const Napi::CallbackInfo& info);
    Napi::Value IngestColumnarFile(const Napi::CallbackInfo& info);
    Napi::Value DebugGetColumnarStoreStats(const Napi::CallbackInfo& info);
    Napi::Value DebugForceColumnarCompaction(const Napi::CallbackInfo& info);
    Napi::Value CommitBatch(const Napi::CallbackInfo& info); 

private:
    std::shared_ptr<StorageEngine> storage_engine_sp_;
    std::string data_dir_path_key_;
    bool is_closed_ = false;  
};

// --- TransactionWrapper ---
// Internal wrapper for C++ Transaction objects
class TransactionWrapper : public Napi::ObjectWrap<TransactionWrapper> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    TransactionWrapper(const Napi::CallbackInfo& info);
    ~TransactionWrapper();

    void SetTransactionAndEngine(std::shared_ptr<Transaction> txn_ptr, IndinisWrapper* engine_wrapper_ptr);
    std::shared_ptr<Transaction> GetInternalTransaction();

    // NAPI Methods
    Napi::Value Put(const Napi::CallbackInfo& info);
    Napi::Value Get(const Napi::CallbackInfo& info);
    Napi::Value GetPrefix(const Napi::CallbackInfo& info);
    Napi::Value Remove(const Napi::CallbackInfo& info);
    Napi::Value Commit(const Napi::CallbackInfo& info);
    Napi::Value Abort(const Napi::CallbackInfo& info);
    Napi::Value GetId(const Napi::CallbackInfo& info);
    Napi::Value Query(const Napi::CallbackInfo& info); 
    Napi::Value Update(const Napi::CallbackInfo& info);
    Napi::Value PaginatedQuery(const Napi::CallbackInfo& info);

private:
    std::shared_ptr<Transaction> txn_;
    IndinisWrapper* engine_wrapper_ = nullptr;
    std::vector<OrderByClause> parseOrderBy(const Napi::Array& jsOrderBy);
    std::vector<FilterCondition> parseFilters(const Napi::Array& jsFilters);
    std::vector<ValueType> parseCursor(const Napi::Array& jsCursor);
};  

#endif // NAPI_WRAPPERS_H
