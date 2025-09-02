/**
 * @file src/napi_transaction_wrapper.cpp
 * @description N-API implementation for the Transaction class wrapper.
 */

#include "napi_wrappers.h"
#include "napi_type_conversions.h"  // For ConvertNapiToValueType and ConvertValueToNapi
#include "../include/indinis.h"       // For Transaction class
#include "../include/storage_engine.h"
#include "../include/types.h"         // For C++ FilterCondition, Record, etc.
#include "../include/storage_error/storage_error.h" // For storage::StorageError

#include <iostream>
#include <vector>
// Helper function from original file (or move to a shared utility file)
static std::string GetNapiValueTypeString(const Napi::Value& val) {
    if (val.IsNull()) return "Null";
    if (val.IsUndefined()) return "Undefined";
    if (val.IsBoolean()) return "Boolean";
    if (val.IsNumber()) return "Number";
    if (val.IsString()) return "String";
    if (val.IsBuffer()) return "Buffer";
    if (val.IsArray()) return "Array";
    if (val.IsObject()) return "Object";
    if (val.IsFunction()) return "Function";
    if (val.IsSymbol()) return "Symbol";
    return "Unknown";
}

Napi::Object ConvertPaginatedResultToNapi(Napi::Env env, const PaginatedQueryResult<Record>& cppResult) {
    Napi::Object jsResult = Napi::Object::New(env);

    // Convert docs
    Napi::Array jsDocs = Napi::Array::New(env, cppResult.docs.size());
    for (size_t i = 0; i < cppResult.docs.size(); ++i) {
        Napi::Object recordObj = Napi::Object::New(env);
        recordObj.Set("key", Napi::String::New(env, cppResult.docs[i].key));
        recordObj.Set("value", ConvertValueToNapi(env, cppResult.docs[i].value));
        jsDocs.Set(i, recordObj);
    }
    jsResult.Set("docs", jsDocs);

    // Convert booleans
    jsResult.Set("hasNextPage", Napi::Boolean::New(env, cppResult.hasNextPage));
    jsResult.Set("hasPrevPage", Napi::Boolean::New(env, cppResult.hasPrevPage));

    // Convert cursors
    auto convertCursor = [&](const std::optional<std::vector<ValueType>>& cursorOpt) -> Napi::Value {
        if (!cursorOpt) return env.Undefined();
        Napi::Array jsCursor = Napi::Array::New(env, cursorOpt->size());
        for (size_t i = 0; i < cursorOpt->size(); ++i) {
            jsCursor.Set(i, ConvertValueToNapi(env, (*cursorOpt)[i]));
        }
        return jsCursor;
    };

    jsResult.Set("startCursor", convertCursor(cppResult.startCursor));
    jsResult.Set("endCursor", convertCursor(cppResult.endCursor));

    return jsResult;
}

static std::vector<ValueType> ConvertNapiArrayToValueVector(Napi::Env env, const Napi::Array& jsArray) {
    std::vector<ValueType> cppVector;
    cppVector.reserve(jsArray.Length());
    for (uint32_t i = 0; i < jsArray.Length(); ++i) {
        Napi::Value element = jsArray.Get(i);
        std::optional<ValueType> valOpt = ConvertNapiToValueType(env, element);
        if (!valOpt) {
            // ConvertNapiToValueType throws on failure, so we just let it propagate.
            // If it didn't throw, we'd throw a specific error here.
            throw Napi::TypeError::New(env, "Unsupported value type found in array for 'array-contains-any' filter.");
        }
        cppVector.push_back(std::move(*valOpt));
    }
    return cppVector;
}

struct AtomicUpdate;
// --- TransactionWrapper Implementation ---

Napi::Object TransactionWrapper::Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "Transaction_Internal", {
        InstanceMethod("put", &TransactionWrapper::Put),
        InstanceMethod("get", &TransactionWrapper::Get),
        InstanceMethod("getPrefix", &TransactionWrapper::GetPrefix),
        InstanceMethod("remove", &TransactionWrapper::Remove),
        InstanceMethod("commit", &TransactionWrapper::Commit),
        InstanceMethod("abort", &TransactionWrapper::Abort),
        InstanceMethod("getId", &TransactionWrapper::GetId),
        InstanceMethod("query", &TransactionWrapper::Query),
        InstanceMethod("update", &TransactionWrapper::Update),
        InstanceMethod("paginatedQuery", &TransactionWrapper::PaginatedQuery) 
    });

    IndinisWrapper::transactionWrapperConstructor = Napi::Persistent(func);
    IndinisWrapper::transactionWrapperConstructor.SuppressDestruct();
    
    return exports;
}

TransactionWrapper::TransactionWrapper(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<TransactionWrapper>(info) {
    // C++ txn_ pointer is set via SetTransactionAndEngine
}

TransactionWrapper::~TransactionWrapper() {
    if (txn_ && !txn_->isCommitted() && !txn_->isAborted()) { // Check if txn_ exists and its state
        LOG_WARN("[NAPI Txn Dtor] TransactionWrapper (TxnID: {}) garbage collected without explicit commit/abort.", txn_->getId());
        if (engine_wrapper_ && engine_wrapper_->GetInternal()) { // Check if engine_wrapper_ is valid
            LOG_INFO("  Attempting to inform StorageEngine to abort TxnID {}.", txn_->getId());
            try {
                // CORRECT WAY: Call StorageEngine's abort method
                engine_wrapper_->GetInternal()->abortTransaction(txn_->getId());
            } catch (const std::exception& e) {
                LOG_ERROR("  Exception during implicit abort in TransactionWrapper destructor for TxnID {}: {}", txn_->getId(), e.what());
            } catch (...) {
                LOG_ERROR("  Unknown exception during implicit abort in TransactionWrapper destructor for TxnID {}.", txn_->getId());
            }
        } else {
            LOG_WARN("  Cannot inform StorageEngine to abort TxnID {} as engine_wrapper_ is invalid. Transaction C++ object phase set locally.", txn_->getId());
            // If engine can't be reached, a local state change is the best we can do for this wrapper.
            // The C++ Transaction dtor might also try to log something if engine_ is available to it.
            txn_->force_abort_internal(); // Fallback: at least mark the C++ object
        }
    }
}


void TransactionWrapper::SetTransactionAndEngine(std::shared_ptr<Transaction> txn_ptr, IndinisWrapper* engine_wrapper_ptr) {
    this->txn_ = txn_ptr; 
    this->engine_wrapper_ = engine_wrapper_ptr; 
}
    
std::shared_ptr<Transaction> TransactionWrapper::GetInternalTransaction() { 
    return this->txn_; 
}

Napi::Value TransactionWrapper::Remove(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!txn_) { Napi::Error::New(env, "Transaction context invalid").ThrowAsJavaScriptException(); return Napi::Boolean::New(env, false); }
    if (txn_->isCommitted() || txn_->isAborted()) { Napi::Error::New(env, "Transaction completed").ThrowAsJavaScriptException(); return Napi::Boolean::New(env, false); }
    if (info.Length() < 1 || !info[0].IsString()) { Napi::TypeError::New(env, "Remove requires key (string)").ThrowAsJavaScriptException(); return Napi::Boolean::New(env, false); }

    std::string key = info[0].As<Napi::String>().Utf8Value();
    bool result = false;

    try {
        result = txn_->remove(key);
    } catch (const std::exception& e) { Napi::Error::New(env, "C++ error during transaction remove: " + std::string(e.what())).ThrowAsJavaScriptException(); return Napi::Boolean::New(env, false); }
    catch (...) { Napi::Error::New(env, "Unknown C++ error during transaction remove").ThrowAsJavaScriptException(); return Napi::Boolean::New(env, false); }
    return Napi::Boolean::New(env, result);
}

Napi::Value TransactionWrapper::Update(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!txn_ || txn_->isCommitted() || txn_->isAborted()) {
        Napi::Error::New(env, "Transaction is not active").ThrowAsJavaScriptException();
        return env.Undefined();
    }
    if (info.Length() < 2 || !info[0].IsString() || !info[1].IsObject()) {
        Napi::TypeError::New(env, "Update requires a key (string) and an operations object").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    std::string key = info[0].As<Napi::String>().Utf8Value();
    Napi::Object js_ops = info[1].As<Napi::Object>();
    
    std::unordered_map<std::string, AtomicUpdate> cpp_updates;

    try {
        Napi::Array fields = js_ops.GetPropertyNames();
        for (uint32_t i = 0; i < fields.Length(); ++i) {
            std::string field_name = fields.Get(i).As<Napi::String>().Utf8Value();
            Napi::Object op_obj = js_ops.Get(field_name).As<Napi::Object>();

            // For now, we only support increment. This can be expanded later.
            if (op_obj.Has("$$indinis_op") && op_obj.Get("$$indinis_op").IsSymbol() && op_obj.Has("value")) {
                Napi::Symbol op_symbol = op_obj.Get("$$indinis_op").As<Napi::Symbol>();
                Napi::Value global_increment_symbol = env.Global().Get("Symbol").As<Napi::Object>().Get("for").As<Napi::Function>().Call({Napi::String::New(env, "indinis.increment")});

                if (op_symbol.StrictEquals(global_increment_symbol)) {
                    if (!op_obj.Get("value").IsNumber()) {
                         throw Napi::TypeError::New(env, "Increment operation for field '" + field_name + "' must have a numeric value.");
                    }
                    double value = op_obj.Get("value").As<Napi::Number>().DoubleValue();
                    
                    AtomicUpdate update_op;
                    update_op.type = AtomicUpdateType::INCREMENT;
                    update_op.value = value; // Store as double to handle JS numbers
                    cpp_updates[field_name] = update_op;
                }
            }
        }
        
        // Call the new C++ transaction method
        txn_->update(key, cpp_updates);

    } catch (const Napi::Error& e) {
        e.ThrowAsJavaScriptException();
    } catch (const std::exception& e) {
        Napi::Error::New(env, "C++ error during transaction update prep: " + std::string(e.what())).ThrowAsJavaScriptException();
    }
    return env.Undefined();
}

Napi::Value TransactionWrapper::Commit(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!engine_wrapper_ || !engine_wrapper_->GetInternal()) {
        Napi::Error::New(env, "Transaction Commit: Engine context invalid").ThrowAsJavaScriptException();
        return Napi::Boolean::New(env, false);
    }
    StorageEngine* engine = engine_wrapper_->GetInternal();
    if (!engine) {
         Napi::Error::New(env, "Transaction cannot commit: StorageEngine instance is not available.").ThrowAsJavaScriptException();
        return Napi::Boolean::New(env, false);
     }

    if (!txn_) { 
        Napi::Error::New(env, "Transaction Commit: C++ transaction not linked.").ThrowAsJavaScriptException(); 
        return Napi::Boolean::New(env, false); 
    }
    if (txn_->isCommitted() || txn_->isAborted()) { 
        LOG_TRACE("[NAPI Txn Commit] TxnID {} already completed (Phase {}). Returning committed status: {}", txn_->getId(), static_cast<int>(txn_->getPhase()), txn_->isCommitted());
        return Napi::Boolean::New(env, txn_->isCommitted()); 
    }

    bool result = false;
    try {
        result = engine->commitTransaction(txn_->getId());
    } catch (const storage::StorageError& e) {
        // Propagate the specific error message from C++ to JS.
        // The engine has already aborted the transaction internally or will upon cleanup.
        Napi::Error::New(env, e.toString()).ThrowAsJavaScriptException();
        return Napi::Boolean::New(env, false); // Return false after throwing

    } catch (const std::exception& e) { 
        Napi::Error::New(env, "C++ exception during engine.commitTransaction: " + std::string(e.what())).ThrowAsJavaScriptException(); 
        return Napi::Boolean::New(env, false); 
    } catch (...) {
        Napi::Error::New(env, "Unknown C++ error during transaction commit").ThrowAsJavaScriptException();
        return Napi::Boolean::New(env, false); 
    }
    return Napi::Boolean::New(env, result);
}


Napi::Value TransactionWrapper::Abort(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (!engine_wrapper_) {
        LOG_WARN("[NAPI Txn Abort] Engine wrapper context missing. Cannot inform engine. Aborting locally if C++ txn exists.");
        if (txn_) { txn_->force_abort_internal(); } // Best effort local abort
        return env.Undefined();
    }
    StorageEngine* engine = engine_wrapper_->GetInternal();
    if (!engine) {
        LOG_WARN("[NAPI Txn Abort] StorageEngine instance not available. Cannot inform engine. Aborting locally if C++ txn exists.");
        if (txn_) { txn_->force_abort_internal(); }
        return env.Undefined();
    }
    if (!txn_) {
        LOG_WARN("[NAPI Txn Abort] No C++ transaction linked to this wrapper. Nothing to abort.");
        return env.Undefined(); 
    }

    // It's generally safe to call abort multiple times or on an already completed transaction.
    // The StorageEngine::abortTransaction method should be idempotent or handle these cases gracefully.
    LOG_TRACE("[NAPI Txn Abort] Calling engine to abort TxnID {}.", txn_->getId());
    try {
        engine->abortTransaction(txn_->getId());
    } catch (const std::exception& e) { 
        // Log C++ exceptions from engine->abortTransaction() but don't let them crash Node.js
        LOG_ERROR("[NAPI Txn Abort] C++ exception during transaction abort via engine (TxnID {}): {}", txn_->getId(), e.what());
    } catch (...) { 
        LOG_ERROR("[NAPI Txn Abort] Unknown C++ exception during transaction abort via engine (TxnID {}).", txn_->getId());
    }
    
    // The C++ Transaction object's phase is updated by StorageEngine::abortTransaction.
    // NAPI abort usually doesn't return a value indicating success/failure of the abort itself,
    // as it's a "best effort" to discard changes.
    return env.Undefined();
}

Napi::Value TransactionWrapper::GetId(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!txn_) { Napi::Error::New(env, "Transaction context invalid").ThrowAsJavaScriptException(); return env.Null(); }

    TxnId id = 0;
    try {
        id = txn_->getId();
    } catch (...) { /* Should not happen if txn_ is valid */ Napi::Error::New(env, "Unknown C++ error getting transaction ID").ThrowAsJavaScriptException(); return env.Null(); }

    // NAPI Numbers are doubles
    return Napi::Number::New(env, static_cast<double>(id));
}

Napi::Value TransactionWrapper::Get(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!txn_) { 
        Napi::Error::New(env, "Transaction context invalid (txn_ is null)").ThrowAsJavaScriptException(); 
        return env.Null(); 
    }
    if (txn_->isAborted()) { 
        Napi::Error::New(env, "Transaction is aborted").ThrowAsJavaScriptException(); 
        return env.Null(); 
    }
    // Allow reads from committed transactions if desired (Snapshot Isolation interpretation)
    // if (txn_->isCommitted()) {
    //     LOG_TRACE("[NAPI TxnWrapper GET] TxnID {} is committed. Reading from its snapshot.", txn_->getId());
    // }
    if (info.Length() < 1 || !info[0].IsString()) { 
        Napi::TypeError::New(env, "Get requires key (string)").ThrowAsJavaScriptException(); 
        return env.Null(); 
    }

    std::string key = info[0].As<Napi::String>().Utf8Value();
    LOG_TRACE("[NAPI TxnWrapper GET] START. TxnID: {}, Key: '{}'", (txn_ ? txn_->getId() : 0), format_key_for_print(key));
    std::optional<Record> recordOpt;

     try {
         recordOpt = txn_->get(key); // This calls C++ Transaction::get
    } catch (const std::exception& e) {
        LOG_ERROR("  [NAPI TxnWrapper GET] C++ exception from txn_->get() for key '{}': {}", format_key_for_print(key), e.what());
        Napi::Error::New(env, "C++ error during transaction get: " + std::string(e.what())).ThrowAsJavaScriptException();
        return env.Null();
    } catch (...) {
        LOG_ERROR("  [NAPI TxnWrapper GET] Unknown C++ exception from txn_->get() for key '{}'", format_key_for_print(key));
        Napi::Error::New(env, "Unknown C++ error during transaction get").ThrowAsJavaScriptException();
        return env.Null();
    }

    if (!recordOpt) {
        LOG_TRACE("  [NAPI TxnWrapper GET] txn_->get() returned std::nullopt for key '{}'. Returning JS null.", format_key_for_print(key));
        return env.Null();
    }

    const Record& cpp_record = *recordOpt;
    LOG_TRACE("  [NAPI TxnWrapper GET] txn_->get() returned a Record for key '{}'. C++ ValueType index: {}. Converting to NAPI.",
              format_key_for_print(key), cpp_record.value.index());
    
    Napi::Value napi_value_result = ConvertValueToNapi(env, cpp_record.value); // This logs internally
    
    if (env.IsExceptionPending()) {
        LOG_ERROR("  [NAPI TxnWrapper GET] Exception pending after ConvertValueToNapi for key '{}'.", format_key_for_print(key));
        // Exception already thrown by ConvertValueToNapi or Napi::Value creation
        return env.Null(); // Or allow exception to propagate
    }
    
    // Corrected logging for NAPI value type
    LOG_TRACE("[NAPI TxnWrapper GET] END. Key: '{}'. NAPI value type: {}", 
              format_key_for_print(key), 
              GetNapiValueTypeString(napi_value_result)); // Use helper
    return napi_value_result;
}

Napi::Value TransactionWrapper::GetPrefix(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!txn_) { Napi::Error::New(env, "Transaction context invalid").ThrowAsJavaScriptException(); return env.Null(); }
    if (txn_->isAborted()) { Napi::Error::New(env, "Transaction aborted").ThrowAsJavaScriptException(); return env.Null(); }
    if (info.Length() < 1 || !info[0].IsString()) { Napi::TypeError::New(env, "GetPrefix requires prefix (string)").ThrowAsJavaScriptException(); return env.Null(); }

    std::string prefix = info[0].As<Napi::String>().Utf8Value();
    size_t limit = 0;
    if (info.Length() > 1 && info[1].IsNumber()) {
        int64_t limit_arg = info[1].As<Napi::Number>().Int64Value();
        if (limit_arg > 0) limit = static_cast<size_t>(limit_arg);
        else if (limit_arg < 0) { Napi::TypeError::New(env, "Limit cannot be negative").ThrowAsJavaScriptException(); return env.Null(); }
    }

    std::vector<Record> records; // C++ returns vector<Record>
    try {
        records = txn_->getPrefix(prefix, limit);
    } catch (const std::exception& e) { 
        // This will catch the exception thrown from HashMemTable
        Napi::Error::New(env, "C++ error during transaction getPrefix: " + std::string(e.what())).ThrowAsJavaScriptException(); 
        return env.Null(); 
    } catch (...) { 
        Napi::Error::New(env, "Unknown C++ error during transaction getPrefix").ThrowAsJavaScriptException(); 
        return env.Null(); 
    }
    // Convert vector<Record> to Napi::Array of { key: string, value: StorageValue }
    Napi::Array resultArray = Napi::Array::New(env, records.size());
    for (size_t i = 0; i < records.size(); ++i) {
        const auto& record = records[i];
        Napi::Object recordObj = Napi::Object::New(env);
        recordObj.Set("key", Napi::String::New(env, record.key));
        recordObj.Set("value", ConvertValueToNapi(env, record.value));
        // Check for errors during value conversion
        if (env.IsExceptionPending()) { return env.Null(); }
        resultArray.Set(i, recordObj);
    }

    return resultArray;
}


Napi::Value TransactionWrapper::Put(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!txn_) { Napi::Error::New(env, "Transaction context invalid").ThrowAsJavaScriptException(); return Napi::Boolean::New(env, false); }
    if (txn_->isCommitted() || txn_->isAborted()) { Napi::Error::New(env, "Transaction completed").ThrowAsJavaScriptException(); return Napi::Boolean::New(env, false); }
    if (info.Length() < 2 || !info[0].IsString()) { Napi::TypeError::New(env, "Put requires key (string) and value").ThrowAsJavaScriptException(); return Napi::Boolean::New(env, false); }

    std::string key = info[0].As<Napi::String>().Utf8Value();
    Napi::Value jsValue = info[1];

    std::optional<ValueType> valueOpt = ConvertNapiToValueType(env, jsValue);
    if (!valueOpt) {
        if (!env.IsExceptionPending()) {
            Napi::TypeError::New(env, "Unsupported value type for put. Use number, string, or Buffer.").ThrowAsJavaScriptException();
        }
        return Napi::Boolean::New(env, false);
    }
    
    // --- NEW: Parse the overwrite option ---
    bool overwrite = false; // Default to false for safety
    if (info.Length() > 2 && info[2].IsObject()) {
        Napi::Object options = info[2].As<Napi::Object>();
        if (options.Has("overwrite")) {
            Napi::Value overwriteVal = options.Get("overwrite");
            if (overwriteVal.IsBoolean()) {
                overwrite = overwriteVal.As<Napi::Boolean>().Value();
            } else {
                Napi::TypeError::New(env, "The 'overwrite' option must be a boolean.").ThrowAsJavaScriptException();
                return Napi::Boolean::New(env, false);
            }
        }
    }
    // --- END NEW ---

    bool result = false;
    try {
         // Pass the overwrite flag to the C++ method
         result = txn_->put(key, *valueOpt, overwrite);
    } catch (const std::exception& e) { Napi::Error::New(env, "C++ error during transaction put: " + std::string(e.what())).ThrowAsJavaScriptException(); return Napi::Boolean::New(env, false); }
    catch (...) { Napi::Error::New(env, "Unknown C++ error during transaction put").ThrowAsJavaScriptException(); return Napi::Boolean::New(env, false); }

    return Napi::Boolean::New(env, result);
}

Napi::Value TransactionWrapper::Query(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    // --- 1. Pre-condition Checks ---
    if (!txn_) {
        Napi::Error::New(env, "Transaction context is invalid.").ThrowAsJavaScriptException();
        return env.Null();
    }
    if (txn_->isAborted() || txn_->isCommitted()) {
        Napi::Error::New(env, "Transaction has already been completed.").ThrowAsJavaScriptException();
        return env.Null();
    }

    // --- 2. Argument Validation ---
    if (info.Length() != 5) {
        Napi::TypeError::New(env, "Internal Query requires exactly 5 arguments: storePath, filters, sortBy, aggregationPlan, limit.").ThrowAsJavaScriptException();
        return env.Null();
    }

    if (!info[0].IsString()) {
        Napi::TypeError::New(env, "Argument 0 (storePath) must be a string.").ThrowAsJavaScriptException();
        return env.Null();
    }
    if (!info[1].IsArray()) {
        Napi::TypeError::New(env, "Argument 1 (filters) must be an array.").ThrowAsJavaScriptException();
        return env.Null();
    }
    // sortBy (index 2) can be an object, null, or undefined.
    if (!info[2].IsObject() && !info[2].IsNull() && !info[2].IsUndefined()) {
        Napi::TypeError::New(env, "Argument 2 (sortBy) must be an object or null.").ThrowAsJavaScriptException();
        return env.Null();
    }
    // aggregationPlan (index 3) can be an object, null, or undefined.
    if (!info[3].IsObject() && !info[3].IsNull() && !info[3].IsUndefined()) {
        Napi::TypeError::New(env, "Argument 3 (aggregationPlan) must be an object or null.").ThrowAsJavaScriptException();
        return env.Null();
    }
    if (!info[4].IsNumber()) {
        Napi::TypeError::New(env, "Argument 4 (limit) must be a number.").ThrowAsJavaScriptException();
        return env.Null();
    }



    try {

        // --- 3. Parse All Arguments ---
        std::string storePath = info[0].As<Napi::String>().Utf8Value();
        Napi::Array jsFilters = info[1].As<Napi::Array>();
        Napi::Value sortByJs = info[2];
        Napi::Value aggPlanJs = info[3];
        size_t limit = info[4].As<Napi::Number>().Int64Value();
        // --- Parse SortBy ---
        std::optional<std::pair<std::string, IndexSortOrder>> sortByCppOpt;
        if (sortByJs.IsObject()) {
            Napi::Object sortByJsObj = sortByJs.As<Napi::Object>();
            if (sortByJsObj.Has("field") && sortByJsObj.Get("field").IsString() &&
                sortByJsObj.Has("direction") && sortByJsObj.Get("direction").IsString()) {
                
                std::string field = sortByJsObj.Get("field").As<Napi::String>().Utf8Value();
                std::string direction = sortByJsObj.Get("direction").As<Napi::String>().Utf8Value();
                IndexSortOrder order = (direction == "desc") ? IndexSortOrder::DESCENDING : IndexSortOrder::ASCENDING;
                sortByCppOpt = std::make_pair(field, order);
            }
        } else if (!sortByJs.IsNull() && !sortByJs.IsUndefined()) {
            throw Napi::TypeError::New(env, "Argument 3 (sortBy) must be an object or null.");
        }

        // --- Parse AggregationPlan ---
        std::optional<engine::columnar::AggregationPlan> aggPlanCppOpt;
        if (aggPlanJs.IsObject()) {
            aggPlanCppOpt = engine::columnar::AggregationPlan{};
            Napi::Object jsPlanObj = aggPlanJs.As<Napi::Object>();

            // Parse 'groupBy' array
            if (jsPlanObj.Has("groupBy") && jsPlanObj.Get("groupBy").IsArray()) {
                Napi::Array jsGroupBy = jsPlanObj.Get("groupBy").As<Napi::Array>();
                for (uint32_t i = 0; i < jsGroupBy.Length(); i++) {
                    aggPlanCppOpt->group_by_fields.push_back(jsGroupBy.Get(i).As<Napi::String>());
                }
            }

            // Parse 'aggregations' map
            if (jsPlanObj.Has("aggregations") && jsPlanObj.Get("aggregations").IsObject()) {
                Napi::Object jsAggsMap = jsPlanObj.Get("aggregations").As<Napi::Object>();
                Napi::Array aggResultFieldNames = jsAggsMap.GetPropertyNames();
                for (uint32_t i = 0; i < aggResultFieldNames.Length(); i++) {
                    std::string result_field_name = aggResultFieldNames.Get(i).As<Napi::String>();
                    Napi::Object jsAggSpec = jsAggsMap.Get(result_field_name).As<Napi::Object>();
                    
                    engine::columnar::AggregationSpec spec;
                    spec.result_field_name = result_field_name;
                    spec.field = jsAggSpec.Get("field").As<Napi::String>();
                    std::string opStr = jsAggSpec.Get("op").As<Napi::String>();

                    if (opStr == "SUM") spec.op = engine::columnar::AggregationType::SUM;
                    else if (opStr == "COUNT") spec.op = engine::columnar::AggregationType::COUNT;
                    else if (opStr == "AVG") spec.op = engine::columnar::AggregationType::AVG;
                    else if (opStr == "MIN") spec.op = engine::columnar::AggregationType::MIN;
                    else if (opStr == "MAX") spec.op = engine::columnar::AggregationType::MAX;
                    else throw Napi::TypeError::New(env, "Unsupported aggregation operator: " + opStr);

                    aggPlanCppOpt->aggregations.push_back(spec);
                }
            }
        } else if (!aggPlanJs.IsNull() && !aggPlanJs.IsUndefined()) {
            throw Napi::TypeError::New(env, "Argument 4 (aggregationPlan) must be an object or null.");
        }
        
        // --- Convert JS Filters to C++ Filters ---
        std::vector<FilterCondition> cppFilters;
        cppFilters.reserve(jsFilters.Length());
        for (uint32_t i = 0; i < jsFilters.Length(); i++) {
            Napi::Object jsFilter = jsFilters.Get(i).As<Napi::Object>();
            FilterCondition cppFilter;

            cppFilter.field = jsFilter.Get("field").As<Napi::String>().Utf8Value();
            std::string opStr = jsFilter.Get("operator").As<Napi::String>().Utf8Value();
            
            // Map operator string to C++ enum
            if (opStr == "==") cppFilter.op = FilterOperator::EQUAL;
            else if (opStr == ">") cppFilter.op = FilterOperator::GREATER_THAN;
            else if (opStr == ">=") cppFilter.op = FilterOperator::GREATER_THAN_OR_EQUAL;
            else if (opStr == "<") cppFilter.op = FilterOperator::LESS_THAN;
            else if (opStr == "<=") cppFilter.op = FilterOperator::LESS_THAN_OR_EQUAL;
            else if (opStr == "array-contains") cppFilter.op = FilterOperator::ARRAY_CONTAINS;
            else if (opStr == "array-contains-any") cppFilter.op = FilterOperator::ARRAY_CONTAINS_ANY;
            else throw Napi::TypeError::New(env, "Unsupported filter operator: " + opStr);

            Napi::Value jsValue = jsFilter.Get("value");

            if (cppFilter.op == FilterOperator::ARRAY_CONTAINS_ANY) {
                if (!jsValue.IsArray()) {
                    throw Napi::TypeError::New(env, "'array-contains-any' requires the 'value' to be an array.");
                }
                cppFilter.value = ConvertNapiArrayToValueVector(env, jsValue.As<Napi::Array>());
                if (env.IsExceptionPending()) return env.Null();
            } else {
                std::optional<ValueType> valOpt = ConvertNapiToValueType(env, jsValue);
                if (!valOpt) {
                    if (!env.IsExceptionPending()) {
                        throw Napi::TypeError::New(env, "Unsupported value type for filter on field '" + cppFilter.field + "'.");
                    }
                    return env.Null();
                }
                cppFilter.value = std::move(*valOpt);
            }
            cppFilters.push_back(std::move(cppFilter));
        }

        // --- 4. Call Core C++ Logic ---
        std::vector<Record> results = txn_->query(storePath, cppFilters, sortByCppOpt, aggPlanCppOpt, limit);
        
        LOG_TRACE("[NAPI TxnWrapper::Query] Txn {}: C++ query returned {} records.", txn_->getId(), results.size());

        // --- 5. Convert C++ Results to JS Array ---
        Napi::Array napiResults = Napi::Array::New(env, results.size());
        for (size_t i = 0; i < results.size(); ++i) {
            Napi::Object recordObj = Napi::Object::New(env);
            recordObj.Set("key", Napi::String::New(env, results[i].key));
            recordObj.Set("value", ConvertValueToNapi(env, results[i].value));

            if (env.IsExceptionPending()) {
                // An error occurred during NAPI value conversion.
                return env.Null();
            }
            napiResults.Set(i, recordObj);
        }
        return napiResults;

    } catch (const storage::StorageError& e) {
        LOG_ERROR("[NAPI TxnWrapper::Query] Txn {}: Caught storage::StorageError: {}", txn_->getId(), e.toDetailedString());
        Napi::Error::New(env, e.toString()).ThrowAsJavaScriptException();
        return env.Null();
    } catch (const std::exception& e) {
        LOG_ERROR("[NAPI TxnWrapper::Query] Txn {}: Caught std::exception: {}", txn_->getId(), e.what());
        Napi::Error::New(env, "C++ error during query execution: " + std::string(e.what())).ThrowAsJavaScriptException();
        return env.Null();
    } catch (...) {
        LOG_ERROR("[NAPI TxnWrapper::Query] Txn {}: Caught unknown C++ exception.", txn_->getId());
        Napi::Error::New(env, "An unknown C++ error occurred during query execution.").ThrowAsJavaScriptException();
        return env.Null();
    }
}


std::vector<OrderByClause> TransactionWrapper::parseOrderBy(const Napi::Array& jsOrderBy) {
    std::vector<OrderByClause> orderByCpp;
    orderByCpp.reserve(jsOrderBy.Length());

    for (uint32_t i = 0; i < jsOrderBy.Length(); ++i) {
        Napi::Value val = jsOrderBy.Get(i);
        if (!val.IsObject()) {
            throw Napi::TypeError::New(Env(), "Each item in the 'orderBy' array must be an object.");
        }
        Napi::Object clauseObj = val.As<Napi::Object>();
        
        if (!clauseObj.Has("field") || !clauseObj.Get("field").IsString() ||
            !clauseObj.Has("direction") || !clauseObj.Get("direction").IsString()) {
            throw Napi::TypeError::New(Env(), "OrderBy clause object must have 'field' and 'direction' string properties.");
        }

        orderByCpp.push_back({
            clauseObj.Get("field").As<Napi::String>(),
            clauseObj.Get("direction").As<Napi::String>().Utf8Value() == "desc" ? IndexSortOrder::DESCENDING : IndexSortOrder::ASCENDING
        });
    }

    if (orderByCpp.empty()) {
        throw Napi::TypeError::New(Env(), "The 'orderBy' array cannot be empty for a paginated query.");
    }

    return orderByCpp;
}

std::vector<FilterCondition> TransactionWrapper::parseFilters(const Napi::Array& jsFilters) {
    Napi::Env env = Env(); // Get the environment from the ObjectWrap base
    std::vector<FilterCondition> cppFilters;
    cppFilters.reserve(jsFilters.Length());

    for (uint32_t i = 0; i < jsFilters.Length(); ++i) {
        Napi::Value val = jsFilters.Get(i);
        if (!val.IsObject()) {
            throw Napi::TypeError::New(env, "Each item in the 'filters' array must be an object.");
        }
        Napi::Object jsFilter = val.As<Napi::Object>();
        FilterCondition cppFilter;

        // 1. Parse 'field'
        if (!jsFilter.Has("field") || !jsFilter.Get("field").IsString()) {
            throw Napi::TypeError::New(env, "Filter object requires a 'field' string property.");
        }
        cppFilter.field = jsFilter.Get("field").As<Napi::String>().Utf8Value();

        // 2. Parse 'operator' and map to enum
        if (!jsFilter.Has("operator") || !jsFilter.Get("operator").IsString()) {
            throw Napi::TypeError::New(env, "Filter object requires an 'operator' string property.");
        }
        std::string opStr = jsFilter.Get("operator").As<Napi::String>().Utf8Value();

        if (opStr == "==") cppFilter.op = FilterOperator::EQUAL;
        else if (opStr == ">") cppFilter.op = FilterOperator::GREATER_THAN;
        else if (opStr == ">=") cppFilter.op = FilterOperator::GREATER_THAN_OR_EQUAL;
        else if (opStr == "<") cppFilter.op = FilterOperator::LESS_THAN;
        else if (opStr == "<=") cppFilter.op = FilterOperator::LESS_THAN_OR_EQUAL;
        else if (opStr == "array-contains") cppFilter.op = FilterOperator::ARRAY_CONTAINS;
        else if (opStr == "array-contains-any") cppFilter.op = FilterOperator::ARRAY_CONTAINS_ANY;
        else {
            throw Napi::TypeError::New(env, "Unsupported filter operator: '" + opStr + "'");
        }

        // 3. Parse 'value' based on the operator
        if (!jsFilter.Has("value")) {
            throw Napi::TypeError::New(env, "Filter object requires a 'value' property.");
        }
        Napi::Value jsValue = jsFilter.Get("value");

        if (cppFilter.op == FilterOperator::ARRAY_CONTAINS_ANY) {
            if (!jsValue.IsArray()) {
                throw Napi::TypeError::New(env, "The 'value' for an 'array-contains-any' filter must be an array.");
            }
            // Use the helper to convert the array of values
            cppFilter.value = ConvertNapiArrayToValueVector(env, jsValue.As<Napi::Array>());
        } else {
            // For all other operators, the value is a single primitive
            std::optional<ValueType> valOpt = ConvertNapiToValueType(env, jsValue);
            if (!valOpt) {
                // ConvertNapiToValueType already threw an error, so we just let it propagate.
                // We add a fallback throw here in case that behavior changes.
                throw Napi::TypeError::New(env, "Unsupported data type for filter value on field '" + cppFilter.field + "'.");
            }
            cppFilter.value = std::move(*valOpt);
        }
        
        cppFilters.push_back(std::move(cppFilter));
    }
    return cppFilters;
}

std::vector<ValueType> TransactionWrapper::parseCursor(const Napi::Array& jsCursor) {
    std::vector<ValueType> cppCursor;
    cppCursor.reserve(jsCursor.Length());

    for (uint32_t i = 0; i < jsCursor.Length(); ++i) {
        auto valOpt = ConvertNapiToValueType(Env(), jsCursor.Get(i));
        if (!valOpt) {
            // Error was already thrown by ConvertNapiToValueType
            throw Napi::TypeError::New(Env(), "Invalid value type found in cursor array.");
        }
        cppCursor.push_back(*valOpt);
    }
    return cppCursor;
}

// --- The Refactored PaginatedQuery Method ---

Napi::Value TransactionWrapper::PaginatedQuery(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);

    if (!txn_ || txn_->isCommitted() || txn_->isAborted()) {
        deferred.Reject(Napi::Error::New(env, "Transaction is not active.").Value());
        return deferred.Promise();
    }

    try {
        if (info.Length() < 1 || !info[0].IsObject()) {
            throw Napi::TypeError::New(env, "PaginatedQuery requires a single options object argument.");
        }
        Napi::Object options = info[0].As<Napi::Object>();

        // --- Delegate Parsing to Helper Methods ---
        std::string storePath = options.Get("storePath").As<Napi::String>();
        size_t limit = options.Get("limit").As<Napi::Number>().Int64Value();
        std::vector<OrderByClause> orderByCpp = parseOrderBy(options.Get("orderBy").As<Napi::Array>());
        std::vector<FilterCondition> cppFilters = parseFilters(options.Get("filters").As<Napi::Array>());

        // --- FULL CURSOR PARSING LOGIC ---
        std::optional<std::vector<ValueType>> startCursor, endCursor;
        bool startExclusive = false, endExclusive = false;

        if (options.Has("startAfter") && options.Get("startAfter").IsArray()) {
            startCursor = parseCursor(options.Get("startAfter").As<Napi::Array>());
            startExclusive = true;
        } else if (options.Has("startAt") && options.Get("startAt").IsArray()) {
            startCursor = parseCursor(options.Get("startAt").As<Napi::Array>());
            startExclusive = false;
        }
        
        if (options.Has("endBefore") && options.Get("endBefore").IsArray()) {
            endCursor = parseCursor(options.Get("endBefore").As<Napi::Array>());
            endExclusive = true;
        } else if (options.Has("endAt") && options.Get("endAt").IsArray()) {
            endCursor = parseCursor(options.Get("endAt").As<Napi::Array>());
            endExclusive = false;
        }
        // --- END FULL CURSOR PARSING ---

        // --- Call the C++ Core Logic with all parameters ---
        PaginatedQueryResult<Record> cppResult = txn_->paginatedQuery(
            storePath,
            cppFilters,
            orderByCpp,
            limit,
            startCursor,
            startExclusive,
            endCursor,
            endExclusive
        );

        deferred.Resolve(ConvertPaginatedResultToNapi(env, cppResult));

    } catch (const Napi::Error& e) {
        deferred.Reject(e.Value());
    } catch (const storage::StorageError& e) {
        deferred.Reject(Napi::Error::New(env, e.toString()).Value());
    } catch (const std::exception& e) {
        deferred.Reject(Napi::Error::New(env, "C++ error during paginated query: " + std::string(e.what())).Value());
    }

    return deferred.Promise();
}