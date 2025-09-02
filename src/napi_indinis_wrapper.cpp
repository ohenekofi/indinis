// @src/napi_indinis_wrapper.cpp
#include "napi_wrappers.h"
#include "napi_globals.h"
#include "../include/indinis.h"
#include "../include/storage_engine.h"
#include "../include/index_manager.h" 
#include "../include/encryption_library.h"
#include "../include/disk_manager.h"
#include "../include/cache.h"
#include "../include/columnar/column_types.h"
#include "../include/lsm_tree.h"
#include "../include/lsm/rcu_memtable_adapter.h"
#include "../include/columnar/columnar_store.h"
#include "../include/lsm/leveled_compaction_strategy.h"  //might delete this ..its not needed
#include "napi_sstable_builder_wrapper.h" 
#include "napi_workers.h"
#include "napi_batch_commit_worker.h" 
#include "napi_type_conversions.h" 
#include <iostream>
#include <filesystem>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

class LSMTree; // Forward declaration


LSMTree* GetLsmStoreFromNapi(const Napi::CallbackInfo& info, IndinisWrapper* wrapper) {
    Napi::Env env = info.Env();
    if (info.Length() < 1 || !info[0].IsString()) {
        Napi::TypeError::New(env, "First argument must be the storePath string.").ThrowAsJavaScriptException();
        return nullptr;
    }
    std::string store_path = info[0].As<Napi::String>().Utf8Value();
    StorageEngine* engine = wrapper->GetInternal();
    if (!engine) return nullptr;
    return engine->getLsmStore(store_path);
}

static lsm::memtable::RCUReclamationManager* GetRcuReclamationManager(const Napi::CallbackInfo& info, IndinisWrapper* wrapper) {
    Napi::Env env = info.Env();
    if (info.Length() < 1 || !info[0].IsString()) {
        Napi::TypeError::New(env, "First argument must be the storePath string.").ThrowAsJavaScriptException();
        return nullptr;
    }
    std::string store_path = info[0].As<Napi::String>().Utf8Value();
    
    StorageEngine* engine = wrapper->GetInternal();
    if (!engine) return nullptr; // Should be caught by caller

    LSMTree* store = engine->getLsmStore(store_path);
    if (!store) return nullptr; // Store doesn't exist yet, so no manager

    auto partitioned_memtable = store->getActivePartitionedMemTable();
    if (!partitioned_memtable) return nullptr;
    
    const auto& partitions = partitioned_memtable->getPartitions();
    if (partitions.empty()) return nullptr;

    auto* mem_rep = partitions[0]->getMemTableRep();
    if (!mem_rep || mem_rep->GetType() != engine::lsm::MemTableType::RCU) {
        Napi::Error::New(env, "Store '" + store_path + "' is not configured with RCU memtables.").ThrowAsJavaScriptException();
        return nullptr;
    }

    auto* adapter = static_cast<engine::lsm::RCUMemTableRepAdapter*>(mem_rep);
    
    // Use the new, correctly defined getter method.
    auto* rcu_memtable = adapter->getInternalRcuMemtable();
    
    if (!rcu_memtable) {
        Napi::Error::New(env, "Internal error: RCU adapter holds a null rcu_memtable pointer.").ThrowAsJavaScriptException();
        return nullptr;
    }

    return rcu_memtable->getReclamationManagerForDebug();
}

// --- Static Member Definition ---
Napi::FunctionReference IndinisWrapper::transactionWrapperConstructor;

// --- IndinisWrapper Implementation ---
StorageEngine* IndinisWrapper::GetInternal() {
    if (is_closed_ || !storage_engine_sp_) {
        return nullptr;
    }
    return storage_engine_sp_.get();
}

std::shared_ptr<StorageEngine> IndinisWrapper::GetInternalSharedPtr() {
     if (is_closed_) { return nullptr; }
     return storage_engine_sp_;
}

engine::columnar::ColumnSchema parseJsSchema(Napi::Env env, const Napi::Object& jsSchema) {
    engine::columnar::ColumnSchema schema;

    if (!jsSchema.Has("storePath") || !jsSchema.Get("storePath").IsString()) {
        throw Napi::TypeError::New(env, "Schema must have a 'storePath' string property.");
    }
    schema.store_path = jsSchema.Get("storePath").As<Napi::String>().Utf8Value();

    if (jsSchema.Has("schemaVersion") && jsSchema.Get("schemaVersion").IsNumber()) {
        schema.schema_version = jsSchema.Get("schemaVersion").As<Napi::Number>().Uint32Value();
    } // Defaults to 1

    if (!jsSchema.Has("columns") || !jsSchema.Get("columns").IsArray()) {
        throw Napi::TypeError::New(env, "Schema must have a 'columns' array property.");
    }
    
    Napi::Array jsColumns = jsSchema.Get("columns").As<Napi::Array>();
    if (jsColumns.Length() == 0) {
        throw Napi::TypeError::New(env, "Schema 'columns' array cannot be empty.");
    }

    for (uint32_t i = 0; i < jsColumns.Length(); ++i) {
        Napi::Value val = jsColumns.Get(i);
        if (!val.IsObject()) throw Napi::TypeError::New(env, "Each item in 'columns' must be an object.");
        Napi::Object jsCol = val.As<Napi::Object>();
        
        engine::columnar::ColumnDefinition colDef;

        if (!jsCol.Has("name") || !jsCol.Get("name").IsString()) throw Napi::TypeError::New(env, "Column definition requires a 'name' string.");
        colDef.name = jsCol.Get("name").As<Napi::String>().Utf8Value();
        
        if (!jsCol.Has("column_id") || !jsCol.Get("column_id").IsNumber()) throw Napi::TypeError::New(env, "Column definition requires a numeric 'column_id'.");
        colDef.column_id = jsCol.Get("column_id").As<Napi::Number>().Uint32Value();

        if (!jsCol.Has("type") || !jsCol.Get("type").IsString()) throw Napi::TypeError::New(env, "Column definition requires a 'type' string.");
        std::string typeStr = jsCol.Get("type").As<Napi::String>().Utf8Value();
        
        // Map string type to enum
        if (typeStr == "STRING") {
            colDef.type = engine::columnar::ColumnType::STRING;
        } else if (typeStr == "INT32") {
            colDef.type = engine::columnar::ColumnType::INT32;
        } else if (typeStr == "INT64") {
            colDef.type = engine::columnar::ColumnType::INT64;
        } else if (typeStr == "FLOAT") {
            colDef.type = engine::columnar::ColumnType::FLOAT;
        } else if (typeStr == "DOUBLE") {
            colDef.type = engine::columnar::ColumnType::DOUBLE;
        } else if (typeStr == "BOOLEAN") {
            colDef.type = engine::columnar::ColumnType::BOOLEAN;
        } else if (typeStr == "TIMESTAMP") {
            colDef.type = engine::columnar::ColumnType::TIMESTAMP;
        } else if (typeStr == "BINARY") {
            colDef.type = engine::columnar::ColumnType::BINARY;
        } else if (typeStr == "JSON") {
            colDef.type = engine::columnar::ColumnType::JSON;
        } else if (typeStr == "ARRAY_STRING") {
            colDef.type = engine::columnar::ColumnType::ARRAY_STRING;
        } else if (typeStr == "ARRAY_INT64") {
            colDef.type = engine::columnar::ColumnType::ARRAY_INT64;
        } else {
            // If the string from JS doesn't match any known type, throw an error.
            throw Napi::TypeError::New(env, "Unsupported column type provided in schema: '" + typeStr + "'");
        }

        if (jsCol.Has("nullable") && jsCol.Get("nullable").IsBoolean()) {
            colDef.nullable = jsCol.Get("nullable").As<Napi::Boolean>().Value();
        }

        schema.columns.push_back(colDef);
    }
    schema.build_map(); // Build the internal name->id map
    return schema;
}


std::string parse_modify_op_value(Napi::Env env, Napi::Object js_modify_payload) {
    json j; // Use nlohmann::json to build the C++ representation
    Napi::Array keys = js_modify_payload.GetPropertyNames();

    for (uint32_t i = 0; i < keys.Length(); ++i) {
        Napi::Value key_val = keys.Get(i);
        std::string field_name = key_val.As<Napi::String>();
        Napi::Value field_val = js_modify_payload.Get(field_name);

        if (field_val.IsObject()) {
            Napi::Object field_obj = field_val.As<Napi::Object>();
            // Check for our atomic increment sentinel
            if (field_obj.Has("$$indinis_op") && field_obj.Get("$$indinis_op").IsSymbol()) {
                Napi::Symbol op_symbol = field_obj.Get("$$indinis_op").As<Napi::Symbol>();
                Napi::Value global_increment_symbol = env.Global().Get("Symbol").As<Napi::Object>().Get("for").As<Napi::Function>().Call({Napi::String::New(env, "indinis.increment")});

                if (op_symbol.StrictEquals(global_increment_symbol)) {
                    double increment_val = field_obj.Get("value").As<Napi::Number>().DoubleValue();
                    // Serialize as a specific JSON structure that C++ will recognize
                    j[field_name] = {
                        {"$$indinis_op", "INCREMENT"},
                        {"value", increment_val}
                    };
                    continue; // Go to the next field
                }
            }
        }
        
        // If it's not a recognized atomic op, treat it as a simple merge value.
        // We convert it to JSON to handle all types (string, number, bool, null).
        // This requires a robust Napi::Value -> nlohmann::json converter. For simplicity, we'll
        // assume a helper or handle basic types here.
        if (field_val.IsString()) j[field_name] = field_val.As<Napi::String>().Utf8Value();
        else if (field_val.IsNumber()) j[field_name] = field_val.As<Napi::Number>().DoubleValue();
        else if (field_val.IsBoolean()) j[field_name] = field_val.As<Napi::Boolean>().Value();
        else if (field_val.IsNull()) j[field_name] = nullptr;
        // Note: Buffers and other types would need handling here.
    }
    return j.dump();
}

Napi::Object IndinisWrapper::Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "Indinis", {
        InstanceMethod("beginTransaction_internal", &IndinisWrapper::BeginTransaction),
        InstanceMethod("createIndex_internal", &IndinisWrapper::CreateIndex),
        InstanceMethod("deleteIndex_internal", &IndinisWrapper::DeleteIndex),
        InstanceMethod("listIndexes_internal", &IndinisWrapper::ListIndexes),
        InstanceMethod("close_internal", &IndinisWrapper::Close),
        InstanceMethod("debug_scanBTree", &IndinisWrapper::DebugScanBTree),
        InstanceMethod("forceCheckpoint_internal", &IndinisWrapper::ForceCheckpoint),       
        InstanceMethod("getCheckpointHistory_internal", &IndinisWrapper::GetCheckpointHistory),
        InstanceMethod("changeDatabasePassword_internal", &IndinisWrapper::ChangeDatabasePassword),
        InstanceMethod("getCacheStats_internal", &IndinisWrapper::GetCacheStats),
        InstanceMethod("debug_verifyFreeList", &IndinisWrapper::DebugVerifyFreeList),
        InstanceMethod("debug_analyzeFragmentation", &IndinisWrapper::DebugAnalyzeFragmentation),
        InstanceMethod("debug_defragment", &IndinisWrapper::DebugDefragment),
        InstanceMethod("debug_getDatabaseStats", &IndinisWrapper::DebugGetDatabaseStats),
        InstanceMethod("registerStoreSchema_internal", &IndinisWrapper::RegisterStoreSchema),
        InstanceMethod("debug_getLsmStoreStats", &IndinisWrapper::DebugGetLsmStoreStats),
        InstanceMethod("debug_getMemTableTunerStats", &IndinisWrapper::DebugGetMemTableTunerStats),
        InstanceMethod("debug_getWriteBufferManagerStats", &IndinisWrapper::DebugGetWriteBufferManagerStats),
        InstanceMethod("debug_getLsmFlushCount", &IndinisWrapper::DebugGetLsmFlushCount),
        InstanceMethod("debug_getLsmVersionStats", &IndinisWrapper::DebugGetLsmVersionStats), 
        InstanceMethod("debug_getLsmRcuStats", &IndinisWrapper::DebugGetLsmRcuStats),
        InstanceMethod("debug_pauseRcuReclamation", &IndinisWrapper::DebugPauseRcuReclamation),
        InstanceMethod("debug_resumeRcuReclamation", &IndinisWrapper::DebugResumeRcuReclamation), 
        InstanceMethod("debug_getThreadPoolStats_internal", &IndinisWrapper::DebugGetThreadPoolStats),
        InstanceMethod("ingestFile_internal", &IndinisWrapper::IngestExternalFile),
        InstanceMethod("debug_getSSTableBuilder", &IndinisWrapper::DebugGetSSTableBuilder),
        InstanceMethod("debug_serializeValue", &IndinisWrapper::DebugSerializeValue),
        InstanceMethod("ingestColumnarFile_internal", &IndinisWrapper::IngestColumnarFile),
        InstanceMethod("debug_getColumnarStoreStats", &IndinisWrapper::DebugGetColumnarStoreStats),
        InstanceMethod("debug_forceColumnarCompaction", &IndinisWrapper::DebugForceColumnarCompaction),
        InstanceMethod("commitBatch_internal", &IndinisWrapper::CommitBatch)


    });
    SSTableBuilderWrapper::Init(env, exports); 
    exports.Set("Indinis", func);
    return exports;
}

IndinisWrapper::IndinisWrapper(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<IndinisWrapper>(info), storage_engine_sp_(nullptr), is_closed_(false) {
    Napi::Env env = info.Env();

    // 1. Validate and get dataDir argument
    if (info.Length() < 1 || !info[0].IsString()) { 
        Napi::TypeError::New(env, "Indinis constructor: dataDir (string) is required as the first argument.").ThrowAsJavaScriptException();
        return;
    }
    std::string data_dir_from_js = info[0].As<Napi::String>().Utf8Value();
    if (data_dir_from_js.empty()) {
        Napi::TypeError::New(env, "Indinis constructor: dataDir path cannot be empty.").ThrowAsJavaScriptException();
        return;
    }

    // Normalize the path to use as a consistent key for the singleton map
    try {
        // Store the canonical path. This will be the key for g_active_engine_instances
        this->data_dir_path_key_ = std::filesystem::absolute(data_dir_from_js).lexically_normal().string();
    } catch (const std::filesystem::filesystem_error& fs_err) {
        Napi::Error::New(env, "Indinis constructor: Invalid data directory path '" + data_dir_from_js + "': " + fs_err.what()).ThrowAsJavaScriptException();
        return;
    }
    
    LOG_INFO("[NAPI IndinisWrapper Ctor] Requested dataDir: '{}', Canonical Key for Engine Map: '{}'", 
             data_dir_from_js, this->data_dir_path_key_);

    // 2. Initialize C++ Options from JS options object (info[1])
    ExtWALManagerConfig effective_wal_config; // C++ struct
    std::chrono::seconds checkpoint_interval_cpp(60); 
    size_t sstable_block_size_bytes_cpp = 64 * 1024; 
    CompressionType sstable_compression_type_cpp = CompressionType::ZSTD; 
    int sstable_compression_level_cpp = 0; 
    std::optional<std::string> encryption_password_from_js;
    EncryptionScheme selected_encryption_scheme_cpp = EncryptionScheme::NONE; 
    int kdf_iterations_from_js = 0; 
    bool enable_cache_cpp = false; 
    cache::CacheConfig cache_config_cpp; // Uses its default constructor
    engine::lsm::MemTableType selected_memtable_type_cpp = engine::lsm::MemTableType::SKIP_LIST; // Default

    size_t min_compaction_threads_cpp = 2; // Set sensible defaults here
    size_t max_compaction_threads_cpp = std::thread::hardware_concurrency();
    engine::lsm::CompactionStrategyType selected_compaction_strategy_cpp = engine::lsm::CompactionStrategyType::LEVELED;
    engine::lsm::LeveledCompactionConfig leveled_config_cpp;

    //  Create a C++ config object for Universal strategy >>>
    engine::lsm::UniversalCompactionConfig universal_config_cpp;
    engine::lsm::FIFOCompactionConfig fifo_config_cpp;
    engine::lsm::HybridCompactionConfig hybrid_config_cpp;

    if (info.Length() > 1 && info[1].IsObject()) {
        Napi::Object js_options = info[1].As<Napi::Object>();
        LOG_TRACE("[NAPI IndinisWrapper Ctor] Parsing JS options object.");

        // Parse checkpointIntervalSeconds
        if (js_options.Has("checkpointIntervalSeconds")) {
            Napi::Value intervalVal = js_options.Get("checkpointIntervalSeconds");
            if (intervalVal.IsNumber()) {
                int seconds = intervalVal.As<Napi::Number>().Int32Value();
                if (seconds >= 0) checkpoint_interval_cpp = std::chrono::seconds(seconds);
                else { Napi::TypeError::New(env, "options.checkpointIntervalSeconds must be non-negative.").ThrowAsJavaScriptException(); return; }
            } else { Napi::TypeError::New(env, "options.checkpointIntervalSeconds must be a number.").ThrowAsJavaScriptException(); return; }
        }

        // Parse walOptions
        if (js_options.Has("walOptions")) {
            Napi::Value walOptsVal = js_options.Get("walOptions");
            if (walOptsVal.IsObject()) {
                Napi::Object js_wal_opts = walOptsVal.As<Napi::Object>();
                if (js_wal_opts.Has("wal_directory") && js_wal_opts.Get("wal_directory").IsString()) {
                    effective_wal_config.wal_directory = js_wal_opts.Get("wal_directory").As<Napi::String>().Utf8Value();
                }
                if (js_wal_opts.Has("wal_file_prefix") && js_wal_opts.Get("wal_file_prefix").IsString()) {
                    effective_wal_config.wal_file_prefix = js_wal_opts.Get("wal_file_prefix").As<Napi::String>().Utf8Value();
                }
                if (js_wal_opts.Has("segment_size") && js_wal_opts.Get("segment_size").IsNumber()) {
                    uint64_t seg_size = js_wal_opts.Get("segment_size").As<Napi::Number>().Uint32Value(); // JS number to uint64_t
                    if (seg_size > 0) effective_wal_config.segment_size = seg_size; 
                    else { Napi::TypeError::New(env, "walOptions.segment_size must be positive.").ThrowAsJavaScriptException(); return; }
                }
                if (js_wal_opts.Has("background_flush_enabled") && js_wal_opts.Get("background_flush_enabled").IsBoolean()) {
                    effective_wal_config.background_flush_enabled = js_wal_opts.Get("background_flush_enabled").As<Napi::Boolean>().Value();
                }
                if (js_wal_opts.Has("flush_interval_ms") && js_wal_opts.Get("flush_interval_ms").IsNumber()) {
                     uint32_t flush_ms = js_wal_opts.Get("flush_interval_ms").As<Napi::Number>().Uint32Value();
                     if (flush_ms > 0) effective_wal_config.flush_interval_ms.store(flush_ms);
                     else { Napi::TypeError::New(env, "walOptions.flush_interval_ms must be positive.").ThrowAsJavaScriptException(); return; }
                }
                if (js_wal_opts.Has("sync_on_commit_record") && js_wal_opts.Get("sync_on_commit_record").IsBoolean()) {
                    effective_wal_config.sync_on_commit_record = js_wal_opts.Get("sync_on_commit_record").As<Napi::Boolean>().Value();
                }
            } else { Napi::TypeError::New(env, "options.walOptions must be an object.").ThrowAsJavaScriptException(); return; }
        }
        // Resolve WAL directory path relative to canonical data_dir_path_key_ if relative
        if (!effective_wal_config.wal_directory.empty() &&
            !std::filesystem::path(effective_wal_config.wal_directory).is_absolute()) {
            effective_wal_config.wal_directory = (std::filesystem::path(this->data_dir_path_key_) / effective_wal_config.wal_directory).lexically_normal().string();
        } else if (effective_wal_config.wal_directory.empty()) { // Default if still empty
            effective_wal_config.wal_directory = (std::filesystem::path(this->data_dir_path_key_) / "wal_data").lexically_normal().string();
        }


        // Parse SSTable options
        if (js_options.Has("sstableDataBlockUncompressedSizeKB")) {
            Napi::Value val = js_options.Get("sstableDataBlockUncompressedSizeKB");
            if (val.IsNumber()) {
                int kb = val.As<Napi::Number>().Int32Value();
                if (kb > 0) sstable_block_size_bytes_cpp = static_cast<size_t>(kb) * 1024;
                else { Napi::TypeError::New(env, "options.sstableDataBlockUncompressedSizeKB must be a positive number.").ThrowAsJavaScriptException(); return;}
            } else { Napi::TypeError::New(env, "options.sstableDataBlockUncompressedSizeKB must be a number.").ThrowAsJavaScriptException(); return; }
        }
         if (js_options.Has("sstableCompressionType")) {
            Napi::Value val = js_options.Get("sstableCompressionType");
            if (val.IsString()) {
                std::string type_str = val.As<Napi::String>().Utf8Value();
                if (type_str == "NONE") sstable_compression_type_cpp = CompressionType::NONE;
                else if (type_str == "ZSTD") sstable_compression_type_cpp = CompressionType::ZSTD;
                else if (type_str == "LZ4") sstable_compression_type_cpp = CompressionType::LZ4; 
                else { Napi::TypeError::New(env, "options.sstableCompressionType must be 'NONE', 'ZSTD', or 'LZ4'.").ThrowAsJavaScriptException(); return; }
            } else { Napi::TypeError::New(env, "options.sstableCompressionType must be a string.").ThrowAsJavaScriptException(); return; }
        }
        if (js_options.Has("sstableCompressionLevel")) {
            Napi::Value val = js_options.Get("sstableCompressionLevel");
            if (val.IsNumber()) {
                sstable_compression_level_cpp = val.As<Napi::Number>().Int32Value();
            } else { Napi::TypeError::New(env, "options.sstableCompressionLevel must be a number.").ThrowAsJavaScriptException(); return; }
        }

        // Parse EncryptionOptions
        if (js_options.Has("encryptionOptions")) {
            Napi::Value encOptsVal = js_options.Get("encryptionOptions");
            if (encOptsVal.IsObject()) {
                Napi::Object js_enc_opts = encOptsVal.As<Napi::Object>();
                if (js_enc_opts.Has("password") && js_enc_opts.Get("password").IsString()) { 
                    encryption_password_from_js = js_enc_opts.Get("password").As<Napi::String>().Utf8Value();
                }
                if (js_enc_opts.Has("scheme") && js_enc_opts.Get("scheme").IsString()) { 
                    std::string scheme_str = js_enc_opts.Get("scheme").As<Napi::String>().Utf8Value();
                                        if (scheme_str == "AES256_GCM_PBKDF2") {
                        selected_encryption_scheme_cpp = EncryptionScheme::AES256_GCM_PBKDF2;
                    } else if (scheme_str == "NONE") {
                        selected_encryption_scheme_cpp = EncryptionScheme::NONE;
                    } else {
                        // Throw an error for any unsupported scheme string from JS.
                        Napi::TypeError::New(env, "Unsupported encryption scheme: '" + scheme_str + "'. Use 'AES256_GCM_PBKDF2' or 'NONE'.").ThrowAsJavaScriptException();
                        return;
                    }
                    // else keep default NONE or let C++ logic handle invalid string
                }
                if (js_enc_opts.Has("kdfIterations") && js_enc_opts.Get("kdfIterations").IsNumber()) { 
                    kdf_iterations_from_js = js_enc_opts.Get("kdfIterations").As<Napi::Number>().Int32Value();
                }
            } else { Napi::TypeError::New(env, "options.encryptionOptions must be an object.").ThrowAsJavaScriptException(); return; }
        }
        // Default encryption scheme if password provided but no scheme
        if (encryption_password_from_js.has_value() && !encryption_password_from_js->empty() && 
            selected_encryption_scheme_cpp == EncryptionScheme::NONE) {
            selected_encryption_scheme_cpp = EncryptionScheme::AES256_GCM_PBKDF2;
        }
        if ((!encryption_password_from_js.has_value() || encryption_password_from_js->empty()) && 
             selected_encryption_scheme_cpp != EncryptionScheme::NONE) {
            selected_encryption_scheme_cpp = EncryptionScheme::NONE; // Force NONE if no password
        }

        // Parse Cache Options
        if (js_options.Has("enableCache")) {
            Napi::Value enableCacheVal = js_options.Get("enableCache");
            if (enableCacheVal.IsBoolean()) enable_cache_cpp = enableCacheVal.As<Napi::Boolean>().Value();
            else { Napi::TypeError::New(env, "options.enableCache must be a boolean.").ThrowAsJavaScriptException(); return; }
        }
        if (enable_cache_cpp && js_options.Has("cacheOptions")) {
            Napi::Value cacheOptsVal = js_options.Get("cacheOptions");
            if (cacheOptsVal.IsObject()) {
                Napi::Object js_cache_opts = cacheOptsVal.As<Napi::Object>();
                if (js_cache_opts.Has("maxSize") && js_cache_opts.Get("maxSize").IsNumber()) {
                    int64_t ms = js_cache_opts.Get("maxSize").As<Napi::Number>().Int64Value();
                    if (ms > 0) cache_config_cpp.max_size = static_cast<size_t>(ms);
                }
                if (js_cache_opts.Has("policy") && js_cache_opts.Get("policy").IsString()) {
                    std::string pol_str = js_cache_opts.Get("policy").As<Napi::String>().Utf8Value();
                    if (pol_str == "LRU") cache_config_cpp.policy = cache::EvictionPolicy::LRU;
                    else if (pol_str == "LFU") cache_config_cpp.policy = cache::EvictionPolicy::LFU;
                }
                if (js_cache_opts.Has("defaultTTLMilliseconds") && js_cache_opts.Get("defaultTTLMilliseconds").IsNumber()) {
                    int64_t ttl_ms = js_cache_opts.Get("defaultTTLMilliseconds").As<Napi::Number>().Int64Value();
                     if (ttl_ms >= 0) cache_config_cpp.default_ttl = std::chrono::milliseconds(ttl_ms);
                }
                if (js_cache_opts.Has("enableStats") && js_cache_opts.Get("enableStats").IsBoolean()) {
                    cache_config_cpp.enable_stats = js_cache_opts.Get("enableStats").As<Napi::Boolean>().Value();
                }
            } else { Napi::TypeError::New(env, "options.cacheOptions must be an object if enableCache is true.").ThrowAsJavaScriptException(); return;}
        }
        if (js_options.Has("lsmOptions")) {
            Napi::Value lsmOptsVal = js_options.Get("lsmOptions");
            if (!lsmOptsVal.IsObject()) {
                Napi::TypeError::New(env, "options.lsmOptions must be an object.").ThrowAsJavaScriptException();
                return; // Stop constructor execution
            }

            Napi::Object js_lsm_opts = lsmOptsVal.As<Napi::Object>();
            if (js_lsm_opts.Has("defaultMemTableType") && js_lsm_opts.Get("defaultMemTableType").IsString()) {
                std::string type_str = js_lsm_opts.Get("defaultMemTableType").As<Napi::String>().Utf8Value();
                
                LOG_INFO("[NAPI IndinisWrapper Ctor] Found lsmOptions.defaultMemTableType: '{}'", type_str);

                if (type_str == "SkipList") {
                    selected_memtable_type_cpp = engine::lsm::MemTableType::SKIP_LIST;
                } else if (type_str == "Vector") {
                    selected_memtable_type_cpp = engine::lsm::MemTableType::VECTOR;
                } else if (type_str == "Hash") {
                    selected_memtable_type_cpp = engine::lsm::MemTableType::HASH_TABLE;
                } else if (type_str == "PrefixHash") {
                    selected_memtable_type_cpp = engine::lsm::MemTableType::PREFIX_HASH_TABLE;
                } else if (type_str == "CompressedSkipList") {
                    selected_memtable_type_cpp = engine::lsm::MemTableType::COMPRESSED_SKIP_LIST;
                } else if (type_str == "RCU") {
                    selected_memtable_type_cpp = engine::lsm::MemTableType::RCU;
                } else {
                    Napi::TypeError::New(env, "Invalid lsmOptions.defaultMemTableType. Must be one of: 'SkipList', 'Vector', 'Hash', 'PrefixHash', 'CompressedSkipList', or 'RCU'.").ThrowAsJavaScriptException();
                    return;
                }
            }

            if (js_lsm_opts.Has("hybridCompactionOptions") && js_lsm_opts.Get("hybridCompactionOptions").IsObject()) {
                Napi::Object js_hybrid_opts = js_lsm_opts.Get("hybridCompactionOptions").As<Napi::Object>();
                
                // Parse the threshold
                if (js_hybrid_opts.Has("leveledToUniversalThreshold") && js_hybrid_opts.Get("leveledToUniversalThreshold").IsNumber()) {
                    int threshold = js_hybrid_opts.Get("leveledToUniversalThreshold").As<Napi::Number>().Int32Value();
                    if (threshold > 0) {
                        hybrid_config_cpp.leveled_to_universal_threshold = static_cast<size_t>(threshold);
                    }
                }

                // Parse nested Leveled options
                if (js_hybrid_opts.Has("leveledOptions") && js_hybrid_opts.Get("leveledOptions").IsObject()) {
                    Napi::Object js_leveled_opts = js_hybrid_opts.Get("leveledOptions").As<Napi::Object>();
                    // (Use the same parsing logic as before for LeveledCompactionConfig,
                    // but populate hybrid_config_cpp.leveled_config)
                }

                // Parse nested Universal options
                if (js_hybrid_opts.Has("universalOptions") && js_hybrid_opts.Get("universalOptions").IsObject()) {
                    Napi::Object js_univ_opts = js_hybrid_opts.Get("universalOptions").As<Napi::Object>();
                    // (Use the same parsing logic as before for UniversalCompactionConfig,
                    // but populate hybrid_config_cpp.universal_config)
                }
            }
            if (js_lsm_opts.Has("compactionStrategy") && js_lsm_opts.Get("compactionStrategy").IsString()) {
                std::string strategy_str = js_lsm_opts.Get("compactionStrategy").As<Napi::String>().Utf8Value();
                LOG_INFO("[NAPI IndinisWrapper Ctor] Found lsmOptions.compactionStrategy: '{}'", strategy_str);

                if (strategy_str == "LEVELED") {
                    selected_compaction_strategy_cpp = engine::lsm::CompactionStrategyType::LEVELED;
                } else if (strategy_str == "UNIVERSAL") {
                    selected_compaction_strategy_cpp = engine::lsm::CompactionStrategyType::UNIVERSAL;
                } else if (strategy_str == "FIFO") {
                    selected_compaction_strategy_cpp = engine::lsm::CompactionStrategyType::FIFO;
                } else {
                    Napi::TypeError::New(env, "Invalid lsmOptions.compactionStrategy. Must be 'LEVELED', 'UNIVERSAL', or 'FIFO'.").ThrowAsJavaScriptException();
                    return;
                }
            }
            if (js_lsm_opts.Has("leveledCompactionOptions") && js_lsm_opts.Get("leveledCompactionOptions").IsObject()) {
                Napi::Object js_leveled_opts = js_lsm_opts.Get("leveledCompactionOptions").As<Napi::Object>();
                
                // Helper lambda to safely parse a numeric property
                auto parse_size_t = [&](const char* propName, size_t& target) {
                    if (js_leveled_opts.Has(propName) && js_leveled_opts.Get(propName).IsNumber()) {
                        double val = js_leveled_opts.Get(propName).As<Napi::Number>().DoubleValue();
                        if (val >= 0) target = static_cast<size_t>(val);
                    }
                };
                auto parse_double = [&](const char* propName, double& target) {
                    if (js_leveled_opts.Has(propName) && js_leveled_opts.Get(propName).IsNumber()) {
                        target = js_leveled_opts.Get(propName).As<Napi::Number>().DoubleValue();
                    }
                };

                parse_size_t("l0CompactionTrigger", leveled_config_cpp.l0_compaction_trigger);
                parse_size_t("l0SlowdownTrigger", leveled_config_cpp.l0_slowdown_trigger);
                parse_size_t("l0StopTrigger", leveled_config_cpp.l0_stop_trigger);
                parse_size_t("sstableTargetSizeBytes", leveled_config_cpp.sstable_target_size);
                parse_double("levelSizeMultiplier", leveled_config_cpp.level_size_multiplier);
                parse_size_t("maxCompactionBytes", leveled_config_cpp.max_compaction_bytes);
                parse_size_t("maxFilesPerCompaction", leveled_config_cpp.max_files_per_compaction);
            }
            if (js_lsm_opts.Has("fifoCompactionOptions") && js_lsm_opts.Get("fifoCompactionOptions").IsObject()) {
                Napi::Object js_fifo_opts = js_lsm_opts.Get("fifoCompactionOptions").As<Napi::Object>();
                if (js_fifo_opts.Has("ttlSeconds") && js_fifo_opts.Get("ttlSeconds").IsNumber()) {
                    fifo_config_cpp.ttl_seconds = std::chrono::seconds(js_fifo_opts.Get("ttlSeconds").As<Napi::Number>().Int64Value());
                }
                // ... parse other fifo options ...
            }
            if (js_lsm_opts.Has("universalCompactionOptions") && js_lsm_opts.Get("universalCompactionOptions").IsObject()) {
                Napi::Object js_univ_opts = js_lsm_opts.Get("universalCompactionOptions").As<Napi::Object>();
                
                auto parse_size_t = [&](const char* propName, size_t& target) { /* ... same helper ... */ };
                auto parse_double = [&](const char* propName, double& target) { /* ... same helper ... */ };

                parse_size_t("sizeRatioPercent", universal_config_cpp.size_ratio_percent);
                parse_size_t("minMergeWidth", universal_config_cpp.min_merge_width);
                parse_size_t("maxMergeWidth", universal_config_cpp.max_merge_width);
                parse_size_t("l0CompactionTrigger", universal_config_cpp.l0_compaction_trigger);
                parse_size_t("l0SlowdownTrigger", universal_config_cpp.l0_slowdown_trigger);
                parse_size_t("l0StopTrigger", universal_config_cpp.l0_stop_trigger);
                parse_double("maxSpaceAmplification", universal_config_cpp.max_space_amplification);
            }
            // Future: Parse rcuOptions here if needed
            // if (js_lsm_opts.Has("rcuOptions") && js_lsm_opts.Get("rcuOptions").IsObject()) {
            //     Napi::Object rcuOpts = js_lsm_opts.Get("rcuOptions").As<Napi::Object>();
            //     // Parse gracePeriodMilliseconds, memoryThresholdBytes, etc.
            //     // and store them in a C++ RCUConfig struct to be passed to the StorageEngine.
            // }
        }
        if (js_options.Has("minCompactionThreads")) {
            Napi::Value val = js_options.Get("minCompactionThreads");
            if (val.IsNumber()) {
                int32_t min_threads = val.As<Napi::Number>().Int32Value();
                if (min_threads > 0) min_compaction_threads_cpp = static_cast<size_t>(min_threads);
            } else {
                Napi::TypeError::New(env, "options.minCompactionThreads must be a positive number.").ThrowAsJavaScriptException();
                return;
            }
        }
        if (js_options.Has("maxCompactionThreads")) {
            Napi::Value val = js_options.Get("maxCompactionThreads");
            if (val.IsNumber()) {
                int32_t max_threads = val.As<Napi::Number>().Int32Value();
                if (max_threads >= (int32_t)min_compaction_threads_cpp) {
                    max_compaction_threads_cpp = static_cast<size_t>(max_threads);
                } else {
                    Napi::TypeError::New(env, "options.maxCompactionThreads must be greater than or equal to minCompactionThreads.").ThrowAsJavaScriptException();
                    return;
                }
            } else {
                Napi::TypeError::New(env, "options.maxCompactionThreads must be a number.").ThrowAsJavaScriptException();
                return;
            }
        }
    } else { // No options object provided, set default WAL directory based on canonical data_dir_path_key_
        effective_wal_config.wal_directory = (std::filesystem::path(this->data_dir_path_key_) / "wal_data").lexically_normal().string();
    }

    if (env.IsExceptionPending()) {
        LOG_ERROR("[NAPI IndinisWrapper Ctor] Exception pending after parsing options for path: {}", this->data_dir_path_key_);
        return;
    }

    // 3. Singleton Logic: Get or Create StorageEngine instance
    std::lock_guard<std::mutex> lock(g_engine_instances_mutex); // Lock the global map
    
    auto it = g_active_engine_instances.find(this->data_dir_path_key_);
    if (it != g_active_engine_instances.end()) {
        // Engine for this path already exists, re-use its shared_ptr
        this->storage_engine_sp_ = it->second;
        LOG_INFO("[NAPI IndinisWrapper Ctor] Re-using existing StorageEngine instance for path key: {}", this->data_dir_path_key_);
        // Note: Options from this constructor call are effectively ignored if an engine for this path already exists.
        // The engine uses the options it was *first* initialized with.
        // This could be logged as a warning if options differ significantly.
    } else {
        // No existing engine for this path, create a new one
        LOG_INFO("[NAPI IndinisWrapper Ctor] Creating NEW StorageEngine instance for path key: {}", this->data_dir_path_key_);
        LOG_INFO("[NAPI IndinisWrapper Ctor]   Effective WAL Config for new engine - Dir: '{}', SegSize: {}, Prefix: '{}'",
                 effective_wal_config.wal_directory, effective_wal_config.segment_size, effective_wal_config.wal_file_prefix);

        try {
            // Use the canonical this->data_dir_path_key_ for the StorageEngine constructor
            this->storage_engine_sp_ = std::make_shared<StorageEngine>(
                this->data_dir_path_key_,
                    checkpoint_interval_cpp,
                    effective_wal_config,
                    sstable_block_size_bytes_cpp,
                    sstable_compression_type_cpp,
                    sstable_compression_level_cpp,
                    encryption_password_from_js,
                    selected_encryption_scheme_cpp,
                    kdf_iterations_from_js,
                    enable_cache_cpp,
                    cache_config_cpp,
                    selected_memtable_type_cpp,
                    min_compaction_threads_cpp, 
                    max_compaction_threads_cpp,
                    selected_compaction_strategy_cpp,
                    leveled_config_cpp,
                    universal_config_cpp,
                    fifo_config_cpp,
                    hybrid_config_cpp  
            );
            g_active_engine_instances[this->data_dir_path_key_] = this->storage_engine_sp_;
            LOG_INFO("[NAPI IndinisWrapper Ctor] New StorageEngine created and cached for path key: {}", this->data_dir_path_key_);
        } catch (const std::exception& e) {
            g_active_engine_instances.erase(this->data_dir_path_key_); // Clean up map if constructor failed
            Napi::Error::New(env, "Indinis C++ engine initialization failed: " + std::string(e.what())).ThrowAsJavaScriptException();
            return;
        } catch (...) {
            g_active_engine_instances.erase(this->data_dir_path_key_);
            Napi::Error::New(env, "Unknown C++ error during Indinis engine initialization").ThrowAsJavaScriptException();
            return;
        }
    }
    // g_engine_instances_mutex is unlocked here
}

IndinisWrapper::~IndinisWrapper() {
    LOG_INFO("[NAPI IndinisWrapper Dtor] Destructor called for path key: '{}'. Current is_closed_ state: {}", 
             data_dir_path_key_.empty() ? "UNKNOWN (key empty)" : data_dir_path_key_, 
             is_closed_);

    if (!is_closed_) {
        // This scenario means the JS Indinis object was garbage collected without an explicit db.close() call.
        // We should attempt to release resources gracefully, similar to what Close() would do.
        LOG_WARN("[NAPI IndinisWrapper Dtor] Destructor called without explicit JS close() for path key: '{}'. Attempting cleanup.",
                 data_dir_path_key_);

        if (storage_engine_sp_) { // If we still hold a shared_ptr to an engine
            std::lock_guard<std::mutex> lock(g_engine_instances_mutex);

            // Check the use_count of the shared_ptr held by the global map for this key.
            // If this IndinisWrapper's storage_engine_sp_ is about to be destroyed,
            // and the map's shared_ptr would then be the *only* remaining one,
            // we should remove it from the map to allow the C++ StorageEngine to destruct.
            auto map_it = g_active_engine_instances.find(data_dir_path_key_);
            if (map_it != g_active_engine_instances.end()) {
                // map_it->second is the shared_ptr in the global map
                // this->storage_engine_sp_ is the shared_ptr in this wrapper instance

                // If the shared_ptr in the map is the same instance as ours,
                // and its use_count is 2 (one here, one in map), then erasing from map
                // and resetting our local one will trigger destruction.
                // If use_count is 1 (only in map, our sp already reset by Close), map erase triggers.
                // If use_count > 2 (other JS wrappers + map), map entry remains.
                if (map_it->second.get() == storage_engine_sp_.get()) { // Ensure it's the same engine instance
                    if (map_it->second.use_count() <= 2) { // Our wrapper + map, or just map (if close already ran some logic)
                        LOG_INFO("[NAPI IndinisWrapper Dtor] Path key '{}': This seems to be the last/one of last strong references. Removing from global cache.", data_dir_path_key_);
                        g_active_engine_instances.erase(map_it);
                        // storage_engine_sp_.reset() will happen automatically when this dtor finishes for the member.
                        // The C++ StorageEngine destructor will run if its ref count hits zero.
                    } else {
                        LOG_INFO("[NAPI IndinisWrapper Dtor] Path key '{}': Other JS wrappers likely still exist (map use_count: {}). Engine will persist.", 
                                 data_dir_path_key_, map_it->second.use_count());
                    }
                } else if (map_it->second) { // Map has an engine, but not the one this wrapper points to (should not happen with current singleton logic)
                     LOG_WARN("[NAPI IndinisWrapper Dtor] Path key '{}': Mismatch between wrapper's engine and map's engine. Global map use_count: {}. Wrapper's use_count before reset: {}", 
                              data_dir_path_key_, map_it->second.use_count(), storage_engine_sp_.use_count());
                } else { // Map has an entry but the shared_ptr is null (should not happen)
                     LOG_WARN("[NAPI IndinisWrapper Dtor] Path key '{}': Global map has a null shared_ptr for this key. Removing.", data_dir_path_key_);
                     g_active_engine_instances.erase(map_it);
                }
            } else {
                LOG_INFO("[NAPI IndinisWrapper Dtor] Path key '{}' not found in global active engine cache (possibly already cleaned by Close() or another wrapper). Wrapper's use_count before reset: {}", 
                         data_dir_path_key_, storage_engine_sp_.use_count());
            }
        } else {
             LOG_INFO("[NAPI IndinisWrapper Dtor] Path key '{}': storage_engine_sp_ is already null. No further map cleanup needed from this instance.", data_dir_path_key_);
        }
        // The member storage_engine_sp_ will be reset automatically, decrementing its ref count.
        // If this was the last strong reference (considering g_active_engine_instances),
        // the C++ StorageEngine destructor will run.
    } else {
        LOG_TRACE("[NAPI IndinisWrapper Dtor] Destructor called for path key: '{}', was already explicitly closed. No specific map action from here.",
                  data_dir_path_key_);
    }
    // storage_engine_sp_ (std::shared_ptr member) is automatically reset/destructed here,
    // which decrements the reference count of the C++ StorageEngine.
    LOG_TRACE("[NAPI IndinisWrapper Dtor] Finished for path key: '{}'.", 
              data_dir_path_key_.empty() ? "UNKNOWN" : data_dir_path_key_);
}

Napi::Value IndinisWrapper::RegisterStoreSchema(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);

    if (is_closed_ || !storage_engine_sp_) {
        deferred.Reject(Napi::Error::New(env, "Indinis engine not initialized or closed").Value());
        return deferred.Promise();
    }

    if (info.Length() < 1 || !info[0].IsObject()) {
        deferred.Reject(Napi::TypeError::New(env, "registerStoreSchema requires one argument: a schema object.").Value());
        return deferred.Promise();
    }

    try {
        Napi::Object jsSchema = info[0].As<Napi::Object>();
        engine::columnar::ColumnSchema cppSchema = parseJsSchema(env, jsSchema);
        
        bool success = storage_engine_sp_->registerStoreSchema(cppSchema);
        
        deferred.Resolve(Napi::Boolean::New(env, success));
    } catch (const Napi::Error& e) {
        deferred.Reject(e.Value()); // Re-throw JS-based errors
    } catch (const std::exception& e) {
        deferred.Reject(Napi::Error::New(env, "C++ error registering schema: " + std::string(e.what())).Value());
    }

    return deferred.Promise();
}

Napi::Value IndinisWrapper::DebugVerifyFreeList(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);
    if (is_closed_ || !storage_engine_sp_ ) { // Use storage_engine_sp_
        deferred.Reject(Napi::Error::New(env, "Engine not available...").Value());
        return deferred.Promise();
    }
    SimpleDiskManager* dm = storage_engine_sp_->getDiskManager(); // Use storage_engine_sp_
    if (!dm) {
        deferred.Reject(Napi::Error::New(env, "DiskManager not available for debug_verifyFreeList.").Value());
        return deferred.Promise();
    }

    try {
        bool result = dm->verifyFreeList();
        deferred.Resolve(Napi::Boolean::New(env, result));

    } catch (const std::exception& e) {
        deferred.Reject(Napi::Error::New(env, "C++ std::exception in verifyFreeList: " + std::string(e.what())).Value());
    }
    return deferred.Promise();
}

Napi::Value IndinisWrapper::DebugAnalyzeFragmentation(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);
    if (is_closed_ || !storage_engine_sp_ ) {
        deferred.Reject(Napi::Error::New(env, "Engine not available for debug_analyzeFragmentation.").Value());
        return deferred.Promise();
    }
    SimpleDiskManager* dm = storage_engine_sp_->getDiskManager(); // Use getter
    if (!dm) {
        deferred.Reject(Napi::Error::New(env, "DiskManager not available for debug_analyzeFragmentation.").Value());
        return deferred.Promise();
    }

    try {
        SimpleDiskManager::DefragmentationStats cppStats = dm->analyzeFragmentation();
        Napi::Object jsStats = Napi::Object::New(env);
        jsStats.Set("total_pages", Napi::Number::New(env, static_cast<double>(cppStats.total_pages)));
        jsStats.Set("allocated_pages", Napi::Number::New(env, static_cast<double>(cppStats.allocated_pages)));
        jsStats.Set("free_pages", Napi::Number::New(env, static_cast<double>(cppStats.free_pages)));
        jsStats.Set("fragmentation_gaps", Napi::Number::New(env, static_cast<double>(cppStats.fragmentation_gaps)));
        jsStats.Set("largest_gap", Napi::Number::New(env, static_cast<double>(cppStats.largest_gap)));
        jsStats.Set("potential_pages_saved", Napi::Number::New(env, static_cast<double>(cppStats.potential_pages_saved)));
        jsStats.Set("fragmentation_ratio", Napi::Number::New(env, cppStats.fragmentation_ratio));
        deferred.Resolve(jsStats);

    } catch (const std::exception& e) {
        deferred.Reject(Napi::Error::New(env, "C++ std::exception in analyzeFragmentation: " + std::string(e.what())).Value());
    }
    return deferred.Promise();
}

Napi::Value IndinisWrapper::DebugDefragment(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);
    if (is_closed_ || !storage_engine_sp_ ) {
        deferred.Reject(Napi::Error::New(env, "Engine not available for debug_defragment.").Value());
        return deferred.Promise();
    }
    SimpleDiskManager* dm = storage_engine_sp_->getDiskManager(); // Use getter
    if (!dm) {
        deferred.Reject(Napi::Error::New(env, "DiskManager not available for debug_defragment.").Value());
        return deferred.Promise();
    }

    if (info.Length() < 1 || !info[0].IsString()) {
        deferred.Reject(Napi::TypeError::New(env, "Defragment mode ('CONSERVATIVE' or 'AGGRESSIVE') required.").Value());
        return deferred.Promise();
    }
    std::string modeStr = info[0].As<Napi::String>().Utf8Value();
    SimpleDiskManager::DefragmentationMode mode;
    if (modeStr == "CONSERVATIVE") mode = SimpleDiskManager::DefragmentationMode::CONSERVATIVE;
    else if (modeStr == "AGGRESSIVE") mode = SimpleDiskManager::DefragmentationMode::AGGRESSIVE;
    else {
        deferred.Reject(Napi::TypeError::New(env, "Invalid defragment mode string.").Value());
        return deferred.Promise();
    }

    try {
        // For JS testing, direct callback might be complex. We rely on promise resolve/reject.
        bool result = dm->defragment(mode, nullptr); 
        deferred.Resolve(Napi::Boolean::New(env, result));

    } catch (const std::exception& e) {
        deferred.Reject(Napi::Error::New(env, "C++ std::exception in defragment: " + std::string(e.what())).Value());
    }
    return deferred.Promise();
}


Napi::Value IndinisWrapper::DebugGetDatabaseStats(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);
    if (is_closed_ || !storage_engine_sp_ ) {
        deferred.Reject(Napi::Error::New(env, "Engine not available for debug_getDatabaseStats.").Value());
        return deferred.Promise();
    }
    SimpleDiskManager* dm = storage_engine_sp_->getDiskManager(); // Use getter
    if (!dm) {
        deferred.Reject(Napi::Error::New(env, "DiskManager not available for debug_getDatabaseStats.").Value());
        return deferred.Promise();
    }

    try {
        SimpleDiskManager::DatabaseStats cppStats = dm->getDatabaseStats();
        Napi::Object jsStats = Napi::Object::New(env);
        jsStats.Set("file_size_bytes", Napi::Number::New(env, static_cast<double>(cppStats.file_size_bytes)));
        jsStats.Set("total_pages", Napi::Number::New(env, static_cast<double>(cppStats.total_pages)));
        jsStats.Set("allocated_pages", Napi::Number::New(env, static_cast<double>(cppStats.allocated_pages)));
        jsStats.Set("free_pages", Napi::Number::New(env, static_cast<double>(cppStats.free_pages)));
        jsStats.Set("next_page_id", Napi::Number::New(env, static_cast<double>(cppStats.next_page_id)));
        jsStats.Set("num_btrees", Napi::Number::New(env, static_cast<double>(cppStats.num_btrees)));
        jsStats.Set("utilization_ratio", Napi::Number::New(env, cppStats.utilization_ratio));
        deferred.Resolve(jsStats);

    } catch (const std::exception& e) {
        deferred.Reject(Napi::Error::New(env, "C++ std::exception in getDatabaseStats: " + std::string(e.what())).Value());
    }
    return deferred.Promise();
}

// Add this new method implementation within IndinisWrapper
Napi::Value IndinisWrapper::GetCacheStats(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);

    if (is_closed_ || !storage_engine_sp_) {
        deferred.Reject(Napi::Error::New(env, "Indinis engine not initialized or closed").Value());
        return deferred.Promise();
    }

    try {
        std::optional<cache::CacheStats> stats_opt = storage_engine_sp_->getCacheStats();
        if (stats_opt) {
            const cache::CacheStats& stats = *stats_opt;
            Napi::Object statsObj = Napi::Object::New(env);
            statsObj.Set("hits", Napi::Number::New(env, static_cast<double>(stats.get_hits())));
            statsObj.Set("misses", Napi::Number::New(env, static_cast<double>(stats.get_misses())));
            statsObj.Set("evictions", Napi::Number::New(env, static_cast<double>(stats.get_evictions())));
            statsObj.Set("expiredRemovals", Napi::Number::New(env, static_cast<double>(stats.get_expired_removals())));
            statsObj.Set("hitRate", Napi::Number::New(env, stats.get_hit_rate()));
            deferred.Resolve(statsObj);
        } else {
            deferred.Resolve(env.Null()); // Stats not enabled or cache disabled
        }
    } catch (const std::exception& e) {
        deferred.Reject(Napi::Error::New(env, "C++ error getting cache stats: " + std::string(e.what())).Value());
    } catch (...) {
        deferred.Reject(Napi::Error::New(env, "Unknown C++ error getting cache stats").Value());
    }
    return deferred.Promise();
}




Napi::Value IndinisWrapper::DebugScanBTree(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    IndexManager* indexManager = nullptr;
    if (!is_closed_ && this->storage_engine_sp_) {
        indexManager = this->storage_engine_sp_->getIndexManager();
    }
    if (!indexManager) {
        Napi::Error::New(env, "Indinis engine or index manager not initialized or closed").ThrowAsJavaScriptException();
        return env.Null();
    }

    // Expect: indexName (string), startKey (Buffer), endKey (Buffer)
    if (info.Length() < 3 || !info[0].IsString() || !info[1].IsBuffer() || !info[2].IsBuffer()) {
        Napi::TypeError::New(env, "debug_scanBTree requires indexName (string), startKey (Buffer), endKey (Buffer)").ThrowAsJavaScriptException();
        return env.Null();
    }

    std::string index_name = info[0].As<Napi::String>().Utf8Value();
    
    Napi::Buffer<char> startKeyNapiBuffer = info[1].As<Napi::Buffer<char>>();
    std::string start_key_cpp(startKeyNapiBuffer.Data(), startKeyNapiBuffer.Length());

    Napi::Buffer<char> endKeyNapiBuffer = info[2].As<Napi::Buffer<char>>();
    std::string end_key_cpp(endKeyNapiBuffer.Data(), endKeyNapiBuffer.Length());

    LOG_TRACE("  [NAPI DebugScanBTree] Index: '", index_name, 
              "', StartKey (hex): '", format_key_for_print(start_key_cpp), // Use your existing debug formatter
              "', EndKey (hex): '", format_key_for_print(end_key_cpp), "'");

    std::shared_ptr<BTree> btree = indexManager->getIndexBTree(index_name);
    if (!btree) {
        Napi::Error::New(env, "Index '" + index_name + "' not found.").ThrowAsJavaScriptException();
        return env.Null();
    }

    try {
        // NEW: The C++ function now returns a vector of pairs
        std::vector<std::pair<std::string, std::string>> cpp_results = btree->rangeScanKeys(start_key_cpp, end_key_cpp, true, true);

        Napi::Array resultArray = Napi::Array::New(env, cpp_results.size());
        for (size_t i = 0; i < cpp_results.size(); ++i) {
            const auto& pair = cpp_results[i];
            
            Napi::Object resultObj = Napi::Object::New(env);
            resultObj.Set("key", Napi::Buffer<char>::Copy(env, pair.first.data(), pair.first.length()));
            resultObj.Set("value", Napi::Buffer<char>::Copy(env, pair.second.data(), pair.second.length()));
            
            resultArray.Set(i, resultObj);
        }
        return resultArray;

    } catch (const std::exception& e) {
        Napi::Error::New(env, "C++ error during debug BTree scan: " + std::string(e.what())).ThrowAsJavaScriptException();
        return env.Null();
    } catch (...) {
        Napi::Error::New(env, "Unknown C++ error during debug BTree scan").ThrowAsJavaScriptException();
        return env.Null();
    }
}

Napi::Value IndinisWrapper::ChangeDatabasePassword(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);

    if (is_closed_ || !storage_engine_sp_) {
        deferred.Reject(Napi::Error::New(env, "Indinis engine not initialized or closed").Value());
        return deferred.Promise();
    }

    if (info.Length() < 2 || !info[0].IsString() || !info[1].IsString() ||
        (info.Length() > 2 && !info[2].IsNumber())) {
        deferred.Reject(Napi::TypeError::New(env, "changeDatabasePassword requires oldPassword (string), newPassword (string), and optional newKdfIterations (number)").Value());
        return deferred.Promise();
    }

    std::string old_password = info[0].As<Napi::String>().Utf8Value();
    std::string new_password = info[1].As<Napi::String>().Utf8Value();
    int new_kdf_iterations = 0;
    if (info.Length() > 2) {
        new_kdf_iterations = info[2].As<Napi::Number>().Int32Value();
    }

    try {
        // Add a new method to StorageEngine C++ class
        bool result = storage_engine_sp_->changeMasterPassword(old_password, new_password, new_kdf_iterations);
        deferred.Resolve(Napi::Boolean::New(env, result));
    } catch (const CryptoException& ce) { // Catch specific crypto errors for better messages
        deferred.Reject(Napi::Error::New(env, "Cryptographic error changing password: " + std::string(ce.what())).Value());
    } catch (const std::exception& e) {
        deferred.Reject(Napi::Error::New(env, "C++ error changing password: " + std::string(e.what())).Value());
    } catch (...) {
        deferred.Reject(Napi::Error::New(env, "Unknown C++ error changing password").Value());
    }

    return deferred.Promise();
}


Napi::Value IndinisWrapper::BeginTransaction(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_closed_ || !storage_engine_sp_) {
        Napi::Error::New(env, "Indinis engine is not initialized or has been closed").ThrowAsJavaScriptException();
        return env.Null();
    }

    std::shared_ptr<Transaction> cpp_txn_ptr; // Renamed for clarity
    try {
        cpp_txn_ptr = storage_engine_sp_->beginTransaction();
        if (!cpp_txn_ptr) {
            Napi::Error::New(env, "Internal error: Failed to begin C++ transaction (returned null)").ThrowAsJavaScriptException();
            return env.Null();
        }
    } catch (const std::exception& e) {
        Napi::Error::New(env, "C++ error beginning transaction: " + std::string(e.what())).ThrowAsJavaScriptException();
        return env.Null();
    } catch (...) {
        Napi::Error::New(env, "Unknown C++ error beginning transaction").ThrowAsJavaScriptException();
        return env.Null();
    }

    if (!IndinisWrapper::transactionWrapperConstructor || IndinisWrapper::transactionWrapperConstructor.IsEmpty()) {
        Napi::Error::New(env, "Internal setup error: TransactionWrapper constructor not available").ThrowAsJavaScriptException();
        return env.Null();
    }
    Napi::Object txnObj = IndinisWrapper::transactionWrapperConstructor.New({});
    if (txnObj.IsEmpty()) {
        Napi::Error::New(env, "Internal setup error: Failed to create new TransactionWrapper instance").ThrowAsJavaScriptException();
        return env.Null();
    }
    TransactionWrapper* wrapper_instance = TransactionWrapper::Unwrap(txnObj); // Renamed
    if (!wrapper_instance) {
        Napi::Error::New(env, "Internal setup error: Failed to unwrap TransactionWrapper instance").ThrowAsJavaScriptException();
        return env.Null();
    }

    // **CORRECTED CALL**
    wrapper_instance->SetTransactionAndEngine(cpp_txn_ptr, this); 

    return txnObj;
}

/**
 * @brief N-API binding for the `db.createIndex` method.
 *
 * This function handles the asynchronous creation of a secondary index. It receives the
 * store path, index name, and an options object from JavaScript, validates them,
 * constructs a C++ IndexDefinition, and calls the IndexManager to perform the operation.
 *
 * @param info The Napi::CallbackInfo containing the JS arguments:
 *   - arg 0: storePath (string)
 *   - arg 1: indexName (string)
 *   - arg 2: options (object)
 * @return A Napi::Promise that resolves with a status string ('CREATED' or 'EXISTED')
 *         or rejects with an error.
 */
Napi::Value IndinisWrapper::CreateIndex(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);

    // 1. Get the C++ IndexManager instance
    IndexManager* indexManager = nullptr;
    if (!is_closed_ && this->storage_engine_sp_) {
        indexManager = this->storage_engine_sp_->getIndexManager();
    }
    if (!indexManager) {
        deferred.Reject(Napi::Error::New(env, "Indinis engine or index manager not initialized or closed.").Value());
        return deferred.Promise();
    }

    // 2. Validate JavaScript Arguments
    if (info.Length() < 3 || !info[0].IsString() || !info[1].IsString() || !info[2].IsObject()) {
        deferred.Reject(Napi::TypeError::New(env, "createIndex requires storePath (string), indexName (string), and options (object).").Value());
        return deferred.Promise();
    }
    std::string store_path = info[0].As<Napi::String>().Utf8Value();
    std::string index_name = info[1].As<Napi::String>().Utf8Value();
    Napi::Object options = info[2].As<Napi::Object>();

    // 3. Parse JS Options and Build C++ IndexDefinition
    IndexDefinition definition;
    definition.name = index_name;
    definition.storePath = store_path;

    try {
        // --- Parse Optional Boolean Flags ---
        if (options.Has("unique")) {
            Napi::Value uniqueVal = options.Get("unique");
            if (!uniqueVal.IsBoolean()) {
                throw Napi::TypeError::New(env, "Index option 'unique' must be a boolean.");
            }
            definition.is_unique = uniqueVal.As<Napi::Boolean>().Value();
        }

        if (options.Has("multikey")) {
            Napi::Value multikeyVal = options.Get("multikey");
            if (!multikeyVal.IsBoolean()) {
                throw Napi::TypeError::New(env, "Index option 'multikey' must be a boolean.");
            }
            definition.is_multikey = multikeyVal.As<Napi::Boolean>().Value();
        }

        // --- Parse Field Definitions and Enforce Constraints ---
        if (options.Has("field")) { // Simple index (can be multikey, can be unique)
            if (!options.Get("field").IsString()) {
                throw Napi::TypeError::New(env, "Index option 'field' must be a string.");
            }
            std::string fieldName = options.Get("field").As<Napi::String>().Utf8Value();
            IndexSortOrder order = IndexSortOrder::ASCENDING;
            if (options.Has("order")) {
                std::string orderStr = options.Get("order").As<Napi::String>().Utf8Value();
                if (orderStr == "desc") order = IndexSortOrder::DESCENDING;
            }
            definition.fields.emplace_back(fieldName, order);

        } else if (options.Has("fields")) { // Compound index
            if (definition.is_multikey) {
                // This constraint is critical. A multikey index on multiple fields is ambiguous.
                throw Napi::Error::New(env, "A 'multikey' index can only be defined on a single 'field', not on compound 'fields'.");
            }
            if (!options.Get("fields").IsArray()) {
                throw Napi::TypeError::New(env, "Index option 'fields' must be an array.");
            }
            Napi::Array fieldsArray = options.Get("fields").As<Napi::Array>();
            uint32_t numFields = fieldsArray.Length();
            if (numFields == 0) {
                throw Napi::TypeError::New(env, "Index option 'fields' array cannot be empty.");
            }

            for (uint32_t i = 0; i < numFields; ++i) {
                Napi::Value fieldElement = fieldsArray.Get(i);
                std::string fieldName;
                IndexSortOrder order = IndexSortOrder::ASCENDING;

                if (fieldElement.IsString()) {
                    fieldName = fieldElement.As<Napi::String>().Utf8Value();
                } else if (fieldElement.IsObject()) {
                    Napi::Object fieldObj = fieldElement.As<Napi::Object>();
                    if (!fieldObj.Has("name") || !fieldObj.Get("name").IsString()) {
                        throw Napi::TypeError::New(env, "Field object in 'fields' must have a 'name' of type string.");
                    }
                    fieldName = fieldObj.Get("name").As<Napi::String>().Utf8Value();
                    if (fieldObj.Has("order")) {
                        if (!fieldObj.Get("order").IsString()) throw Napi::TypeError::New(env, "Field option 'order' must be 'asc' or 'desc'.");
                        std::string orderStr = fieldObj.Get("order").As<Napi::String>().Utf8Value();
                        if (orderStr == "desc") order = IndexSortOrder::DESCENDING;
                    }
                } else {
                    throw Napi::TypeError::New(env, "Elements in 'fields' array must be strings or objects like { name: string, order?: 'asc'|'desc' }.");
                }

                if (fieldName.empty()) throw Napi::TypeError::New(env, "Field name in index definition cannot be empty.");
                definition.fields.emplace_back(fieldName, order);
            }
        } else {
            throw Napi::TypeError::New(env, "Index options object must contain either a 'field' (string) or 'fields' (array) property.");
        }

        // Final check on the constructed C++ definition, which includes cross-property constraints.
        if (!definition.isValid()) {
            throw Napi::Error::New(env, "Constructed C++ index definition is invalid (e.g., unique and multikey, or missing required fields).");
        }

    } catch (const Napi::Error& e) {
        deferred.Reject(e.Value());
        return deferred.Promise();
    } catch (const std::exception& e) {
        deferred.Reject(Napi::Error::New(env, "C++ error during index definition parsing: " + std::string(e.what())).Value());
        return deferred.Promise();
    }

    // --- 4. Call the C++ Method Asynchronously ---
    try {
        CreateIndexResult result = indexManager->createIndex(definition);
        
        std::string resultStr;
        switch(result) {
            case CreateIndexResult::CREATED_AND_BACKFILLING:
                resultStr = "CREATED";
                break;
            case CreateIndexResult::ALREADY_EXISTS:
                resultStr = "EXISTED";
                break;
            case CreateIndexResult::FAILED:
                deferred.Reject(Napi::Error::New(env, "Index creation failed for '" + index_name + "'. Check C++ logs for details.").Value());
                return deferred.Promise();
        }
        deferred.Resolve(Napi::String::New(env, resultStr));

    } catch (const std::exception& e) {
        deferred.Reject(Napi::Error::New(env, "C++ error during call to createIndex for '" + index_name + "': " + std::string(e.what())).Value());
    } catch (...) {
        deferred.Reject(Napi::Error::New(env, "Unknown C++ error while creating index '" + index_name + "'").Value());
    }
    
    return deferred.Promise();
}

Napi::Value IndinisWrapper::ForceCheckpoint(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);

    if (is_closed_ || !storage_engine_sp_) {
        deferred.Reject(Napi::Error::New(env, "Indinis engine not initialized or closed").Value());
        return deferred.Promise();
    }

    try {
        bool result = storage_engine_sp_->forceCheckpoint();
        deferred.Resolve(Napi::Boolean::New(env, result));
    } catch (const std::exception& e) {
        deferred.Reject(Napi::Error::New(env, "C++ error forcing checkpoint: " + std::string(e.what())).Value());
    } catch (...) {
        deferred.Reject(Napi::Error::New(env, "Unknown C++ error forcing checkpoint").Value());
    }
    return deferred.Promise();
}

Napi::Value IndinisWrapper::GetCheckpointHistory(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);

    if (is_closed_ || !storage_engine_sp_) {
        deferred.Reject(Napi::Error::New(env, "Indinis engine not initialized or closed").Value());
        return deferred.Promise();
    }
    
    try {
        std::vector<CheckpointInfo> history_cpp = storage_engine_sp_->getCheckpointHistory();
        Napi::Array history_napi = Napi::Array::New(env, history_cpp.size());

        for (size_t i = 0; i < history_cpp.size(); ++i) {
            const auto& cp_info = history_cpp[i];
            Napi::Object obj_napi = Napi::Object::New(env);
            
            obj_napi.Set("checkpoint_id", Napi::Number::New(env, static_cast<double>(cp_info.checkpoint_id)));
            
            std::string status_str;
            switch (cp_info.status) {
                case CheckpointStatus::NOT_STARTED: status_str = "NOT_STARTED"; break;
                case CheckpointStatus::IN_PROGRESS: status_str = "IN_PROGRESS"; break;
                case CheckpointStatus::COMPLETED:   status_str = "COMPLETED"; break;
                case CheckpointStatus::FAILED:      status_str = "FAILED"; break;
                default: status_str = "UNKNOWN";
            }
            obj_napi.Set("status", Napi::String::New(env, status_str));

            // Convert std::chrono::system_clock::time_point to milliseconds since epoch for JS Date
            obj_napi.Set("start_time_ms", Napi::Number::New(env, 
                std::chrono::duration_cast<std::chrono::milliseconds>(cp_info.start_time.time_since_epoch()).count()));
            obj_napi.Set("end_time_ms", Napi::Number::New(env, 
                std::chrono::duration_cast<std::chrono::milliseconds>(cp_info.end_time.time_since_epoch()).count()));
            
            obj_napi.Set("checkpoint_begin_wal_lsn", Napi::String::New(env, std::to_string(cp_info.checkpoint_begin_wal_lsn))); // Send LSN as string for BigInt
            obj_napi.Set("checkpoint_end_wal_lsn", Napi::String::New(env, std::to_string(cp_info.checkpoint_end_wal_lsn)));

            Napi::Array active_txns_napi = Napi::Array::New(env, cp_info.active_txns_at_begin.size());
            uint32_t txn_idx = 0;
            for (TxnId tid : cp_info.active_txns_at_begin) {
                active_txns_napi.Set(txn_idx++, Napi::String::New(env, std::to_string(tid))); // Send TxnId as string for BigInt
            }
            obj_napi.Set("active_txns_at_begin", active_txns_napi);
            obj_napi.Set("error_msg", Napi::String::New(env, cp_info.error_msg));
            
            history_napi.Set(i, obj_napi);
        }
        deferred.Resolve(history_napi);

    } catch (const std::exception& e) {
        deferred.Reject(Napi::Error::New(env, "C++ error getting checkpoint history: " + std::string(e.what())).Value());
    } catch (...) {
        deferred.Reject(Napi::Error::New(env, "Unknown C++ error getting checkpoint history").Value());
    }
    return deferred.Promise();
}

// NAPI Wrapper for deleteIndex
Napi::Value IndinisWrapper::DeleteIndex(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);

    IndexManager* indexManager = nullptr;
    if (!is_closed_ && this->storage_engine_sp_) { indexManager = this->storage_engine_sp_->getIndexManager(); }
    if (!indexManager) {
        deferred.Reject(Napi::Error::New(env, "Indinis engine or index manager not initialized or closed").Value());
        return deferred.Promise();
     }

    if (info.Length() < 1 || !info[0].IsString()) {
        deferred.Reject(Napi::TypeError::New(env, "Internal deleteIndex requires index name (string)").Value());
        return deferred.Promise();
    }
    std::string index_name = info[0].As<Napi::String>().Utf8Value();
     if (index_name.empty()) {
         deferred.Reject(Napi::TypeError::New(env, "Index name cannot be empty").Value());
        return deferred.Promise();
    }

    // Call C++ method
     try {
        bool result = indexManager->deleteIndex(index_name);
        deferred.Resolve(Napi::Boolean::New(env, result));
    } catch (const std::exception& e) {
         deferred.Reject(Napi::Error::New(env, "C++ error deleting index '" + index_name + "': " + std::string(e.what())).Value());
    } catch (...) {
        deferred.Reject(Napi::Error::New(env, "Unknown C++ error deleting index '" + index_name + "'").Value());
    }
    return deferred.Promise();
}

// NAPI Wrapper for listIndexes
Napi::Value IndinisWrapper::ListIndexes(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);

    IndexManager* indexManager = nullptr;
    if (!is_closed_ && this->storage_engine_sp_) {
        indexManager = this->storage_engine_sp_->getIndexManager();
    }
    if (!indexManager) {
        deferred.Reject(Napi::Error::New(env, "Indinis engine or index manager not initialized or closed").Value());
        return deferred.Promise();
    }

    // --- MODIFICATION: Prepare optional storePath for C++ call ---
    std::optional<std::string> storePathOpt;
    if (info.Length() > 0 && info[0].IsString()) {
        storePathOpt = info[0].As<Napi::String>().Utf8Value();
    }
    // If no argument is provided, storePathOpt remains std::nullopt.

    try {
        // Call the updated C++ method
        std::vector<IndexDefinition> definitions = indexManager->listIndexes(storePathOpt);

        // The rest of the conversion logic remains exactly the same
        Napi::Array resultArray = Napi::Array::New(env, definitions.size());
        for (size_t i = 0; i < definitions.size(); ++i) {
            const auto& def = definitions[i];
            Napi::Object jsDef = Napi::Object::New(env);
            jsDef.Set("name", Napi::String::New(env, def.name));
            jsDef.Set("storePath", Napi::String::New(env, def.storePath));
            
            // Add unique and multikey flags to the JS object
            jsDef.Set("unique", Napi::Boolean::New(env, def.is_unique));
            jsDef.Set("multikey", Napi::Boolean::New(env, def.is_multikey));

            Napi::Array jsFields = Napi::Array::New(env, def.fields.size());
            for (size_t j = 0; j < def.fields.size(); ++j) {
                const auto& field = def.fields[j];
                Napi::Object jsField = Napi::Object::New(env);
                jsField.Set("name", Napi::String::New(env, field.name));
                jsField.Set("order", Napi::String::New(env, (field.order == IndexSortOrder::ASCENDING) ? "asc" : "desc"));
                jsFields.Set(j, jsField);
            }
            jsDef.Set("fields", jsFields);
            resultArray.Set(i, jsDef);
        }
        deferred.Resolve(resultArray);

    } catch (const std::exception& e) {
        deferred.Reject(Napi::Error::New(env, "C++ error listing indexes: " + std::string(e.what())).Value());
    } catch (...) {
        deferred.Reject(Napi::Error::New(env, "Unknown C++ error listing indexes").Value());
    }
    return deferred.Promise();
}

Napi::Value IndinisWrapper::Close(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);

    if (is_closed_) {
        LOG_TRACE("[NAPI IndinisWrapper Close] Already closed for path key: {}", data_dir_path_key_);
        deferred.Resolve(env.Undefined());
        return deferred.Promise();
    }
    LOG_INFO("[NAPI IndinisWrapper Close] Explicit close() called for path key: {}", data_dir_path_key_);

    if (storage_engine_sp_) {
        std::lock_guard<std::mutex> lock(g_engine_instances_mutex); // Lock the global map
        
        auto map_it = g_active_engine_instances.find(data_dir_path_key_);
        if (map_it != g_active_engine_instances.end() && map_it->second.get() == storage_engine_sp_.get()) {
            // Check if this wrapper's shared_ptr and the map's shared_ptr are the only strong references
            // A use_count of 2 for map_it->second indicates this. (1 for map, 1 for storage_engine_sp_ in this wrapper)
            // If storage_engine_sp_ is reset, map_it->second.use_count() would become 1.
            // More simply: if our storage_engine_sp_ is valid, and its use_count is 2 (itself + map),
            // then after we reset our storage_engine_sp_, the map's will be 1. Erasing from map triggers dtor.
            if (storage_engine_sp_.use_count() == 2) { 
                 LOG_INFO("[NAPI IndinisWrapper Close] Path key '{}': This wrapper holds one of the last two strong refs. Removing from global cache.", data_dir_path_key_);
                g_active_engine_instances.erase(map_it); 
                // Now, when storage_engine_sp_.reset() happens, it will be the true last reference.
            } else if (storage_engine_sp_.use_count() > 2) {
                 LOG_INFO("[NAPI IndinisWrapper Close] Path key '{}': Other JS wrappers or strong refs exist (current wrapper use_count: {} including map). Engine will persist in cache.", 
                     data_dir_path_key_, storage_engine_sp_.use_count());
            } else { // <= 1 (should not be 0 if storage_engine_sp_ is valid)
                 LOG_WARN("[NAPI IndinisWrapper Close] Path key '{}': Unexpected use_count {} for storage_engine_sp_ before reset. Likely only map holds it or it's already being destructed.", 
                     data_dir_path_key_, storage_engine_sp_.use_count());
                 // If use_count is 1, it means only this->storage_engine_sp_ holds it.
                 // The map entry must have been removed, or this is a logic error.
                 // For safety, try to erase from map again if it exists.
                 if(map_it != g_active_engine_instances.end()){ // Re-check after potential map changes
                    g_active_engine_instances.erase(map_it);
                 }
            }
        } else if (map_it != g_active_engine_instances.end() && map_it->second.get() != storage_engine_sp_.get()) {
             LOG_WARN("[NAPI IndinisWrapper Close] Path key '{}': Mismatch between wrapper's engine and map's engine on close. This is unexpected.", data_dir_path_key_);
        }
        // Else: entry not in map, means it was already cleaned up.
        
        try {
            storage_engine_sp_.reset(); // Release this wrapper's ownership.
                                       // If g_active_engine_instances also released it (or was the last one),
                                       // StorageEngine destructor runs.
            LOG_INFO("[NAPI IndinisWrapper Close] C++ StorageEngine reference released for path key: {}", data_dir_path_key_);
        } catch (const std::exception& e) {
             LOG_ERROR("[NAPI IndinisWrapper Close] Exception during C++ StorageEngine reset for {}: {}", data_dir_path_key_, e.what());
        }
    }
    
    is_closed_ = true;
    deferred.Resolve(env.Undefined());
    return deferred.Promise();
}

// ADD NEW NAPI METHOD IMPLEMENTATIONS:
Napi::Value IndinisWrapper::DebugGetLsmStoreStats(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    LSMTree* store = GetLsmStoreFromNapi(info, this);
    if (!store) {
        return env.Null();
    }
    
    try {
        LSMTree::MemTableStatsSnapshot stats = store->getActiveMemTableStats();
        Napi::Object result = Napi::Object::New(env);
        result.Set("totalSizeBytes", Napi::Number::New(env, static_cast<double>(stats.total_size_bytes)));
        result.Set("recordCount", Napi::Number::New(env, static_cast<double>(stats.record_count)));
        
        Napi::Array partitions = Napi::Array::New(env, stats.partition_stats.size());
        for (size_t i = 0; i < stats.partition_stats.size(); ++i) {
            Napi::Object p_stat = Napi::Object::New(env);
            p_stat.Set("partitionId", Napi::Number::New(env, static_cast<double>(stats.partition_stats[i].partition_id)));
            p_stat.Set("writeCount", Napi::Number::New(env, static_cast<double>(stats.partition_stats[i].write_count)));
            p_stat.Set("readCount", Napi::Number::New(env, static_cast<double>(stats.partition_stats[i].read_count)));
            p_stat.Set("currentSizeBytes", Napi::Number::New(env, static_cast<double>(stats.partition_stats[i].current_size_bytes)));
            partitions.Set(i, p_stat);
        }
        result.Set("partitions", partitions);
        return result;
    } catch (const std::exception& e) {
        Napi::Error::New(env, "C++ error: " + std::string(e.what())).ThrowAsJavaScriptException();
        return env.Null();
    }
}

Napi::Value IndinisWrapper::DebugGetMemTableTunerStats(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_closed_ || !storage_engine_sp_) return env.Null();

    if (info.Length() < 1 || !info[0].IsString()) {
        Napi::TypeError::New(env, "storePath (string) is required.").ThrowAsJavaScriptException();
        return env.Null();
    }
    std::string store_path = info[0].As<Napi::String>().Utf8Value();

    try {
        auto stats_opt = storage_engine_sp_->getMemTableTunerStatsForStore(store_path);
        if (!stats_opt) return env.Null();
        
        Napi::Object result = Napi::Object::New(env);
        result.Set("currentTargetSizeBytes", Napi::Number::New(env, static_cast<double>(stats_opt->current_target_size_bytes)));
        result.Set("smoothedWriteRateBps", Napi::Number::New(env, stats_opt->smoothed_write_rate_bps));
        return result;
    } catch (const std::exception& e) {
        Napi::Error::New(env, "C++ error: " + std::string(e.what())).ThrowAsJavaScriptException();
        return env.Null();
    }
}

Napi::Value IndinisWrapper::DebugGetWriteBufferManagerStats(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_closed_ || !storage_engine_sp_) return env.Null();

    try {
        auto stats_opt = storage_engine_sp_->getWriteBufferManagerStats();
        if (!stats_opt) return env.Null();

        Napi::Object result = Napi::Object::New(env);
        result.Set("currentMemoryUsageBytes", Napi::Number::New(env, static_cast<double>(stats_opt->current_memory_usage_bytes)));
        result.Set("bufferSizeBytes", Napi::Number::New(env, static_cast<double>(stats_opt->buffer_size_bytes)));
        result.Set("immutableMemtablesCount", Napi::Number::New(env, static_cast<double>(stats_opt->immutable_memtables_count)));
        return result;
    } catch (const std::exception& e) {
        Napi::Error::New(env, "C++ error: " + std::string(e.what())).ThrowAsJavaScriptException();
        return env.Null();
    }
}

Napi::Value IndinisWrapper::DebugGetLsmFlushCount(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    LSMTree* store = GetLsmStoreFromNapi(info, this);
    if (!store) return env.Null();

    try {
        uint64_t count = store->getFlushCount();
        return Napi::Number::New(env, static_cast<double>(count));
    } catch (const std::exception& e) {
        Napi::Error::New(env, "C++ error: " + std::string(e.what())).ThrowAsJavaScriptException();
        return env.Null();
    }
}

Napi::Value IndinisWrapper::DebugGetLsmRcuStats(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    LSMTree* store = GetLsmStoreFromNapi(info, this);
    if (!store) return env.Null();
    
    try {
        auto partitioned_memtable = store->getActivePartitionedMemTable();
        if (!partitioned_memtable) return env.Null();
        
        const auto& partitions = partitioned_memtable->getPartitions();
        if (partitions.empty()) return env.Null();
        
        // For this test, we aggregate stats from all partitions.
        // In a real scenario, you might return stats per-partition.
        uint64_t totalReads = 0, totalWrites = 0, totalDeletes = 0, memoryReclamations = 0;
        size_t activeReaders = 0, pendingReclamations = 0, currentMemoryUsage = 0, peakMemoryUsage = 0;

        bool is_rcu = true;
        for (const auto& partition : partitions) {
            auto* rep = partition->getMemTableRep();
            
            if (rep) {
                // Get the actual type from the virtual function.
                engine::lsm::MemTableType actual_type = rep->GetType();
                LOG_INFO("  - Partition has MemTableRep of type: {} (Name: {})", 
                         static_cast<int>(actual_type), rep->GetName());

                // Check if the type matches what we expect for this debug function.
                if (actual_type != engine::lsm::MemTableType::RCU) {
                    LOG_ERROR("    -> MISMATCH! This debug function is for RCU stats, but the store is using a different memtable type ({}).", rep->GetName());
                    is_rcu = false;
                    break; // Exit the loop immediately upon finding a non-RCU partition.
                }

                // Since we have confirmed the type, it's safe to use static_cast.
                // This is more efficient and safer than dynamic_cast when the type is known.
                auto* adapter = static_cast<engine::lsm::RCUMemTableRepAdapter*>(rep);
                
                // Get the statistics snapshot from the adapter.
                ::lsm::memtable::RCUStatisticsSnapshot stats = adapter->getRcuStatistics();
                
                // Aggregate the statistics from this partition.
                totalReads += stats.total_reads;
                totalWrites += stats.total_writes;
                totalDeletes += stats.total_deletes;
                memoryReclamations += stats.memory_reclamations;
                activeReaders += stats.active_readers;
                pendingReclamations += stats.pending_reclamations;
                currentMemoryUsage += stats.current_memory_usage;
                peakMemoryUsage += stats.peak_memory_usage; // Summing peak usage is a reasonable approximation of total peak

            } else {
                LOG_ERROR("  - Partition has a null MemTableRep pointer! This indicates a critical error.");
                is_rcu = false;
                break;
            }
        }
        
        if (!is_rcu) {
            Napi::Error::New(env, "Store is not configured with RCU memtables.").ThrowAsJavaScriptException();
            return env.Null();
        }
        Napi::Object result = Napi::Object::New(env);
        // JS BigInt can handle uint64_t
        result.Set("totalReads", Napi::BigInt::New(env, totalReads));
        result.Set("totalWrites", Napi::BigInt::New(env, totalWrites));
        result.Set("totalDeletes", Napi::BigInt::New(env, totalDeletes));
        result.Set("memoryReclamations", Napi::BigInt::New(env, memoryReclamations));
        // size_t can be represented as a number
        result.Set("activeReaders", Napi::Number::New(env, activeReaders));
        result.Set("pendingReclamations", Napi::Number::New(env, pendingReclamations));
        result.Set("currentMemoryUsage", Napi::Number::New(env, currentMemoryUsage));
        result.Set("peakMemoryUsage", Napi::Number::New(env, peakMemoryUsage));
        
        return result;

    } catch (const std::exception& e) {
        Napi::Error::New(env, "C++ error getting RCU stats: " + std::string(e.what())).ThrowAsJavaScriptException();
        return env.Null();
    }
}

Napi::Value IndinisWrapper::DebugGetLsmVersionStats(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    LSMTree* store = GetLsmStoreFromNapi(info, this);
    if (!store) {
        return env.Null();
    }
    
    try {
        LSMTree::VersionStatsSnapshot stats = store->getVersionStats();
        Napi::Object result = Napi::Object::New(env);

        result.Set("currentVersionId", Napi::Number::New(env, static_cast<double>(stats.current_version_id)));
        result.Set("liveVersionsCount", Napi::Number::New(env, stats.live_versions_count));

        Napi::Array levels = Napi::Array::New(env, stats.sstables_per_level.size());
        for (size_t i = 0; i < stats.sstables_per_level.size(); ++i) {
            levels.Set(i, Napi::Number::New(env, stats.sstables_per_level[i]));
        }
        result.Set("sstablesPerLevel", levels);

        return result;
    } catch (const std::exception& e) {
        Napi::Error::New(env, "C++ error getting LSM Version stats: " + std::string(e.what())).ThrowAsJavaScriptException();
        return env.Null();
    }
}



/**
 * @brief N-API method to pause the RCU reclamation worker thread for a store.
 */
Napi::Value IndinisWrapper::DebugPauseRcuReclamation(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_closed_ || !storage_engine_sp_) {
        Napi::Error::New(env, "Engine not available.").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    try {
        lsm::memtable::RCUReclamationManager* manager = GetRcuReclamationManager(info, this);
        if (manager) {
            manager->debug_pause_reclamation_.store(true, std::memory_order_relaxed);
            LOG_INFO("[NAPI DEBUG] Paused RCU reclamation for store '{}'.", info[0].As<Napi::String>().Utf8Value());
        }
    } catch (const Napi::Error& e) {
        e.ThrowAsJavaScriptException();
    } catch (const std::exception& e) {
        Napi::Error::New(env, "C++ error in DebugPauseRcuReclamation: " + std::string(e.what())).ThrowAsJavaScriptException();
    }
    
    return env.Undefined();
}

/**
 * @brief N-API method to resume the RCU reclamation worker thread for a store.
 */
Napi::Value IndinisWrapper::DebugResumeRcuReclamation(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_closed_ || !storage_engine_sp_) {
        Napi::Error::New(env, "Engine not available.").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    try {
        lsm::memtable::RCUReclamationManager* manager = GetRcuReclamationManager(info, this);
        if (manager) {
            manager->debug_pause_reclamation_.store(false, std::memory_order_relaxed);
            LOG_INFO("[NAPI DEBUG] Resumed RCU reclamation for store '{}'.", info[0].As<Napi::String>().Utf8Value());
        }
    } catch (const Napi::Error& e) {
        e.ThrowAsJavaScriptException();
    } catch (const std::exception& e) {
        Napi::Error::New(env, "C++ error in DebugResumeRcuReclamation: " + std::string(e.what())).ThrowAsJavaScriptException();
    }
    
    return env.Undefined();
}


Napi::Value IndinisWrapper::DebugGetThreadPoolStats(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_closed_ || !storage_engine_sp_) {
        // It's better to return null or an empty object than throw if the engine is gone.
        return env.Null();
    }

    // 1. Call the new getter on the StorageEngine.
    auto pool = storage_engine_sp_->getSharedCompactionPool();
    if (!pool) {
        // This could happen if the pool failed to initialize but the engine didn't.
        return env.Null();
    }

    Napi::Object stats = Napi::Object::New(env);

    // 2. Use the new public getters on the ThreadPool instance.
    stats.Set("name", Napi::String::New(env, pool->getName()));
    stats.Set("minThreads", Napi::Number::New(env, pool->getMinThreads()));
    stats.Set("maxThreads", Napi::Number::New(env, pool->getMaxThreads()));
    stats.Set("currentThreadCount", Napi::Number::New(env, pool->getCurrentThreadCount()));
    stats.Set("activeThreadCount", Napi::Number::New(env, pool->getActiveThreads()));
    stats.Set("totalPendingTasks", Napi::Number::New(env, pool->getQueueDepth()));
    
    return stats;
}


Napi::Value IndinisWrapper::IngestExternalFile(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (is_closed_ || !storage_engine_sp_) {
        Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);
        deferred.Reject(Napi::Error::New(env, "Indinis engine is not initialized or has been closed.").Value());
        return deferred.Promise();
    }

    if (info.Length() < 2 || !info[0].IsString() || !info[1].IsString()) {
        Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);
        deferred.Reject(Napi::TypeError::New(env, "ingestFile requires storePath (string) and filePath (string).").Value());
        return deferred.Promise();
    }

    std::string store_path = info[0].As<Napi::String>().Utf8Value();
    std::string file_path = info[1].As<Napi::String>().Utf8Value();
    bool move_file = (info.Length() > 2 && info[2].IsBoolean()) ? info[2].As<Napi::Boolean>().Value() : true;

    // Create and queue the AsyncWorker.
    auto worker = new IngestFileWorker(env, GetInternalSharedPtr(), store_path, file_path, move_file);
    worker->Queue();
    
    // Return the promise that the worker will resolve or reject.
    return worker->Promise();
}

Napi::Value IndinisWrapper::DebugGetSSTableBuilder(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    // This is a factory method. It takes the same arguments as the SSTableBuilderWrapper
    // constructor and returns a new instance of it.
    Napi::Value filepath = info[0];
    Napi::Value options = info[1];
    return SSTableBuilderWrapper::constructor.New({filepath, options});
}


Napi::Value IndinisWrapper::DebugSerializeValue(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 1) {
        Napi::TypeError::New(env, "serializeValue requires one argument.").ThrowAsJavaScriptException();
        return env.Null();
    }
    auto value_opt = ConvertNapiToValueType(env, info[0]);
    if (!value_opt) {
        // Error already thrown by ConvertNapiToValueType
        return env.Null();
    }
    std::string serialized_binary = LogRecord::serializeValueTypeBinary(*value_opt);
    return Napi::Buffer<char>::Copy(env, serialized_binary.data(), serialized_binary.size());
}

//  --- Implementation of the N-API method for direct columnar ingestion
Napi::Value IndinisWrapper::IngestColumnarFile(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (is_closed_ || !storage_engine_sp_) {
        Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);
        deferred.Reject(Napi::Error::New(env, "Indinis engine is not initialized or has been closed.").Value());
        return deferred.Promise();
    }

    if (info.Length() < 2 || !info[0].IsString() || !info[1].IsString()) {
        Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);
        deferred.Reject(Napi::TypeError::New(env, "ingestColumnarFile requires storePath (string) and filePath (string).").Value());
        return deferred.Promise();
    }

    std::string store_path = info[0].As<Napi::String>().Utf8Value();
    std::string file_path = info[1].As<Napi::String>().Utf8Value();
    bool move_file = (info.Length() > 2 && info[2].IsBoolean()) ? info[2].As<Napi::Boolean>().Value() : true;

    // Create and queue the AsyncWorker.
    auto worker = new ColumnarIngestFileWorker(env, GetInternalSharedPtr(), store_path, file_path, move_file);
    worker->Queue();
    
    // Return the promise that the worker will resolve or reject.
    return worker->Promise();
}

/**
 * @brief N-API method to expose columnar store statistics for debugging and monitoring.
 * @param info Napi::CallbackInfo containing storePath (string) as the first argument.
 * @return A Napi::Object with detailed statistics or null if the store doesn't exist.
 */
Napi::Value IndinisWrapper::DebugGetColumnarStoreStats(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_closed_ || !storage_engine_sp_) {
        Napi::Error::New(env, "Engine is not initialized or has been closed.").ThrowAsJavaScriptException();
        return env.Null();
    }

    if (info.Length() < 1 || !info[0].IsString()) {
        Napi::TypeError::New(env, "debug_getColumnarStoreStats requires a storePath (string) as the first argument.").ThrowAsJavaScriptException();
        return env.Null();
    }
    std::string store_path = info[0].As<Napi::String>().Utf8Value();
    
    try {
        engine::columnar::ColumnarStore* store = storage_engine_sp_->getOrCreateColumnarStore(store_path);
        if (!store) {
            // This is a valid state if the store has no schema, so we return null.
            return env.Null();
        }

        // --- THIS IS THE REAL IMPLEMENTATION ---
        
        // 1. Call the new C++ method to get a thread-safe snapshot of the stats.
        engine::columnar::ColumnarStoreStats cpp_stats = store->getStats();

        // 2. Create a new JavaScript object to hold the results.
        Napi::Object js_stats = Napi::Object::New(env);

        // 3. Populate the JS object from the C++ struct.
        js_stats.Set("ingestionQueueDepth", Napi::Number::New(env, static_cast<double>(cpp_stats.ingestion_queue_depth)));
        js_stats.Set("activeBufferCount", Napi::Number::New(env, static_cast<double>(cpp_stats.active_buffer_count)));
        js_stats.Set("totalRowsInActiveBuffers", Napi::Number::New(env, static_cast<double>(cpp_stats.total_rows_in_active_buffers)));
        js_stats.Set("totalBytesInActiveBuffers", Napi::Number::New(env, static_cast<double>(cpp_stats.total_bytes_in_active_buffers)));
        js_stats.Set("immutableBufferCount", Napi::Number::New(env, static_cast<double>(cpp_stats.immutable_buffer_count)));
        js_stats.Set("totalRowsInImmutableBuffers", Napi::Number::New(env, static_cast<double>(cpp_stats.total_rows_in_immutable_buffers)));
        js_stats.Set("totalBytesInImmutableBuffers", Napi::Number::New(env, static_cast<double>(cpp_stats.total_bytes_in_immutable_buffers)));

        Napi::Array js_levels = Napi::Array::New(env, cpp_stats.files_per_level.size());
        for (size_t i = 0; i < cpp_stats.files_per_level.size(); ++i) {
            js_levels.Set(i, Napi::Number::New(env, static_cast<double>(cpp_stats.files_per_level[i])));
        }
        js_stats.Set("filesPerLevel", js_levels);

        return js_stats;

    } catch (const std::exception& e) {
        Napi::Error::New(env, "C++ error getting columnar stats: " + std::string(e.what())).ThrowAsJavaScriptException();
        return env.Null();
    }
}


Napi::Value IndinisWrapper::DebugForceColumnarCompaction(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);

    if (is_closed_ || !storage_engine_sp_) {
        deferred.Reject(Napi::Error::New(env, "Indinis engine not initialized or closed").Value());
        return deferred.Promise();
    }
    if (info.Length() < 1 || !info[0].IsString()) {
        deferred.Reject(Napi::TypeError::New(env, "debug_forceColumnarCompaction requires a storePath (string).").Value());
        return deferred.Promise();
    }
    std::string store_path = info[0].As<Napi::String>().Utf8Value();

    try {
        engine::columnar::ColumnarStore* store = storage_engine_sp_->getOrCreateColumnarStore(store_path);
        if (!store) {
            deferred.Reject(Napi::Error::New(env, "Columnar store for path '" + store_path + "' not found.").Value());
            return deferred.Promise();
        }
        
        // We need a public method on ColumnarStore to trigger this.
        // This is an async operation, so we can't easily wait for it.
        // The best we can do is signal the scheduler.
        store->notifyScheduler();
        
        deferred.Resolve(Napi::Boolean::New(env, true));
    } catch (const std::exception& e) {
        deferred.Reject(Napi::Error::New(env, "C++ error forcing columnar compaction: " + std::string(e.what())).Value());
    }

    return deferred.Promise();
}

Napi::Value IndinisWrapper::CommitBatch(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (is_closed_ || !storage_engine_sp_) {
        Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);
        deferred.Reject(Napi::Error::New(env, "Indinis engine is not initialized or has been closed.").Value());
        return deferred.Promise();
    }
    if (info.Length() < 1 || !info[0].IsArray()) {
        Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);
        deferred.Reject(Napi::TypeError::New(env, "commitBatch requires an array of operations.").Value());
        return deferred.Promise();
    }

    Napi::Array jsOps = info[0].As<Napi::Array>();
    std::vector<BatchOperation> cppOps;
    cppOps.reserve(jsOps.Length());

    try {
        for (uint32_t i = 0; i < jsOps.Length(); ++i) {
            Napi::Object jsOp = jsOps.Get(i).As<Napi::Object>();
            BatchOperation op;
            
            std::string opStr = jsOp.Get("op").As<Napi::String>();
            if (opStr == "make") op.type = BatchOperationType::MAKE;
            else if (opStr == "modify") op.type = BatchOperationType::MODIFY;
            else if (opStr == "remove") op.type = BatchOperationType::REMOVE;
            else { throw Napi::TypeError::New(env, "Invalid batch operation type: " + opStr); }

            op.key = jsOp.Get("key").As<Napi::String>();
            
            if (op.type == BatchOperationType::MAKE) {
                Napi::Object valueObj = jsOp.Get("value").As<Napi::Object>();
                // For MAKE, we just stringify the whole data object.
                op.value = env.Global().Get("JSON").As<Napi::Object>().Get("stringify").As<Napi::Function>().Call({valueObj}).As<Napi::String>();
                op.overwrite = jsOp.Get("overwrite").As<Napi::Boolean>();
            } else if (op.type == BatchOperationType::MODIFY) {
                // *** THIS IS THE FIX ***
                // Use our new helper to parse the complex modify payload.
                Napi::Object valueObj = jsOp.Get("value").As<Napi::Object>();
                op.value = parse_modify_op_value(env, valueObj);
            }
            
            cppOps.push_back(op);
        }
    } catch (const Napi::Error& e) {
        // A JS-level error occurred during parsing.
        Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);
        deferred.Reject(e.Value());
        return deferred.Promise();
    }

    auto worker = new BatchCommitWorker(env, GetInternalSharedPtr(), std::move(cppOps));
    worker->Queue();
    return worker->Promise();
}