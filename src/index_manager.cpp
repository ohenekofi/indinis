// @src/index_manager.cpp
#include "../include/index_manager.h"
#include "../include/indinis.h" // Include full StorageEngine definition
#include "../include/btree.h"   // Ensure BTree definition is included
#include "../include/debug_utils.h"
#include "../include/encoding_utils.h"
#include "../include/types.h"
#include "../include/storage_engine.h"
#include "../include/storage_error/error_codes.h"
#include "../include/storage_error/storage_error.h" // <<< ENSURE THIS IS PRESENT

#include <nlohmann/json.hpp>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <iostream>
#include <iomanip>
#include <algorithm> // For std::find if needed
#include <map>       // Potentially used by helpers
#include <set>       // Potentially used by helpers
#include <regex>

namespace fs = std::filesystem;
using json = nlohmann::json;

// Define constants locally if ONLY used here, otherwise prefer types.h or BTree:: static const
namespace {
    //const std::string INDEX_TOMBSTONE = ""; // Keep local if only used here
    std::string sanitizePathForIndexName(const std::string& path) {
        std::string sanitized = path;
        // Replace all slashes with a safe character
        std::replace(sanitized.begin(), sanitized.end(), '/', '_');
        std::replace(sanitized.begin(), sanitized.end(), '\\', '_');
        // Remove any other characters that might be problematic in a filename
        sanitized = std::regex_replace(sanitized, std::regex("[^a-zA-Z0-9_-]"), "");
        return sanitized;
    }
}
//const std::string INDEX_TOMBSTONE = "";

// --- IndexManager Method Implementations ---

IndexManager::IndexManager(SimpleBufferPoolManager& bpm, const std::string& data_dir)
    : bpm_(bpm),
      base_data_dir_(data_dir),
      engine_(nullptr), // Initially null, set later via setStorageEngine
      stop_backfill_workers_{false}
{
    LOG_INFO("[IndexManager] Initializing...");

    // 1. Ensure the directory for index metadata and files exists.
    std::string index_root_dir = (fs::path(base_data_dir_) / "indices").string();
    try {
        fs::create_directories(index_root_dir);
    } catch (const fs::filesystem_error& e) {
        LOG_FATAL("Failed to create index directory '{}': {}", index_root_dir, e.what());
        // This is a fatal error; if we can't create the directory, we can't function.
        throw std::runtime_error("Fatal: Failed to create index directory: " + index_root_dir);
    }

    // 2. Load all existing index definitions and their corresponding B-Trees from disk.
    // This populates the index_definitions_ and indices_ maps.
    loadIndices();

    // The logic to populate `known_store_paths_` has been removed as it was faulty.
    // The check for default index creation is now handled more robustly in `ensureStoreExistsAndIndexed`.
    LOG_INFO("[IndexManager] Loaded {} definitions and {} B-Trees.", 
             index_definitions_.size(), indices_.size());

    // 3. Start the background worker threads for asynchronous index backfilling.
    backfill_workers_.reserve(NUM_BACKFILL_WORKERS);
    for (size_t i = 0; i < NUM_BACKFILL_WORKERS; ++i) {
        backfill_workers_.emplace_back(&IndexManager::backfillWorkerLoop, this);
    }
    LOG_INFO("[IndexManager] Started {} background index backfill workers.", NUM_BACKFILL_WORKERS);
}


IndexManager::~IndexManager() {
    LOG_INFO("[IndexManager] Shutting down...");

    // --- NEW: Gracefully stop worker threads ---
    stop_backfill_workers_.store(true);
    backfill_cv_.notify_all(); // Wake up all waiting workers
    for (auto& worker : backfill_workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    LOG_INFO("[IndexManager] All background backfill workers stopped.");
    
    // The BTree shared_ptrs will be released, but BPM handles flushing.
    // persistIndices() is effectively a no-op for data persistence.
}

// --- Setter for engine pointer ---
void IndexManager::setStorageEngine(StorageEngine* engine) {
    engine_ = engine;
}

// --- Worker Loop Implementation ---
void IndexManager::backfillWorkerLoop() {
    LOG_INFO("[IndexBackfillWorker ThdID: {}] Worker thread started.", std::this_thread::get_id());
    while (!stop_backfill_workers_) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(backfill_mutex_);
            backfill_cv_.wait(lock, [this] {
                return stop_backfill_workers_ || !backfill_tasks_.empty();
            });

            if (stop_backfill_workers_ && backfill_tasks_.empty()) {
                break;
            }
            if (backfill_tasks_.empty()) {
                continue;
            }
            task = std::move(backfill_tasks_.front());
            backfill_tasks_.pop();
        }
        try {
            LOG_INFO("[IndexBackfillWorker ThdID: {}] Starting a backfill task.", std::this_thread::get_id());
            task();
            LOG_INFO("[IndexBackfillWorker ThdID: {}] Finished a backfill task.", std::this_thread::get_id());
        } catch (const std::exception& e) {
            LOG_ERROR("[IndexBackfillWorker ThdID: {}] Exception during backfill task: {}", std::this_thread::get_id(), e.what());
        } catch (...) {
            LOG_ERROR("[IndexBackfillWorker ThdID: {}] Unknown exception during backfill task.", std::this_thread::get_id());
        }
    }
    LOG_INFO("[IndexBackfillWorker ThdID: {}] Worker thread stopping.", std::this_thread::get_id());
}


// --- Path Helpers ---
std::string IndexManager::getIndexPath(const std::string& indexName) const {
    // This path is only used for deleting the file now, BTree doesn't load/persist directly
    return (fs::path(base_data_dir_) / "indices" / (indexName + ".idx")).string();
}

// Definition of encodeValueToStringOrderPreserving (if not already defined elsewhere)
std::string IndexManager::encodeValueToStringOrderPreserving(const ValueType& value) const {
    return std::visit([&value](auto&& arg) -> std::string { // Capture 'value' by reference
    // Alternatively: [this, &value] if you also need 'this' inside, though not in this specific lambda.
    // Or even just default capture by reference: [&]
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, int64_t>) {
            return Indinis::encodeInt64OrderPreserving(arg);
        } else if constexpr (std::is_same_v<T, double>) {
            return Indinis::encodeDoubleOrderPreserving(arg);
        } else if constexpr (std::is_same_v<T, std::string>) {
            return Indinis::encodeStringOrderPreserving(arg);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            return Indinis::encodeBinaryOrderPreserving(arg);
        } else {
            // Now 'value' is captured and can be accessed
            LOG_ERROR("IndexManager::encodeValueToStringOrderPreserving: Unsupported type in ValueType variant. Index: ", value.index());
            throw std::logic_error("Unsupported type in ValueType variant for encoding");
        }
    }, value); // This 'value' is the std::variant object being visited
}


std::string IndexManager::getDefinitionsPath() const {
    return (fs::path(base_data_dir_) / "indices" / "_index_definitions.json").string();
}

void IndexManager::ensureStoreExistsAndIndexed(const std::string& storePath) {
    // This is a critical path, ensure it's performant.
    // Construct the expected default index name first.
    const std::string defaultIndexName = "idx_default_" + sanitizePathForIndexName(storePath) + "_name";

    // First, check with a shared lock for performance. This is the common path.
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (index_definitions_.count(defaultIndexName)) {
            return; // Fast path: The default index already exists.
        }
    }

    // If not found, acquire a unique lock to create it.
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    // Double-check after acquiring the unique lock to prevent a race condition
    // where another thread created it while we were waiting for the lock.
    if (index_definitions_.count(defaultIndexName)) {
        return;
    }

    // If still not found, this is the first write. Create the default index.
    LOG_INFO("[IndexManager] First write for store path '{}'. Creating default index '{}'.", storePath, defaultIndexName);

    IndexDefinition defaultIndex;
    defaultIndex.name = defaultIndexName;
    defaultIndex.storePath = storePath;
    defaultIndex.fields.push_back({ "name", IndexSortOrder::ASCENDING });
    defaultIndex.is_unique = false;
    defaultIndex.is_multikey = false;
    defaultIndex.type = IndexType::B_TREE;

    // Call the internal, non-locking method to create the index.
    createIndexInternal(defaultIndex);

    LOG_INFO("[IndexManager] Default index '{}' created for store path '{}'.", defaultIndex.name, storePath);
}

// --- NEW: createIndexInternal (Refactored from original createIndex) ---
void IndexManager::createIndexInternal(const IndexDefinition& definition) {
    // This method assumes:
    // 1. The definition is valid.
    // 2. An index with this name does not already exist.
    // 3. The caller holds a unique_lock on `mutex_`.

    try {
        LOG_INFO("[IndexManager] Internal creation of index '{}' for path '{}'.", definition.name, definition.storePath);

        // 1. Create the in-memory B-Tree object. The BTree constructor will handle
        //    reading its root page ID from the DiskManager. If it doesn't exist, it will be INVALID_PAGE_ID.
        auto btree = std::make_shared<BTree>(definition.name, bpm_);
        
        // 2. Add the new index definition and B-Tree object to the in-memory maps.
        index_definitions_[definition.name] = definition;
        indices_[definition.name] = btree;

        // 3. Persist the updated list of index definitions to the JSON file.
        persistDefinitions();

    } catch (const std::exception& e) {
        // If anything fails (e.g., B-Tree constructor, file I/O in persist),
        // we must clean up the partial state to avoid inconsistency.
        LOG_ERROR("[IndexManager] Exception during createIndexInternal for '{}': {}", definition.name, e.what());
        index_definitions_.erase(definition.name);
        indices_.erase(definition.name);
        // Rethrow to notify the public-facing caller of the failure.
        throw;
    }
}



// --- Definition Persistence (Internal, needs external lock) ---
void IndexManager::persistDefinitions() {
    // Assumes unique_lock is held by caller (createIndex, deleteIndex)
    json j_root = json::array();
    for (const auto& [name, def] : index_definitions_) {
        json j_def; j_def["name"]=def.name; 
        j_def["storePath"]=def.storePath;
        j_def["is_unique"] = def.is_unique;
        json j_fields = json::array();
        for(const auto& f : def.fields){ j_fields.push_back({{"name", f.name}, {"order", f.order==IndexSortOrder::ASCENDING?"asc":"desc"}});}
        j_def["fields"] = j_fields;
        j_root.push_back(j_def);
    }
    std::string defPath = getDefinitionsPath();
    std::ofstream file(defPath);
    if (file.is_open()) {
        try { file << j_root.dump(4); file.close(); }
        catch (const std::exception& e) { std::cerr << "Error writing JSON definitions: " << e.what() << std::endl; if(file.is_open()) file.close();}
    } else {
        std::cerr << "Error: Failed to open index definitions file for writing: " << defPath << std::endl;
    }
}

void IndexManager::loadDefinitions() {
    // Assumes unique_lock is held by caller (loadIndices)
    std::string defPath = getDefinitionsPath();
    std::ifstream file(defPath);
    if (!file.is_open()) return;

    try {
        json j_root; file >> j_root; file.close();
        if (!j_root.is_array()) throw std::runtime_error("Invalid format, expected root array");

        for (const auto& j_def : j_root) {
            IndexDefinition def;
            def.name = j_def.value("name", "");
            def.storePath = j_def.value("storePath", "");
            def.is_unique = j_def.value("is_unique", false);
            if (j_def.contains("fields") && j_def["fields"].is_array()) {
                for (const auto& j_field : j_def["fields"]) {
                    std::string fieldName = j_field.value("name", "");
                    IndexSortOrder order = (j_field.value("order", "asc") == "desc") ? IndexSortOrder::DESCENDING : IndexSortOrder::ASCENDING;
                     if (!fieldName.empty()) def.fields.emplace_back(fieldName, order);
                }
            }
            if (def.isValid()) { index_definitions_[def.name] = def; }
            else { std::cerr << "Warning: Skipping invalid index definition loaded: " << def.name << std::endl; }
        }
    } catch (const std::exception& e) {
        std::cerr << "Error parsing index definitions file " << defPath << ": " << e.what() << std::endl;
        if(file.is_open()) file.close();
    }
}

CreateIndexResult IndexManager::createIndex(const IndexDefinition& definition) {
    // 1. Validate the input definition.
    if (!definition.isValid()) {
        LOG_ERROR("[IndexManager] Attempted to create an invalid index definition for name '{}'.", definition.name);
        return CreateIndexResult::FAILED;
    }

    // 2. Acquire a unique lock to ensure thread-safe modification of index metadata.
    std::unique_lock<std::shared_mutex> lock(mutex_);

    // 3. Check if an index with this name already exists.
    if (index_definitions_.count(definition.name)) {
        LOG_INFO("[IndexManager] createIndex call for '{}' skipped: index already exists.", definition.name);
        return CreateIndexResult::ALREADY_EXISTS;
    }

    try {
        // 4. Call the internal, non-locking helper to perform the creation.
        createIndexInternal(definition);

        // 5. Update the cache of known store paths.
        //known_store_paths_.insert(definition.storePath);

        // 6. Define and schedule the background backfill task.
        if (!engine_) {
            LOG_ERROR("[IndexManager] StorageEngine pointer is not set! Cannot schedule backfill for new index '{}'.", definition.name);
            // The index is created, but it will be empty. This is a configuration error.
            return CreateIndexResult::FAILED;
        }

        auto backfill_task = [this, definition]() {
            const std::string& index_name = definition.name;
            LOG_INFO("[Backfill Task] Starting for index '{}' on path '{}'.", index_name, definition.storePath);

            try {
                if (!engine_) { // Double-check engine pointer inside the thread.
                    throw storage::StorageError(storage::ErrorCode::INTERNAL_ERROR, "Engine pointer is null at start of backfill task.");
                }

                const std::string prefix_to_scan = definition.storePath + "/";
                const size_t BATCH_SIZE = 500;
                std::string last_key_processed = "";
                size_t total_records_indexed = 0;

                while (!stop_backfill_workers_.load()) {
                    // Fetch the next batch of documents from the primary storage (LSM-tree).
                    std::vector<Record> batch = engine_->getPrefix(
                        prefix_to_scan, 
                        std::numeric_limits<TxnId>::max(), // Read all committed data
                        BATCH_SIZE, 
                        last_key_processed // Paginate from the last key of the previous batch
                    );

                    if (batch.empty()) {
                        break; // No more documents in the store path.
                    }

                    // Process each document in the batch.
                    for (const auto& record : batch) {
                        if (stop_backfill_workers_.load()) {
                            LOG_INFO("[Backfill Task] Shutdown requested. Halting backfill for index '{}'.", index_name);
                            return;
                        }
                        
                        // For a backfill, there is no "old record" state from the index's perspective.
                        // We treat every existing document as a new insertion into this index.
                        this->updateIndices(record.key, std::nullopt, record, record.commit_txn_id);
                    }

                    total_records_indexed += batch.size();
                    last_key_processed = batch.back().key;
                    LOG_TRACE("[Backfill Task] Index '{}': Processed batch of {}, total indexed so far: {}.",
                              index_name, batch.size(), total_records_indexed);
                }

                LOG_INFO("[Backfill Task] Index '{}' backfilling complete. Total records indexed: {}.", index_name, total_records_indexed);

            } catch (const storage::StorageError& e) {
                LOG_ERROR("[Backfill Task] StorageError during backfill for index '{}': {}", index_name, e.toDetailedString());
            } catch (const std::exception& e) {
                LOG_ERROR("[Backfill Task] Exception during backfill for index '{}': {}", index_name, e.what());
            } catch (...) {
                LOG_ERROR("[Backfill Task] Unknown exception during backfill for index '{}'.", index_name);
            }
        };

        // 7. Add the task to the queue and notify a worker thread.
        {
            std::lock_guard<std::mutex> task_lock(backfill_mutex_);
            backfill_tasks_.push(backfill_task);
        }
        backfill_cv_.notify_one();

        LOG_INFO("[IndexManager] Index '{}' created successfully. Backfill task submitted.", definition.name);
        return CreateIndexResult::CREATED_AND_BACKFILLING;

    } catch (const std::exception& e) {
         // This catches errors from createIndexInternal. The state should already be clean.
         LOG_ERROR("[IndexManager] Top-level error creating index '{}': {}", definition.name, e.what());
         return CreateIndexResult::FAILED;
    }
}

bool IndexManager::deleteIndex(const std::string& name) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    bool definition_removed = (index_definitions_.erase(name) > 0);
    bool btree_removed = (indices_.erase(name) > 0);

    if (definition_removed) {
        try { persistDefinitions(); }
        catch (const std::exception& e) { std::cerr << "Error persisting definitions after deleting index '" << name << "': " << e.what() << std::endl; }
    }

    // Delete BTree data file
    // ** Important ** This relies on the BTree *not* holding any file handles directly.
    // The data is managed by the BufferPoolManager / DiskManager.
    // Deleting the index here means removing the *definition* and the in-memory
    // BTree object. The actual pages on disk might linger until overwritten or
    // until a proper free-space management system is added to DiskManager.
    if (btree_removed || definition_removed) {
        // Optionally, we could try to delete the associated file IF DiskManager
        // used separate files per BTree, but our SimpleDiskManager uses one file.
        // So, there's no separate file to delete here.
        std::cout << "[IndexManager] Deleted index definition and in-memory BTree object for: '" << name << "'" << std::endl;
    }

    bool result = definition_removed || btree_removed;
    // No need to log if result is false, means it didn't exist.
    return result;
}

bool IndexManager::indexExists(const std::string& name) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return index_definitions_.count(name) > 0;
}

std::optional<IndexDefinition> IndexManager::getIndexDefinition(const std::string& name) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = index_definitions_.find(name);
    if (it != index_definitions_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::vector<IndexDefinition> IndexManager::listIndexes(const std::optional<std::string>& storePath) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::vector<IndexDefinition> defs;
    
    // Reserve space for efficiency, assuming the most common case is listing all.
    if (!storePath.has_value()) {
        defs.reserve(index_definitions_.size());
    }

    for (const auto& [name, definition] : index_definitions_) {
        // If a storePath was provided, filter by it.
        // Otherwise, include all definitions.
        if (!storePath.has_value() || definition.storePath == *storePath) {
            defs.push_back(definition);
        }
    }
    return defs;
}

std::shared_ptr<BTree> IndexManager::getIndexBTree(const std::string& name) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = indices_.find(name);
    if (it != indices_.end()) {
        return it->second;
    }
    return nullptr;
}

// --- Index Update Logic ---

std::optional<ValueType> IndexManager::extractFieldValue(const Record& record, const std::string& fieldName) const {
    if (record.deleted) return std::nullopt;

    if (!std::holds_alternative<std::string>(record.value)) {
        LOG_WARN("extractFieldValue: Record value is not a string for key '", record.key, "'. Cannot parse JSON.");
        return std::nullopt;
    }
    const std::string& json_str = std::get<std::string>(record.value);
    if (json_str.empty()) {
        LOG_TRACE("extractFieldValue: Record value string is empty for key '", record.key, "'. Treating as no field value.");
        return std::nullopt;
    }

    try {
        json j = json::parse(json_str);
        if (j.contains(fieldName)) {
            const auto& field_val_json = j.at(fieldName);

            if (field_val_json.is_null()) {
                return std::nullopt; // Explicit JSON null means true database null for indexing
            }
            // NEW: Handle string-encoded int64_t first
            if (field_val_json.is_string()) {
                std::string s_val = field_val_json.get<std::string>();
                // Try to parse as int64 if it's for a known integer field or looks like an integer.
                // This is a heuristic. A schema or type hint during index definition would be more robust.
                // For this fix, we'll attempt stoll and fall back to string if it fails.
                try {
                    // Check if it's purely numeric (possibly with a leading minus) to avoid stoll exceptions on non-numbers
                    bool is_numeric_looking_string = !s_val.empty();
                    size_t start_pos = 0;
                    if (s_val[0] == '-') {
                        if (s_val.length() == 1) is_numeric_looking_string = false; // Just "-"
                        else start_pos = 1;
                    }
                    if(is_numeric_looking_string) { // Check remaining characters only if not already false
                        for (size_t i = start_pos; i < s_val.length(); ++i) {
                            if (!std::isdigit(s_val[i])) {
                                is_numeric_looking_string = false;
                                break;
                            }
                        }
                    }

                    if (is_numeric_looking_string) {
                        long long ll_val = std::stoll(s_val); // Can throw std::out_of_range or std::invalid_argument
                        return ll_val; // Successfully parsed as int64
                    }
                } catch (const std::out_of_range&) {
                    // Value is numeric string but out of range for long long.
                    // Could potentially try to store as double if very large and fits, or keep as string.
                    // For now, let it fall through to be treated as a regular string.
                    LOG_WARN("extractFieldValue: String value '", s_val, "' for field '", fieldName, "' is numeric but out of int64_t range. Treating as string.");
                } catch (const std::invalid_argument&) {
                    // Not a valid number string, will be treated as a regular string below.
                }
                // If not parsed as int64, return as string.
                return s_val;
            }
            // Original number handling
            if (field_val_json.is_number_integer()) return field_val_json.get<int64_t>();
            if (field_val_json.is_number_float()) return field_val_json.get<double>();
            if (field_val_json.is_boolean()) return static_cast<int64_t>(field_val_json.get<bool>() ? 1 : 0);
            if (field_val_json.is_binary()) {
                 const auto& bin = field_val_json.get_binary(); return std::vector<uint8_t>(bin.begin(), bin.end());
            }
            LOG_WARN("extractFieldValue: Unsupported JSON type for field '", fieldName, "' in key '", record.key, "'. Type: ", field_val_json.type_name());
            return std::nullopt;
        } else {
            return std::nullopt; // Field not present
        }
    } catch (const json::parse_error& e) {
        LOG_ERROR("extractFieldValue: JSON parse error for key '", record.key, "', field '", fieldName, "': ", e.what());
        return std::nullopt;
    } catch (const std::exception& e) {
        LOG_ERROR("extractFieldValue: Generic JSON exception for key '", record.key, "', field '", fieldName, "': ", e.what());
        return std::nullopt;
    }
}

std::string IndexManager::encodeValueToString(const ValueType& value, IndexSortOrder order) const {
    // WARNING: Basic encoding, needs improvement for production use.
    // Keep this implementation for now, but acknowledge it's not order-preserving.
    std::string encoded;
    char type_prefix = ' ';
    try {
        std::visit([&](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, int64_t>) { type_prefix = 'i'; std::stringstream ss; ss << std::showpos << std::internal << std::setw(20) << std::setfill('0') << arg; encoded = ss.str(); }
            else if constexpr (std::is_same_v<T, double>) { type_prefix = 'd'; encoded = std::to_string(arg); } // Incorrect sorting
            else if constexpr (std::is_same_v<T, std::string>) { if (arg == "_null") { type_prefix = 'n'; encoded = ""; } else { type_prefix = 's'; encoded = arg; } } // Needs escaping
            else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) { type_prefix = 'b'; encoded = std::string(reinterpret_cast<const char*>(arg.data()), arg.size()); } // Incorrect sorting
        }, value);
    } catch (const std::bad_variant_access&) { type_prefix = 'z'; encoded = "variant_error"; }
    // TODO: Implement descending order transformation
    return type_prefix + encoded;
}

// Creates ONLY the field part of the key (TxnId added later)
std::optional<std::string> IndexManager::createIndexFieldKey(const Record& record, const IndexDefinition& definition) const {
    if (definition.fields.empty()) { // No fields to index
        LOG_TRACE("      createIndexFieldKey: No fields in index definition '", definition.name, "'. Returning nullopt.");
        return std::nullopt;
    }
    // If record is marked deleted, we might still want to generate its old key for tombstone creation.
    // The calling context (updateIndices) decides if this key is for an old or new version.
    // So, we don't check record.deleted here directly for returning nullopt, but for what value to extract.

    std::string fieldKeyPart; // This will be the concatenation of encoded field values
    bool first_field = true;

    for (const auto& field_def : definition.fields) {
        if (!first_field) {
            // Separator for compound keys. A single null byte.
            // Assumes individual encoded segments do not inherently produce this sequence
            // in a way that would cause ambiguity (e.g. encodeStringOrderPreserving might need to escape).
            // For now, simple null byte separator.
            fieldKeyPart.push_back('\0');
        }

        // extractFieldValue now returns std::nullopt for true DB nulls (field missing or JSON null)
        std::optional<ValueType> fieldValueOpt = extractFieldValue(record, field_def.name);
        
        std::string encoded_segment;
        if (fieldValueOpt) {
            // Field has a concrete value (int, double, string, binary)
            encoded_segment = encodeValueToStringOrderPreserving(*fieldValueOpt);
        } else {
            // True database null (field missing, or JSON was null from extractFieldValue)
            // Encode this as a special sequence that sorts consistently.
            // Using a single byte prefix (0x00) that should sort before any other
            // type prefixes generated by encodeTYPEOrderPreserving functions (assuming they don't start with 0x00).
            encoded_segment.push_back(static_cast<char>(0x00)); // Representation of NULL, sorts first.
            LOG_TRACE("        createIndexFieldKey: Field '", field_def.name, "' is NULL. Encoding as NULL_MARKER.");
        }

        // Apply descending order if specified for this field segment
        if (field_def.order == IndexSortOrder::DESCENDING) {
            std::string temp_segment = encoded_segment; // Create a mutable copy
            for (char& c : temp_segment) {
                c = ~c; // Bitwise NOT for each byte to reverse lexicographical sort order
            }
            encoded_segment = temp_segment;
            LOG_TRACE("        createIndexFieldKey: Field '", field_def.name, "' is DESC. Inverted segment bytes.");
        }
        fieldKeyPart.append(encoded_segment);
        first_field = false;
    }
    // LOG_TRACE("      createIndexFieldKey for def '", definition.name, "' on record key '", record.key, "' produced: '", format_key_for_print(fieldKeyPart) ,"'");
    return fieldKeyPart;
}

/**
 * @brief Updates all relevant secondary indexes based on a document change (insert, update, or delete).
 * 
 * This is the core function called by the StorageEngine during a transaction commit. It calculates
 * the "diff" between the old and new indexable values for a document and applies the necessary
 * changes to the B-Tree indexes. This includes adding new entries, removing old ones via tombstones,
 * and enforcing unique constraints.
 * 
 * @param primary_key The primary key of the document being modified.
 * @param old_record_opt The state of the document *before* the current transaction's changes.
 * @param new_record_opt The new state of the document. `std::nullopt` if the document is being deleted.
 * @param commit_txn_id The transaction ID of the committing transaction.
 * @throws storage::StorageError if a unique constraint is violated.
 */
void IndexManager::updateIndices(
    const std::string& primary_key,
    const std::optional<Record>& old_record_opt,
    const std::optional<Record>& new_record_opt,
    TxnId commit_txn_id)
{
    std::shared_lock<std::shared_mutex> lock(mutex_);

    size_t last_slash_pos = primary_key.find_last_of('/');
    if (last_slash_pos == std::string::npos) {
        return;
    }
    std::string storePath_from_pk = primary_key.substr(0, last_slash_pos);

    for (const auto& [indexName, definition] : index_definitions_) {
        if (definition.storePath != storePath_from_pk) {
            continue;
        }

        auto btree = getIndexBTree(indexName);
        if (!btree) {
            LOG_ERROR("BTree instance for index '{}' not found. Skipping update.", indexName);
            continue;
        }

        // Generate the sets of keys from the old and new record versions.
        // This now correctly handles both simple and multikey indexes.
        std::vector<std::string> old_keys_vec = old_record_opt ? 
            createIndexFieldKeysForRecord(*old_record_opt, definition) : 
            std::vector<std::string>{};
            
        std::vector<std::string> new_keys_vec = (new_record_opt && !new_record_opt->deleted) ? 
            createIndexFieldKeysForRecord(*new_record_opt, definition) : 
            std::vector<std::string>{};

        std::set<std::string> old_key_set(old_keys_vec.begin(), old_keys_vec.end());
        std::set<std::string> new_key_set(new_keys_vec.begin(), new_keys_vec.end());

        // No changes to the set of keys for this index, so we can skip.
        if (old_key_set == new_key_set) {
            continue;
        }

        // --- Uniqueness Check (only applies if the new set has keys) ---
        if (definition.is_unique && !definition.is_multikey && !new_key_set.empty()) {
            const std::string& key_to_check = *new_key_set.begin();
            if (old_key_set.find(key_to_check) == old_key_set.end()) {
                std::optional<std::string> existing_pk = btree->find(key_to_check, commit_txn_id);
                if (existing_pk.has_value() && existing_pk.value() != primary_key) {
                    throw storage::StorageError(storage::ErrorCode::DUPLICATE_KEY,
                        "Unique constraint violation on index '" + definition.name + "'.");
                }
            }
        }
        
        // --- Calculate Diff and Apply Changes ---
        
        // Keys to DELETE: entries that were in the old set but are NOT in the new set.
        std::vector<std::string> keys_to_delete;
        std::set_difference(old_key_set.begin(), old_key_set.end(),
                            new_key_set.begin(), new_key_set.end(),
                            std::back_inserter(keys_to_delete));

        for (const auto& field_key : keys_to_delete) {
            std::string composite_key = BTree::createCompositeKey(field_key, primary_key, commit_txn_id);
            btree->insertEntry(composite_key, BTree::INDEX_TOMBSTONE);
        }

        // Keys to ADD: entries that are in the new set but were NOT in the old set.
        std::vector<std::string> keys_to_add;
        std::set_difference(new_key_set.begin(), new_key_set.end(),
                            old_key_set.begin(), old_key_set.end(),
                            std::back_inserter(keys_to_add));

        for (const auto& field_key : keys_to_add) {
            std::string composite_key = BTree::createCompositeKey(field_key, primary_key, commit_txn_id);
            btree->insertEntry(composite_key, primary_key);
        }
    }
}

std::vector<std::string> IndexManager::createIndexFieldKeysForRecord(const Record& record, const IndexDefinition& definition) const {
    std::vector<std::string> generated_keys;

    // --- A. Handle standard (non-multikey) indexes ---
    if (!definition.is_multikey) {
        // For standard indexes, we still use the original logic.
        auto single_key_opt = createIndexFieldKey(record, definition);
        if (single_key_opt) {
            generated_keys.push_back(*single_key_opt);
        }
        return generated_keys;
    }

    // --- B. Handle MULTIKEY indexes ---
    // Per our validation, a multikey index is not compound.
    if (definition.fields.size() != 1) {
        LOG_WARN("Attempted to generate multikey index entries for compound index '{}'. This is not supported. Skipping.", definition.name);
        return {};
    }

    // --- START OF FIX: Directly parse the raw document value ---
    if (!std::holds_alternative<std::string>(record.value)) {
        // The document's main value is not a string, so it can't be parsed as JSON.
        return {};
    }
    const std::string& json_str = std::get<std::string>(record.value);
    if (json_str.empty()) return {};

    try {
        const auto& field_def = definition.fields[0];
        json doc_json = json::parse(json_str);

        // Check if the document JSON contains the field we want to index.
        if (!doc_json.contains(field_def.name)) {
            return {}; // Field not present, no keys to generate.
        }
        
        const auto& field_json_value = doc_json.at(field_def.name);

        // The field we are indexing MUST be a JSON array for a multikey index.
        if (!field_json_value.is_array()) {
            // This is not an error, just a document that doesn't conform to the
            // expected array structure for this multikey index. We simply generate no keys for it.
            LOG_TRACE("Field '{}' in document '{}' is not an array, skipping for multikey index '{}'.",
                      field_def.name, record.key, definition.name);
            return {};
        }

        // Iterate through each item in the JSON array.
        for (const auto& item : field_json_value) {
            ValueType item_as_valuetype;
            
            // Convert the JSON item to our C++ ValueType variant.
            if (item.is_string()) {
                item_as_valuetype = item.get<std::string>();
            } else if (item.is_number_integer() || item.is_number_unsigned()) {
                item_as_valuetype = item.get<int64_t>();
            } else if (item.is_number_float()) {
                item_as_valuetype = item.get<double>();
            } else if (item.is_boolean()){
                item_as_valuetype = static_cast<int64_t>(item.get<bool>() ? 1 : 0);
            } else {
                // Skip unsupported types within the array like objects or nested arrays.
                LOG_TRACE("Skipping unsupported type '{}' in multikey array for index '{}'.", item.type_name(), definition.name);
                continue;
            }
            
            // Now, encode this single value from the array.
            std::string encoded_segment = encodeValueToStringOrderPreserving(item_as_valuetype);

            if (field_def.order == IndexSortOrder::DESCENDING) {
                for (char& c : encoded_segment) { c = ~c; }
            }
            generated_keys.push_back(encoded_segment);
        }
    } catch (const json::parse_error& e) {
        LOG_WARN("Could not parse document as JSON for multikey index '{}'. Record PK: '{}', Error: {}",
                 definition.name, record.key, e.what());
    }
    // --- END OF FIX ---
    
    return generated_keys;
}


// --- Persistence ---
void IndexManager::loadIndices() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    std::cout << "[IndexManager] Loading indices..." << std::endl;
    index_definitions_.clear();
    indices_.clear();

    try { loadDefinitions(); }
    catch (const std::exception& e) { LOG_ERROR("[IndexManager] Error during loadDefinitions: {}", e.what());  }

    for (const auto& [indexName, definition] : index_definitions_) {
        // BTree path is not directly used by BTree itself anymore
        // std::string btreePath = getIndexPath(indexName);
        try {
            // *** FIX: Pass bpm_ to BTree constructor ***
            auto btree = std::make_shared<BTree>(indexName, bpm_);
            // BTree loads its root ID via bpm_.disk_manager_.readBTreeRootId() in its own constructor
            indices_[indexName] = btree;
        } catch (const std::exception& e) {
             LOG_ERROR("[IndexManager] Error: Failed to load or create BTree for index '{}': {}", indexName, e.what());
        }
    }
        LOG_INFO("[IndexManager] Finished loading indices. Definitions: {}, B-Trees: {}", 
             index_definitions_.size(), indices_.size());
}

void IndexManager::persistIndices() const {
    // This function is now effectively a no-op for BTree data,
    // as persistence is handled by the BufferPoolManager flushing dirty pages.
    // We could optionally call bpm_.flushAllPages() here, but the BPM's
    // destructor should already do that.
    std::shared_lock<std::shared_mutex> lock(mutex_); // Lock still needed to iterate indices_ safely
    LOG_INFO("[IndexManager] Persist Indices called (BTree data handled by BPM, definitions by DDL methods).");
    // No BTree::persist calls needed.
}

std::optional<IndexDefinition> IndexManager::getIndexDefinitionForField(const std::string& storePath, const std::string& fieldName) const {
    std::shared_lock<std::shared_mutex> lock(mutex_); // Read-only lock

    // Iterate through all known index definitions
    for (const auto& [index_name, definition] : index_definitions_) {
        // Check if the index is for the correct store and targets the correct field
        if (definition.storePath == storePath && !definition.fields.empty() && definition.fields[0].name == fieldName) {
            // This is a simple heuristic: it finds any index where the requested field
            // is the *first* field in the index definition. This is the most common
            // and useful case for single-field filters.
            return definition;
        }
    }
    
    // No matching index found
    return std::nullopt;
}
