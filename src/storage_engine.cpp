// @src/storage_engine.cpp
//#include "../include/storage_engine.h"
//#include "../include/lsm_tree.h"
#include "../include/storage_error/storage_error.h"
#include "../include/index_manager.h"
#include "../include/indinis.h" // For Transaction definition (if not fully in types.h)
#include "../include/disk_manager.h"
#include "../include/buffer_pool_manager.h"
//#include "../include/wal.h" // For ExtWALManager and ExtWALManagerConfig
#include "../include/wal/sharded_wal_manager.h"
#include "../include/debug_utils.h"
//#include "../include/types.h" 
#include "../include/encryption_library.h"
#include "../include/columnar/store_schema_manager.h" // <<< Full Schema Manager
#include "../include/columnar/columnar_store.h"       // <<< Full Columnar Store
#include "../include/columnar/query_router.h"      
#include "../../include/threading/adaptive_thread_pool_manager.h"
#include "../include/threading/rate_limiter.h" 
#include <nlohmann/json.hpp> 
#include <magic_enum/magic_enum.hpp> 

#include <filesystem>
#include <iostream>
#include <fstream>
#include <algorithm> // For std::min, std::max
#include <stdexcept> // For exceptions
#include <set>
#include <sstream> // For serializeActiveTxnSet/deserializeActiveTxnSet
#include <iomanip> // For debugging
#include <limits>  // For std::numeric_limits
#include <algorithm>
#include <set>
#include <future>


// --- POISON PILLS - Placed BEFORE any project includes ---
#ifdef KEK_VALIDATION_PLAINTEXT_CSTR
#undef KEK_VALIDATION_PLAINTEXT_CSTR
#pragma message("MSVC_DEBUG: KEK_VALIDATION_PLAINTEXT_CSTR was a MACRO, now undefined in storage_engine.cpp before project includes.")
#endif

#ifdef ENCRYPTED_KEK_VALIDATION_VALUE_SIZE
#undef ENCRYPTED_KEK_VALIDATION_VALUE_SIZE
#pragma message("MSVC_DEBUG: ENCRYPTED_KEK_VALIDATION_VALUE_SIZE was a MACRO, now undefined in storage_engine.cpp before project includes.")
#endif
// --- END POISON PILLS ---
#include "../include/types.h" 
#include "../include/storage_engine.h"
#include "../include/lsm_tree.h"

// For fprintf debug:
#include <cstdio> // For fprintf, fflush
#include <cstddef> // For size_t in %zu

using json = nlohmann::json;
namespace fs = std::filesystem;


static std::string merge_json(const std::string& base_json_str, const std::string& partial_json_str) {
    if (base_json_str.empty()) return partial_json_str;
    if (partial_json_str.empty()) return base_json_str;
    json base = json::parse(base_json_str);
    json patch = json::parse(partial_json_str);
    base.merge_patch(patch); // Nlohmann's merge_patch is perfect for this
    return base.dump();
}

std::vector<ValueType> StorageEngine::extractCursorValues(
    const Record& doc,
    const std::vector<OrderByClause>& orderBy)
{
    std::vector<ValueType> cursor_values;
    cursor_values.reserve(orderBy.size());
    for (const auto& clause : orderBy) {
        auto value_opt = index_manager_->extractFieldValue(doc, clause.field);
        if (value_opt) {
            cursor_values.push_back(*value_opt);
        } else {
            // If a field is missing, it's treated as null for sorting/cursor purposes.
            cursor_values.push_back(std::monostate{});
        }
    }
    return cursor_values;
}

// --- CachedLsmEntry Method Implementations ---
std::string CachedLsmEntry::serialize() const {
    std::ostringstream oss(std::ios::binary);
    oss.write(reinterpret_cast<const char*>(&commit_txn_id), sizeof(commit_txn_id));
    char tombstone_char = is_tombstone ? 1 : 0;
    oss.write(&tombstone_char, 1);

    uint32_t val_len = static_cast<uint32_t>(serialized_value_type.length());
    oss.write(reinterpret_cast<const char*>(&val_len), sizeof(val_len));
    if (val_len > 0) {
        oss.write(serialized_value_type.data(), val_len);
    }
    if (!oss) {
        LOG_ERROR("[CachedLsmEntry] Serialization stream error.");
        return ""; // Indicate error
    }
    return oss.str();
}

bool CachedLsmEntry::deserialize(const std::string& data) {
    if (data.empty()) {
        LOG_WARN("[CachedLsmEntry] Deserialization error: input data is empty.");
        return false;
    }
    std::istringstream iss(data, std::ios::binary);
    if (!iss.read(reinterpret_cast<char*>(&commit_txn_id), sizeof(commit_txn_id))) {
        LOG_WARN("[CachedLsmEntry] Deserialization error: failed to read commit_txn_id. Data size: {}", data.size());
        return false;
    }

    char tombstone_char;
    if (!iss.read(&tombstone_char, 1)) {
        LOG_WARN("[CachedLsmEntry] Deserialization error: failed to read tombstone_char.");
        return false;
    }
    is_tombstone = (tombstone_char == 1);

    uint32_t val_len;
    if (!iss.read(reinterpret_cast<char*>(&val_len), sizeof(val_len))) {
        LOG_WARN("[CachedLsmEntry] Deserialization error: failed to read val_len.");
        return false;
    }

    // Sanity check val_len against remaining stream size
    std::streampos current_pos = iss.tellg();
    std::streampos end_pos = current_pos + static_cast<std::streamoff>(data.size() - (sizeof(commit_txn_id) + 1 + sizeof(val_len))); // Calculate based on total data size
    
    if(current_pos == -1) { //tellg can fail
        LOG_WARN("[CachedLsmEntry] Deserialization error: tellg failed after reading val_len.");
        return false;
    }

    if (val_len > static_cast<uint32_t>(end_pos - current_pos) && val_len > 0) { // Check if val_len is larger than what's left
        LOG_WARN("[CachedLsmEntry] Deserialization error: val_len {} exceeds remaining buffer size {}.", val_len, (end_pos - current_pos));
        return false;
    }
    if (val_len > (100 * 1024 * 1024)) { // Additional sanity for very large values
        LOG_WARN("[CachedLsmEntry] Deserialization error: val_len {} exceeds sanity limit.", val_len);
        return false;
    }


    serialized_value_type.resize(val_len);
    if (val_len > 0) {
        if (!iss.read(&serialized_value_type[0], val_len)) {
            LOG_WARN("[CachedLsmEntry] Deserialization error: failed to read serialized_value_type data (expected {} bytes).", val_len);
            return false;
        }
    }
    // Check if we consumed exactly all data
    iss.peek(); // Try to read one more char
    if (!iss.eof()) {
        LOG_WARN("[CachedLsmEntry] Deserialization error: extraneous data found at the end of serialized string.");
        // return false; // This might be too strict if padding occurs or string has extra nulls
    }
    return true;
}

// --- Helper Functions (serializeActiveTxnSet, deserializeActiveTxnSet remain the same) ---
std::string serializeActiveTxnSet(const std::set<TxnId>& txns) {
    std::ostringstream oss;
    size_t count = txns.size();
    oss.write(reinterpret_cast<const char*>(&count), sizeof(count));
    for (TxnId id : txns) {
        oss.write(reinterpret_cast<const char*>(&id), sizeof(id));
    }
    return oss.str();
}

static bool jsonArrayContainsValue(const std::string& json_array_str, const ValueType& value_to_find) {
    try {
        json j = json::parse(json_array_str);
        if (!j.is_array()) return false;

        for (const auto& item : j) {
            // Compare based on the type of value_to_find
            if (std::holds_alternative<int64_t>(value_to_find) && item.is_number_integer()) {
                if (item.get<int64_t>() == std::get<int64_t>(value_to_find)) return true;
            } else if (std::holds_alternative<double>(value_to_find) && item.is_number()) {
                if (item.get<double>() == std::get<double>(value_to_find)) return true;
            } else if (std::holds_alternative<std::string>(value_to_find) && item.is_string()) {
                if (item.get<std::string>() == std::get<std::string>(value_to_find)) return true;
            }
            // Note: Binary and other types can be added if needed in multikey arrays
        }
    } catch (const json::parse_error& e) {
        LOG_TRACE("  [jsonArrayContainsValue] Could not parse field as JSON array. Error: {}", e.what());
        return false;
    }
    return false; // Not found
}

static std::string invert_bytes(const std::string& input) {
    std::string output = input;
    for (char& c : output) {
        c = ~c;
    }
    return output;
}


static void merge_and_unique_keys(
    std::set<std::string>& destination, 
    const std::vector<std::pair<std::string, std::string>>& source) 
{
    for (const auto& pair : source) {
        destination.insert(pair.second); // pair.second is the primary key
    }
}

bool compare_values(const ValueType& filter_val, const ValueType& doc_val, FilterOperator op) {
    try {
        if (std::holds_alternative<int64_t>(filter_val) && std::holds_alternative<int64_t>(doc_val)) {
            int64_t fv = std::get<int64_t>(filter_val);
            int64_t dv = std::get<int64_t>(doc_val);
            switch (op) {
                case FilterOperator::EQUAL: return dv == fv;
                case FilterOperator::GREATER_THAN: return dv > fv;
                case FilterOperator::GREATER_THAN_OR_EQUAL: return dv >= fv;
                case FilterOperator::LESS_THAN: return dv < fv;
                case FilterOperator::LESS_THAN_OR_EQUAL: return dv <= fv;
            }
        }
        if (std::holds_alternative<double>(filter_val) && std::holds_alternative<double>(doc_val)) {
            double fv = std::get<double>(filter_val);
            double dv = std::get<double>(doc_val);
                switch (op) {
                case FilterOperator::EQUAL: return dv == fv; // Note: exact double equality can be tricky
                case FilterOperator::GREATER_THAN: return dv > fv;
                case FilterOperator::GREATER_THAN_OR_EQUAL: return dv >= fv;
                case FilterOperator::LESS_THAN: return dv < fv;
                case FilterOperator::LESS_THAN_OR_EQUAL: return dv <= fv;
            }
        }
        if (std::holds_alternative<std::string>(filter_val) && std::holds_alternative<std::string>(doc_val)) {
            const std::string& fv = std::get<std::string>(filter_val);
            const std::string& dv = std::get<std::string>(doc_val);
            int cmp = dv.compare(fv);
                switch (op) {
                case FilterOperator::EQUAL: return cmp == 0;
                case FilterOperator::GREATER_THAN: return cmp > 0;
                case FilterOperator::GREATER_THAN_OR_EQUAL: return cmp >= 0;
                case FilterOperator::LESS_THAN: return cmp < 0;
                case FilterOperator::LESS_THAN_OR_EQUAL: return cmp <= 0;
            }
        }
    } catch (const std::bad_variant_access& e) {
        LOG_WARN("  [compare_values] Type mismatch during comparison: {}", e.what());
    }
    return false;
}

bool evaluate_post_filters(const Record& doc, const std::vector<FilterCondition>& post_filters, IndexManager* index_manager) {
    if (post_filters.empty()) {
        return true;
    }
    try {
        for (const auto& filter : post_filters) {
            std::optional<ValueType> doc_field_value_opt = index_manager->extractFieldValue(doc, filter.field);

            // If the document doesn't have the field, it cannot match any filter on that field.
            if (!doc_field_value_opt.has_value()) {
                return false;
            }
            const ValueType& doc_value = *doc_field_value_opt;

            bool filter_passed = false;
            switch (filter.op) {
                case FilterOperator::ARRAY_CONTAINS: {
                    // The document's field must be a string that can be parsed as a JSON array.
                    if (!std::holds_alternative<std::string>(doc_value)) break; 
                    
                    // The filter's value must be a single ValueType.
                    const ValueType* value_to_find_ptr = std::get_if<ValueType>(&filter.value);
                    if (!value_to_find_ptr) break; // Should not happen with this operator

                    filter_passed = jsonArrayContainsValue(std::get<std::string>(doc_value), *value_to_find_ptr);
                    break;
                }
                case FilterOperator::ARRAY_CONTAINS_ANY: {
                    if (!std::holds_alternative<std::string>(doc_value)) break;

                    // The filter's value must be a vector of ValueTypes.
                    const auto* values_to_find_vec_ptr = std::get_if<std::vector<ValueType>>(&filter.value);
                    if (!values_to_find_vec_ptr) break; // Logic error

                    // Check if ANY of the filter values are in the document's array.
                    for (const auto& value_to_find : *values_to_find_vec_ptr) {
                        if (jsonArrayContainsValue(std::get<std::string>(doc_value), value_to_find)) {
                            filter_passed = true;
                            break; // Found one, no need to check others.
                        }
                    }
                    break;
                }
                default: { // Handle standard operators: ==, >, <, etc.
                    // The filter's value must be a single ValueType.
                    const ValueType* filter_single_value_ptr = std::get_if<ValueType>(&filter.value);
                    if (!filter_single_value_ptr) break; // Logic error

                    filter_passed = compare_values(*filter_single_value_ptr, doc_value, filter.op);
                    break;
                }
            }

            // If this filter did not pass, the document is rejected.
            if (!filter_passed) {
                return false;
            }
        }
    } catch (const std::exception& e) {
        LOG_WARN("  [evaluate_post_filters] Exception during post-filtering for key '{}': {}", doc.key, e.what());
        return false;
    }
    
    // If we get here, the document passed all post-filters.
    return true;
}


std::set<TxnId> deserializeActiveTxnSet(const std::string& data) {
    std::set<TxnId> txns;
    if (data.empty()) return txns;
    std::istringstream iss(data);
    size_t count;
    iss.read(reinterpret_cast<char*>(&count), sizeof(count));
    for (size_t i = 0; i < count; ++i) {
        TxnId id;
        iss.read(reinterpret_cast<char*>(&id), sizeof(id));
        if (iss.good()) {
            txns.insert(id);
        } else {
            LOG_WARN("[DeserializeActiveTxnSet] Failed to deserialize a TxnID from log record value.");
            break;
        }
    }
    return txns;
}

void StorageEngine::securelyClearPassword(std::string& password_buffer) {
    if (!password_buffer.empty()) {
        // Volatile to prevent compiler optimizing out the overwrite
        volatile char* p = &password_buffer[0];
        for (size_t i = 0; i < password_buffer.length(); ++i) {
            p[i] = '\0'; 
        }
        // Additional measures (though efficacy depends on compiler/OS)
        #ifdef _MSC_VER
        SecureZeroMemory(&password_buffer[0], password_buffer.length());
        #else
        // For GCC/Clang, a memory barrier might be considered, but volatile char* loop is a common attempt.
        // For truly secure wiping, platform-specific APIs or dedicated crypto libraries are better.
        // This is a best-effort for typical C++.
        #endif
    }
    password_buffer.clear();
    password_buffer.shrink_to_fit(); // Attempt to release memory
}

void StorageEngine::initializeEncryptionState(
    const std::optional<std::string>& encryption_password_opt,
    EncryptionScheme scheme_for_data_encryption, // Scheme to use for actual page/block data
    int kdf_iterations_for_kek_param)            // Iterations to use/store for KEK derivation (0 means let system decide/use default)
{
    std::unique_lock<std::shared_mutex> lock(encryption_state_mutex_); // <<< LOCK ADDED
    // Reset state members
    encryption_active_ = false;
    current_encryption_scheme_ = EncryptionScheme::NONE;
    kdf_iterations_ = 0; // This will be updated to the effectively used/stored value
    database_kek_.clear();
    database_dek_.clear();
    database_global_salt_.clear();

    if (scheme_for_data_encryption == EncryptionScheme::NONE ||
        !encryption_password_opt.has_value() || encryption_password_opt->empty()) {
        LOG_INFO("[StorageEngine] Encryption scheme is NONE or no password provided. Encryption will be INACTIVE.");
        return;
    }

    // Password IS provided and a scheme OTHER THAN NONE is intended.
    std::string temp_password = *encryption_password_opt; // Mutable copy for secure clearing

    LOG_INFO("[StorageEngine] Password provided. Attempting to set up encryption. Intended Data Scheme: {}, User KDF Iterations Param: {}",
             static_cast<int>(scheme_for_data_encryption), kdf_iterations_for_kek_param);

    try {
        loadOrGenerateGlobalSalt(); // Populates this->database_global_salt_
        if (database_global_salt_.empty() || database_global_salt_.size() != EncryptionLibrary::SALT_SIZE) {
            securelyClearPassword(temp_password); // Clear password before throwing
            throw CryptoException("Global salt initialization failed or salt is invalid (empty or wrong size).");
        }

        int iterations_from_file = 0;
        // Attempt to read kdf_iterations_for_kek from an existing dekinfo file header
        std::ifstream dek_info_check_stream(dekinfo_filepath_, std::ios::in | std::ios::binary);
        if (dek_info_check_stream.is_open()) {
            WrappedDEKInfo temp_header_for_iter_check;
            dek_info_check_stream.read(reinterpret_cast<char*>(&temp_header_for_iter_check), WRAPPED_DEK_INFO_HEADER_SIZE);
            if (dek_info_check_stream && dek_info_check_stream.gcount() == static_cast<std::streamsize>(WRAPPED_DEK_INFO_HEADER_SIZE) &&
                temp_header_for_iter_check.magic == WrappedDEKInfo::MAGIC_NUMBER &&
                temp_header_for_iter_check.version == WrappedDEKInfo::VERSION &&
                temp_header_for_iter_check.kdf_iterations_for_kek > 0) { // Ensure stored iterations are positive
                iterations_from_file = temp_header_for_iter_check.kdf_iterations_for_kek;
                LOG_TRACE("[StorageEngine] Pre-read KDF iterations from existing dekinfo file: {}", iterations_from_file);
            } else {
                LOG_TRACE("[StorageEngine] Pre-read from dekinfo file failed or header invalid/iterations 0. Will not use iterations_from_file.");
            }
            dek_info_check_stream.close();
        } else {
            LOG_TRACE("[StorageEngine] dekinfo file not found for pre-reading KDF iterations (expected for new DB).");
        }

        int kdf_iterations_to_use_for_derivation;
        if (kdf_iterations_for_kek_param > 0) {
            kdf_iterations_to_use_for_derivation = kdf_iterations_for_kek_param;
            LOG_TRACE("[StorageEngine] User explicitly provided KDF iterations: {}. Using these for KEK derivation.", kdf_iterations_to_use_for_derivation);
        } else if (iterations_from_file > 0) {
            kdf_iterations_to_use_for_derivation = iterations_from_file;
            LOG_TRACE("[StorageEngine] User did not provide KDF iterations. Using KDF iterations from existing dekinfo file: {}.", kdf_iterations_to_use_for_derivation);
        } else {
            kdf_iterations_to_use_for_derivation = EncryptionLibrary::DEFAULT_PBKDF2_ITERATIONS;
            LOG_TRACE("[StorageEngine] User did not provide KDF iterations, and no valid iterations in dekinfo file. Using default KDF iterations: {}.", kdf_iterations_to_use_for_derivation);
        }

        LOG_TRACE("[StorageEngine] Deriving KEK from password + global salt using {} iterations...", kdf_iterations_to_use_for_derivation);
        database_kek_ = EncryptionLibrary::deriveKeyFromPassword(temp_password, database_global_salt_, kdf_iterations_to_use_for_derivation);
        // Password buffer will be cleared after loadOrCreateDEKInfo

        if (database_kek_.size() != EncryptionLibrary::AES_KEY_SIZE) {
            database_kek_.clear(); // Clear partial/invalid KEK
            securelyClearPassword(temp_password);
            throw CryptoException("KEK derivation produced incorrect key size.");
        }
        LOG_TRACE("[StorageEngine] KEK derived successfully using {} iterations.", kdf_iterations_to_use_for_derivation);

        // Pass the iterations used for the CURRENT KEK derivation to loadOrCreateDEKInfo.
        // loadOrCreateDEKInfo will then use these iterations if it needs to *create* a new dekinfo file,
        // or it will compare them against stored iterations if loading an existing file.
        if (loadOrCreateDEKInfo(database_kek_, kdf_iterations_to_use_for_derivation, scheme_for_data_encryption, this->database_dek_)) {
            // If loadOrCreateDEKInfo was successful:
            // - If it loaded an existing file, this->kdf_iterations_ was updated inside it to header_from_file.kdf_iterations_for_kek.
            // - If it created a new file, this->kdf_iterations_ was updated inside it to kdf_iterations_to_use_for_derivation.
            // In both cases, this->kdf_iterations_ now reflects the actual KDF iterations associated with the active KEK/DEK.
            if (this->database_dek_.empty()){
                 securelyClearPassword(temp_password);
                 throw CryptoException("DEK is empty after loadOrCreateDEKInfo reported success.");
            }
            encryption_active_ = true;
            current_encryption_scheme_ = scheme_for_data_encryption; // Store the scheme for actual data
            // this->kdf_iterations_ is already correctly set by loadOrCreateDEKInfo
            LOG_INFO("[StorageEngine] Encryption ACTIVE. Data Scheme: {}. KEK KDF Iterations effectively used/stored: {}",
                     static_cast<int>(current_encryption_scheme_), this->kdf_iterations_);
        } else {
            // loadOrCreateDEKInfo failed (e.g., wrong password if file existed, or creation error)
            database_kek_.clear();
            database_dek_.clear();
            securelyClearPassword(temp_password);
            throw CryptoException("Failed to load or create DEK information. Password might be incorrect or DEK info file corrupt/unwritable.");
        }
        securelyClearPassword(temp_password); // Ensure cleared in all successful paths after use

    } catch (const CryptoException& ce) {
        LOG_FATAL("[StorageEngine] Cryptographic error during encryption state setup: {}. Encryption will be INACTIVE.", ce.what());
        database_kek_.clear(); database_dek_.clear(); database_global_salt_.clear(); // Clear all crypto material
        encryption_active_ = false; current_encryption_scheme_ = EncryptionScheme::NONE; kdf_iterations_ = 0;
        securelyClearPassword(temp_password); // Ensure cleared on exception too
        throw; // Re-throw critical crypto exceptions to halt engine construction if this is fatal
    } catch (const std::exception& e) {
        LOG_FATAL("[StorageEngine] Standard library error during encryption state setup: {}. Encryption will be INACTIVE.", e.what());
        database_kek_.clear(); database_dek_.clear(); database_global_salt_.clear();
        encryption_active_ = false; current_encryption_scheme_ = EncryptionScheme::NONE; kdf_iterations_ = 0;
        securelyClearPassword(temp_password);
        throw;
    }
    // temp_password should be cleared by now
}

// ==load or create dek
bool StorageEngine::loadOrCreateDEKInfo(
    const std::vector<unsigned char>& kek_candidate,
    int kdf_iterations_used_for_kek,
    EncryptionScheme scheme_for_data_encryption,
    std::vector<unsigned char>& out_dek)
{
    out_dek.clear();
    std::fstream dek_info_file;
    dek_info_file.open(dekinfo_filepath_, std::ios::in | std::ios::out | std::ios::binary);

    if (dek_info_file.is_open()) {
        // File exists, try to load and verify
        return loadExistingDEKInfo(kek_candidate, kdf_iterations_used_for_kek, out_dek);
    } else {
        // File does not exist, create new DEK info
        return createNewDEKInfo(kek_candidate, kdf_iterations_used_for_kek, out_dek);
    }
}

bool StorageEngine::loadExistingDEKInfo(const std::vector<unsigned char>& kek_candidate, 
                                       int kdf_iterations_used_for_kek, 
                                       std::vector<unsigned char>& out_dek) {
    LOG_INFO("[StorageEngine] DEK info file {} exists. Attempting to load and verify.", dekinfo_filepath_);
    
    std::fstream dek_info_file;
    dek_info_file.open(dekinfo_filepath_, std::ios::in | std::ios::out | std::ios::binary);
    
    WrappedDEKInfo header_from_file;
    dek_info_file.read(reinterpret_cast<char*>(&header_from_file), WRAPPED_DEK_INFO_HEADER_SIZE);

    if (!validateDEKFileHeader(header_from_file, kdf_iterations_used_for_kek)) {
        dek_info_file.close();
        return false;
    }

    if (!verifyKEKValidation(header_from_file, kek_candidate)) {
        dek_info_file.close();
        return false;
    }

    // Update the engine's active KDF iteration count
    this->kdf_iterations_ = header_from_file.kdf_iterations_for_kek;
    LOG_TRACE("[StorageEngine] Engine KDF iterations set to {} from dekinfo file.", this->kdf_iterations_);

    return readAndUnwrapDEK(dek_info_file, header_from_file, kek_candidate, out_dek);
}

bool StorageEngine::validateDEKFileHeader(const WrappedDEKInfo& header_from_file, 
                                         int kdf_iterations_used_for_kek) {
    if (header_from_file.magic != WrappedDEKInfo::MAGIC_NUMBER || 
        header_from_file.version != WrappedDEKInfo::VERSION) {
        LOG_ERROR("[StorageEngine] DEK info file {} is corrupt or has invalid header/version. Cannot proceed with this file.", dekinfo_filepath_);
        return false;
    }

    // Informational check on KDF iterations
    if (header_from_file.kdf_iterations_for_kek != kdf_iterations_used_for_kek) {
        LOG_WARN("[StorageEngine] KDF iterations mismatch: DEK info file states KEK used {} iterations, "
                 "current password attempt used {} iterations for KEK derivation. "
                 "KEK validation (using current attempt's KEK) will determine correctness.",
                 header_from_file.kdf_iterations_for_kek, kdf_iterations_used_for_kek);
    }

    return true;
}

bool StorageEngine::verifyKEKValidation(const WrappedDEKInfo& header_from_file, 
                                       const std::vector<unsigned char>& kek_candidate) {
    EncryptionLibrary::EncryptedData kek_validation_data_from_file;
    
    kek_validation_data_from_file.iv.assign(
        header_from_file.kek_validation_iv,
        header_from_file.kek_validation_iv + EncryptionLibrary::AES_GCM_IV_SIZE
    );
    kek_validation_data_from_file.tag.assign(
        header_from_file.kek_validation_tag,
        header_from_file.kek_validation_tag + sizeof(header_from_file.kek_validation_tag)
    );
    kek_validation_data_from_file.data.assign(
        header_from_file.encrypted_kek_validation_value,
        header_from_file.encrypted_kek_validation_value + sizeof(header_from_file.encrypted_kek_validation_value)
    );

    // Debug output
    fprintf(stderr, "DEBUG loadOrCreateDEKInfo (LOAD): KEK Candidate to verify (first 4 bytes hex): %02x%02x%02x%02x\n",
            kek_candidate.size() > 0 ? kek_candidate[0] : 0,
            kek_candidate.size() > 1 ? kek_candidate[1] : 0,
            kek_candidate.size() > 2 ? kek_candidate[2] : 0,
            kek_candidate.size() > 3 ? kek_candidate[3] : 0);
    fprintf(stderr, "DEBUG loadOrCreateDEKInfo (LOAD): Validation IV (first 4 bytes hex): %02x%02x%02x%02x, Full Size: %zu\n",
            kek_validation_data_from_file.iv.size() > 0 ? kek_validation_data_from_file.iv[0] : 0,
            kek_validation_data_from_file.iv.size() > 1 ? kek_validation_data_from_file.iv[1] : 0,
            kek_validation_data_from_file.iv.size() > 2 ? kek_validation_data_from_file.iv[2] : 0,
            kek_validation_data_from_file.iv.size() > 3 ? kek_validation_data_from_file.iv[3] : 0,
            kek_validation_data_from_file.iv.size());
    fprintf(stderr, "DEBUG loadOrCreateDEKInfo (LOAD): Validation Tag (first 4 bytes hex): %02x%02x%02x%02x, Full Size: %zu\n",
            kek_validation_data_from_file.tag.size() > 0 ? kek_validation_data_from_file.tag[0] : 0,
            kek_validation_data_from_file.tag.size() > 1 ? kek_validation_data_from_file.tag[1] : 0,
            kek_validation_data_from_file.tag.size() > 2 ? kek_validation_data_from_file.tag[2] : 0,
            kek_validation_data_from_file.tag.size() > 3 ? kek_validation_data_from_file.tag[3] : 0,
            kek_validation_data_from_file.tag.size());
    fprintf(stderr, "DEBUG loadOrCreateDEKInfo (LOAD): Validation Ciphertext (first 4 bytes hex): %02x%02x%02x%02x, Full Size: %zu\n",
            kek_validation_data_from_file.data.size() > 0 ? kek_validation_data_from_file.data[0] : 0,
            kek_validation_data_from_file.data.size() > 1 ? kek_validation_data_from_file.data[1] : 0,
            kek_validation_data_from_file.data.size() > 2 ? kek_validation_data_from_file.data[2] : 0,
            kek_validation_data_from_file.data.size() > 3 ? kek_validation_data_from_file.data[3] : 0,
            kek_validation_data_from_file.data.size());
    fflush(stderr);

    if (!EncryptionLibrary::verifyKekValidationValue(kek_validation_data_from_file, kek_candidate, KEK_VALIDATION_PLAINTEXT)) {
        LOG_ERROR("[StorageEngine] KEK validation failed for DEK info file {}. Incorrect password for the stored KDF iterations or corrupt file.", dekinfo_filepath_);
        return false;
    }

    LOG_INFO("[StorageEngine] KEK validation successful for existing DEK info file using KDF iterations: {}.", 
             header_from_file.kdf_iterations_for_kek);
    return true;
}

bool StorageEngine::readAndUnwrapDEK(std::fstream& dek_info_file, 
                                    const WrappedDEKInfo& header_from_file, 
                                    const std::vector<unsigned char>& kek_candidate, 
                                    std::vector<unsigned char>& out_dek) {
    // Validate package size
    if (header_from_file.wrapped_dek_package_size == 0 || 
        header_from_file.wrapped_dek_package_size > (1024 * 4)) {
        LOG_ERROR("[StorageEngine] Invalid wrapped_dek_package_size ({}) in DEK info file {}.", 
                  header_from_file.wrapped_dek_package_size, dekinfo_filepath_);
        return false;
    }

    // Read wrapped DEK package
    std::vector<unsigned char> raw_wrapped_dek_package(header_from_file.wrapped_dek_package_size);
    dek_info_file.seekg(WRAPPED_DEK_INFO_HEADER_SIZE, std::ios::beg);
    if (!dek_info_file) {
        LOG_ERROR("[StorageEngine] Failed to seek to wrapped DEK package in {}.", dekinfo_filepath_);
        return false;
    }

    dek_info_file.read(reinterpret_cast<char*>(raw_wrapped_dek_package.data()), 
                      header_from_file.wrapped_dek_package_size);
    if (!dek_info_file || dek_info_file.gcount() != static_cast<std::streamsize>(header_from_file.wrapped_dek_package_size)) {
        LOG_ERROR("[StorageEngine] Failed to read wrapped DEK package (expected {} bytes) from DEK info file {}. Read {} bytes.",
                  header_from_file.wrapped_dek_package_size, dekinfo_filepath_, dek_info_file.gcount());
        return false;
    }
    dek_info_file.close();

    // Parse wrapped DEK data
    EncryptionLibrary::EncryptedData wrapped_dek_ed;
    size_t current_offset = 0;
    
    if (raw_wrapped_dek_package.size() < static_cast<size_t>(EncryptionLibrary::AES_GCM_IV_SIZE + EncryptionLibrary::AES_GCM_TAG_SIZE)) {
        LOG_ERROR("[StorageEngine] raw_wrapped_dek_package is too small ({}) to contain IV and Tag.", 
                  raw_wrapped_dek_package.size());
        return false;
    }

    wrapped_dek_ed.iv.assign(raw_wrapped_dek_package.begin() + current_offset, 
                            raw_wrapped_dek_package.begin() + current_offset + EncryptionLibrary::AES_GCM_IV_SIZE);
    current_offset += EncryptionLibrary::AES_GCM_IV_SIZE;
    
    wrapped_dek_ed.tag.assign(raw_wrapped_dek_package.begin() + current_offset, 
                             raw_wrapped_dek_package.begin() + current_offset + EncryptionLibrary::AES_GCM_TAG_SIZE);
    current_offset += EncryptionLibrary::AES_GCM_TAG_SIZE;
    
    wrapped_dek_ed.data.assign(raw_wrapped_dek_package.begin() + current_offset, 
                              raw_wrapped_dek_package.end());

    // Unwrap DEK
    try {
        out_dek = EncryptionLibrary::unwrapKeyAES_GCM(wrapped_dek_ed, kek_candidate);
    } catch (const CryptoException& ce) {
        LOG_ERROR("[StorageEngine] Failed to unwrap DEK from {}: {}. KEK (password) or file data might be incorrect/corrupt despite validation passing (highly unlikely if validation is sound).", 
                  dekinfo_filepath_, ce.what());
        return false;
    }

    LOG_INFO("[StorageEngine] DEK successfully unwrapped from {}.", dekinfo_filepath_);
    return true;
}

bool StorageEngine::createNewDEKInfo(const std::vector<unsigned char>& kek_candidate, 
                                    int kdf_iterations_used_for_kek, 
                                    std::vector<unsigned char>& out_dek) {
    LOG_INFO("[StorageEngine] DEK info file {} does not exist. Creating new DEK and info file.", dekinfo_filepath_);

    WrappedDEKInfo new_header_to_write;
    new_header_to_write.kdf_iterations_for_kek = kdf_iterations_used_for_kek;
    this->kdf_iterations_ = kdf_iterations_used_for_kek;

    if (!encryptAndStoreKEKValidation(new_header_to_write, kek_candidate)) {
        return false;
    }

    std::vector<unsigned char> raw_wrapped_dek_package;
    if (!generateAndWrapDEK(kek_candidate, out_dek, raw_wrapped_dek_package)) {
        return false;
    }

    new_header_to_write.wrapped_dek_package_size = static_cast<uint32_t>(raw_wrapped_dek_package.size());
    
    if (!writeDEKInfoFile(new_header_to_write, raw_wrapped_dek_package)) {
        return false;
    }

    LOG_INFO("[StorageEngine] New DEK generated, wrapped, and DEK info file {} created successfully.", dekinfo_filepath_);
    return true;
}

bool StorageEngine::encryptAndStoreKEKValidation(WrappedDEKInfo& header, 
                                                const std::vector<unsigned char>& kek_candidate) {
    auto kek_validation_iv_vec = EncryptionLibrary::generateRandomBytes(EncryptionLibrary::AES_GCM_IV_SIZE);
    
    std::memcpy(header.kek_validation_iv, kek_validation_iv_vec.data(),
                std::min(kek_validation_iv_vec.size(), sizeof(header.kek_validation_iv)));

    EncryptionLibrary::EncryptedData encrypted_validation;
    try {
        encrypted_validation = EncryptionLibrary::encryptKekValidationValue(
            KEK_VALIDATION_PLAINTEXT, kek_candidate, kek_validation_iv_vec
        );
    } catch (const CryptoException& ce) {
        LOG_FATAL("[StorageEngine] Failed to encrypt KEK validation value for new DEK info: {}", ce.what());
        fs::remove(dekinfo_filepath_);
        return false;
    }

    if (encrypted_validation.tag.size() != sizeof(header.kek_validation_tag) ||
        encrypted_validation.data.size() != sizeof(header.encrypted_kek_validation_value)) {
        LOG_ERROR("[StorageEngine] KEK validation encryption produced unexpected sizes for new DEK info. Tag: {} (exp {}), Data: {} (exp {})",
                  encrypted_validation.tag.size(), sizeof(header.kek_validation_tag),
                  encrypted_validation.data.size(), sizeof(header.encrypted_kek_validation_value));
        fs::remove(dekinfo_filepath_);
        return false;
    }

    std::memcpy(header.kek_validation_tag, encrypted_validation.tag.data(), encrypted_validation.tag.size());
    std::memcpy(header.encrypted_kek_validation_value, encrypted_validation.data.data(), encrypted_validation.data.size());
    
    LOG_TRACE("[StorageEngine] KEK validation value encrypted for new DEK info file.");
    return true;
}

bool StorageEngine::generateAndWrapDEK(const std::vector<unsigned char>& kek_candidate, 
                                      std::vector<unsigned char>& out_dek, 
                                      std::vector<unsigned char>& raw_wrapped_dek_package) {
    try {
        out_dek = EncryptionLibrary::generateAESKey();
        auto wrapped_dek_iv_vec = EncryptionLibrary::generateRandomBytes(EncryptionLibrary::AES_GCM_IV_SIZE);
        EncryptionLibrary::EncryptedData wrapped_dek_ed = EncryptionLibrary::wrapKeyAES_GCM(out_dek, kek_candidate, wrapped_dek_iv_vec);

        raw_wrapped_dek_package.insert(raw_wrapped_dek_package.end(), wrapped_dek_ed.iv.begin(), wrapped_dek_ed.iv.end());
        raw_wrapped_dek_package.insert(raw_wrapped_dek_package.end(), wrapped_dek_ed.tag.begin(), wrapped_dek_ed.tag.end());
        raw_wrapped_dek_package.insert(raw_wrapped_dek_package.end(), wrapped_dek_ed.data.begin(), wrapped_dek_ed.data.end());
        
        return true;
    } catch (const CryptoException& ce) {
        LOG_FATAL("[StorageEngine] Failed to generate or wrap DEK for new DEK info: {}", ce.what());
        fs::remove(dekinfo_filepath_);
        return false;
    }
}

bool StorageEngine::writeDEKInfoFile(const WrappedDEKInfo& header, 
                                    const std::vector<unsigned char>& raw_wrapped_dek_package) {
    std::fstream dek_info_file;
    dek_info_file.open(dekinfo_filepath_, std::ios::out | std::ios::binary | std::ios::trunc);
    if (!dek_info_file.is_open()) {
        LOG_FATAL("[StorageEngine] Failed to create DEK info file {} for writing.", dekinfo_filepath_);
        return false;
    }

    // Write header then package
    dek_info_file.write(reinterpret_cast<const char*>(&header), WRAPPED_DEK_INFO_HEADER_SIZE);
    if (header.wrapped_dek_package_size > 0) {
        dek_info_file.write(reinterpret_cast<const char*>(raw_wrapped_dek_package.data()), 
                           header.wrapped_dek_package_size);
    }

    if (!dek_info_file) {
        LOG_FATAL("[StorageEngine] Failed to write to new DEK info file {}.", dekinfo_filepath_);
        dek_info_file.close();
        fs::remove(dekinfo_filepath_);
        return false;
    }

    dek_info_file.close();
    if (dek_info_file.fail()) {
        LOG_FATAL("[StorageEngine] Stream error after closing new DEK info file {}.", dekinfo_filepath_);
        try { 
            fs::remove(dekinfo_filepath_); 
        } catch (const fs::filesystem_error& e) {
            LOG_ERROR("[StorageEngine] Additionally, failed to remove potentially corrupt new DEK info file {}: {}", 
                      dekinfo_filepath_, e.what());
        }
        return false;
    }

    return true;
}
// === end load create dek

/**
 * @brief Constructs the StorageEngine.
 *
 * This is the primary entry point for creating a database instance. It follows a
 * rigorous, ordered process to ensure all components are created and initialized
 * correctly before the engine is ready for use.
 *
 * The startup sequence is:
 * 1.  **Path and Directory Setup**: Resolves all necessary file paths and creates directories.
 * 2.  **Encryption State Initialization**: Loads or creates the database salt and Key
 *     Encryption Key (KEK) / Data Encryption Key (DEK) based on the provided password.
 *     This must happen before any other component that performs I/O is created.
 * 3.  **Component Construction (Phase 1)**: All major manager objects (Disk, BPM, WAL,
 *     LSM, Index, etc.) are constructed. This phase is fast, performs no I/O, and
 *     primarily sets up in-memory state.
 * 4.  **Component Initialization (Phase 2)**: The WAL manager's `initialize()` method
 *     is called. This is a critical step that performs the recovery scan of the WAL.
 * 5.  **Recovery**: The main `performRecovery()` method is called to replay log records
 *     and bring the in-memory state (LSMTree, Indexes) up to date with the durable log.
 * 6.  **Background Threads**: The checkpoint thread is started for periodic background maintenance.
 *
 * @param data_dir The root directory for all database files.
 * @param checkpoint_interval The interval for automatic checkpointing.
 * @param wal_config_param Configuration for the Write-Ahead Log.
 * @param sstable_block_size_bytes Target size for SSTable data blocks.
 * @param sstable_compression Compression type for SSTable data blocks.
 * @param sstable_compression_level Compression level.
 * @param encryption_password Optional password for database encryption.
 * @param data_encryption_scheme_to_use The encryption algorithm to use.
 * @param kdf_iterations_for_kek_param KDF iterations for password-based key derivation.
 * @param enable_data_cache Whether to enable the in-memory data cache.
 * @param cache_config_param Configuration for the data cache.
 */
StorageEngine::StorageEngine(
    const std::string& data_dir,
    std::chrono::seconds checkpoint_interval,
    const ExtWALManagerConfig& wal_config_param,
    size_t sstable_block_size_bytes,
    CompressionType sstable_compression,
    int sstable_compression_level,
    const std::optional<std::string>& encryption_password,
    EncryptionScheme data_encryption_scheme_to_use,
    int kdf_iterations_for_kek_param,
    bool enable_data_cache,
    const cache::CacheConfig& cache_config_param,
    engine::lsm::MemTableType default_memtable_type,
    size_t min_compaction_threads,
    size_t max_compaction_threads,
    engine::lsm::CompactionStrategyType default_compaction_strategy,
    const engine::lsm::LeveledCompactionConfig& leveled_config,
    const engine::lsm::UniversalCompactionConfig& universal_config,
    const engine::lsm::FIFOCompactionConfig& fifo_config,
    const engine::lsm::HybridCompactionConfig& hybrid_config,
    std::shared_ptr<engine::lsm::CompactionFilter> default_compaction_filter 
)
    : data_dir_(data_dir),
      checkpoint_interval_(checkpoint_interval),
      next_txn_id_(1),
      oldest_active_txn_id_(1),
      cache_enabled_(enable_data_cache),
      sstable_block_size_bytes_(sstable_block_size_bytes),
      sstable_compression_type_(sstable_compression),
      sstable_compression_level_(sstable_compression_level),
      default_memtable_type_(default_memtable_type),
      default_compaction_strategy_type_(default_compaction_strategy),
      default_leveled_config_(leveled_config),
      default_universal_config_(universal_config),
      default_fifo_config_(fifo_config),
      default_hybrid_config_(hybrid_config),
    default_compaction_filter_(default_compaction_filter) 
{
    LOG_INFO("[StorageEngine] Initializing with DataDir: {}", data_dir_);

    try {
        // --- 1. Path and Directory Setup ---
        fs::create_directories(data_dir_);
        dekinfo_filepath_ = (fs::path(data_dir_) / "dek.info").string();
        checkpoint_metadata_filepath_ = (fs::path(data_dir_) / "checkpoints" / "checkpoint.meta").string();
        fs::create_directories(fs::path(data_dir_) / "checkpoints");
        fs::create_directories(fs::path(data_dir_) / "data"); // For LSM stores
        fs::create_directories(fs::path(data_dir_) / "indices"); // For B+Tree metadata
        fs::create_directories(fs::path(data_dir_) / "columnar_data"); // For Columnar stores

        ExtWALManagerConfig effective_wal_config = wal_config_param;
        if (effective_wal_config.wal_directory.empty()) {
             effective_wal_config.wal_directory = (fs::path(data_dir_) / "wal_data").string();
        }
        fs::create_directories(effective_wal_config.wal_directory);

        // --- 2. Encryption State Initialization ---
        initializeEncryptionState(encryption_password, data_encryption_scheme_to_use, kdf_iterations_for_kek_param);

        // --- 3. Global Manager Construction ---
        size_t global_write_buffer_size = 256 * 1024 * 1024;
        write_buffer_manager_ = std::make_shared<engine::lsm::WriteBufferManager>(global_write_buffer_size);
        
        system_monitor_ = std::make_unique<engine::threading::SystemMonitor>();
        shared_compaction_pool_ = std::make_shared<engine::threading::ThreadPool>(
            min_compaction_threads, max_compaction_threads, "SharedCompactionPool"
        );
        adaptive_pool_manager_ = std::make_unique<engine::threading::AdaptiveThreadPoolManager>(
            shared_compaction_pool_.get(), system_monitor_.get()
        );
        
        // --- NEW: Instantiate the Compaction Rate Limiter if configured ---
        // This assumes an option like `compactionMaxIoMBps` was parsed from JS options.
        // For now, we'll hardcode a value for demonstration.
        // In a real implementation, this would come from `IndinisOptions`.
        double compactionMaxIoMBps = 20.0; // Example: Throttle to 20 MB/s
        if (compactionMaxIoMBps > 0) {
            size_t rate = static_cast<size_t>(compactionMaxIoMBps * 1024 * 1024);
            size_t capacity = rate * 2; // Allow 2 seconds of burst
            compaction_rate_limiter_ = std::make_shared<engine::threading::TokenBucketRateLimiter>(rate, capacity);
            LOG_INFO("[StorageEngine] Compaction throttling enabled. Rate: {} MB/s, Burst: {} MB", 
                     compactionMaxIoMBps, capacity / (1024*1024));
        }
        
        // --- 4. Core Component Construction ---
        disk_manager_ = std::make_unique<SimpleDiskManager>((fs::path(data_dir_) / "primary.db").string());
        
        bpm_ = std::make_unique<SimpleBufferPoolManager>(
            128, *disk_manager_, true, sstable_compression, sstable_compression_level,
            isEncryptionActive(), getActiveEncryptionScheme(), getDatabaseDEK(), getDatabaseGlobalSalt()
        );

        size_t num_shards = std::max(2u, std::thread::hardware_concurrency() / 4);
        wal_manager_ = std::make_unique<ShardedWALManager>(effective_wal_config, num_shards);

        schema_manager_ = std::make_unique<engine::columnar::StoreSchemaManager>(
            (fs::path(data_dir_) / "columnar_schemas").string()
        );
        
        index_manager_ = std::make_unique<IndexManager>(*bpm_, data_dir_);
        index_manager_->setStorageEngine(this);

        query_router_ = std::make_unique<engine::columnar::QueryRouter>(this);

        if (cache_enabled_) {
            data_cache_ = std::make_unique<cache::Cache>(cache_config_param);
        }

        // --- 5. Discovery of Existing Stores ---
        fs::path lsm_root_dir = fs::path(data_dir_) / "data";
        if (fs::exists(lsm_root_dir) && fs::is_directory(lsm_root_dir)) {
            for (const auto& entry : fs::directory_iterator(lsm_root_dir)) {
                if (entry.is_directory()) {
                    std::string store_name = entry.path().filename().string();
                    LOG_INFO("[StorageEngine] Found existing LSM store directory for '{}'. It will be loaded on first access.", store_name);
                }
            }
        }
        
        // --- 6. WAL Initialization & Recovery ---
        if (!wal_manager_->initialize()) {
            throw storage::StorageError(storage::ErrorCode::STORAGE_RECOVERY_FAILED, 
                                        "Write-Ahead Log (WAL) manager failed to initialize.");
        }
        
        loadLastCheckpointMetadata();
        performRecovery();
        
        // --- 7. Background Thread Startup ---
        if (checkpoint_interval_.count() > 0) {
            shutdown_initiated_.store(false); 
            checkpoint_thread_ = std::thread(&StorageEngine::checkpointThreadLoop, this);
        }
        
        adaptive_pool_manager_->start();

    } catch (const storage::StorageError& e) {
        LOG_FATAL("[StorageEngine] CRITICAL Initialization Error (StorageError): {}. The engine is in an unusable state.", e.toDetailedString());
        throw;
    } catch (const std::exception& e) {
        if (adaptive_pool_manager_) adaptive_pool_manager_->stop();
        if (shared_compaction_pool_) shared_compaction_pool_->stop();
        LOG_FATAL("[StorageEngine] CRITICAL Initialization Error (std::exception): {}. The engine is in an unusable state.", e.what());
        throw storage::StorageError(storage::ErrorCode::INTERNAL_ERROR, "A standard C++ exception occurred during engine initialization.")
            .withDetails(e.what());
    }

    LOG_INFO("[StorageEngine] Initialization complete. Final Next TxnID: {}. Default MemTable Type: {}",
             next_txn_id_.load(), magic_enum::enum_name(default_memtable_type_));
}

bool StorageEngine::ingestExternalFile(
    const std::string& store_path,
    const std::string& external_file_path,
    bool move_file)
{
    LOG_INFO("[StorageEngine] Starting ingestion of file '{}' into store '{}'.", external_file_path, store_path);

    LSMTree* store = getOrCreateLsmStore(store_path);
    if (!store) {
        throw storage::StorageError(storage::ErrorCode::STORAGE_NOT_INITIALIZED,
            "Could not get or create LSM store for path: " + store_path);
    }

    if (!fs::exists(external_file_path)) {
        throw storage::StorageError(storage::ErrorCode::FILE_NOT_FOUND,
            "External file does not exist: " + external_file_path);
    }
    
    std::shared_ptr<engine::lsm::SSTableMetadata> external_file_meta;
    try {
        
        // Call the new, public validation method. No need to know about internal structs.
        external_file_meta = store->validateAndLoadExternalSSTable(external_file_path);
        // -----------------------
        
        if (!external_file_meta) {
            // This case handles non-exception failures from the loading logic.
            throw std::runtime_error("Validation returned null metadata without an exception.");
        }
    } catch (const std::exception& e) {
        throw storage::StorageError(storage::ErrorCode::INVALID_DATA_FORMAT,
            "External file is not a valid SSTable or is corrupt.")
            .withFilePath(external_file_path)
            .withDetails(e.what());
    }
    
    LOG_INFO("[StorageEngine] External file '{}' validated. Key range: ['{}', '{}'].",
             external_file_path, format_key_for_print(external_file_meta->min_key), format_key_for_print(external_file_meta->max_key));

    // Delegate the core ingestion logic to the LSMTree instance.
    try {
        store->ingestFile(external_file_meta, external_file_path, move_file);
    } catch (const std::exception& e) {
        LOG_ERROR("[StorageEngine] Ingestion failed for file '{}': {}", external_file_path, e.what());
        throw;
    }
    
    LOG_INFO("[StorageEngine] Successfully ingested file '{}' into store '{}'.", external_file_path, store_path);
    return true; // Return true on success.
}

LSMTree* StorageEngine::getOrCreateLsmStore(const std::string& store_path) {
    // Phase 1: Fast Path (Read Lock)
    {
        std::unique_lock<std::shared_mutex> lock(lsm_stores_mutex_);
        auto it = lsm_stores_.find(store_path);
        if (it != lsm_stores_.end()) {
            return it->second.get();
        }
    }

    // Phase 2: Slow Path (Write Lock)
        std::unique_lock<std::shared_mutex> lock(lsm_stores_mutex_);
    
    // Double-check to prevent race conditions.
    auto it = lsm_stores_.find(store_path);
    if (it != lsm_stores_.end()) {
        return it->second.get();
    }

    // Phase 3: Creation
    LOG_INFO("[StorageEngine] Lazily creating new LSMTree instance for store path: '{}'", store_path);

    const std::string lsm_data_dir = (fs::path(data_dir_) / "data" / store_path).string();

    try {
        // --- PREPARE THE CONFIGURATION FOR THE NEW LSM-TREE ---
        // This config object will be passed to the LSMTree constructor and stored
        // to be used for creating all future memtables for this store instance.
        engine::lsm::PartitionedMemTable::Config mem_config;
        
        // These settings could be loaded from a per-store configuration file in a more
        // advanced system. For now, we use sensible defaults.
        mem_config.partition_count = 16;
        mem_config.partition_type = default_memtable_type_;
        mem_config.imbalance_threshold = 0.75; // Rebalance if stddev is 75% of the mean size
        mem_config.rebalance_frequency = 2;    // Consider rebalancing every 2nd flush
        mem_config.sample_size = 256;          // Use 256 keys to determine new ranges

        // --- CONSTRUCT THE LSM-TREE WITH THE NEW CONFIG ---
        auto new_store = std::make_unique<LSMTree>(
            mem_config,
            lsm_data_dir,
            16 * 1024 * 1024, // memtable_max_size
            sstable_block_size_bytes_,
            sstable_compression_type_,
            sstable_compression_level_,
            isEncryptionActive(),
            getActiveEncryptionScheme(),
            getDatabaseDEK(),
            getDatabaseGlobalSalt(),
            1, // num_flush_threads (a sensible default)
            shared_compaction_pool_,
            write_buffer_manager_,
            compaction_rate_limiter_,
            default_compaction_strategy_type_,
            default_leveled_config_,
            default_universal_config_,
            default_fifo_config_,
            default_hybrid_config_ ,
            default_compaction_filter_ 
        );

        new_store->setStorageEngine(this);

        LSMTree* ptr = new_store.get();
        lsm_stores_[store_path] = std::move(new_store);
        
        return ptr;

    } catch (const std::exception& e) {
        LOG_ERROR("[StorageEngine] CRITICAL: Failed to create new LSMTree instance for store '{}': {}",
                  store_path, e.what());
        return nullptr;
    }
}

std::shared_ptr<engine::threading::ThreadPool> StorageEngine::getSharedCompactionPool() const {
    return shared_compaction_pool_;
}

static std::string getStorePathFromKey(const std::string& key) {
    size_t first_slash = key.find('/');
    if (first_slash == std::string::npos) {
        // This key belongs to a "root" or default collection with no prefix.
        return "_root"; 
    }
    return key.substr(0, first_slash);
}

StorageEngine::~StorageEngine() {
    LOG_INFO("[StorageEngine] Shutting down...");

    // --- 1. Stop Background Checkpoint Thread ---
    shutdown_initiated_.store(true);
    if (adaptive_pool_manager_) {
        LOG_INFO("  - Stopping Adaptive Pool Manager...");
        adaptive_pool_manager_->stop();
    }
    if (checkpoint_interval_.count() > 0) {
        {
            std::unique_lock<std::mutex> lock(checkpoint_control_mutex_);
            checkpoint_cv_.notify_one();
        }
        if (checkpoint_thread_.joinable()) {
            checkpoint_thread_.join();
            LOG_INFO("  - Checkpoint thread joined.");
        }
    }

    // --- 2. Abort Any Remaining Active Transactions ---
    if (!active_transactions_.empty()) {
        LOG_WARN("  - {} active transactions remaining at shutdown. Forcibly aborting them.", active_transactions_.size());
        // We must copy the IDs first because abortTransaction modifies the map.
        std::vector<TxnId> txn_ids_to_abort;
        {
            std::lock_guard<std::mutex> lock(txn_mutex_);
            for (const auto& pair : active_transactions_) {
                txn_ids_to_abort.push_back(pair.first);
            }
        }
        for (TxnId tid : txn_ids_to_abort) {
            try {
                // abortTransaction is thread-safe and will handle removal from the map.
                abortTransaction(tid);
            } catch (const std::exception& e) {
                LOG_ERROR("    - Exception while aborting TxnID {} during shutdown: {}", tid, e.what());
            }
        }
    }

    // This MUST be done BEFORE we start destructing the LSMTree instances.
    // It unblocks any LSMTree `put` threads that are waiting for memory,
    // allowing the LSMTree destructors to proceed without a deadlock.
    if (write_buffer_manager_) {
        LOG_INFO("  - Shutting down the Write Buffer Manager...");
        write_buffer_manager_->Shutdown();
    }
    // --- 3. Component Shutdown (High-Level to Low-Level) ---
    // The std::unique_ptr members will be destructed in reverse order of their
    // declaration in the header file. We can also be explicit by calling reset().

    // --- Explicitly stop the shared pool. This will stop all worker threads. >>>
    if (shared_compaction_pool_) {
        LOG_INFO("  - Stopping the Shared Compaction Thread Pool...");
        shared_compaction_pool_->stop();
    }

    LOG_INFO("  - Closing Cache (if enabled)...");
    data_cache_.reset();

    LOG_INFO("  - Closing Query Router...");
    query_router_.reset();

    LOG_INFO("  - Closing all ColumnarStore instances...");
    columnar_stores_.clear(); // This calls the destructor for each ColumnarStore.

    LOG_INFO("  - Closing Columnar Schema Manager...");
    schema_manager_.reset();

    LOG_INFO("  - Closing IndexManager...");
    index_manager_.reset();

    LOG_INFO("  - Closing all LSM-Tree store instances...");
    lsm_stores_.clear(); // This calls the destructor for each LSMTree instance.

    LOG_INFO("  - Closing WAL Manager...");
    wal_manager_.reset();

    LOG_INFO("  - Closing Buffer Pool Manager (will flush dirty pages)...");
    bpm_.reset();

    LOG_INFO("  - Closing Disk Manager...");
    disk_manager_.reset();


    LOG_INFO("[StorageEngine] Shutdown complete.");
}

bool StorageEngine::registerStoreSchema(const engine::columnar::ColumnSchema& schema) {
    if (!schema_manager_) {
        LOG_ERROR("Cannot register schema, the schema manager is not initialized.");
        return false;
    }
    return schema_manager_->registerSchema(schema);
}

engine::columnar::StoreSchemaManager* StorageEngine::getSchemaManager() {
    return schema_manager_.get();
}

engine::columnar::ColumnarStore* StorageEngine::getOrCreateColumnarStore(const std::string& store_path) {
    // --- Phase 1: Fast Path (Read Lock) ---
    // First, check if the store already exists using a cheap, shared (read) lock.
    // This allows multiple threads to read existing stores concurrently without blocking.
    {
        std::shared_lock<std::shared_mutex> lock(columnar_stores_mutex_);
        auto it = columnar_stores_.find(store_path);
        if (it != columnar_stores_.end()) {
            return it->second.get();
        }
    }

    // --- Phase 2: Slow Path (Write Lock) ---
    // If not found, we must acquire a more expensive, exclusive (write) lock to create it.
    std::unique_lock<std::shared_mutex> lock(columnar_stores_mutex_);
    
    // Double-check: Another thread might have created the store while we were waiting
    // for the exclusive lock. This prevents redundant creations.
    auto it = columnar_stores_.find(store_path);
    if (it != columnar_stores_.end()) {
        return it->second.get();
    }

    // --- Phase 3: Creation ---
    // If we are here, we are the first thread to create this store instance.
    
    // A columnar store can only be created if a schema for it has been registered.
    if (!schema_manager_->getLatestSchema(store_path)) {
        LOG_TRACE("No columnar schema found for store '{}'. Columnar store will not be created.", store_path);
        return nullptr;
    }

    LOG_INFO("Creating new ColumnarStore instance for store path: '{}'", store_path);
    
    const std::string columnar_data_dir = 
        (fs::path(data_dir_) / "columnar_data" / engine::columnar::StoreSchemaManager::sanitizeStorePathForFilename(store_path)).string();

    try {
        // Create the new store instance.
        auto new_store = std::make_unique<engine::columnar::ColumnarStore>(
            store_path,
            columnar_data_dir,
            schema_manager_.get(),
            16 * 1024 * 1024,                   // write_buffer_max_size
            sstable_compression_type_,
            sstable_compression_level_,
            isEncryptionActive(),
            getActiveEncryptionScheme(),
            getDatabaseDEK()
        );

        new_store->setStorageEngine(this);

        engine::columnar::ColumnarStore* ptr = new_store.get();
        columnar_stores_[store_path] = std::move(new_store);
        
        return ptr;

    } catch (const std::exception& e) {
        LOG_ERROR("[StorageEngine] CRITICAL: Failed to create new ColumnarStore instance for store '{}': {}",
                  store_path, e.what());
        return nullptr;
    }
}

bool StorageEngine::ingestColumnarFile(
    const std::string& store_path,
    const std::string& external_file_path,
    bool move_file)
{
    LOG_INFO("StorageEngine: Ingesting external file '{}' into columnar store '{}'.",
             external_file_path, store_path);

    engine::columnar::ColumnarStore* store = getOrCreateColumnarStore(store_path);
    if (!store) {
        throw storage::StorageError(storage::ErrorCode::STORAGE_NOT_INITIALIZED,
            "Cannot ingest file: Columnar store could not be found or created for path: " + store_path);
    }

    if (!fs::exists(external_file_path)) {
        throw storage::StorageError(storage::ErrorCode::FILE_NOT_FOUND,
            "External file for ingestion does not exist: " + external_file_path);
    }

    try {
        store->ingestFile(external_file_path, move_file);
        return true;
    } catch (const std::exception& e) {
        LOG_ERROR("Ingestion failed for file '{}': {}", external_file_path, e.what());
        // Re-throw as a structured storage error for the N-API layer to catch.
        throw storage::StorageError(storage::ErrorCode::INTERNAL_ERROR, "Columnar file ingestion failed.")
            .withDetails(e.what());
    }
    return false; // Should not be reached
}

bool StorageEngine::isEncryptionActive() const {
    std::shared_lock<std::shared_mutex> lock(encryption_state_mutex_); // <<< LOCK ADDED
    return encryption_active_;
}

EncryptionScheme StorageEngine::getActiveEncryptionScheme() const {
    std::shared_lock<std::shared_mutex> lock(encryption_state_mutex_); // <<< LOCK ADDED
    return current_encryption_scheme_;
}

int StorageEngine::getKdfIterations() const {
    std::shared_lock<std::shared_mutex> lock(encryption_state_mutex_); // <<< LOCK ADDED
    return kdf_iterations_;
}

LSMTree* StorageEngine::getStorage(const std::string& store_path) {
    // This public method now simply delegates to the get-or-create helper.
    return getOrCreateLsmStore(store_path);
}

const std::vector<unsigned char>& StorageEngine::getDatabaseDEK() const {
    // This returns a reference. The caller must be careful about its lifetime
    // relative to the lock if they copy it. For passing to BPM/LSM constructors
    // (which happens while StorageEngine constructor holds unique lock or before concurrent access),
    // it's fine. If called concurrently later, the shared_lock protects reading the vector's state.
    std::shared_lock<std::shared_mutex> lock(encryption_state_mutex_); // <<< LOCK ADDED
    return database_dek_;
}

const std::vector<unsigned char>& StorageEngine::getDatabaseGlobalSalt() const {
    std::shared_lock<std::shared_mutex> lock(encryption_state_mutex_); // <<< LOCK ADDED
    return database_global_salt_;
}

void StorageEngine::loadOrGenerateGlobalSalt() {
    // Assumes data_dir_ is initialized
    fs::path salt_file_path = fs::path(data_dir_) / "indinis.salt";
    std::ifstream salt_file_in(salt_file_path, std::ios::binary);

    if (salt_file_in.is_open()) {
        database_global_salt_.assign(
            (std::istreambuf_iterator<char>(salt_file_in)),
            std::istreambuf_iterator<char>()
        );
        salt_file_in.close();

        if (database_global_salt_.size() == EncryptionLibrary::SALT_SIZE) { // SALT_SIZE from EncryptionLibrary
            LOG_INFO("[StorageEngine] Loaded global database salt ({} bytes) from {}", database_global_salt_.size(), salt_file_path.string());
            return;
        } else {
            LOG_WARN("[StorageEngine] Global salt file {} exists but has incorrect size ({} vs expected {}). Regenerating.", 
                     salt_file_path.string(), database_global_salt_.size(), EncryptionLibrary::SALT_SIZE);
            database_global_salt_.clear(); // Clear potentially invalid salt before regenerating
        }
    } else {
        LOG_INFO("[StorageEngine] Global salt file not found at {}. Will generate a new one.", salt_file_path.string());
    }

    LOG_INFO("[StorageEngine] Generating new global database salt ({} bytes)...", EncryptionLibrary::SALT_SIZE);
    try {
        database_global_salt_ = EncryptionLibrary::generateRandomBytes(EncryptionLibrary::SALT_SIZE);
    } catch (const CryptoException& ce) {
        LOG_FATAL("[StorageEngine] Failed to generate random bytes for global salt: {}. Cannot proceed with encryption setup.", ce.what());
        throw; // Re-throw, as this is a critical failure for encryption
    }
    
    std::ofstream salt_file_out(salt_file_path, std::ios::binary | std::ios::trunc);
    if (salt_file_out.is_open()) {
        salt_file_out.write(reinterpret_cast<const char*>(database_global_salt_.data()), database_global_salt_.size());
        salt_file_out.close(); // Close before checking failbit
        if (!salt_file_out.good()) { 
             LOG_FATAL("[StorageEngine] Failed to write new global database salt to {}. Stream error after close. Cannot ensure salt persistence.", salt_file_path.string());
             // Attempt to delete potentially corrupt salt file
             try { fs::remove(salt_file_path); } catch (const fs::filesystem_error& e) {
                 LOG_ERROR("[StorageEngine] Additionally, failed to remove potentially corrupt salt file {}: {}", salt_file_path.string(), e.what());
             }
             throw CryptoException("Failed to write global database salt to " + salt_file_path.string() + " (stream error after close).");
        }
        LOG_INFO("[StorageEngine] New global database salt successfully written to {}", salt_file_path.string());
    } else {
        LOG_FATAL("[StorageEngine] Failed to open global database salt file {} for writing. Check permissions and path.", salt_file_path.string());
        throw CryptoException("Failed to open global database salt file for writing: " + salt_file_path.string());
    }
}

// Destructor (remains the same as previous correct version)
std::optional<Record> StorageEngine::get(const std::string& key, TxnId reader_txn_id) {
    LOG_TRACE("[StorageEngine GET] START. Key: '{}', ReaderTxnID: {}", format_key_for_print(key), reader_txn_id);

    // --- Phase 1: Check the In-Memory Cache ---
    if (cache_enabled_ && data_cache_) {
        std::optional<std::string> cached_data_str = data_cache_->get(key);
        if (cached_data_str) {
            CachedLsmEntry cached_entry;
            if (cached_entry.deserialize(*cached_data_str)) {
                // Check if the cached version is visible to the current reader.
                if (cached_entry.commit_txn_id != 0 && cached_entry.commit_txn_id < reader_txn_id) {
                    if (cached_entry.is_tombstone) {
                        LOG_TRACE("  [SE GET CacheHit] Visible tombstone found in cache for key '{}'.", key);
                        return std::nullopt; // Definitive deletion found.
                    }
                    
                    std::optional<ValueType> val_type_opt = LogRecord::deserializeValueTypeBinary(cached_entry.serialized_value_type);
                    if (val_type_opt) {
                        Record rec_from_cache;
                        rec_from_cache.key = key;
                        rec_from_cache.value = *val_type_opt;
                        rec_from_cache.commit_txn_id = cached_entry.commit_txn_id;
                        rec_from_cache.deleted = false;
                        LOG_TRACE("  [SE GET CacheHit] Visible data found in cache for key '{}'.", key);
                        return rec_from_cache;
                    }
                }
            } else {
                 LOG_WARN("  [SE GET CacheCorrupt] Failed to deserialize cache entry for key '{}'. Evicting.", key);
                 data_cache_->remove(key);
            }
        }
    }

    // --- Phase 2: Resolve Store and Fetch from LSM-Tree ---
    std::string store_path = getStorePathFromKey(key);
    LSMTree* store = getOrCreateLsmStore(store_path);
    if (!store) {
        LOG_WARN("[StorageEngine GET] Could not get or create LSM store for path '{}'. Key cannot be found.", store_path);
        return std::nullopt;
    }

    std::optional<Record> lsm_record = store->get(key, reader_txn_id);

    // --- Phase 3: Populate Cache if Data was Fetched from LSM ---
    if (cache_enabled_ && data_cache_) {
        // We cache the result from the LSM-Tree, whether it's a record or a definitive tombstone.
        // `lsm_record` will be `std::nullopt` if no visible version exists, which we don't cache.
        if (lsm_record) {
            CachedLsmEntry entry_to_cache;
            entry_to_cache.commit_txn_id = lsm_record->commit_txn_id;
            entry_to_cache.is_tombstone = lsm_record->deleted;
            if (!lsm_record->deleted) {
                entry_to_cache.serialized_value_type = LogRecord::serializeValueTypeBinary(lsm_record->value);
            }
            
            std::string serialized_cache_entry = entry_to_cache.serialize();
            if (!serialized_cache_entry.empty()) {
                data_cache_->put(key, serialized_cache_entry);
                LOG_TRACE("  [SE GET CachePopulate] Populated cache for key '{}' from LSM result.", key);
            }
        }
    }

    LOG_TRACE("[StorageEngine GET] END. Key: '{}'. Returning from LSMTree (has_value: {}).",
              format_key_for_print(key), lsm_record.has_value());
              
    return lsm_record;
}

// init() (remains the same)
void StorageEngine::init() {
    LOG_INFO("[StorageEngine] init() called (currently no-op as init is in constructor).");
}

// beginTransaction() (remains the same)
std::shared_ptr<Transaction> StorageEngine::beginTransaction() {
    std::lock_guard<std::mutex> lock(txn_mutex_);

    if (!wal_manager_ || !wal_manager_->isHealthy()) {
        LOG_ERROR("[StorageEngine] Cannot begin transaction, WAL manager is not healthy or available.");
        throw std::runtime_error("Cannot begin transaction: WAL system not ready.");
    }
    
    TxnId txn_id = next_txn_id_++;
    
    LogRecord begin_log_record;
    begin_log_record.txn_id = txn_id;
    begin_log_record.type = LogRecordType::BEGIN_TXN;
    
    // --- START OF FIX ---

    // 1. Submit the BEGIN_TXN record to the WAL with NORMAL priority.
    //    This returns a future that will eventually contain the assigned LSN.
    std::future<LSN> begin_lsn_future = wal_manager_->appendLogRecord(begin_log_record, WALPriority::NORMAL);
    
    LSN begin_lsn = 0;
    try {
        // 2. Block and wait for the WAL to process the record and return the LSN.
        //    For a BEGIN_TXN record, this wait is typically very short, as it doesn't
        //    require a full fsync, just for the writer thread to process its batch.
        begin_lsn = begin_lsn_future.get();

        if (begin_lsn == 0) {
            // This would indicate a failure inside the WAL writer thread that was
            // propagated via the promise's exception, which future::get() re-throws.
            // So this explicit check is more of a defense-in-depth.
            throw std::runtime_error("WAL returned an invalid LSN of 0 for BEGIN_TXN.");
        }

    } catch (const std::exception& e) {
        LOG_ERROR("[StorageEngine] Failed to log BEGIN_TXN for TxnID {}: {}. Aborting transaction start.", txn_id, e.what());
        next_txn_id_--; // Roll back the ID increment on failure.
        // Re-throw to signal the failure to the caller.
        throw std::runtime_error("Failed to log transaction begin to WAL for TxnID " + std::to_string(txn_id) + ": " + e.what());
    }
    
    // --- END OF FIX ---

    // Now that we have the durable LSN, we can create the transaction object.
    auto txn = std::make_shared<Transaction>(txn_id, begin_lsn, *this); 
    active_transactions_[txn_id] = txn;

    // After creating the transaction, notify all LSM stores that it has started a read phase.
    std::shared_lock<std::shared_mutex> lsm_lock(lsm_stores_mutex_);
    for (const auto& pair : lsm_stores_) {
        // We use the raw pointer as a unique ID for the transaction's lifetime.
        pair.second->BeginTxnRead(txn.get()); 
    }

    updateOldestActiveTxnId();
    
    LOG_INFO("[StorageEngine] Begin Txn: {}, Begin LSN: {}, Oldest Active TxnID: {}", txn_id, begin_lsn, oldest_active_txn_id_.load());
    return txn;
}

// commitTransaction ===============================================
bool StorageEngine::commitTransaction(TxnId txn_id) {
    LOG_INFO("[StorageEngine Commit] Attempting to commit transaction {}", txn_id);

    std::shared_ptr<Transaction> txn_ptr;
    
    // --- Phase 1: Validation and Preparation ---
    // This phase is entirely synchronous and protected by the transaction mutex.
    try {
        // validateTransactionForCommit finds the transaction, checks its phase,
        // processes atomic updates, and performs schema validation.
        // It throws a storage::StorageError on validation failure, which we want to propagate.
        if (!validateTransactionForCommit(txn_id, txn_ptr)) {
            // This case handles non-error conditions like trying to commit an already
            // committed or aborted transaction. validateTransactionForCommit logs the reason.
            return txn_ptr ? txn_ptr->isCommitted() : false;
        }
    } catch (...) {
        // If validateTransactionForCommit throws (e.g., schema violation), the transaction
        // is already aborted internally. We just re-throw the detailed exception.
        LOG_WARN("[StorageEngine Commit] Validation failed for Txn {}. Propagating exception.", txn_id);
        throw;
    }
    
    // If we reach here, txn_ptr is valid and the transaction is ready for the WAL stage.

    // --- Phase 2: Asynchronous WAL Submission and Durability Wait ---
    try {
        std::vector<std::future<LSN>> data_record_futures;
        data_record_futures.reserve(txn_ptr->write_set_.size());

        // 2a. Submit all data records to the WAL with NORMAL priority.
        // We collect their futures but don't wait for them yet.
        for (const auto& [key, record_in_writeset] : txn_ptr->write_set_) {
            LogRecord wal_rec;
            wal_rec.txn_id = txn_id;
            wal_rec.type = record_in_writeset.deleted ? LogRecordType::DATA_DELETE : LogRecordType::DATA_PUT;
            wal_rec.key = key;
            wal_rec.value = LogRecord::serializeValueTypeBinary(record_in_writeset.value);

            data_record_futures.push_back(wal_manager_->appendLogRecord(wal_rec, WALPriority::NORMAL));
        }
        LOG_TRACE("  [SE Commit] Txn {}: Submitted {} data records to WAL manager.", txn_id, data_record_futures.size());

        // 2b. Submit the final COMMIT record with IMMEDIATE priority.
        // This is the critical record that makes the transaction durable.
        LogRecord commit_rec;
        commit_rec.txn_id = txn_id;
        commit_rec.type = LogRecordType::COMMIT_TXN;
        auto commit_future = wal_manager_->appendLogRecord(commit_rec, WALPriority::IMMEDIATE);
        LOG_TRACE("  [SE Commit] Txn {}: Submitted COMMIT record to WAL manager. Waiting for durability...", txn_id);
        
        // 2c. Block and wait for the COMMIT record's future to resolve.
        // The GroupCommitManager's writer thread will set this future's value
        // only *after* the batch containing this record has been successfully synced to disk.
        LSN commit_lsn = commit_future.get(); // This is the synchronous blocking point.
        
        if (commit_lsn == 0) {
            // This should not happen if the promise was fulfilled, but a defensive check is good.
            // An exception from get() is the more likely failure path.
            throw std::runtime_error("WAL commit returned an invalid LSN of 0.");
        }
        
        // DURABILITY GUARANTEED. Update transaction state.
        txn_ptr->setCommitLsn(commit_lsn);
        txn_ptr->setPhase(TransactionPhase::WAL_COMMIT_DURABLE);
        LOG_INFO("  [SE Commit] Txn {}: COMMIT record is durable at LSN {}. Phase -> WAL_COMMIT_DURABLE.", txn_id, commit_lsn);

        // Optional: Wait for data record futures. In a high-performance system, this is
        // not strictly necessary as they were in the same or an earlier batch than the
        // COMMIT record. But for absolute certainty in all edge cases, we can wait.
        for (auto& f : data_record_futures) {
            f.get(); // This should return almost instantly.
        }

    } catch (const std::exception& e) {
        LOG_ERROR("[StorageEngine Commit] CRITICAL FAILURE: Txn {}: Exception during WAL submission/wait: {}. Transaction is now in an indeterminate state (may or may not be durable).", txn_id, e.what());
        // At this point, the transaction might be durable in the WAL but we failed before
        // applying it to memory. The recovery process is now responsible for ensuring consistency.
        // We must not proceed to apply changes to memory. We simply abort the in-memory transaction object.
        handleWALFailure(txn_id, txn_ptr);
        // We re-throw to signal the catastrophic failure to the caller.
        throw;
    }

    // --- Phase 3: Apply to In-Memory Structures ---
    // This phase is non-blocking and happens only after durability is guaranteed.
    try {
        applyToMemoryStructures(txn_id, txn_ptr); // This sets the phase to FULLY_APPLIED
    } catch (const std::exception& e) {
        // This is a very serious but recoverable-on-restart error. The data is durable in the WAL.
        // The in-memory state is now inconsistent with the durable log.
        // Recovery on the next startup will fix this.
        handleMemoryApplyFailure(txn_id, txn_ptr, txn_ptr->getCommitLsn());
        // We still consider the commit "successful" from a durability perspective
        // and proceed to finalize its state, but we must re-throw to signal the issue.
        finalizeTransactionState(txn_id);
        throw; // Re-throw the error so the caller knows the in-memory apply failed.
    }
    
    // --- Phase 4: Finalization ---
    // Remove the successfully applied transaction from the active list.
    finalizeTransactionState(txn_id);

    LOG_INFO("[StorageEngine Commit] Txn {} successfully committed and applied.", txn_id);
    return true;
}

// Phase 1: Transaction Validation and WAL Preparation
bool StorageEngine::validateTransactionForCommit(TxnId txn_id, std::shared_ptr<Transaction>& txn_ptr) {
    // Acquire a lock to safely access the active_transactions_ map.
    std::lock_guard<std::mutex> lock(txn_mutex_);
    LOG_TRACE("  [SE CommitOpt THIS={:p}] TxnID {}: Acquired txn_mutex_. Validating transaction.", static_cast<void*>(this), txn_id);
    
    // 1. Find the transaction in the active list.
    auto it = active_transactions_.find(txn_id);
    if (it == active_transactions_.end()) {
        LOG_WARN("  [SE CommitOpt THIS={:p}] TxnID {}: Not found in active list. Cannot commit.", static_cast<void*>(this), txn_id);
        return false; // Transaction doesn't exist.
    }
    txn_ptr = it->second;

    // 2. Check the transaction's current state.
    TransactionPhase currentPhase = txn_ptr->getPhase();
    LOG_TRACE("    [SE CommitOpt THIS={:p}] TxnID {}: Current phase is {}.", static_cast<void*>(this), txn_id, static_cast<int>(currentPhase));
    
    if (currentPhase == TransactionPhase::FULLY_APPLIED || currentPhase == TransactionPhase::COMMITTED_FINAL) {
        LOG_INFO("  [SE CommitOpt THIS={:p}] TxnID {}: Already committed. Idempotent success.", static_cast<void*>(this), txn_id);
        return true;
    }
    if (currentPhase == TransactionPhase::ABORTED) {
        LOG_WARN("  [SE CommitOpt THIS={:p}] TxnID {}: Attempt to commit an already aborted transaction.", static_cast<void*>(this), txn_id);
        return false;
    }
    if (currentPhase != TransactionPhase::ACTIVE) {
        LOG_ERROR("  [SE CommitOpt THIS={:p}] TxnID {}: Transaction in unexpected phase {} for commit. This may indicate a logic error.", static_cast<void*>(this), txn_id, static_cast<int>(currentPhase));
        return false;
    }
    
    // 3. Perform schema validation (the main gatekeeping logic).
    try {
        // First, apply any pending atomic updates to get the final state of documents.
        // This modifies the transaction's writeset in-place.
        processAtomicUpdates(txn_id, txn_ptr); 

        // Now, iterate through the finalized writeset and validate each record.
        for (const auto& pair : txn_ptr->write_set_) {
            const std::string& key = pair.first;
            const Record& record_to_validate = pair.second;

            // Determine the store path from the key (e.g., 'users' from 'users/user123').
            size_t last_slash = key.find_last_of('/');
            if (last_slash == std::string::npos) {
                // This key doesn't belong to a store, so no schema can apply.
                continue; 
            }
            
            std::string storePath = key.substr(0, last_slash);

            // Check if a columnar schema is registered for this store path.
            auto schema_opt = schema_manager_->getLatestSchema(storePath);
            if (schema_opt) {
                // A schema exists, so validation is mandatory.
                auto validation_error = engine::columnar::SchemaValidator::validate(record_to_validate, *schema_opt);
                
                if (validation_error) {
                    // Validation failed. The entire transaction must be aborted.
                    std::string error_msg = "Transaction validation failed for key '" + key + "': " + *validation_error;
                    LOG_WARN("  [SE CommitOpt THIS={:p}] TxnID {}: {}", this, txn_id, error_msg);
                    
                    // Abort the transaction internally and remove it from the active list.
                    txn_ptr->force_abort_internal();
                    active_transactions_.erase(it);
                    updateOldestActiveTxnId();
                    
                    // Throw a structured error that will be caught by the calling function
                    // and propagated to the user.
                    throw storage::StorageError(storage::ErrorCode::SCHEMA_VIOLATION, error_msg);
                }
            }
        }
    } catch (...) {
        // If processAtomicUpdates or the validation throws an exception,
        // it should be caught by the outer commitTransaction function. We re-throw it here.
        // The transaction state has already been handled (aborted) inside the schema validation block.
        throw; 
    }
    
    // 4. If all validations pass, transition the transaction to the next phase.
    txn_ptr->setPhase(TransactionPhase::WAL_COMMIT_WRITTEN);
    LOG_TRACE("    [SE CommitOpt THIS={:p}] TxnID {}: Phase -> WAL_COMMIT_WRITTEN. Validation complete.", this, txn_id);
    return true;
}

void StorageEngine::prepareWALRecords(TxnId txn_id, std::shared_ptr<Transaction> txn_ptr, std::vector<LogRecord>& wal_records_for_txn) {
    LOG_TRACE("    [SE CommitOpt THIS={:p}] TxnID {}: Phase set to WAL_COMMIT_WRITTEN. Preparing WAL records (writeset size: {}).",
              static_cast<void*>(this), txn_id, txn_ptr->write_set_.size());

    for (const auto& pair : txn_ptr->write_set_) {
        const std::string& key = pair.first;
        Record record_in_writeset = pair.second; 
        record_in_writeset.commit_txn_id = txn_id; // Set commit_txn_id now

        LogRecord wal_rec;
        wal_rec.txn_id = txn_id;
        wal_rec.type = record_in_writeset.deleted ? LogRecordType::DATA_DELETE : LogRecordType::DATA_PUT;
        wal_rec.key = key;
        wal_rec.value = LogRecord::serializeValueTypeBinary(record_in_writeset.value);
        wal_records_for_txn.push_back(wal_rec);
        LOG_TRACE("      Prepared WAL record: LSN (pending), TxnID {}, Type {}, Key '{}', ValueLen {}",
                  wal_rec.txn_id, static_cast<int>(wal_rec.type), format_key_for_print(wal_rec.key), wal_rec.value.length());
    }
    
    LogRecord commit_wal_rec_obj;
    commit_wal_rec_obj.txn_id = txn_id;
    commit_wal_rec_obj.type = LogRecordType::COMMIT_TXN;
    wal_records_for_txn.push_back(commit_wal_rec_obj);
    LOG_TRACE("      Prepared COMMIT_TXN WAL record for TxnID {}.", txn_id);
}

bool StorageEngine::writeToWAL(TxnId txn_id, std::shared_ptr<Transaction> txn_ptr, 
                               std::vector<LogRecord>& wal_records_for_txn, LSN& out_commit_lsn) {
                               
    LOG_TRACE("  [SE Commit] TxnID {}: Writing {} records to WAL.", txn_id, wal_records_for_txn.size());
    
    if (!wal_manager_ || !wal_manager_->isHealthy()) {
        LOG_ERROR("  [SE Commit] TxnID {}: WAL manager not healthy. Aborting commit logic.", txn_id);
        handleWALFailure(txn_id, txn_ptr);
        return false;
    }

    try {
        std::vector<std::future<LSN>> data_record_futures;
        data_record_futures.reserve(wal_records_for_txn.size());

        // This approach submits all records and then waits. It's slightly different
        // from the one I generated previously but also correct.
        for (LogRecord& record : wal_records_for_txn) {
            bool is_commit_record = (record.type == LogRecordType::COMMIT_TXN);
            
            // --- START OF FIX ---
            if (is_commit_record) {
                // For the COMMIT record, we need to block and ensure it's durable.
                out_commit_lsn = wal_manager_->appendAndForceFlushLogRecord(record);
                if (out_commit_lsn == 0) {
                    throw std::runtime_error("WAL commit returned an invalid LSN of 0 for COMMIT_TXN record.");
                }
                txn_ptr->setCommitLsn(out_commit_lsn);
            } else {
                // For data records, we can submit them asynchronously. We don't need to wait for them
                // individually, as waiting for the subsequent COMMIT record guarantees they are also durable.
                // We're not using the futures here, but this shows the async path.
                (void)wal_manager_->appendLogRecord(record, WALPriority::NORMAL);
            }
            // --- END OF FIX ---
        }
        
        txn_ptr->setPhase(TransactionPhase::WAL_COMMIT_DURABLE);
        LOG_INFO("  [SE Commit] TxnID {}: All WAL records are durable. COMMIT LSN: {}. Phase -> WAL_COMMIT_DURABLE.",
                 txn_id, out_commit_lsn);

    } catch (const std::exception& e_wal) {
        LOG_ERROR("  [SE Commit] TxnID {}: Exception during WAL write/flush: {}. Aborting transaction.", txn_id, e_wal.what());
        handleWALFailure(txn_id, txn_ptr);
        return false;
    }
    
    return true;
}

void StorageEngine::applyToMemoryStructures(TxnId txn_id, std::shared_ptr<Transaction> txn_ptr) {
    LOG_INFO("  [SE Commit] Txn {}: Applying to memory structures.", txn_id);
    txn_ptr->setPhase(TransactionPhase::APPLYING_TO_MEMORY);

    const TxnId oldest_active_for_lsm_put = getOldestActiveTxnId();

    // The entire writeset is processed.
    for (const auto& [key, record_from_writeset] : txn_ptr->write_set_) {
        Record record_to_apply = record_from_writeset;
        record_to_apply.commit_txn_id = txn_id;
        record_to_apply.timestamp = std::chrono::system_clock::now();

        // 1. Route to the correct LSM-Tree store.
        const std::string store_path = getStorePathFromKey(key);
        LSMTree* store = getOrCreateLsmStore(store_path);
        if (!store) {
            throw storage::StorageError(storage::ErrorCode::INTERNAL_ERROR,
                "Failed to get/create LSM store for path '" + store_path + "' during commit apply phase.");
        }

        // 2. Fetch the "before" image of the record for indexing.
        const std::optional<Record> old_record_for_idx = store->get(key, txn_id);

        // 3. Apply the new version to the target LSM-Tree's memtable.
        store->put(key, record_to_apply, oldest_active_for_lsm_put);

        // 4. Update secondary indexes.
        index_manager_->ensureStoreExistsAndIndexed(store_path);
        index_manager_->updateIndices(
            key,
            old_record_for_idx,
            record_to_apply.deleted ? std::nullopt : std::optional<Record>(record_to_apply),
            txn_id
        );

        // 5. Invalidate the cache.
        if (cache_enabled_ && data_cache_) {
            data_cache_->remove(key);
        }
        
        // 6. Shadow the write to the columnar store.
        if (engine::columnar::ColumnarStore* shadow_store = getOrCreateColumnarStore(store_path)) {
            if (record_to_apply.deleted) {
                shadow_store->shadowDelete(key);
            } else {
                shadow_store->shadowInsert(record_to_apply);
            }
        }
    }

    txn_ptr->setPhase(TransactionPhase::FULLY_APPLIED);
    LOG_INFO("  [SE Commit] Txn {}: Apply to memory complete. Phase -> FULLY_APPLIED.", txn_id);
}

void StorageEngine::processAtomicUpdates(TxnId txn_id, std::shared_ptr<Transaction> txn_ptr) {
    if (!txn_ptr->update_set_.empty()) {
        LOG_TRACE("    [SE CommitOpt] Processing {} atomic update operations for TxnID {}.", txn_ptr->update_set_.size(), txn_id);
        for (const auto& [key, operations] : txn_ptr->update_set_) {
            
            std::optional<Record> base_record_opt;
            
            // Check if the base record already exists within this transaction's writeset.
            auto ws_it = txn_ptr->write_set_.find(key);
            if (ws_it != txn_ptr->write_set_.end()) {
                // Use the version from the writeset as the base for the atomic op.
                LOG_TRACE("      Found base record for atomic op in transaction's writeset for key '{}'.", key);
                base_record_opt = ws_it->second;
            } else {
                // If not in the writeset, fetch the latest committed version from storage.
                LOG_TRACE("      Base record not in writeset. Fetching from storage for key '{}'.", key);
                base_record_opt = this->get(key, txn_id);
            }
            
            if (!base_record_opt.has_value() || base_record_opt->deleted) {
                throw storage::StorageError(storage::ErrorCode::KEY_NOT_FOUND, 
                    "Cannot apply atomic update: document with key '" + key + "' does not exist.");
            }
            
            Record modified_record = *base_record_opt;
            
            if (!std::holds_alternative<std::string>(modified_record.value)) {
                throw storage::StorageError(storage::ErrorCode::SCHEMA_VIOLATION,
                    "Cannot apply atomic update to key '" + key + "': existing value is not a JSON string document.");
            }
            json doc_json = json::parse(std::get<std::string>(modified_record.value));

            // Apply all atomic operations for this key.
            for (const auto& [field, op] : operations) {
                if (op.type == AtomicUpdateType::INCREMENT) {
                    double increment_value = std::get<double>(op.value);
                    if (!doc_json.contains(field) || !doc_json[field].is_number()) {
                        throw storage::StorageError(storage::ErrorCode::SCHEMA_VIOLATION,
                            "Cannot increment field '" + field + "' on key '" + key + "': field does not exist or is not a number.");
                    }
                    double current_value = doc_json[field].get<double>();
                    doc_json[field] = current_value + increment_value;
                }
                // Add other atomic operations (e.g., ARRAY_APPEND) here in the future.
            }
            
            modified_record.value = doc_json.dump();
            // Overwrite the writeset with the final, merged result.
            // This ensures the main application loop below handles a single, consolidated state.
            txn_ptr->write_set_[key] = modified_record; 
            LOG_TRACE("      Atomically modified key '{}' and placed final result in writeset.", key);
        }
    }
}

void StorageEngine::applyWritesetToStores(TxnId txn_id, std::shared_ptr<Transaction> txn_ptr) {
    // Determine the oldest active transaction ID *before* the loop. This value will be
    // passed to the LSMTree's put method for its internal MVCC garbage collection logic.
    const TxnId oldest_active_for_lsm_put = getOldestActiveTxnId();

    LOG_TRACE("  [SE Commit Apply] TxnID {}: Applying writeset ({} items) to memory stores. Oldest active for LSM put: {}.",
              txn_id, txn_ptr->write_set_.size(), oldest_active_for_lsm_put);

    // Iterate through every key-value pair in the transaction's final writeset.
    for (const auto& [key, record_from_writeset] : txn_ptr->write_set_) {
        
        // Create a mutable copy and finalize its metadata for this commit.
        Record record_to_apply = record_from_writeset;
        record_to_apply.commit_txn_id = txn_id;
        record_to_apply.timestamp = std::chrono::system_clock::now();

        LOG_TRACE("    [Apply Key] '{}', Deleted: {}", format_key_for_print(key), record_to_apply.deleted);

        // 1. Route the operation to the correct LSM-Tree instance based on the key.
        const std::string store_path = getStorePathFromKey(key);
        LSMTree* store = getOrCreateLsmStore(store_path);
        if (!store) {
            // This is a critical failure. If we can't get a store instance (e.g., due to
            // disk permission errors creating its directory), we cannot apply the write.
            throw storage::StorageError(storage::ErrorCode::INTERNAL_ERROR,
                "Failed to get or create LSM store for path '" + store_path + "' during commit apply phase.");
        }

        // 2. Fetch the "before" image of the record. This is essential for the IndexManager
        //    to correctly calculate the diff (which old index entries to remove, which new ones to add).
        //    We use `txn_id` as the read snapshot to see the state just before this commit.
        const std::optional<Record> old_record_for_idx = store->get(key, txn_id);

        // 3. Apply the new version to the target LSM-Tree's memtable.
        store->put(key, record_to_apply, oldest_active_for_lsm_put);

        // 4. Update all relevant secondary indexes.
        index_manager_->ensureStoreExistsAndIndexed(store_path); // Creates default index on first write if needed.
        index_manager_->updateIndices(
            key,
            old_record_for_idx,
            record_to_apply.deleted ? std::nullopt : std::optional<Record>(record_to_apply),
            txn_id
        );

        // 5. Invalidate the corresponding entry in the high-level data cache.
        if (cache_enabled_ && data_cache_) {
            data_cache_->remove(key);
            LOG_TRACE("      Invalidated cache for key '{}'.", key);
        }
        
        // 6. Shadow the write to the appropriate columnar store if one is configured.
        if (engine::columnar::ColumnarStore* shadow_store = getOrCreateColumnarStore(store_path)) {
            LOG_TRACE("      Shadowing write for key '{}' to columnar store for path '{}'.", key, store_path);
            if (record_to_apply.deleted) {
                shadow_store->shadowDelete(key);
            } else {
                shadow_store->shadowInsert(record_to_apply);
            }
        }
    }
}

void StorageEngine::finalizeTransactionState(TxnId txn_id) {
    std::shared_ptr<Transaction> txn_ptr;
    std::lock_guard<std::mutex> lock(txn_mutex_);
    LOG_TRACE("  [SE CommitOpt THIS={:p}] TxnID {}: Acquired txn_mutex_ for final cleanup.", static_cast<void*>(this), txn_id);

    
    auto it = active_transactions_.find(txn_id);
    if (it != active_transactions_.end()) {
        txn_ptr = it->second;
        active_transactions_.erase(it);
        LOG_TRACE("    Removed TxnID {} from active_transactions_.", txn_id);

    }
    
    // --- START OF FIX ---
    // If we found the transaction, notify stores that its read phase is over.
    if (txn_ptr) {
        std::shared_lock<std::shared_mutex> lsm_lock(lsm_stores_mutex_);
        for (const auto& pair : lsm_stores_) {
            pair.second->EndTxnRead(txn_ptr.get());
        }
    }
    // --- END OF FIX ---

    updateOldestActiveTxnId();
    LOG_TRACE("  [SE CommitOpt THIS={:p}] TxnID {}: Releasing txn_mutex_ after final cleanup.", static_cast<void*>(this), txn_id);

}

void StorageEngine::handleWALFailure(TxnId txn_id, std::shared_ptr<Transaction> txn_ptr) {
    // Revert phase and manage active_transactions_ under lock
    std::lock_guard<std::mutex> revert_lock(txn_mutex_);
    if(txn_ptr) txn_ptr->setPhase(TransactionPhase::ACTIVE); // Revert tentative phase
    // Transaction is not committed, but also not formally aborted in WAL yet. 
    // If we want to ensure it's cleaned up:
    if(txn_ptr) txn_ptr->force_abort_internal(); // Mark in-memory as aborted
    active_transactions_.erase(txn_id);
    updateOldestActiveTxnId(); // Must be called under txn_mutex_
}

void StorageEngine::handleMemoryApplyFailure(TxnId txn_id, std::shared_ptr<Transaction> txn_ptr, LSN local_commit_lsn) {
    LOG_FATAL("  [SE CommitOpt THIS={:p}] TxnID {}: CRITICAL ERROR - Exception applying changes to memory *after* WAL commit durable (LSN {}). Data inconsistency possible until recovery. Shutting down or restricting mode is advised.",
            this, txn_id, local_commit_lsn);
    
    // Mark as problematic but effectively committed due to WAL. The system is in a dangerous state.
    if (txn_ptr) {
        // Can't set to ABORTED as it's durably in the WAL. FULLY_APPLIED is a lie, but it removes it from the active list.
        // A more robust system might have a special "COMMIT_FAILED_IN_MEMORY" phase.
        txn_ptr->setPhase(TransactionPhase::FULLY_APPLIED); 
    }
    // Clean up from the active transaction list even on failure, as recovery will handle it from now on.
    std::lock_guard<std::mutex> lock(txn_mutex_);
    active_transactions_.erase(txn_id);
    updateOldestActiveTxnId();
}

// end of committransacstion logi handling

std::optional<cache::CacheStats> StorageEngine::getCacheStats() const {
    if (cache_enabled_ && data_cache_) {
        const cache::CacheStats* live_stats_ptr = data_cache_->get_stats();
        if (live_stats_ptr) {
            // 1. Create a copy of the CacheStats object.
            //    This will use the (implicitly generated) copy constructor of CacheStats.
            //    The atomic members will have their current values copied.
            //    The mutex member will also be copy-constructed.
            cache::CacheStats stats_snapshot = *live_stats_ptr;

            // 2. Construct and return the std::optional from this local copy.
            return stats_snapshot; // This uses std::optional's constructor that takes a T by value (or rvalue ref via move)
        }
    }
    return std::nullopt;
}

bool StorageEngine::changeMasterPassword(
    const std::string& old_password_str_const,
    const std::string& new_password_str_const,
    int new_kdf_iterations_param) {

    std::unique_lock<std::shared_mutex> lock(encryption_state_mutex_);
    LOG_INFO("[StorageEngine] Attempting to change master password.");

    if (!encryption_active_ || database_dek_.empty() || database_kek_.empty()) {
        throw std::runtime_error("Encryption not active or core keys not loaded, cannot change password.");
    }
    if (new_password_str_const.empty()) {
        throw std::invalid_argument("New password cannot be empty.");
    }

    std::string old_password = old_password_str_const;
    std::string new_password = new_password_str_const;
    std::vector<unsigned char> new_kek;

    try {
        // 1. Verify old password by checking if the current in-memory KEK is correct.
        //    We don't need to re-derive the old KEK; we already have it. We just need to
        //    re-verify it against the provided old_password to be certain.
        std::vector<unsigned char> old_kek_candidate = EncryptionLibrary::deriveKeyFromPassword(
            old_password, this->database_global_salt_, this->kdf_iterations_);
        
        if (old_kek_candidate != this->database_kek_) {
             throw CryptoException("Incorrect old password provided.");
        }
        LOG_INFO("[StorageEngine changePass] Old password verified successfully.");
        securelyClearPassword(old_password);
        old_kek_candidate.assign(old_kek_candidate.size(), 0); // Securely clear

        // 2. Derive new KEK.
        int effective_new_kdf_iterations = (new_kdf_iterations_param > 0)
                                           ? new_kdf_iterations_param
                                           : EncryptionLibrary::DEFAULT_PBKDF2_ITERATIONS;
        new_kek = EncryptionLibrary::deriveKeyFromPassword(
            new_password, this->database_global_salt_, effective_new_kdf_iterations);
        securelyClearPassword(new_password);

        // 3. Re-wrap the existing DEK with the new KEK.
        auto new_wrapped_dek_iv = EncryptionLibrary::generateIV();
        EncryptionLibrary::EncryptedData new_wrapped_dek_ed = EncryptionLibrary::wrapKeyAES_GCM(
            this->database_dek_, new_kek, new_wrapped_dek_iv
        );

        // 4. Create new KEK validation value with the new KEK.
        auto new_kek_validation_iv = EncryptionLibrary::generateIV();
        EncryptionLibrary::EncryptedData new_encrypted_validation = EncryptionLibrary::encryptKekValidationValue(
            EncryptionLibrary::KEK_VALIDATION_PLAINTEXT, new_kek, new_kek_validation_iv
        );

        // 5. Prepare the new WrappedDEKInfo header and package.
        WrappedDEKInfo new_dek_info_header;
        new_dek_info_header.kdf_iterations_for_kek = effective_new_kdf_iterations;
        std::memcpy(new_dek_info_header.kek_validation_iv, new_kek_validation_iv.data(), new_kek_validation_iv.size());
        std::memcpy(new_dek_info_header.kek_validation_tag, new_encrypted_validation.tag.data(), new_encrypted_validation.tag.size());
        std::memcpy(new_dek_info_header.encrypted_kek_validation_value, new_encrypted_validation.data.data(), new_encrypted_validation.data.size());

        std::vector<unsigned char> new_raw_wrapped_dek_package;
        new_raw_wrapped_dek_package.insert(new_raw_wrapped_dek_package.end(), new_wrapped_dek_ed.iv.begin(), new_wrapped_dek_ed.iv.end());
        new_raw_wrapped_dek_package.insert(new_raw_wrapped_dek_package.end(), new_wrapped_dek_ed.tag.begin(), new_wrapped_dek_ed.tag.end());
        new_raw_wrapped_dek_package.insert(new_raw_wrapped_dek_package.end(), new_wrapped_dek_ed.data.begin(), new_wrapped_dek_ed.data.end());
        new_dek_info_header.wrapped_dek_package_size = static_cast<uint32_t>(new_raw_wrapped_dek_package.size());

        // 6. Atomically update dek.info file.
        std::string temp_dekinfo_filepath = dekinfo_filepath_ + ".tmp_pass_change";
        std::ofstream temp_file(temp_dekinfo_filepath, std::ios::binary | std::ios::trunc);
        if (!temp_file.is_open()) {
            throw std::runtime_error("Failed to open temporary DEK info file for writing.");
        }
        temp_file.write(reinterpret_cast<const char*>(&new_dek_info_header), WRAPPED_DEK_INFO_HEADER_SIZE);
        if (new_dek_info_header.wrapped_dek_package_size > 0) {
            temp_file.write(reinterpret_cast<const char*>(new_raw_wrapped_dek_package.data()), new_dek_info_header.wrapped_dek_package_size);
        }
        temp_file.close();
        if (temp_file.fail()) {
            throw std::runtime_error("Stream error after closing temporary DEK info file.");
        }
        fs::rename(temp_dekinfo_filepath, dekinfo_filepath_);

        // 7. Update in-memory state.
        this->database_kek_ = new_kek;
        this->kdf_iterations_ = effective_new_kdf_iterations;

        LOG_INFO("Master password changed successfully. New KDF iterations: {}", this->kdf_iterations_);
        return true;

    } catch (...) {
        // Ensure sensitive buffers are cleared on any exception.
        securelyClearPassword(old_password);
        securelyClearPassword(new_password);
        new_kek.assign(new_kek.size(), 0);
        throw;
    }
}

// abortTransaction()
void StorageEngine::abortTransaction(TxnId txn_id) {
    LOG_INFO("[StorageEngine Abort] Aborting Txn ID: {}", txn_id);
    
    std::shared_ptr<Transaction> txn_ptr_local;
    bool needs_wal_abort_record = false;

    // --- Phase 1: Determine if a WAL record is needed ---
    {
        std::lock_guard<std::mutex> lock(txn_mutex_);
        auto it = active_transactions_.find(txn_id);
        if (it != active_transactions_.end()) {
            txn_ptr_local = it->second;
            // We only need to write an ABORT record if the transaction hasn't already
            // reached a durable commit state. If it has, it's too late to abort.
            if (txn_ptr_local->getPhase() < TransactionPhase::WAL_COMMIT_DURABLE) {
                needs_wal_abort_record = true;
            }
        } else {
            LOG_TRACE("[StorageEngine Abort] Txn {} not in active list; already completed or aborted.", txn_id);
            return; // Nothing to do.
        }
    } // txn_mutex_ released

    // --- Phase 2: Log ABORT to WAL (if necessary) ---
    // This is done outside the txn_mutex_ to avoid holding it during WAL submission.
    if (needs_wal_abort_record) {
        if (!wal_manager_ || !wal_manager_->isHealthy()) {
            LOG_WARN("[StorageEngine Abort] Txn {}: WAL manager not healthy. ABORT record may not be logged.", txn_id);
        } else {
            LogRecord abort_log_record;
            abort_log_record.txn_id = txn_id;
            abort_log_record.type = LogRecordType::ABORT_TXN;

            // --- CORRECTED LOGIC ---
            // Submit the record asynchronously and discard the future. We don't need to wait.
            // Using HIGH priority gives it a better chance of being flushed soon.
            (void)wal_manager_->appendLogRecord(abort_log_record, WALPriority::HIGH);
            LOG_TRACE("[StorageEngine Abort] Txn {}: ABORT_TXN record submitted to WAL manager.", txn_id);
            // --- END CORRECTED LOGIC ---
        }
    }

    // --- Phase 3: Update In-Memory State ---
    {
        std::lock_guard<std::mutex> lock(txn_mutex_); 
        auto it = active_transactions_.find(txn_id);
        if (it != active_transactions_.end()) {
            // It's crucial to mark the in-memory object as aborted.
            it->second->force_abort_internal();
            
            // Remove from the active list.
            active_transactions_.erase(it);
            updateOldestActiveTxnId(); 
            LOG_INFO("  Txn {} removed from active list due to abort. New Oldest Active TxnID: {}", txn_id, oldest_active_txn_id_.load());
        }
    } 
}

std::pair<const FilterCondition*, IndexDefinition> StorageEngine::selectIndexForQuery(
    const std::string& storePath,
    const std::vector<FilterCondition>& filters,
    const std::optional<std::pair<std::string, IndexSortOrder>>& sortBy)
{
    LOG_TRACE("  [SE Query Planner] Starting index selection for store '{}'. Filters: {}, SortBy: {}",
              storePath, filters.size(), sortBy.has_value() ? sortBy->first : "none");

    // 1. Gather all indexes relevant to the specified storePath.
    std::vector<IndexDefinition> relevant_indexes;
    for (const auto& index_def : index_manager_->listIndexes()) {
        if (index_def.storePath == storePath) {
            relevant_indexes.push_back(index_def);
        }
    }

    if (relevant_indexes.empty()) {
        throw storage::StorageError(storage::ErrorCode::SCHEMA_VIOLATION, "Query cannot be executed because no indexes exist for the store path: " + storePath);
    }

    // 2. Define a structure to hold and score potential query plans.
    struct CandidatePlan {
        const FilterCondition* filter;
        IndexDefinition index_def;
        int score = 0;
    };
    std::vector<CandidatePlan> candidate_plans;

    // 3. Evaluate every filter against every relevant index to generate candidate plans.
    for (const auto& filter : filters) {
        for (const auto& index_def : relevant_indexes) {
            // This planner currently only uses simple (single-field) indexes.
            if (index_def.fields.size() != 1 || index_def.fields[0].name != filter.field) {
                continue; // This index does not match the filter's field.
            }

            // --- Explicit Error for Mismatched Index Type ---
            bool op_requires_multikey = (filter.op == FilterOperator::ARRAY_CONTAINS || filter.op == FilterOperator::ARRAY_CONTAINS_ANY);
            
            if (op_requires_multikey && !index_def.is_multikey) {
                // The user is trying an array operator on a standard index. This is a specific schema violation.
                throw storage::StorageError(storage::ErrorCode::SCHEMA_VIOLATION, 
                    "Operator '" + std::string(magic_enum::enum_name(filter.op)) + "' requires a multikey index, but the index '" +
                    index_def.name + "' on field '" + filter.field + "' is not a multikey index.");
            }
            if (!op_requires_multikey && index_def.is_multikey) {
                // The user is trying a standard operator on a multikey index. This is also not supported.
                 throw storage::StorageError(storage::ErrorCode::SCHEMA_VIOLATION, 
                    "Operator '" + std::string(magic_enum::enum_name(filter.op)) + "' cannot be used on the multikey index '" +
                    index_def.name + "'. Use 'arrayContains' or 'arrayContainsAny'.");
            }

            CandidatePlan plan;
            plan.filter = &filter;
            plan.index_def = index_def;
            
            // --- Scoring Logic (now assumes index type is correct) ---
            switch (filter.op) {
                case FilterOperator::EQUAL:
                case FilterOperator::ARRAY_CONTAINS:
                    plan.score = 100;
                    break;
                case FilterOperator::ARRAY_CONTAINS_ANY:
                    plan.score = 90;
                    break;
                case FilterOperator::GREATER_THAN:
                case FilterOperator::GREATER_THAN_OR_EQUAL:
                case FilterOperator::LESS_THAN:
                case FilterOperator::LESS_THAN_OR_EQUAL:
                    plan.score = 50;
                    break;
            }

            // If a valid plan was found, check for a sorting bonus.
            if (plan.score > 0) {
                if (sortBy.has_value() && sortBy->first == filter.field) {
                    if (sortBy->second == index_def.fields[0].order) {
                        plan.score += 20; // Perfect match, no in-memory sort needed.
                    } else {
                        plan.score += 10; // Can use index, but results need reversing.
                    }
                }
                candidate_plans.push_back(plan);
            }
        }
    }

    // 4. Determine the best plan.
    if (candidate_plans.empty()) {
        // Enforce the "No Index, No Query" rule.
        throw storage::StorageError(storage::ErrorCode::SCHEMA_VIOLATION, "Query requires an index on at least one of the filter fields, but none was found for store: " + storePath);
    }

    // Sort candidates by score, descending, to find the best one.
    std::sort(candidate_plans.begin(), candidate_plans.end(), 
              [](const CandidatePlan& a, const CandidatePlan& b) {
        return a.score > b.score;
    });

    const CandidatePlan& best_plan = candidate_plans.front();

  
    LOG_INFO("  [SE Query Planner] Selected Best Plan: Using index '", best_plan.index_def.name,
             "' (Score: ", best_plan.score, ") for filter on field '",
             best_plan.filter->field, "' with operator '",
             magic_enum::enum_name(best_plan.filter->op), "'.");
             

    return {best_plan.filter, best_plan.index_def};
}

std::vector<Record> StorageEngine::query(
    TxnId reader_txn_id,
    const std::string& storePath,
    const std::vector<FilterCondition>& filters,
    const std::optional<std::pair<std::string, IndexSortOrder>>& sortBy,
    const std::optional<engine::columnar::AggregationPlan>& aggPlan,
    size_t limit)
{
    if (!query_router_) {
        LOG_ERROR("Query router not initialized. Cannot execute query.");
        throw storage::StorageError(storage::ErrorCode::STORAGE_NOT_INITIALIZED, "Query router is not available.");
    }

    // --- 1. ROUTING DECISION ---
    // The router analyzes the query shape to determine the optimal execution path.
    engine::columnar::QueryRouter::ExecutionPath path = query_router_->routeQuery(storePath, filters, aggPlan);

    switch (path) {
        // --- ANALYTICAL PATH ---
        case engine::columnar::QueryRouter::ExecutionPath::COLUMNAR_ONLY:
        {
            // Attempt to get or create the columnar store for this path.
            // This will only succeed if a schema has been registered.
            engine::columnar::ColumnarStore* shadow_store = getOrCreateColumnarStore(storePath);
            
            if (shadow_store) {
                LOG_INFO("Query Router: Using COLUMNAR_ONLY path for query on store '{}'.", storePath);
                try {
                    // Delegate the entire query to the columnar engine.
                    return shadow_store->executeQuery(filters, aggPlan, limit);
                } catch (const std::exception& e) {
                    LOG_ERROR("Error during columnar query execution for store '{}': {}. Falling back to LSM path.", storePath, e.what());
                    // On failure, we gracefully fall through to the LSM path.
                }
            } else {
                LOG_WARN("Columnar path chosen, but store for '{}' not found or couldn't be created. Falling back to LSM.", storePath);
                // Fallthrough to LSM_ONLY case.
            }
        }
        
        // --- OLTP / FALLBACK PATH ---
        case engine::columnar::QueryRouter::ExecutionPath::LSM_ONLY:
        default:
        {
            LOG_INFO("Query Router: Using LSM_ONLY (B-Tree/Index) path for query on store '{}'.", storePath);

            // An aggregation query cannot be handled by the LSM path. This indicates a logic error in the router
            // or an unsupported query shape. We must throw an error.
            if (aggPlan) {
                throw storage::StorageError(storage::ErrorCode::NOT_IMPLEMENTED, "Aggregations are only supported on stores with a columnar schema and are routed to the analytical engine.");
            }
            
            // Delegate to the dedicated, proven method for B-Tree/LSM queries.
            return this->queryLsm(reader_txn_id, storePath, filters, sortBy, limit);
        }
    }

    // This line should ideally be unreachable if all cases are handled.
    return {};
}

LSMTree* StorageEngine::getLsmStore(const std::string& store_path) {
    std::shared_lock<std::shared_mutex> lock(lsm_stores_mutex_);
    auto it = lsm_stores_.find(store_path);
    if (it != lsm_stores_.end()) {
        return it->second.get();
    }
    return nullptr;
}

std::vector<Record> StorageEngine::queryLsm(
    TxnId reader_txn_id,
    const std::string& storePath,
    const std::vector<FilterCondition>& filters,
    const std::optional<std::pair<std::string, IndexSortOrder>>& sortBy,
    size_t limit)
{
    // --- Handle edge case of no filters (full collection scan) ---
    if (filters.empty()) {
        LOG_DEBUG("  [SE Query/LSM] No filters provided. Delegating to getPrefix for full store scan.");
        return this->getPrefix(storePath, reader_txn_id, limit);
    }

    try {
        // --- Step 1: Select the best index and identify post-filters ---
        auto [primary_filter, selected_index_def] = selectIndexForQuery(storePath, filters, sortBy);
        
        std::vector<FilterCondition> post_filters;
        for (const auto& filter : filters) {
            if (&filter != primary_filter) {
                post_filters.push_back(filter);
            }
        }
        LOG_INFO("  [SE Query/LSM] Primary filter on field '{}' will use index '{}'. Post-filters to apply: {}.",
                 primary_filter->field, selected_index_def.name, post_filters.size());

        // Get the B-Tree instance for the selected index
        auto btree = index_manager_->getIndexBTree(selected_index_def.name);
        if (!btree) {
            throw storage::StorageError(storage::ErrorCode::INTERNAL_ERROR, "Index '" + selected_index_def.name + "' has definition but no B-Tree instance.");
        }

        // --- Step 2: Prepare B-Tree Scan Parameters ---
        std::set<std::string> candidate_primary_keys;
        
        if (primary_filter->op == FilterOperator::ARRAY_CONTAINS_ANY) {
            const auto& values_to_find = std::get<std::vector<ValueType>>(primary_filter->value);
            LOG_DEBUG("  [SE Query/LSM] Executing ARRAY_CONTAINS_ANY with {} values.", values_to_find.size());
            for (const auto& val : values_to_find) {
                std::string encoded_value = index_manager_->encodeValueToStringOrderPreserving(val);
                auto index_results = btree->rangeScanKeys(encoded_value, encoded_value, true, true);
                merge_and_unique_keys(candidate_primary_keys, index_results);
            }
        } else {
            std::string start_key_part, end_key_part;
            bool start_inclusive = false, end_inclusive = false;

            const ValueType& filter_value = std::get<ValueType>(primary_filter->value);
            std::string encoded_value = index_manager_->encodeValueToStringOrderPreserving(filter_value);

            switch (primary_filter->op) {
                case FilterOperator::EQUAL:
                case FilterOperator::ARRAY_CONTAINS:
                    start_key_part = end_key_part = encoded_value;
                    start_inclusive = end_inclusive = true;
                    break;
                case FilterOperator::GREATER_THAN:
                    start_key_part = encoded_value;
                    start_inclusive = false;
                    break;
                case FilterOperator::GREATER_THAN_OR_EQUAL:
                    start_key_part = encoded_value;
                    start_inclusive = true;
                    break;
                case FilterOperator::LESS_THAN:
                    end_key_part = encoded_value;
                    end_inclusive = false;
                    break;
                case FilterOperator::LESS_THAN_OR_EQUAL:
                    end_key_part = encoded_value;
                    end_inclusive = true;
                    break;
                default:
                    throw storage::StorageError(storage::ErrorCode::INTERNAL_ERROR, "Unsupported primary filter operator in LSM query execution.");
            }

            if (selected_index_def.fields[0].order == IndexSortOrder::DESCENDING) {
                LOG_DEBUG("  [SE Query/LSM] Descending index detected. Transforming logical range to physical scan range.");
                std::string physical_start = end_key_part.empty() ? "" : invert_bytes(end_key_part);
                std::string physical_end = start_key_part.empty() ? "" : invert_bytes(start_key_part);
                start_key_part = physical_start;
                end_key_part = physical_end;
                std::swap(start_inclusive, end_inclusive);
            }
            
            auto index_results = btree->rangeScanKeys(start_key_part, end_key_part, start_inclusive, end_inclusive);
            merge_and_unique_keys(candidate_primary_keys, index_results);
        }

        LOG_INFO("  [SE Query/LSM] Index scan complete. Found {} candidate primary keys.", candidate_primary_keys.size());

        // --- Step 3: Fetch Documents, Post-Filter, and Collect ---
        std::vector<Record> final_results;
        for (const auto& primary_key : candidate_primary_keys) {
            std::optional<Record> doc_opt = this->get(primary_key, reader_txn_id);

            if (doc_opt && evaluate_post_filters(*doc_opt, post_filters, index_manager_.get())) {
                final_results.push_back(*doc_opt);
                if (limit > 0 && final_results.size() >= limit) {
                    break;
                }
            }
        }
        LOG_INFO("  [SE Query/LSM] Post-filtering complete. {} documents remain.", final_results.size());
        
        // --- Step 4: Final Sort (if necessary) ---
        if (sortBy.has_value() && !final_results.empty()) {
            std::stable_sort(final_results.begin(), final_results.end(), 
                [&](const Record& a, const Record& b) {
                auto val_a_opt = index_manager_->extractFieldValue(a, sortBy->first);
                auto val_b_opt = index_manager_->extractFieldValue(b, sortBy->first);

                if (!val_a_opt.has_value()) return false;
                if (!val_b_opt.has_value()) return true;

                if (sortBy->second == IndexSortOrder::ASCENDING) {
                    return *val_a_opt < *val_b_opt;
                } else {
                    return *val_b_opt < *val_a_opt;
                }
            });
            LOG_DEBUG("  [SE Query/LSM] Final results sorted in memory.");
        }
        
        LOG_INFO("[StorageEngine Query/LSM] Finished. Returning {} documents.", final_results.size());
        return final_results;

    } catch (const storage::StorageError& e) {
        LOG_ERROR("[StorageEngine Query/LSM] Txn {}: Caught storage::StorageError: {}", reader_txn_id, e.toDetailedString());
        throw;
    } catch (const std::exception& e) {
        LOG_ERROR("[StorageEngine Query/LSM] Txn {}: Caught std::exception: {}", reader_txn_id, e.what());
        throw storage::StorageError(storage::ErrorCode::INTERNAL_ERROR, "LSM query execution failed: " + std::string(e.what()));
    }
}

// performRecovery()
// --- REFACTORED RECOVERY IMPLEMENTATION ---

void StorageEngine::performRecovery() {
    LOG_INFO("[StorageEngine] Starting application-level WAL recovery...");
    if (!wal_manager_ || !wal_manager_->isHealthy()) {
        throw std::runtime_error("WAL system unhealthy, cannot perform recovery.");
    }

    std::vector<LogRecord> all_wal_records;
    wal_manager_->recoverAllShards(all_wal_records);
    
    LSN recovery_start_lsn = 1;
    if (last_successful_checkpoint_metadata_.checkpoint_id > 0 && 
        last_successful_checkpoint_metadata_.checkpoint_begin_lsn > 0) {
        recovery_start_lsn = last_successful_checkpoint_metadata_.checkpoint_begin_lsn;
    }

    std::vector<LogRecord> records_to_replay;
    std::copy_if(all_wal_records.begin(), all_wal_records.end(), std::back_inserter(records_to_replay),
        [recovery_start_lsn](const LogRecord& rec) {
            return rec.lsn >= recovery_start_lsn;
        });

    LOG_INFO("[Recovery] Processing {} log records from LSN {} onwards.", records_to_replay.size(), recovery_start_lsn);

    if (records_to_replay.empty()) {
        // No new records since last checkpoint, just finalize state.
        TxnId max_txn_from_checkpoint = 0; 
        if (last_successful_checkpoint_metadata_.checkpoint_id > 0) {
            for(TxnId tid : last_successful_checkpoint_metadata_.active_txns_at_checkpoint_begin) {
                max_txn_from_checkpoint = std::max(max_txn_from_checkpoint, tid);
            }
        }
        finalizeRecovery(max_txn_from_checkpoint);
        return;
    }

    // Phase 1: Analysis
    std::set<TxnId> committed_txns;
    std::set<TxnId> aborted_txns;
    std::map<TxnId, std::vector<LogRecord>> pending_data_ops;
    TxnId max_txn_id_seen_in_wal = analysisPass(records_to_replay, committed_txns, aborted_txns, pending_data_ops);

    // Phase 2: Redo
    redoPass(committed_txns, pending_data_ops);

    // Phase 3: Finalization
    TxnId max_txn_from_checkpoint = 0;
    if (last_successful_checkpoint_metadata_.checkpoint_id > 0) {
        for(TxnId tid : last_successful_checkpoint_metadata_.active_txns_at_checkpoint_begin) {
            max_txn_from_checkpoint = std::max(max_txn_from_checkpoint, tid);
        }
    }
    finalizeRecovery(std::max(max_txn_id_seen_in_wal, max_txn_from_checkpoint));
    
    LOG_INFO("[StorageEngine] Application-level WAL recovery finished.");
}

TxnId StorageEngine::analysisPass(const std::vector<LogRecord>& records_to_replay,
                                  std::set<TxnId>& committed_txns,
                                  std::set<TxnId>& aborted_txns,
                                  std::map<TxnId, std::vector<LogRecord>>& pending_data_ops) {
    LOG_INFO("[Recovery Analysis Pass] Analyzing {} records...", records_to_replay.size());
    TxnId max_txn_id_seen = 0;

    // --- Iterate over `records_to_replay` instead of the undefined `relevant_records` ---
    for (const auto& record : records_to_replay) {
        max_txn_id_seen = std::max(max_txn_id_seen, record.txn_id);

        switch (record.type) {
            case LogRecordType::COMMIT_TXN:
                committed_txns.insert(record.txn_id);
                aborted_txns.erase(record.txn_id);
                break;
            case LogRecordType::ABORT_TXN:
                aborted_txns.insert(record.txn_id);
                committed_txns.erase(record.txn_id);
                break;
            case LogRecordType::DATA_PUT:
            case LogRecordType::DATA_DELETE:
                pending_data_ops[record.txn_id].push_back(record);
                break;
            default:
                // Ignore other record types like BEGIN_TXN, CHECKPOINT, etc. in this pass.
                break;
        }
    }
    LOG_INFO("[Recovery Analysis Pass] Found {} committed and {} aborted transactions in replay span.",
             committed_txns.size(), aborted_txns.size());
    return max_txn_id_seen;
}

std::optional<StorageEngine::MemTableTunerStats> StorageEngine::getMemTableTunerStatsForStore(const std::string& store_path) {
    LSMTree* store = getLsmStore(store_path);
    if (!store) {
        return std::nullopt;
    }

    engine::lsm::MemTableTuner* tuner = store->getTuner();
    if (tuner) {
        return MemTableTunerStats{
            tuner->GetTargetMemTableSize(),
            tuner->getSmoothedWriteRateBps()
        };
    }

    return std::nullopt;
}

std::optional<StorageEngine::WriteBufferManagerStats> StorageEngine::getWriteBufferManagerStats() const {
    if (!write_buffer_manager_) {
        return std::nullopt;
    }
    return WriteBufferManagerStats{
        write_buffer_manager_->GetMemoryUsage(),
        write_buffer_manager_->GetBufferSize(),
        write_buffer_manager_->getImmutableMemtablesCount()
    };
}

void StorageEngine::redoPass(const std::set<TxnId>& committed_txns,
                             const std::map<TxnId, std::vector<LogRecord>>& pending_data_ops)
{
    LOG_INFO("[Recovery Redo Pass] Replaying {} committed transactions...", committed_txns.size());

    // It's important to replay transactions in their commit order to ensure correctness.
    // Iterating through the std::set of committed_txns provides a sorted (ascending TxnID)
    // order, which is a reliable proxy for commit order.
    for (TxnId txn_id_to_replay : committed_txns) {
        auto it = pending_data_ops.find(txn_id_to_replay);
        if (it == pending_data_ops.end()) {
            // This is a valid case: a transaction might have committed with no data operations.
            LOG_TRACE("  [Redo Txn {}] No data operations found to replay.", txn_id_to_replay);
            continue;
        }

        const auto& data_operations = it->second;
        LOG_INFO("  [Redo Txn {}] Replaying {} data operation(s).", txn_id_to_replay, data_operations.size());

        for (const auto& data_op_record : data_operations) {
            try {
                // 1. Determine the target LSM-Tree store for this operation.
                std::string store_path = getStorePathFromKey(data_op_record.key);
                LSMTree* store = getOrCreateLsmStore(store_path);
                if (!store) {
                    LOG_ERROR("    [Redo LSN {}] Could not get or create LSM store for path '{}'. Skipping this record.",
                              data_op_record.lsn, store_path);
                    continue;
                }

                // 2. Reconstruct the full Record object from the WAL log entry.
                Record record_to_replay;
                record_to_replay.key = data_op_record.key;
                record_to_replay.txn_id = data_op_record.txn_id;
                record_to_replay.commit_txn_id = data_op_record.txn_id; // During recovery, commit_id = txn_id
                record_to_replay.deleted = (data_op_record.type == LogRecordType::DATA_DELETE);
                record_to_replay.timestamp = std::chrono::system_clock::now(); // Timestamp is not critical for recovery correctness

                if (!record_to_replay.deleted) {
                    auto value_opt = LogRecord::deserializeValueTypeBinary(data_op_record.value);
                    if (!value_opt) {
                        LOG_ERROR("    [Redo LSN {}] Failed to deserialize value for PUT operation on key '{}'. Skipping record.",
                                  data_op_record.lsn, data_op_record.key);
                        continue;
                    }
                    record_to_replay.value = *value_opt;
                }
                
                // 3. Get the state of the record *before* this redo operation for indexing.
                // We use a "read from the future" transaction ID to ensure we see the absolute latest
                // committed state currently in the LSM-Tree from previous recovery steps.
                std::optional<Record> state_before_redo = store->get(data_op_record.key, std::numeric_limits<TxnId>::max());

                // 4. Apply the change to the LSM-Tree's memory layer.
                // The `oldest_active_txn_id` is 0 during recovery, as there are no active transactions.
                store->put(data_op_record.key, record_to_replay, 0);

                // 5. Apply the corresponding change to all relevant secondary indexes.
                index_manager_->updateIndices(
                    data_op_record.key,
                    state_before_redo,
                    record_to_replay.deleted ? std::nullopt : std::optional<Record>(record_to_replay),
                    data_op_record.txn_id
                );

                // Note: We do not need to shadow writes to the columnar store during recovery.
                // The columnar store will be rebuilt or reconciled from the primary LSM-Tree
                // in a separate process if necessary.

            } catch (const std::exception& e_redo) {
                // Log the error but continue recovery with the next record. A single failed
                // record replay should not halt the entire database startup.
                LOG_ERROR("    [Redo LSN {}] CRITICAL: Exception during replay for key '{}': {}. State for this key may be inconsistent.",
                          data_op_record.lsn, data_op_record.key, e_redo.what());
            }
        }
    }
}

void StorageEngine::finalizeRecovery(TxnId max_txn_id_seen) {
    // Set the next transaction ID to one greater than the highest ID encountered
    // during recovery (from either the WAL or the last checkpoint).
    if (max_txn_id_seen >= next_txn_id_.load()) {
        next_txn_id_.store(max_txn_id_seen + 1);
    }

    // After recovery, there are no active transactions, so the oldest active ID
    // is simply the next one to be assigned.
    oldest_active_txn_id_.store(next_txn_id_.load());

    LOG_INFO("[Recovery Finalize] State finalized. Next TxnID: {}, Oldest Active TxnID: {}",
             next_txn_id_.load(), oldest_active_txn_id_.load());
}

// tryPerformCheckpoint()
// --- REFACTORED CHECKPOINT IMPLEMENTATION ---

bool StorageEngine::tryPerformCheckpoint() {
    bool expected_in_progress = false;
    if (!checkpoint_in_progress_.compare_exchange_strong(expected_in_progress, true)) {
        LOG_INFO("[Checkpoint] Already in progress, skipping this attempt.");
        return false;
    }

    // Use a guard to ensure `checkpoint_in_progress_` is reset to false on any exit path.
    auto guard = std::shared_ptr<void>(nullptr, [this](void*){ 
        checkpoint_in_progress_.store(false, std::memory_order_release);
    });

    uint64_t checkpoint_id = 0;
    CheckpointInfo cp_info_for_history;
    cp_info_for_history.start_time = std::chrono::system_clock::now();
    
    try {
        // Phase 1: Begin the checkpoint
        std::set<TxnId> active_txns;
        LSN begin_lsn = beginCheckpoint(checkpoint_id, active_txns);
        
        cp_info_for_history.checkpoint_id = checkpoint_id;
        cp_info_for_history.status = CheckpointStatus::IN_PROGRESS;
        cp_info_for_history.checkpoint_begin_wal_lsn = begin_lsn;
        cp_info_for_history.active_txns_at_begin = active_txns;

        // Phase 2: Flush memory
        flushMemoryToDisk();

        // Phase 3: Finish the checkpoint
        finishCheckpoint(checkpoint_id, begin_lsn, active_txns);
        
        cp_info_for_history.status = CheckpointStatus::COMPLETED;
        LOG_INFO("[Checkpoint] --- Checkpoint (ID {}) COMPLETED Successfully ---", checkpoint_id);

    } catch (const std::exception& e) {
        LOG_ERROR("[Checkpoint] Exception during checkpoint (ID {}): {}", checkpoint_id, e.what());
        cp_info_for_history.error_msg = e.what();
        cp_info_for_history.status = CheckpointStatus::FAILED;
        // The guard will reset `checkpoint_in_progress_`
        return false;
    }

    cp_info_for_history.end_time = std::chrono::system_clock::now();
    { 
        std::lock_guard<std::mutex> hist_lock(checkpoint_history_mutex_);
        checkpoint_history_.push_back(cp_info_for_history);
        if (checkpoint_history_.size() > MAX_CHECKPOINT_HISTORY) {
            checkpoint_history_.pop_front();
        }
    }
    
    return true;
}

LSN StorageEngine::beginCheckpoint(uint64_t& checkpoint_id, std::set<TxnId>& active_txns_at_begin) {
    checkpoint_id = next_checkpoint_id_.load(std::memory_order_relaxed);
    LOG_INFO("[Checkpoint ID {}] Starting Phase 1: Begin.", checkpoint_id);

    active_txns_at_begin = getActiveTransactionIdsForCheckpoint();

    LogRecord cp_begin_log;
    cp_begin_log.type = LogRecordType::CHECKPOINT_BEGIN;
    cp_begin_log.txn_id = 0; // System record
    cp_begin_log.key = "CP_BEGIN_ID_" + std::to_string(checkpoint_id);
    cp_begin_log.value = serializeActiveTxnSet(active_txns_at_begin);

    // --- START OF FIX ---
    // Use the new, single-argument, synchronous WAL API.
    LSN begin_lsn = wal_manager_->appendAndForceFlushLogRecord(cp_begin_log);
    // --- END OF FIX ---
    
    if (begin_lsn == 0) {
        throw std::runtime_error("Failed to write/flush CHECKPOINT_BEGIN log record.");
    }
    LOG_INFO("[Checkpoint ID {}] CHECKPOINT_BEGIN record durable at LSN {}.", checkpoint_id, begin_lsn);
    return begin_lsn;
}

void StorageEngine::flushMemoryToDisk() {
    LOG_INFO("[Checkpoint] Starting Phase 2: Flush Memory To Disk.");

    // 1. Flush all LSM-Tree Memtables
    // We must iterate through all existing LSM-Tree instances and flush each one.
    {
        std::shared_lock<std::shared_mutex> lock(lsm_stores_mutex_); 
        if (!lsm_stores_.empty()) {
            LOG_INFO("  - Flushing memtables for {} LSM store(s)...", lsm_stores_.size());
            for (const auto& [store_path, lsm_store_ptr] : lsm_stores_) {
                if (lsm_store_ptr) {
                    LOG_TRACE("    - Flushing active memtable for store '{}'...", store_path);
                    // The 'true' argument ensures this is a synchronous, blocking flush.
                    lsm_store_ptr->flushActiveMemTable(true);
                }
            }
        }
    }

    // 2. Flush the Buffer Pool Manager
    // This is a critical step that writes all modified B+Tree pages (from the
    // IndexManager) and any other dirty pages to the main database file.
    if (bpm_) {
        LOG_INFO("  - Flushing all dirty pages from the Buffer Pool Manager...");
        if (!bpm_->flushAllPages()) {
            // This is a warning, not a fatal error for the checkpoint.
            // A failed page flush might be retried later, but it means the checkpoint
            // might not represent the absolute latest state for that specific page.
            // The WAL will ensure correctness upon recovery regardless.
            LOG_WARN("  - bpm_->flushAllPages() reported one or more errors. Checkpoint will continue.");
        }
    }

    // 3. (Future Enhancement) Flush all Columnar Store Buffers
    // Similar to the LSM-Tree loop, we would iterate through all columnar_stores_
    // and call a synchronous flush method on each.
    // {
    //     std::lock_guard<std::mutex> lock(columnar_stores_mutex_);
    //     if (!columnar_stores_.empty()) {
    //         LOG_INFO("  - Flushing write buffers for {} columnar store(s)...", columnar_stores_.size());
    //         for (const auto& [store_path, columnar_store_ptr] : columnar_stores_) {
    //             if (columnar_store_ptr) {
    //                 columnar_store_ptr->forceFlush(true); // Assuming a synchronous flush method
    //             }
    //         }
    //     }
    // }

    LOG_INFO("[Checkpoint] Memory flush phase complete.");
}

void StorageEngine::finishCheckpoint(uint64_t checkpoint_id, LSN begin_lsn, const std::set<TxnId>& active_txns_at_begin) {
    LOG_INFO("[Checkpoint ID {}] Starting Phase 3: Finish.", checkpoint_id);
    
    LogRecord cp_end_log;
    cp_end_log.type = LogRecordType::CHECKPOINT_END;
    cp_end_log.txn_id = 0; // System record
    cp_end_log.key = "CP_END_ID_" + std::to_string(checkpoint_id);
    cp_end_log.value = std::to_string(begin_lsn);

    // --- START OF FIX ---
    // Use the new, single-argument, synchronous WAL API.
    LSN end_lsn = wal_manager_->appendAndForceFlushLogRecord(cp_end_log);
    // --- END OF FIX ---

    if (end_lsn == 0) {
        throw std::runtime_error("Failed to write/flush CHECKPOINT_END log record.");
    }
    LOG_INFO("[Checkpoint ID {}] CHECKPOINT_END record durable at LSN {}.", checkpoint_id, end_lsn);

    // Create and persist the metadata for this successful checkpoint.
    CheckpointMetadata new_cp_meta;
    new_cp_meta.checkpoint_id = checkpoint_id;
    new_cp_meta.checkpoint_begin_lsn = begin_lsn;
    new_cp_meta.timestamp = std::chrono::system_clock::now(); 
    new_cp_meta.active_txns_at_checkpoint_begin = active_txns_at_begin;

    persistCheckpointMetadata(new_cp_meta);
    
    // Update the in-memory copy of the last successful checkpoint.
    { 
        std::lock_guard<std::mutex> hist_lock(checkpoint_history_mutex_); 
        last_successful_checkpoint_metadata_ = new_cp_meta; 
    }
    
    // Atomically increment for the next checkpoint.
    next_checkpoint_id_.fetch_add(1, std::memory_order_relaxed);
    
    // Trigger WAL truncation.
    if (wal_manager_) {
        LOG_INFO("[Checkpoint ID {}] Requesting WAL truncation for LSNs before {}.", checkpoint_id, begin_lsn);
        if (!wal_manager_->truncateSegmentsBeforeLSN(begin_lsn)) {
            LOG_WARN("[Checkpoint ID {}] WAL truncation process reported issues.", checkpoint_id);
        }
    }
}

std::vector<CheckpointInfo> StorageEngine::getCheckpointHistory() const { // <<< ADDED const
    std::lock_guard<std::mutex> lock(checkpoint_history_mutex_); // Works because checkpoint_history_mutex_ is mutable
    // Create a copy to return
    return std::vector<CheckpointInfo>(checkpoint_history_.begin(), checkpoint_history_.end());
}


bool StorageEngine::forceCheckpoint() {
    LOG_INFO("[StorageEngine] Manual checkpoint requested.");
    {
        std::lock_guard<std::mutex> lock(checkpoint_control_mutex_);
        force_checkpoint_request_.store(true);
    }
    checkpoint_cv_.notify_one(); // Wake up the checkpoint thread
    // Optionally, wait here for the checkpoint to complete if forceCheckpoint should be synchronous
    // For now, it's asynchronous (just signals the thread)
    return true; // Indicates request was made
}

std::vector<Record> StorageEngine::getPrefix(
    const std::string& prefix,
    TxnId txn_id,
    size_t limit,
    const std::string& start_key_exclusive)
{
    // The store path is derived from the prefix itself.
    std::string store_path = getStorePathFromKey(prefix);
    
    // Retrieve the specific LSM-Tree instance for this store.
    LSMTree* store = getOrCreateLsmStore(store_path);
    
    if (!store) {
        // If the store couldn't be created or found, no records can match the prefix.
        LOG_WARN("[StorageEngine::getPrefix] Could not get or create LSM store for path '{}'. Returning empty result.", store_path);
        return {};
    }
    
    // Delegate the entire prefix scan operation to the specialized LSM-Tree instance.
    return store->getPrefix(prefix, txn_id, limit, start_key_exclusive);
}


TxnId StorageEngine::getOldestActiveTxnId() const {
    return oldest_active_txn_id_.load(std::memory_order_relaxed);
}

std::set<TxnId> StorageEngine::getActiveTransactionIdsForCheckpoint() {
    std::lock_guard<std::mutex> lock(txn_mutex_); // <<< LOCK ADDED
    std::set<TxnId> active_ids;
    LOG_TRACE("[Checkpoint] Getting active transaction IDs. Current active_transactions_ size: {}", active_transactions_.size());
    for (const auto& pair : active_transactions_) {
        active_ids.insert(pair.first);
        LOG_TRACE("  [Checkpoint] Active TxnID: {}", pair.first);
    }
    LOG_INFO("[Checkpoint] Found {} active transaction IDs.", active_ids.size());
    return active_ids;
}

void StorageEngine::persistCheckpointMetadata(const CheckpointMetadata& meta) {
    // Simple: Overwrite the single metadata file. Robust systems might use temp + rename.
    std::ofstream meta_file(checkpoint_metadata_filepath_, std::ios::binary | std::ios::trunc);
    if (meta_file.is_open()) {
        meta.serialize(meta_file);
        meta_file.close();
        if (meta_file.good()) {
             LOG_INFO("[StorageEngine] Persisted checkpoint metadata: {}", meta.toString());
        } else {
             LOG_ERROR("[StorageEngine] Failed to write checkpoint metadata to {}.", checkpoint_metadata_filepath_);
        }
    } else {
        LOG_ERROR("[StorageEngine] Could not open checkpoint metadata file {} for writing.", checkpoint_metadata_filepath_);
    }
}

void StorageEngine::loadLastCheckpointMetadata() {
    std::lock_guard<std::mutex> lock(checkpoint_history_mutex_); // <<< LOCK ADDED
    // ... (rest of existing loadLastCheckpointMetadata logic) ...
    // This now safely modifies last_successful_checkpoint_metadata_ and next_checkpoint_id_
    std::ifstream meta_file(checkpoint_metadata_filepath_, std::ios::binary);
    if (meta_file.is_open()) {
        if (last_successful_checkpoint_metadata_.deserialize(meta_file)) {
            LOG_INFO("[StorageEngine] Successfully loaded last checkpoint metadata: {}", last_successful_checkpoint_metadata_.toString());
            next_checkpoint_id_.store(last_successful_checkpoint_metadata_.checkpoint_id + 1, std::memory_order_relaxed);
        } else {
            LOG_WARN("[StorageEngine] Could not deserialize checkpoint metadata from {}. Starting fresh.", checkpoint_metadata_filepath_);
            last_successful_checkpoint_metadata_ = {}; // Reset to default
            next_checkpoint_id_.store(1, std::memory_order_relaxed); // Reset next ID
        }
        meta_file.close();
    } else {
        LOG_INFO("[StorageEngine] No checkpoint metadata file found at {}. Assuming first run.", checkpoint_metadata_filepath_);
        last_successful_checkpoint_metadata_ = {}; // Reset to default
        next_checkpoint_id_.store(1, std::memory_order_relaxed);
    }
}

void StorageEngine::checkpointThreadLoop() {
    LOG_INFO("[CheckpointThread] Started. Interval: {}s", checkpoint_interval_.count());
    while (!shutdown_initiated_.load()) {
        bool perform_now = false;
        {
            std::unique_lock<std::mutex> lock(checkpoint_control_mutex_);
            if (checkpoint_cv_.wait_for(lock, checkpoint_interval_, [this] {
                return shutdown_initiated_.load() || force_checkpoint_request_.load();
            })) {
                // Woken up by shutdown or force
                if (shutdown_initiated_.load()) break;
                if (force_checkpoint_request_.load()) {
                    perform_now = true;
                    force_checkpoint_request_.store(false); // Reset the flag
                }
            } else {
                // Timed out (interval elapsed)
                perform_now = true;
            }
        } // lock released

        if (shutdown_initiated_.load()) break;

        if (perform_now) {
            LOG_TRACE("[CheckpointThread] Interval elapsed or forced. Attempting checkpoint.");
            try {
                tryPerformCheckpoint();
            } catch (const std::exception& e) {
                LOG_ERROR("[CheckpointThread] Exception during scheduled checkpoint: {}", e.what());
                // std::this_thread::sleep_for(std::chrono::seconds(10)); // Optional back-off
            }
        }
    }
    LOG_INFO("[CheckpointThread] Stopped.");
}


void StorageEngine::updateOldestActiveTxnId() {
    // This method MUST be called when txn_mutex_ is already HELD by the caller.
    // Or, it needs to acquire it itself, but that would require recursive mutex if called from begin/commit/abort.
    // For simplicity and safety, assume caller holds txn_mutex_.
    // If txn_mutex_ were a shared_mutex, this would take a shared_lock if called independently.

    TxnId min_id = std::numeric_limits<TxnId>::max();
    bool any_active = false;
    for (const auto& pair : active_transactions_) { // Safe: txn_mutex_ held by caller
        min_id = std::min(min_id, pair.first);
        any_active = true;
    }

    TxnId value_to_store = any_active ? min_id : next_txn_id_.load(std::memory_order_relaxed);
    oldest_active_txn_id_.store(value_to_store, std::memory_order_relaxed);

    LOG_TRACE("  Updated oldest_active_txn_id_ = {}", value_to_store);
}

PaginatedQueryResult<Record> StorageEngine::paginatedQuery(
    TxnId reader_txn_id,
    const std::string& storePath,
    const std::vector<FilterCondition>& filters,
    const std::vector<OrderByClause>& orderBy,
    size_t limit,
    const std::optional<std::vector<ValueType>>& startCursor,
    bool startExclusive,
    const std::optional<std::vector<ValueType>>& endCursor,
    bool endExclusive)
{
    LOG_INFO("--- [StorageEngine::paginatedQuery START] ---");
    LOG_INFO("  - Store: '{}', Limit: {}, ReaderTxnID: {}", storePath, limit, reader_txn_id);

    PaginatedQueryResult<Record> result;
    if (limit == 0) return result;

    // --- Step 1: Index Selection (Unchanged) ---
    if (orderBy.empty()) {
        throw storage::StorageError(storage::ErrorCode::INVALID_CONFIGURATION, "Paginated query requires at least one .orderBy() clause.");
    }
    const auto& primary_order_clause = orderBy[0];
    auto indexes = index_manager_->listIndexes(storePath);
    std::optional<IndexDefinition> selected_index;
    for (const auto& idx : indexes) {
        if (!idx.fields.empty() && idx.fields.size() == 1 &&
            idx.fields[0].name == primary_order_clause.field) {
            selected_index = idx;
            break;
        }
    }
    if (!selected_index) {
        throw storage::StorageError(storage::ErrorCode::SCHEMA_VIOLATION, "Paginated query requires a B-Tree index on the first orderBy field ('" + primary_order_clause.field + "').");
    }
    auto btree = index_manager_->getIndexBTree(selected_index->name);
    if (!btree) {
        throw storage::StorageError(storage::ErrorCode::INTERNAL_ERROR, "Index '" + selected_index->name + "' has definition but no B-Tree instance.");
    }
    LOG_INFO("  - Query Planner: Selected index '{}' for primary sort.", selected_index->name);

    // --- Step 2 & 3: Fetch ALL Candidate Docs and De-duplicate (Unchanged) ---
    std::vector<std::pair<std::string, std::string>> btree_candidates = btree->paginatedScan(orderBy, startCursor, startExclusive, endCursor, endExclusive, reader_txn_id);
    std::map<std::string, Record> latest_visible_docs;
    for (const auto& [composite_key, primary_key] : btree_candidates) {
        if (latest_visible_docs.find(primary_key) == latest_visible_docs.end()) {
            auto record_opt = this->get(primary_key, reader_txn_id);
            if (record_opt && !record_opt->deleted) {
                latest_visible_docs[primary_key] = *record_opt;
            }
        }
    }
    std::vector<Record> records_to_sort;
    for (auto const& [pk, record] : latest_visible_docs) {
        records_to_sort.push_back(record);
    }
    LOG_INFO("  - Fetched {} unique, visible documents for in-memory processing.", records_to_sort.size());

    // --- Step 4: Perform In-Memory Compound Sort (Unchanged) ---
    std::stable_sort(records_to_sort.begin(), records_to_sort.end(),
        [&](const Record& a, const Record& b) {
            for (const auto& clause : orderBy) {
                auto val_a_opt = index_manager_->extractFieldValue(a, clause.field);
                auto val_b_opt = index_manager_->extractFieldValue(b, clause.field);
                ValueType val_a = val_a_opt.has_value() ? *val_a_opt : std::monostate{};
                ValueType val_b = val_b_opt.has_value() ? *val_b_opt : std::monostate{};
                if (val_a != val_b) {
                    return (clause.direction == IndexSortOrder::ASCENDING) ? (val_a < val_b) : (val_b < val_a);
                }
            }
            return false;
        }
    );
    LOG_INFO("  - In-memory compound sort complete.");

    // --- Step 5: Find Cursor Position and Slice Results (THE FIX) ---
    auto start_iterator = records_to_sort.begin();
    if (startCursor) {
        // Find the first element that is strictly greater than the cursor document.
        start_iterator = std::upper_bound(records_to_sort.begin(), records_to_sort.end(), *startCursor,
            [&](const std::vector<ValueType>& cursor_vals, const Record& record) {
                // This custom comparator is the heart of the fix.
                // It compares a record against the cursor values.
                for (size_t i = 0; i < orderBy.size(); ++i) {
                    auto field_val_opt = index_manager_->extractFieldValue(record, orderBy[i].field);
                    ValueType field_val = field_val_opt.has_value() ? *field_val_opt : std::monostate{};
                    const ValueType& cursor_val = cursor_vals[i];

                    if (field_val != cursor_val) {
                        return (orderBy[i].direction == IndexSortOrder::ASCENDING) ? (cursor_val < field_val) : (field_val < cursor_val);
                    }
                }
                return false; // They are identical
            });
    }

    // --- Step 6: Apply Limit, Set Pagination State, and Generate Cursors ---
    std::vector<Record> final_docs;
    for (auto it = start_iterator; it != records_to_sort.end() && final_docs.size() < (limit + 1); ++it) {
        final_docs.push_back(*it);
    }
    
    result.hasPrevPage = startCursor.has_value();
    result.hasNextPage = final_docs.size() > limit;
    
    if (result.hasNextPage) {
        final_docs.pop_back();
    }
    
    result.docs = std::move(final_docs);

    if (!result.docs.empty()) {
        result.startCursor = extractCursorValues(result.docs.front(), orderBy);
        result.endCursor = extractCursorValues(result.docs.back(), orderBy);
    }
    
    LOG_INFO("--- [StorageEngine::paginatedQuery END] --- Returning {} docs.", result.docs.size());
    return result;
}

void StorageEngine::commitBatch(const std::vector<BatchOperation>& operations) {
    if (operations.empty()) {
        return;
    }

    std::shared_ptr<Transaction> txn = beginTransaction();
    try {
        for (const auto& op : operations) {
            switch (op.type) {
                case BatchOperationType::MAKE: {
                    if (!op.value) throw std::invalid_argument("MAKE operation requires a value.");
                    // The value is a full JSON string, which is a valid ValueType.
                    txn->put(op.key, *op.value, op.overwrite);
                    break;
                }
                
                case BatchOperationType::MODIFY: {
                    if (!op.value) throw std::invalid_argument("MODIFY operation requires a value.");
                    
                    // Fetch the current state of the document within the transaction.
                    std::optional<Record> current_record_opt = txn->get(op.key);
                    if (!current_record_opt) {
                        throw storage::StorageError(storage::ErrorCode::KEY_NOT_FOUND,
                            "Cannot modify document that does not exist: " + op.key);
                    }
                    if (!std::holds_alternative<std::string>(current_record_opt->value)) {
                        throw storage::StorageError(storage::ErrorCode::SCHEMA_VIOLATION,
                            "Cannot modify non-JSON document: " + op.key);
                    }
                    
                    json doc_json = json::parse(std::get<std::string>(current_record_opt->value));
                    json patch_json = json::parse(*op.value);

                    // *** THIS IS THE CRITICAL FIX LOGIC ***
                    // Iterate through the patch. If we find an atomic op sentinel, handle it.
                    // Otherwise, it's a simple merge field.
                    json simple_merge_patch = json::object();

                    for (auto& [field, patch_value] : patch_json.items()) {
                        if (patch_value.is_object() && patch_value.contains("$$indinis_op")) {
                            std::string op_type = patch_value["$$indinis_op"];
                            if (op_type == "INCREMENT") {
                                if (!doc_json.contains(field) || !doc_json[field].is_number()) {
                                    throw storage::StorageError(storage::ErrorCode::SCHEMA_VIOLATION,
                                        "Cannot increment field '" + field + "' on key '" + op.key + "': field does not exist or is not a number.");
                                }
                                double current_val = doc_json[field].get<double>();
                                double increment_val = patch_value["value"].get<double>();
                                doc_json[field] = current_val + increment_val;
                            }
                            // Add else-if for other atomic ops here in the future
                        } else {
                            // This is a simple field to merge. Add it to our simple_merge_patch.
                            simple_merge_patch[field] = patch_value;
                        }
                    }

                    // Apply the simple merge patch after handling atomics.
                    if (!simple_merge_patch.empty()) {
                        doc_json.merge_patch(simple_merge_patch);
                    }
                    
                    // Put the final, modified document back into the transaction's writeset.
                    txn->put(op.key, doc_json.dump(), true);
                    break;
                }

                case BatchOperationType::REMOVE: {
                    txn->remove(op.key);
                    break;
                }
            }
        }
        commitTransaction(txn->getId());
    } catch (...) {
        abortTransaction(txn->getId());
        throw;
    }
}