// @include/index_manager.h
#pragma once // Keep this at the very top
#include "../include/encoding_utils.h"
// --- Standard Library Includes ---
#include <string>
#include <vector>
#include <memory>       // For std::shared_ptr
#include <optional>     // For std::optional
#include <shared_mutex> // For std::shared_mutex, std::unique_lock, std::shared_lock
#include <map>          // For std::map
#include <filesystem>   // For std::filesystem
#include <nlohmann/json.hpp> // External JSON library
#include <shared_mutex> 
#include <thread>
#include <queue>
#include <functional>
#include <condition_variable>
#include <atomic>
#include <set>

// --- Project-Specific Type Definitions ---
// CRITICAL: Include the file defining basic types like TxnId, Record, ValueType, IndexDefinition FIRST
#include "types.h" // Or "indinis_types.h" if that's the actual filename

// --- Project Dependencies (Full class definitions needed) ---
// Need the full definition for bpm_ member and BTree members/return types
#include "buffer_pool_manager.h" // Defines SimpleBufferPoolManager
#include "btree.h"               // Defines BTree

// --- Forward Declarations ---
// If IndexManager only holds a reference/pointer to StorageEngine in the header,
// a forward declaration is sufficient here. The full include will be in index_manager.cpp.
class StorageEngine;

// Platform-specific includes and byte swap functions
#ifdef _WIN32
    // Windows specific includes and definitions
    #ifndef WIN32_LEAN_AND_MEAN
        #define WIN32_LEAN_AND_MEAN
    #endif
    #include <winsock2.h> // For byte swap functions like ntohl, htonl
    // Define htobe64 and be64toh manually or using Windows functions if available
    // Manual implementation is safer for portability across Windows versions/SDKs
    inline uint64_t htobe64_manual(uint64_t host_64) {
        // Check endianness at runtime (less ideal) or assume common Windows (little-endian)
        // Assuming little-endian for simplicity here. If big-endian Windows exists, this needs adjustment.
        #if BYTE_ORDER == LITTLE_ENDIAN // This check might require compiler-specific defines or runtime check
            uint64_t ret;
            char *src = reinterpret_cast<char*>(&host_64);
            char *dst = reinterpret_cast<char*>(&ret);
            dst[0] = src[7];
            dst[1] = src[6];
            dst[2] = src[5];
            dst[3] = src[4];
            dst[4] = src[3];
            dst[5] = src[2];
            dst[6] = src[1];
            dst[7] = src[0];
            return ret;
        #else // Assume big-endian or add specific handling
             return host_64;
        #endif
    }
     inline uint64_t be64toh_manual(uint64_t big_endian_64) {
        // Assuming little-endian host
        #if BYTE_ORDER == LITTLE_ENDIAN
             uint64_t ret;
             char *src = reinterpret_cast<char*>(&big_endian_64);
             char *dst = reinterpret_cast<char*>(&ret);
             dst[0] = src[7];
             dst[1] = src[6];
             dst[2] = src[5];
             dst[3] = src[4];
             dst[4] = src[3];
             dst[5] = src[2];
             dst[6] = src[1];
             dst[7] = src[0];
             return ret;
        #else
             return big_endian_64;
        #endif
     }
     // Use the manual versions
     #define htobe64 htobe64_manual
     #define be64toh be64toh_manual

#else
    // POSIX specific includes (Linux, macOS)
    #include <arpa/inet.h> // For htonl, ntohl
    #include <endian.h>    // For htobe64, be64toh (more standard than arpa/inet.h for 64-bit)
                           // Note: endian.h might also not be universal, but better than arpa/inet.h for 64bit
                           // If endian.h is not found on some POSIX, htobe64/be64toh might need manual impl there too.
#endif

namespace fs = std::filesystem;
using json = nlohmann::json;

enum class CreateIndexResult {
    CREATED_AND_BACKFILLING,
    ALREADY_EXISTS,
    FAILED
};

namespace {
    const char COMPOSITE_KEY_SEPARATOR = '\0';
    const std::string INDEX_TOMBSTONE = ""; // Empty string represents tombstone in BTree value
    std::string encodeTxnIdDescending(TxnId txn_id) {
        uint64_t be_val = htobe64(txn_id);
        uint64_t inverted_val = ~be_val; // Bitwise NOT
        return std::string(reinterpret_cast<const char*>(&inverted_val), sizeof(inverted_val));
    }
} // end anonymous namespace


// Anonymous namespace for helpers (IF NEEDED in the header - often better in .cpp)
// namespace { ... }

/**
 * @brief Manages B+Tree indexes for the storage engine.
 *
 * Handles index creation, deletion, loading, persistence, and updates based on
 * record changes provided by the StorageEngine.
 */
class IndexManager {
private:
    // --- Member Variables ---
    SimpleBufferPoolManager& bpm_; // Reference requires full definition from buffer_pool_manager.h
    // StorageEngine& engine_;       // Reference requires forward declaration (or full include if methods called in header)
    std::string base_data_dir_;
    std::map<std::string, IndexDefinition> index_definitions_; // Requires types.h and <map>
    std::map<std::string, std::shared_ptr<BTree>> indices_;     // Requires btree.h, <map>, <memory>
    mutable std::shared_mutex mutex_;                           // Requires <shared_mutex>
    StorageEngine* engine_ = nullptr; 
    std::set<std::string> known_store_paths_;

    // --- Internal Helper Methods (Declarations) ---
    std::string getIndexPath(const std::string& indexName) const;
    std::string getDefinitionsPath() const;
    void persistDefinitions(); // Needs lock held
    void loadDefinitions();   // Needs lock held
    void createIndexInternal(const IndexDefinition& definition); // <<< NEW: Internal non-locking version

    // Field processing helpers (declarations need types.h)
    
    std::string encodeValueToString(const ValueType& value, IndexSortOrder order) const;

    std::optional<std::string> createIndexFieldKey(const Record& record, const IndexDefinition& definition) const;

    // --- NEW: Background Backfilling Worker Members ---
    std::vector<std::thread> backfill_workers_;
    std::queue<std::function<void()>> backfill_tasks_;
    std::mutex backfill_mutex_;
    std::condition_variable backfill_cv_;
    std::atomic<bool> stop_backfill_workers_{false};
    static constexpr size_t NUM_BACKFILL_WORKERS = 2; // Can be configurable

    // --- NEW: Worker thread function ---
    void backfillWorkerLoop();

public:
    // --- Constructor & Destructor ---
    // Corrected constructor signature based on index_manager.cpp
    IndexManager(SimpleBufferPoolManager& bpm, const std::string& data_dir); // Removed StorageEngine& engine param as it's not initialized here. Pass bpm reference.
    ~IndexManager();

    // ---  Setter for engine pointer ---
    void setStorageEngine(StorageEngine* engine);
    
    std::vector<std::string> createIndexFieldKeysForRecord(const Record &record, const IndexDefinition &definition) const;
    // --- Public API ---
    CreateIndexResult createIndex(const IndexDefinition& definition); // MODIFIED return type
    bool deleteIndex(const std::string& name);
    bool indexExists(const std::string& name) const;
    std::optional<IndexDefinition> getIndexDefinition(const std::string& name) const; // Needs types.h, <optional>
    std::vector<IndexDefinition> listIndexes(const std::optional<std::string>& storePath = std::nullopt) const;
    std::shared_ptr<BTree> getIndexBTree(const std::string& name) const; // Needs btree.h, <memory>
    std::string encodeValueToStringOrderPreserving(const ValueType& value) const; // Add this line
    std::optional<IndexDefinition> getIndexDefinitionForField(const std::string& storePath, const std::string& fieldName) const;
    // Main update function called by StorageEngine
    void updateIndices(
        const std::string& primary_key,
        const std::optional<Record>& old_record, // Needs types.h, <optional>
        const std::optional<Record>& new_record,
        TxnId commit_txn_id); // Needs types.h

    // Persistence (Called by StorageEngine or destructor)
    // These might not need to be public if only called internally
    void loadIndices();
    void persistIndices() const;
    void ensureStoreExistsAndIndexed(const std::string& storePath);
    std::optional<ValueType> extractFieldValue(const Record& record, const std::string& fieldName) const;

}; // End class IndexManager
