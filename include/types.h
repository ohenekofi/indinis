// @include/types.h

#pragma once
#include "../src/vendor/zlib/zlib.h" 
// Standard Library Includes (keep these at the top)
#include <string>
#include <vector>
#include <map>
#include <set>
#include <unordered_set>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <shared_mutex>
#include <memory>
#include <optional>
#include <variant>    // For std::variant, std::monostate, std::holds_alternative
#include <chrono>
#include <cstdint>    // For fixed-width integers like uint64_t, uint32_t, uint8_t
#include <utility>    // For std::pair
#include <sstream>    // For toString methods
#include <iomanip>    // For std::setw, std::setfill (in toString)
#include <iostream>   // For std::ostream, std::istream (if method decls are here)
#include <iosfwd>     // For forward declarations of stream types if full include not needed here
#include <cstring>    // For std::memset, std::char_traits (for C++17 length) - though char_traits no longer needed for KEK_VALIDATION_PLAINTEXT_LEN
#include <limits>     // For std::numeric_limits


// --- Foundational Data Types ---
using TxnId = uint64_t;
static constexpr TxnId INVALID_TXN_ID = 0;

using PageId = uint64_t;
static constexpr PageId INVALID_PAGE_ID = 0;

using LSN = uint64_t; // Log Sequence Number

// --- Indexing Enums ---
enum class IndexSortOrder {
    ASCENDING,
    DESCENDING
};


// --- Compression and Encryption Enums ---
enum class CompressionType : uint8_t {
    NONE = 0,
    ZSTD = 1,
    LZ4  = 2,
};

enum class EncryptionScheme : uint8_t {
    NONE = 0,
    //AES256_CBC_PBKDF2 = 1, // Legacy/Confidentiality only
    AES256_GCM_PBKDF2 = 2, // AEAD: Confidentiality, Integrity, Authenticity
};

// --- Constants for Encryption Primitives (Fundamental sizes) ---
static constexpr int PAGE_ENCRYPTION_AES_KEY_SIZE    = 32;  // 256-bit AES keys
static constexpr int PAGE_ENCRYPTION_AES_IV_SIZE   = 16;  // Max IV size for header fields (used by AES-CBC, and as max field size for GCM IV if shorter)
static constexpr int PAGE_ENCRYPTION_SALT_SIZE   = 16;  // For PBKDF2 salt
static constexpr int PAGE_ENCRYPTION_AUTH_TAG_SIZE = 16;  // 128-bit GCM authentication tag

// --- KEK Validation Plaintext and its Encrypted Size ---
// Define as a char array for consistent sizeof behavior
static constexpr char KEK_VALIDATION_PLAINTEXT_CSTR[] = "IndinisDB_KEK_Validation_OK_Value_v1.0";

// Calculate length robustly using sizeof on the array
static constexpr size_t KEK_VALIDATION_PLAINTEXT_LEN = sizeof(KEK_VALIDATION_PLAINTEXT_CSTR) - 1; // Subtract 1 for the null terminator

// For GCM, ciphertext size generally equals plaintext size.
static constexpr size_t ENCRYPTED_KEK_VALIDATION_VALUE_SIZE = KEK_VALIDATION_PLAINTEXT_LEN;

// std::string version for runtime use (e.g., passing to encryption functions)
// Note: This will create the string object when it's first used if it's a function-local static,
// or at program startup if it's a global static.
// Using KEK_VALIDATION_PLAINTEXT_CSTR directly in functions is often fine too.
static const std::string KEK_VALIDATION_PLAINTEXT = KEK_VALIDATION_PLAINTEXT_CSTR;


inline uint32_t calculate_payload_checksum(const uint8_t* data, size_t len) {
    if (data == nullptr || len == 0) {
        return 0; // Or a specific checksum for empty data
    }
    uLong crc = crc32(0L, Z_NULL, 0);
    crc = crc32(crc, data, static_cast<uInt>(len));
    return static_cast<uint32_t>(crc);
}


// --- On-Disk Page Structure Header (for B-Tree pages primarily) ---
#pragma pack(push, 1)
struct OnDiskPageHeader {
    uint8_t flags;
    uint32_t payload_size_on_disk;
    uint32_t uncompressed_logical_size;
    LSN page_lsn;
    unsigned char iv[PAGE_ENCRYPTION_AES_IV_SIZE];
    unsigned char salt[PAGE_ENCRYPTION_SALT_SIZE];
    unsigned char auth_tag[PAGE_ENCRYPTION_AUTH_TAG_SIZE];
    uint32_t payload_checksum;

    void set_flags(CompressionType c_type, EncryptionScheme e_scheme) {
        flags = (static_cast<uint8_t>(c_type) & 0x0F) | ((static_cast<uint8_t>(e_scheme) << 4) & 0xF0);
    }
    CompressionType get_compression_type() const { return static_cast<CompressionType>(flags & 0x0F); }
    EncryptionScheme get_encryption_scheme() const { return static_cast<EncryptionScheme>((flags >> 4) & 0x0F); }

    OnDiskPageHeader() : flags(0), payload_size_on_disk(0), uncompressed_logical_size(0), page_lsn(0), payload_checksum(0) { // Init checksum
        std::memset(iv, 0, sizeof(iv));
        std::memset(salt, 0, sizeof(salt));
        std::memset(auth_tag, 0, sizeof(auth_tag));
        set_flags(CompressionType::NONE, EncryptionScheme::NONE);
    }
};
#pragma pack(pop)
static_assert(sizeof(OnDiskPageHeader) == (1 + 4 + 4 + 8 + PAGE_ENCRYPTION_AES_IV_SIZE + PAGE_ENCRYPTION_SALT_SIZE + PAGE_ENCRYPTION_AUTH_TAG_SIZE + 4), "OnDiskPageHeader size mismatch with checksum."); // Adjusted size
static constexpr size_t ON_DISK_PAGE_HEADER_SIZE = sizeof(OnDiskPageHeader);


// --- SSTable On-Disk Block Structure Header ---
#pragma pack(push, 1)
struct SSTableDataBlockHeader {
    uint8_t flags;
    uint32_t compressed_payload_size_bytes; // Size of the data *after* this header
    uint32_t uncompressed_content_size_bytes;
    uint32_t num_records_in_block;
    unsigned char iv[PAGE_ENCRYPTION_AES_IV_SIZE];
    unsigned char salt[PAGE_ENCRYPTION_SALT_SIZE];
    unsigned char auth_tag[PAGE_ENCRYPTION_AUTH_TAG_SIZE];
    uint32_t payload_checksum; // *** NEW: Checksum of the compressed_payload_size_bytes data ***


    void set_flags(CompressionType c_type, EncryptionScheme e_scheme) {
        flags = (static_cast<uint8_t>(c_type) & 0x0F) | ((static_cast<uint8_t>(e_scheme) << 4) & 0xF0);
    }
    CompressionType get_compression_type() const { return static_cast<CompressionType>(flags & 0x0F); }
    EncryptionScheme get_encryption_scheme() const { return static_cast<EncryptionScheme>((flags >> 4) & 0x0F); }

     SSTableDataBlockHeader() : flags(0), compressed_payload_size_bytes(0), uncompressed_content_size_bytes(0), num_records_in_block(0), payload_checksum(0) { // Init checksum
        std::memset(iv, 0, sizeof(iv));
        std::memset(salt, 0, sizeof(salt));
        std::memset(auth_tag, 0, sizeof(auth_tag));
        set_flags(CompressionType::NONE, EncryptionScheme::NONE);
    }
};
#pragma pack(pop)
static_assert(sizeof(SSTableDataBlockHeader) == (1 + 4 + 4 + 4 + PAGE_ENCRYPTION_AES_IV_SIZE + PAGE_ENCRYPTION_SALT_SIZE + PAGE_ENCRYPTION_AUTH_TAG_SIZE + 4), "SSTableDataBlockHeader size mismatch with checksum."); // Adjusted size
static constexpr size_t SSTABLE_DATA_BLOCK_HEADER_SIZE = sizeof(SSTableDataBlockHeader);


// --- SSTable Block Index Entry (Metadata stored in SSTable footer) ---
#pragma pack(push, 1)
struct SSTableBlockIndexEntry {
    std::streampos block_offset_in_file; // Changed from uint64_t for fstream compatibility
    uint32_t compressed_payload_size_bytes;
    uint32_t uncompressed_payload_size_bytes; // Corrected name from _logical_
    CompressionType compression_type;
    EncryptionScheme encryption_scheme;
    uint32_t num_records;

    SSTableBlockIndexEntry()
        : block_offset_in_file(0), compressed_payload_size_bytes(0),
          uncompressed_payload_size_bytes(0), compression_type(CompressionType::NONE),
          encryption_scheme(EncryptionScheme::NONE), num_records(0) {}
};
#pragma pack(pop)
// Size assertion for SSTableBlockIndexEntry might be platform-dependent due to std::streampos.
// It's usually equivalent to long long or similar. For now, we assume it's consistently handled.


// --- ValueType Definition for Record values ---
using ValueType = std::variant<
    std::monostate,         // index 0 (represents null or empty value)
    int64_t,                // index 1
    double,                 // index 2
    std::string,            // index 3
    std::vector<uint8_t>,    // index 4 (for binary data)
    bool                    // index 5 (NEW)
>;

// --- Indexing Structures ---
struct IndexField {
    std::string name;
    IndexSortOrder order = IndexSortOrder::ASCENDING;
    // Default constructor implicitly fine. Custom one for convenience.
    IndexField(std::string n = "", IndexSortOrder o = IndexSortOrder::ASCENDING) : name(std::move(n)), order(o) {}
};

enum class IndexType : uint8_t {
    B_TREE,
    FULL_TEXT
};

struct IndexDefinition {
    std::string name;
    std::vector<IndexField> fields;
    std::string storePath;
    
    IndexType type;       
    bool is_unique;      
    bool is_multikey = false;

    // --- START OF FIX: Add a default constructor ---
    IndexDefinition() 
        : type(IndexType::B_TREE), 
          is_unique(false), 
          is_multikey(false) 
    {}
    // --- END OF FIX ---

    bool isValid() const {
        if (name.empty() || fields.empty() || storePath.empty()) {
            return false;
        }
        // A unique index cannot also be a multikey index, as one doc could generate multiple identical keys.
        if (is_unique && is_multikey) {
            return false;
        }
        // A multikey index must be on a single field, not a compound of fields.
        if (is_multikey && fields.size() > 1) {
            return false;
        }
        return true;
    }
};



// --- Record Structures (MVCC) ---
struct Record {
    std::string key;
    ValueType value;
    TxnId txn_id; // Transaction that created this version
    TxnId commit_txn_id; // Transaction that committed this version (0 if uncommitted)
    std::chrono::system_clock::time_point timestamp; // When this version was created/updated
    bool deleted = false; // Tombstone marker
};

struct VersionedRecord {
    Record current; // The most recent version (committed or uncommitted by current txn)
    std::vector<Record> versions; // Historical committed versions
    mutable std::shared_mutex mutex; // Protects access to current and versions

    VersionedRecord();
    VersionedRecord(const Record& record);

    std::optional<Record> getVisibleVersion(TxnId reader_txn_id) const;
    void addVersion(const Record& record, TxnId oldest_active_txn_id);
};


// --- WAL Specific Enums and Structs ---
enum class LogRecordType : uint8_t {
    INVALID = 0,
    BEGIN_TXN,
    COMMIT_TXN,
    ABORT_TXN,
    DATA_PUT,        // For new data or updates
    DATA_DELETE,     // For logical deletes (tombstones)
    CHECKPOINT_BEGIN,
    CHECKPOINT_END,
    // Potential future types:
    // INDEX_CREATE, INDEX_DROP, SCHEMA_CHANGE etc.
};

// --- Metadata for KEK/DEK Management ---
#pragma pack(push, 1)
struct WrappedDEKInfo {
    static constexpr uint32_t MAGIC_NUMBER = 0xDEADD0DE; // Or some other chosen magic
    static constexpr uint16_t VERSION = 1;

    uint32_t magic;
    uint16_t version;
    int kdf_iterations_for_kek; // Iterations used for the KEK that wrapped DEK in this file
    // KEK Validation Data (IV, Tag, Ciphertext of known plaintext)
    unsigned char kek_validation_iv[PAGE_ENCRYPTION_AES_IV_SIZE];   // Use consistent IV size field
    unsigned char kek_validation_tag[PAGE_ENCRYPTION_AUTH_TAG_SIZE];
    unsigned char encrypted_kek_validation_value[ENCRYPTED_KEK_VALIDATION_VALUE_SIZE]; // Uses the corrected size
    // Wrapped DEK Package (IV + Tag + Ciphertext of actual DEK)
    uint32_t wrapped_dek_package_size; // Size of the package that follows this header

    WrappedDEKInfo() : magic(MAGIC_NUMBER), version(VERSION), kdf_iterations_for_kek(0), wrapped_dek_package_size(0) {
        std::memset(kek_validation_iv, 0, sizeof(kek_validation_iv));
        std::memset(kek_validation_tag, 0, sizeof(kek_validation_tag));
        std::memset(encrypted_kek_validation_value, 0, sizeof(encrypted_kek_validation_value));
    }
};
#pragma pack(pop)

static_assert(sizeof(WrappedDEKInfo) ==
    (sizeof(uint32_t) + sizeof(uint16_t) + sizeof(int) +           // magic, version, kdf_iterations
     PAGE_ENCRYPTION_AES_IV_SIZE + PAGE_ENCRYPTION_AUTH_TAG_SIZE + // kek_validation_iv, kek_validation_tag
     ENCRYPTED_KEK_VALIDATION_VALUE_SIZE +                         // encrypted_kek_validation_value (now 38)
     sizeof(uint32_t)),                                            // wrapped_dek_package_size
    "WrappedDEKInfo size mismatch. Check #pragma pack or member sizes.");
static constexpr size_t WRAPPED_DEK_INFO_HEADER_SIZE = sizeof(WrappedDEKInfo);


// --- Checkpoint Related Metadata ---
struct CheckpointMetadata {
    uint64_t checkpoint_id = 0;
    LSN checkpoint_begin_lsn = 0; // LSN of the CHECKPOINT_BEGIN record in WAL
    std::chrono::system_clock::time_point timestamp;
    std::set<TxnId> active_txns_at_checkpoint_begin; // Transactions active when checkpoint started

    // Method declarations if definitions are in types.cpp
    void serialize(std::ostream& out) const;
    bool deserialize(std::istream& in);
    std::string toString() const;
};

struct LogRecord {
    LSN lsn = 0;
    TxnId txn_id = 0;
    LogRecordType type = LogRecordType::INVALID;
    std::string key;      // For DATA_PUT, DATA_DELETE
    std::string value;    // For DATA_PUT (binary serialized ValueType), or other specific payload for other types

    // Method declarations if definitions are in types.cpp or wal_record.cpp
    void serialize(std::ostream& out) const;
    bool deserialize(std::istream& in);
    uint32_t getDiskSize() const; // Calculates serialized size on disk (including framing/CRC)

    // Static helpers to convert ValueType to/from a binary string for the 'value' field
    static std::string serializeValueTypeBinary(const ValueType& value);
    static std::optional<ValueType> deserializeValueTypeBinary(const std::string& data_str);
};

enum class CheckpointStatus {
    NOT_STARTED,
    IN_PROGRESS,
    COMPLETED,
    FAILED
};

struct CheckpointInfo { // For user-facing history
    uint64_t checkpoint_id = 0;
    CheckpointStatus status = CheckpointStatus::NOT_STARTED;
    std::chrono::system_clock::time_point start_time;
    std::chrono::system_clock::time_point end_time;
    LSN checkpoint_begin_wal_lsn = 0;
    LSN checkpoint_end_wal_lsn = 0;
    std::set<TxnId> active_txns_at_begin;
    std::string error_msg;

    std::string toString() const;
};

enum class FilterOperator {
    EQUAL,
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
    ARRAY_CONTAINS, 
    ARRAY_CONTAINS_ANY
};

struct FilterCondition {
    std::string field;
    FilterOperator op;
    std::variant<ValueType, std::vector<ValueType>> value;
};

enum class AtomicUpdateType : uint8_t {
    INCREMENT
    // Future types could go here: ARRAY_APPEND, BITWISE_OR, etc.
};

struct AtomicUpdate {
    AtomicUpdateType type;
    // Using a variant allows for different types of update values in the future.
    // For now, we only need a double for incrementing.
    std::variant<double> value;
};

struct OrderByClause {
    std::string field;
    IndexSortOrder direction = IndexSortOrder::ASCENDING;
};

template<typename T>
struct PaginatedQueryResult {
    std::vector<T> docs;
    bool hasNextPage = false;
    bool hasPrevPage = false;
    std::optional<std::vector<ValueType>> startCursor;
    std::optional<std::vector<ValueType>> endCursor;
};


enum class BatchOperationType {
    MAKE,
    MODIFY,
    REMOVE
};

struct BatchOperation {
    BatchOperationType type;
    std::string key;

    // This now holds either:
    // - For MAKE: A std::string containing the full JSON document.
    // - For MODIFY: A std::string containing a JSON object of simple merge fields AND atomic op sentinels.
    // - For REMOVE: This is not used.
    std::optional<std::string> value;

    bool overwrite = false;
};