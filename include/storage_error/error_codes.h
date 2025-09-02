// include/storage_error/error_codes.h
#pragma once

namespace storage {

// Forward declarations (not strictly needed for enums, but good practice if other types were here)

/**
 * @brief Comprehensive error codes for LSM/B-tree storage operations
 */
enum class ErrorCode : int {
    // Success
    OK = 0,
    
    // Storage Engine Errors (1000-1999)
    STORAGE_CORRUPTION = 1001,
    STORAGE_FULL = 1002,
    STORAGE_LOCKED = 1003,
    STORAGE_NOT_INITIALIZED = 1004,
    STORAGE_ALREADY_INITIALIZED = 1005,
    STORAGE_VERSION_MISMATCH = 1006,
    STORAGE_BACKUP_FAILED = 1007,
    STORAGE_RECOVERY_FAILED = 1008,
    STORAGE_BACKFILL_FAILED = 1009,
    
    // LSM Tree Specific Errors (2000-2999)
    LSM_COMPACTION_FAILED = 2001,
    LSM_MEMTABLE_FULL = 2002,
    LSM_LEVEL_OVERFLOW = 2003,
    LSM_BLOOM_FILTER_ERROR = 2004,
    LSM_SSTABLE_CORRUPTION = 2005,
    LSM_MANIFEST_ERROR = 2006,
    LSM_WAL_CORRUPTION = 2007,
    LSM_FLUSH_FAILED = 2008,
    LSM_MERGE_CONFLICT = 2009,
    
    // B-Tree Specific Errors (3000-3999)
    BTREE_NODE_CORRUPTION = 3001,
    BTREE_SPLIT_FAILED = 3002,
    BTREE_MERGE_FAILED = 3003,
    BTREE_REBALANCE_FAILED = 3004,
    BTREE_HEIGHT_EXCEEDED = 3005,
    BTREE_KEY_TOO_LARGE = 3006,
    BTREE_VALUE_TOO_LARGE = 3007,
    BTREE_CONCURRENT_MODIFICATION = 3008, // Already present in concurrency
    
    
    // I/O and File System Errors (4000-4999)
    IO_READ_ERROR = 4001,
    IO_WRITE_ERROR = 4002,
    IO_SEEK_ERROR = 4003,
    IO_FLUSH_ERROR = 4004,
    IO_SYNC_ERROR = 4005,
    FILE_NOT_FOUND = 4006,
    FILE_PERMISSION_DENIED = 4007,
    FILE_ALREADY_EXISTS = 4008,
    DIRECTORY_NOT_FOUND = 4009,
    DISK_FULL = 4010,
    NETWORK_UNAVAILABLE = 4011,
    
    // Data Validation Errors (5000-5999)
    INVALID_KEY = 5001,
    INVALID_VALUE = 5002,
    KEY_NOT_FOUND = 5003,
    DUPLICATE_KEY = 5004,
    CHECKSUM_MISMATCH = 5005,
    INVALID_DATA_FORMAT = 5006,
    SCHEMA_VIOLATION = 5007,
    ENCODING_ERROR = 5008,
    COMPRESSION_ERROR = 5009,
    
    // Concurrency Errors (6000-6999)
    LOCK_TIMEOUT = 6001,
    DEADLOCK_DETECTED = 6002,
    TRANSACTION_CONFLICT = 6003,
    CONCURRENT_MODIFICATION = 6004, // Duplicate of 3008, choose one or make specific
    THREAD_POOL_EXHAUSTED = 6005,
    RESOURCE_BUSY = 6006,
    
    // Memory Errors (7000-7999)
    OUT_OF_MEMORY = 7001,
    MEMORY_CORRUPTION = 7002,
    BUFFER_OVERFLOW = 7003,
    INVALID_POINTER = 7004,
    MEMORY_LEAK_DETECTED = 7005,
    
    // Configuration Errors (8000-8999)
    INVALID_CONFIGURATION = 8001,
    MISSING_REQUIRED_OPTION = 8002,
    OPTION_OUT_OF_RANGE = 8003,
    CONFLICTING_OPTIONS = 8004,
    
    // Network/Replication Errors (9000-9999)
    REPLICATION_LAG = 9001,
    CONSENSUS_FAILED = 9002,
    NODE_UNREACHABLE = 9003,
    SPLIT_BRAIN_DETECTED = 9004,
    
    // Generic Errors (10000+)
    TIMEOUT = 10001,
    CANCELLED = 10002,
    NOT_IMPLEMENTED = 10003,
    INTERNAL_ERROR = 10004,
    UNKNOWN_ERROR = 10005
};

/**
 * @brief Error severity levels
 */
enum class ErrorSeverity {
    INFO,       // Informational, operation can continue
    WARNING,    // Warning, operation succeeded but with issues
    ERROR,      // Error, operation failed but system is stable
    CRITICAL,   // Critical error, system stability may be compromised
    FATAL       // Fatal error, immediate shutdown required
};

/**
 * @brief Error categories for grouping related errors
 */
enum class ErrorCategory {
    STORAGE_ENGINE,
    LSM_TREE,
    BTREE,
    IO_FILESYSTEM,
    DATA_VALIDATION,
    CONCURRENCY,
    MEMORY,
    CONFIGURATION,
    NETWORK_REPLICATION,
    GENERIC
};

} // namespace storage