//src/error_utils.cpp

#include "storage_error/error_utils.h"
// No other specific includes needed if error_codes.h has enums only

namespace storage {
namespace error_utils {

std::string_view errorCodeToString(ErrorCode code) {
    switch (code) {
        case ErrorCode::OK: return "OK";
        
        // Storage Engine Errors
        case ErrorCode::STORAGE_CORRUPTION: return "STORAGE_CORRUPTION";
        case ErrorCode::STORAGE_FULL: return "STORAGE_FULL";
        case ErrorCode::STORAGE_LOCKED: return "STORAGE_LOCKED";
        case ErrorCode::STORAGE_NOT_INITIALIZED: return "STORAGE_NOT_INITIALIZED";
        case ErrorCode::STORAGE_ALREADY_INITIALIZED: return "STORAGE_ALREADY_INITIALIZED";
        case ErrorCode::STORAGE_VERSION_MISMATCH: return "STORAGE_VERSION_MISMATCH";
        case ErrorCode::STORAGE_BACKUP_FAILED: return "STORAGE_BACKUP_FAILED";
        case ErrorCode::STORAGE_RECOVERY_FAILED: return "STORAGE_RECOVERY_FAILED";
        
        // LSM Tree Errors
        case ErrorCode::LSM_COMPACTION_FAILED: return "LSM_COMPACTION_FAILED";
        case ErrorCode::LSM_MEMTABLE_FULL: return "LSM_MEMTABLE_FULL";
        case ErrorCode::LSM_LEVEL_OVERFLOW: return "LSM_LEVEL_OVERFLOW";
        case ErrorCode::LSM_BLOOM_FILTER_ERROR: return "LSM_BLOOM_FILTER_ERROR";
        case ErrorCode::LSM_SSTABLE_CORRUPTION: return "LSM_SSTABLE_CORRUPTION";
        case ErrorCode::LSM_MANIFEST_ERROR: return "LSM_MANIFEST_ERROR";
        case ErrorCode::LSM_WAL_CORRUPTION: return "LSM_WAL_CORRUPTION";
        case ErrorCode::LSM_FLUSH_FAILED: return "LSM_FLUSH_FAILED";
        case ErrorCode::LSM_MERGE_CONFLICT: return "LSM_MERGE_CONFLICT";
        
        // B-Tree Errors
        case ErrorCode::BTREE_NODE_CORRUPTION: return "BTREE_NODE_CORRUPTION";
        case ErrorCode::BTREE_SPLIT_FAILED: return "BTREE_SPLIT_FAILED";
        case ErrorCode::BTREE_MERGE_FAILED: return "BTREE_MERGE_FAILED";
        case ErrorCode::BTREE_REBALANCE_FAILED: return "BTREE_REBALANCE_FAILED";
        case ErrorCode::BTREE_HEIGHT_EXCEEDED: return "BTREE_HEIGHT_EXCEEDED";
        case ErrorCode::BTREE_KEY_TOO_LARGE: return "BTREE_KEY_TOO_LARGE";
        case ErrorCode::BTREE_VALUE_TOO_LARGE: return "BTREE_VALUE_TOO_LARGE";
        case ErrorCode::BTREE_CONCURRENT_MODIFICATION: return "BTREE_CONCURRENT_MODIFICATION";
        
        // I/O Errors
        case ErrorCode::IO_READ_ERROR: return "IO_READ_ERROR";
        case ErrorCode::IO_WRITE_ERROR: return "IO_WRITE_ERROR";
        case ErrorCode::IO_SEEK_ERROR: return "IO_SEEK_ERROR";
        case ErrorCode::IO_FLUSH_ERROR: return "IO_FLUSH_ERROR";
        case ErrorCode::IO_SYNC_ERROR: return "IO_SYNC_ERROR";
        case ErrorCode::FILE_NOT_FOUND: return "FILE_NOT_FOUND";
        case ErrorCode::FILE_PERMISSION_DENIED: return "FILE_PERMISSION_DENIED";
        case ErrorCode::FILE_ALREADY_EXISTS: return "FILE_ALREADY_EXISTS";
        case ErrorCode::DIRECTORY_NOT_FOUND: return "DIRECTORY_NOT_FOUND";
        case ErrorCode::DISK_FULL: return "DISK_FULL";
        case ErrorCode::NETWORK_UNAVAILABLE: return "NETWORK_UNAVAILABLE";
        
        // Data Validation Errors
        case ErrorCode::INVALID_KEY: return "INVALID_KEY";
        case ErrorCode::INVALID_VALUE: return "INVALID_VALUE";
        case ErrorCode::KEY_NOT_FOUND: return "KEY_NOT_FOUND";
        case ErrorCode::DUPLICATE_KEY: return "DUPLICATE_KEY";
        case ErrorCode::CHECKSUM_MISMATCH: return "CHECKSUM_MISMATCH";
        case ErrorCode::INVALID_DATA_FORMAT: return "INVALID_DATA_FORMAT";
        case ErrorCode::SCHEMA_VIOLATION: return "SCHEMA_VIOLATION";
        case ErrorCode::ENCODING_ERROR: return "ENCODING_ERROR";
        case ErrorCode::COMPRESSION_ERROR: return "COMPRESSION_ERROR";
        
        // Concurrency Errors
        case ErrorCode::LOCK_TIMEOUT: return "LOCK_TIMEOUT";
        case ErrorCode::DEADLOCK_DETECTED: return "DEADLOCK_DETECTED";
        case ErrorCode::TRANSACTION_CONFLICT: return "TRANSACTION_CONFLICT";
        case ErrorCode::CONCURRENT_MODIFICATION: return "CONCURRENT_MODIFICATION";
        case ErrorCode::THREAD_POOL_EXHAUSTED: return "THREAD_POOL_EXHAUSTED";
        case ErrorCode::RESOURCE_BUSY: return "RESOURCE_BUSY";
        
        // Memory Errors
        case ErrorCode::OUT_OF_MEMORY: return "OUT_OF_MEMORY";
        case ErrorCode::MEMORY_CORRUPTION: return "MEMORY_CORRUPTION";
        case ErrorCode::BUFFER_OVERFLOW: return "BUFFER_OVERFLOW";
        case ErrorCode::INVALID_POINTER: return "INVALID_POINTER";
        case ErrorCode::MEMORY_LEAK_DETECTED: return "MEMORY_LEAK_DETECTED";
        
        // Configuration Errors
        case ErrorCode::INVALID_CONFIGURATION: return "INVALID_CONFIGURATION";
        case ErrorCode::MISSING_REQUIRED_OPTION: return "MISSING_REQUIRED_OPTION";
        case ErrorCode::OPTION_OUT_OF_RANGE: return "OPTION_OUT_OF_RANGE";
        case ErrorCode::CONFLICTING_OPTIONS: return "CONFLICTING_OPTIONS";
        
        // Network/Replication Errors
        case ErrorCode::REPLICATION_LAG: return "REPLICATION_LAG";
        case ErrorCode::CONSENSUS_FAILED: return "CONSENSUS_FAILED";
        case ErrorCode::NODE_UNREACHABLE: return "NODE_UNREACHABLE";
        case ErrorCode::SPLIT_BRAIN_DETECTED: return "SPLIT_BRAIN_DETECTED";
        
        // Generic Errors
        case ErrorCode::TIMEOUT: return "TIMEOUT";
        case ErrorCode::CANCELLED: return "CANCELLED";
        case ErrorCode::NOT_IMPLEMENTED: return "NOT_IMPLEMENTED";
        case ErrorCode::INTERNAL_ERROR: return "INTERNAL_ERROR";
        case ErrorCode::UNKNOWN_ERROR: return "UNKNOWN_ERROR";
        
        default: return "UNKNOWN_ERROR_CODE_DETAIL"; // Should not happen if all codes are covered
    }
}

std::string_view severityToString(ErrorSeverity severity) {
    switch (severity) {
        case ErrorSeverity::INFO: return "INFO";
        case ErrorSeverity::WARNING: return "WARNING";
        case ErrorSeverity::ERROR: return "ERROR";
        case ErrorSeverity::CRITICAL: return "CRITICAL";
        case ErrorSeverity::FATAL: return "FATAL";
        default: return "UNKNOWN_SEVERITY";
    }
}

std::string_view categoryToString(ErrorCategory category) {
    switch (category) {
        case ErrorCategory::STORAGE_ENGINE: return "STORAGE_ENGINE";
        case ErrorCategory::LSM_TREE: return "LSM_TREE";
        case ErrorCategory::BTREE: return "BTREE";
        case ErrorCategory::IO_FILESYSTEM: return "IO_FILESYSTEM";
        case ErrorCategory::DATA_VALIDATION: return "DATA_VALIDATION";
        case ErrorCategory::CONCURRENCY: return "CONCURRENCY";
        case ErrorCategory::MEMORY: return "MEMORY";
        case ErrorCategory::CONFIGURATION: return "CONFIGURATION";
        case ErrorCategory::NETWORK_REPLICATION: return "NETWORK_REPLICATION";
        case ErrorCategory::GENERIC: return "GENERIC";
        default: return "UNKNOWN_CATEGORY";
    }
}

ErrorSeverity getErrorSeverity(ErrorCode code) {
    // ... (Implementation as provided in the original single file) ...
    // This mapping logic will live here.
    switch (code) {
        case ErrorCode::OK:
            return ErrorSeverity::INFO;
            
        case ErrorCode::STORAGE_CORRUPTION:
        case ErrorCode::MEMORY_CORRUPTION:
        case ErrorCode::LSM_SSTABLE_CORRUPTION:
        case ErrorCode::BTREE_NODE_CORRUPTION:
        case ErrorCode::LSM_WAL_CORRUPTION:
        case ErrorCode::SPLIT_BRAIN_DETECTED:
            return ErrorSeverity::FATAL;
            
        case ErrorCode::OUT_OF_MEMORY:
        case ErrorCode::DISK_FULL:
        case ErrorCode::STORAGE_FULL:
        case ErrorCode::DEADLOCK_DETECTED:
        case ErrorCode::BUFFER_OVERFLOW:
        case ErrorCode::MEMORY_LEAK_DETECTED:
        case ErrorCode::THREAD_POOL_EXHAUSTED:
            return ErrorSeverity::CRITICAL;
            
        case ErrorCode::IO_READ_ERROR:
        case ErrorCode::IO_WRITE_ERROR:
        case ErrorCode::FILE_NOT_FOUND:
        case ErrorCode::FILE_PERMISSION_DENIED:
        case ErrorCode::LSM_COMPACTION_FAILED:
        case ErrorCode::LSM_FLUSH_FAILED:
        case ErrorCode::BTREE_SPLIT_FAILED:
        case ErrorCode::BTREE_MERGE_FAILED:
        case ErrorCode::CHECKSUM_MISMATCH:
        case ErrorCode::INVALID_DATA_FORMAT:
        case ErrorCode::TIMEOUT:
        case ErrorCode::LOCK_TIMEOUT:
        case ErrorCode::TRANSACTION_CONFLICT:
        case ErrorCode::STORAGE_LOCKED:
        case ErrorCode::INVALID_CONFIGURATION:
        case ErrorCode::CONSENSUS_FAILED:
            return ErrorSeverity::ERROR;
            
        case ErrorCode::LSM_MEMTABLE_FULL:
        case ErrorCode::REPLICATION_LAG:
        case ErrorCode::RESOURCE_BUSY:
        case ErrorCode::CONCURRENT_MODIFICATION: // BTree and general concurrency
        case ErrorCode::OPTION_OUT_OF_RANGE:
        case ErrorCode::CONFLICTING_OPTIONS:
        case ErrorCode::NODE_UNREACHABLE:
            return ErrorSeverity::WARNING;
            
        case ErrorCode::KEY_NOT_FOUND:
        case ErrorCode::DUPLICATE_KEY:
        case ErrorCode::CANCELLED:
        case ErrorCode::NOT_IMPLEMENTED:
            return ErrorSeverity::INFO;
            
        default: // For any other codes not explicitly listed, default to ERROR
            return ErrorSeverity::ERROR;
    }
}

ErrorCategory getErrorCategory(ErrorCode code) {
    // ... (Implementation as provided in the original single file) ...
    // This mapping logic will live here.
    int code_value = static_cast<int>(code);
    
    if (code_value >= 1000 && code_value < 2000) {
        return ErrorCategory::STORAGE_ENGINE;
    } else if (code_value >= 2000 && code_value < 3000) {
        return ErrorCategory::LSM_TREE;
    } else if (code_value >= 3000 && code_value < 4000) {
        return ErrorCategory::BTREE;
    } else if (code_value >= 4000 && code_value < 5000) {
        return ErrorCategory::IO_FILESYSTEM;
    } else if (code_value >= 5000 && code_value < 6000) {
        return ErrorCategory::DATA_VALIDATION;
    } else if (code_value >= 6000 && code_value < 7000) {
        return ErrorCategory::CONCURRENCY;
    } else if (code_value >= 7000 && code_value < 8000) {
        return ErrorCategory::MEMORY;
    } else if (code_value >= 8000 && code_value < 9000) {
        return ErrorCategory::CONFIGURATION;
    } else if (code_value >= 9000 && code_value < 10000) {
        return ErrorCategory::NETWORK_REPLICATION;
    } else {
        return ErrorCategory::GENERIC;
    }
}


// Predicates
bool isStorageError(ErrorCode code) { return getErrorCategory(code) == ErrorCategory::STORAGE_ENGINE; }
bool isLsmError(ErrorCode code) { return getErrorCategory(code) == ErrorCategory::LSM_TREE; }
bool isBtreeError(ErrorCode code) { return getErrorCategory(code) == ErrorCategory::BTREE; }
bool isIoError(ErrorCode code) { return getErrorCategory(code) == ErrorCategory::IO_FILESYSTEM; }
bool isRecoverable(ErrorCode code) {
    ErrorSeverity severity = getErrorSeverity(code);
    return severity != ErrorSeverity::FATAL && severity != ErrorSeverity::CRITICAL;
}
bool isCritical(ErrorCode code) {
    ErrorSeverity severity = getErrorSeverity(code);
    return severity == ErrorSeverity::CRITICAL || severity == ErrorSeverity::FATAL;
}

} // namespace error_utils
} // namespace storage