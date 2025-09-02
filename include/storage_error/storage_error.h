//include/storage_error/storage_error.h

#pragma once

#include "error_codes.h" // Needs ErrorCode, ErrorSeverity, ErrorCategory
#include <string>
#include <string_view> // For return types in error_utils if called here
#include <optional>
#include <chrono>
#include <vector> // For recent_errors in ErrorContext if it were here
#include <unordered_map> // For context map

namespace storage {

// Forward declaration for error_utils functions if needed (e.g. if used in inline methods)
// However, since implementations are in .cpp, full inclusion will be there.
// For now, we can assume error_utils will be included by users or by storage_error.cpp

/**
 * @brief Detailed error information with context
 */
class StorageError {
public:
    ErrorCode code;
    ErrorSeverity severity; // Will be set based on code
    ErrorCategory category; // Will be set based on code
    std::string message;
    std::string details;
    std::string suggested_action;
    std::optional<std::string> file_path;
    std::optional<size_t> line_number;
    std::optional<std::string> function_name;
    std::chrono::system_clock::time_point timestamp;
    std::optional<ErrorCode> underlying_error;
    std::unordered_map<std::string, std::string> context;

    // Constructors
    StorageError(ErrorCode code, const std::string& message = "");
    StorageError(ErrorCode code, const std::string& message, const std::string& details);
    
    // Builder pattern for detailed error construction
    StorageError& withDetails(const std::string& details);
    StorageError& withSuggestedAction(const std::string& action);
    StorageError& withLocation(const std::string& file, size_t line, const std::string& function);
    StorageError& withUnderlyingError(ErrorCode underlying);
    StorageError& withContext(const std::string& key, const std::string& value);
    StorageError& withFilePath(const std::string& path);
    
    // Utility methods
    bool isRecoverable() const;
    bool requiresShutdown() const;
    std::string toString() const;
    std::string toDetailedString() const;
    std::string toJson() const;
    
    // Static factory methods for common errors
    static StorageError corruption(const std::string& details);
    static StorageError ioError(const std::string& operation, const std::string& path);
    static StorageError outOfMemory(size_t requested_bytes);
    static StorageError timeout(const std::string& operation, std::chrono::milliseconds duration);
    static StorageError keyNotFound(const std::string& key);
    static StorageError compactionFailed(const std::string& reason);
};

} // namespace storage