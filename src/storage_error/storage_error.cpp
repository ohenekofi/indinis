// src/storage_error.cpp
#include "storage_error/storage_error.h"
#include "storage_error/error_utils.h" // For error_utils::errorCodeToString etc.
#include <sstream>
#include <iomanip> // For std::put_time
#include <chrono>  // For std::chrono types

namespace storage {

// StorageError Implementation
StorageError::StorageError(ErrorCode code, const std::string& message)
    : code(code)
    , severity(error_utils::getErrorSeverity(code)) // Use helper
    , category(error_utils::getErrorCategory(code)) // Use helper
    , message(message.empty() ? std::string(error_utils::errorCodeToString(code)) : message) // Use helper
    , timestamp(std::chrono::system_clock::now()) {
}

StorageError::StorageError(ErrorCode code, const std::string& message, const std::string& details)
    : StorageError(code, message) { // Delegate to the other constructor
    this->details = details;
}

StorageError& StorageError::withDetails(const std::string& details_param) { // Renamed param
    this->details = details_param;
    return *this;
}

StorageError& StorageError::withSuggestedAction(const std::string& action) {
    this->suggested_action = action;
    return *this;
}

StorageError& StorageError::withLocation(const std::string& file, size_t line, const std::string& function) {
    this->file_path = file;
    this->line_number = line;
    this->function_name = function;
    return *this;
}

StorageError& StorageError::withUnderlyingError(ErrorCode underlying) {
    this->underlying_error = underlying;
    return *this;
}

StorageError& StorageError::withContext(const std::string& key, const std::string& value) {
    this->context[key] = value;
    return *this;
}

StorageError& StorageError::withFilePath(const std::string& path) {
    this->file_path = path; // Note: 'file_path' member used by withLocation also
    return *this;
}

bool StorageError::isRecoverable() const {
    // Delegate to error_utils or use severity directly
    // return error_utils::isRecoverable(code);
    return severity != ErrorSeverity::FATAL && severity != ErrorSeverity::CRITICAL;
}

bool StorageError::requiresShutdown() const {
    return severity == ErrorSeverity::FATAL;
}

std::string StorageError::toString() const {
    std::ostringstream oss;
    oss << "[" << error_utils::severityToString(severity) << "] "
        << error_utils::errorCodeToString(code) << " (" << static_cast<int>(code) << "): "
        << message;
    return oss.str();
}

std::string StorageError::toDetailedString() const {
    std::ostringstream oss;
    
    oss << "Error Details:\n";
    oss << "  Code: " << error_utils::errorCodeToString(code) 
        << " (" << static_cast<int>(code) << ")\n";
    oss << "  Severity: " << error_utils::severityToString(severity) << "\n";
    oss << "  Category: " << error_utils::categoryToString(category) << "\n";
    oss << "  Message: " << message << "\n";
    
    if (!details.empty()) {
        oss << "  Details: " << details << "\n";
    }
    
    if (!suggested_action.empty()) {
        oss << "  Suggested Action: " << suggested_action << "\n";
    }
    
    if (file_path && line_number && function_name) {
        oss << "  Location: " << *function_name << " at " << *file_path << ":" << *line_number << "\n";
    } else if (file_path) {
        oss << "  File Path: " << *file_path << "\n";
    }
    
    if (underlying_error) {
        oss << "  Underlying Error: " << error_utils::errorCodeToString(*underlying_error) 
            << " (" << static_cast<int>(*underlying_error) << ")\n";
    }
    
    if (!context.empty()) {
        oss << "  Context:\n";
        for (const auto& [key, value] : context) {
            oss << "    " << key << ": " << value << "\n";
        }
    }
    
    // Format timestamp
    auto time_t_val = std::chrono::system_clock::to_time_t(timestamp); // Renamed variable
    // Note: std::localtime is not thread-safe on all platforms.
    // For robust multithreaded logging, consider alternatives or mutex protection if an issue.
    // For error objects themselves, this is usually fine as they are created and then read.
    oss << "  Timestamp: " << std::put_time(std::localtime(&time_t_val), "%Y-%m-%d %H:%M:%S") << "\n";
    
    return oss.str();
}

std::string StorageError::toJson() const {
    // ... (Implementation as provided, ensuring it uses error_utils for string conversions) ...
    // For brevity, I'll assume the provided toJson implementation is moved here and
    // any calls like errorCodeToString(code) are changed to error_utils::errorCodeToString(code)
    std::ostringstream oss;
    oss << "{";
    oss << "\"code\":" << static_cast<int>(code) << ",";
    oss << "\"code_name\":\"" << error_utils::errorCodeToString(code) << "\",";
    oss << "\"severity\":\"" << error_utils::severityToString(severity) << "\",";
    oss << "\"category\":\"" << error_utils::categoryToString(category) << "\",";
    oss << "\"message\":\"" << message << "\""; // No trailing comma for last guaranteed field
    
    if (!details.empty()) {
        oss << ",\"details\":\"" << details << "\"";
    }
    if (!suggested_action.empty()) {
        oss << ",\"suggested_action\":\"" << suggested_action << "\"";
    }
    if (file_path && line_number && function_name) {
        oss << ",\"location\":{\"file\":\"" << *file_path << "\",\"line\":" << *line_number << ",\"function\":\"" << *function_name << "\"}";
    } else if (file_path) {
        oss << ",\"file_path\":\"" << *file_path << "\"";
    }
    if (underlying_error) {
        oss << ",\"underlying_error_code\":" << static_cast<int>(*underlying_error);
        oss << ",\"underlying_error_name\":\"" << error_utils::errorCodeToString(*underlying_error) << "\"";
    }
    if (!context.empty()) {
        oss << ",\"context\":{";
        bool first = true;
        for (const auto& [key, value] : context) {
            if (!first) oss << ",";
            oss << "\"" << key << "\":\"" << value << "\"";
            first = false;
        }
        oss << "}";
    }
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(timestamp.time_since_epoch());
    oss << ",\"timestamp_ms\":" << ms.count();
    oss << "}";
    return oss.str();
}


// Static factory methods
StorageError StorageError::corruption(const std::string& details_param) { // Renamed param
    return StorageError(ErrorCode::STORAGE_CORRUPTION, "Storage corruption detected")
        .withDetails(details_param)
        .withSuggestedAction("Run storage verification and repair tools");
}

StorageError StorageError::ioError(const std::string& operation, const std::string& path) {
    return StorageError(ErrorCode::IO_READ_ERROR, "I/O operation failed") // Default to read, can be more specific
        .withDetails("Operation: " + operation)
        .withFilePath(path)
        .withSuggestedAction("Check file permissions and disk space");
}

StorageError StorageError::outOfMemory(size_t requested_bytes) {
    return StorageError(ErrorCode::OUT_OF_MEMORY, "Memory allocation failed")
        .withDetails("Requested: " + std::to_string(requested_bytes) + " bytes")
        .withSuggestedAction("Reduce memory usage or increase available memory");
}

StorageError StorageError::timeout(const std::string& operation, std::chrono::milliseconds duration) {
    return StorageError(ErrorCode::TIMEOUT, "Operation timed out")
        .withDetails("Operation: " + operation + ", Duration: " + std::to_string(duration.count()) + "ms")
        .withSuggestedAction("Increase timeout value or check system performance");
}

StorageError StorageError::keyNotFound(const std::string& key) {
    return StorageError(ErrorCode::KEY_NOT_FOUND, "Key not found")
        .withDetails("Key: " + key)
        .withContext("key", key);
}

StorageError StorageError::compactionFailed(const std::string& reason) {
    return StorageError(ErrorCode::LSM_COMPACTION_FAILED, "LSM compaction failed")
        .withDetails(reason)
        .withSuggestedAction("Check disk space and retry compaction");
}

} // namespace storage