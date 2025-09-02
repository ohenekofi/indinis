//include/storage_error/error_context.h
#pragma once

#include "storage_error.h" // Needs StorageError
#include <memory>          // For std::shared_ptr
#include <vector>
#include <unordered_map>
#include <mutex>           // For std::mutex

namespace storage {

// Forward declaration
class ErrorHandler;

/**
 * @brief Error context for tracking error patterns and metrics
 */
class ErrorContext {
private:
    std::shared_ptr<ErrorHandler> handler_;
    std::vector<StorageError> recent_errors_;
    std::unordered_map<ErrorCode, size_t> error_counts_;
    mutable std::mutex mutex_; // mutable to allow locking in const methods
    
    static constexpr size_t MAX_RECENT_ERRORS = 100; // Define a constant

public:
    ErrorContext(std::shared_ptr<ErrorHandler> handler = nullptr);
    
    void setErrorHandler(std::shared_ptr<ErrorHandler> handler);
    void reportError(const StorageError& error);
    void reportError(ErrorCode code, const std::string& message); // Convenience overload
    
    // Error statistics
    size_t getErrorCount(ErrorCode code) const;
    size_t getTotalErrorCount() const;
    std::vector<StorageError> getRecentErrors(size_t count = 10) const;
    
    // Error pattern detection
    bool hasRepeatedErrors(ErrorCode code, size_t threshold = 5) const;
    bool hasEscalatingErrors() const; // Checks if recent errors are getting more severe
    
    // Clear errors (for testing or reset)
    void clearErrors();
};

} // namespace storage