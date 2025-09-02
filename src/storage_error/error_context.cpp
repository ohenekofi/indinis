//src/error_context.cpp

#include "storage_error/error_context.h"
#include "storage_error/error_handler.h" // Full definition needed
#include <algorithm> // For std::min

namespace storage {

ErrorContext::ErrorContext(std::shared_ptr<ErrorHandler> handler)
    : handler_(std::move(handler)) {
}

void ErrorContext::setErrorHandler(std::shared_ptr<ErrorHandler> handler) {
    std::lock_guard<std::mutex> lock(mutex_);
    handler_ = std::move(handler);
}

void ErrorContext::reportError(const StorageError& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Update statistics
    error_counts_[error.code]++;
    
    // Keep recent errors
    recent_errors_.push_back(error);
    if (recent_errors_.size() > MAX_RECENT_ERRORS) { // Use constant
        recent_errors_.erase(recent_errors_.begin());
    }
    
    // Call error handler if available
    if (handler_) {
        if (error.severity == ErrorSeverity::CRITICAL || error.severity == ErrorSeverity::FATAL) {
            handler_->handleCriticalError(error);
        } else {
            handler_->handleError(error);
        }
    }
}

void ErrorContext::reportError(ErrorCode code, const std::string& message) {
    // The STORAGE_ERROR macro is for creating errors with location.
    // If this is called from various places, it's fine. Or create a simple StorageError.
    reportError(StorageError(code, message));
}

size_t ErrorContext::getErrorCount(ErrorCode code) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = error_counts_.find(code);
    return it != error_counts_.end() ? it->second : 0;
}

size_t ErrorContext::getTotalErrorCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t total = 0;
    for (const auto& pair : error_counts_) { // Use structured binding
        total += pair.second;
    }
    return total;
}

std::vector<StorageError> ErrorContext::getRecentErrors(size_t count) const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t actual_count = std::min(count, recent_errors_.size());
    if (actual_count == 0) {
        return {};
    }
    return std::vector<StorageError>(
        recent_errors_.end() - actual_count, // Corrected calculation
        recent_errors_.end()
    );
}

bool ErrorContext::hasRepeatedErrors(ErrorCode code, size_t threshold) const {
    return getErrorCount(code) >= threshold;
}

bool ErrorContext::hasEscalatingErrors() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (recent_errors_.size() < 5) { // Need at least 5 for a meaningful trend check
        return false;
    }
    
    // Get last 5 errors for simplicity
    auto check_errors = std::vector<StorageError>(recent_errors_.end() - 5, recent_errors_.end());

    int severity_trend = 0;
    for (size_t i = 1; i < check_errors.size(); ++i) {
        if (static_cast<int>(check_errors[i].severity) > static_cast<int>(check_errors[i-1].severity)) {
            severity_trend++;
        } else if (static_cast<int>(check_errors[i].severity) < static_cast<int>(check_errors[i-1].severity)) {
            severity_trend--; // Consider de-escalation too
        }
    }
    
    // Example: if 3 out of 4 comparisons show escalation
    return severity_trend >= 3; 
}

void ErrorContext::clearErrors() {
    std::lock_guard<std::mutex> lock(mutex_);
    recent_errors_.clear();
    error_counts_.clear();
}

} // namespace storage