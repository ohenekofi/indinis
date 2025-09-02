//include/storage_error/error_handler.h
#pragma once

#include "storage_error.h" // Needs StorageError

namespace storage {

/**
 * @brief Error handler interface for custom error processing
 */
class ErrorHandler {
public:
    virtual ~ErrorHandler() = default;
    virtual void handleError(const StorageError& error) = 0;
    virtual void handleCriticalError(const StorageError& error) = 0;
};

} // namespace storage