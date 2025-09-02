//include/storage_error/error_utils.h
#pragma once

#include "error_codes.h" // Needs ErrorCode, ErrorSeverity, ErrorCategory
#include "storage_error.h" // Needed for STORAGE_ERROR macros
#include "result.h"        // Needed for RETURN_IF_ERROR macros

#include <string>
#include <string_view>

namespace storage {
namespace error_utils {
    
    // Convert error code to string
    std::string_view errorCodeToString(ErrorCode code);
    std::string_view severityToString(ErrorSeverity severity);
    std::string_view categoryToString(ErrorCategory category);
    
    // Get error metadata
    ErrorSeverity getErrorSeverity(ErrorCode code);
    ErrorCategory getErrorCategory(ErrorCode code);
    
    // Error code predicates
    bool isStorageError(ErrorCode code);
    bool isLsmError(ErrorCode code);
    bool isBtreeError(ErrorCode code);
    bool isIoError(ErrorCode code);
    bool isRecoverable(ErrorCode code); // Checks severity
    bool isCritical(ErrorCode code);    // Checks severity

} // namespace error_utils

// Helper macros for error reporting with location info
// These must be in the header.
#define STORAGE_ERROR(code, message) \
    storage::StorageError(code, message).withLocation(__FILE__, __LINE__, __FUNCTION__)

#define STORAGE_ERROR_WITH_DETAILS(code, message, details) \
    storage::StorageError(code, message, details).withLocation(__FILE__, __LINE__, __FUNCTION__)

// Convenience macros for common patterns with Result<T>
#define RETURN_IF_ERROR(result_expression) \
    do { \
        auto&& _tmp_status_macro_result = (result_expression); \
        if (!_tmp_status_macro_result.isOk()) { \
            return std::move(_tmp_status_macro_result.error()); \
        } \
    } while(0)

#define ASSIGN_OR_RETURN_IMPL(var, result_obj, result_expression) \
    auto result_obj = (result_expression); \
    if (!result_obj.isOk()) { \
        return std::move(result_obj.error()); \
    } \
    var = std::move(result_obj.value())
    /* Note: var must be declared before use or use auto var = ... if C++17 decltype can deduce properly */

// Better ASSIGN_OR_RETURN requiring var to be declared first
#define ASSIGN_OR_RETURN(var, result_expression) \
    do { \
        auto&& _tmp_assign_macro_result = (result_expression); \
        if (!_tmp_assign_macro_result.isOk()) { \
            return std::move(_tmp_assign_macro_result.error()); \
        } \
        var = std::move(_tmp_assign_macro_result.value()); \
    } while(0)

// Alternative ASSIGN_OR_RETURN that declares the variable (C++17 structured binding style)
// Usage: ASSIGN_OR_RETURN_AUTO(my_value, function_that_returns_result());
#define ASSIGN_OR_RETURN_AUTO(var_name, result_expression) \
    auto _tmp_auto_macro_result = (result_expression); \
    if (!_tmp_auto_macro_result.isOk()) { \
        return std::move(_tmp_auto_macro_result.error()); \
    } \
    auto var_name = std::move(_tmp_auto_macro_result.value())


} // namespace storage