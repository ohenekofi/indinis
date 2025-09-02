//include/storage_error/result.h
#pragma once

#include "storage_error.h" // Needs StorageError
#include <optional>
#include <stdexcept> // For std::runtime_error in value()
#include <utility>   // For std::move

namespace storage {

/**
 * @brief Result type that can contain either a value or an error
 */
template<typename T>
class Result {
private:
    std::optional<T> value_;
    std::optional<StorageError> error_;

public:
    // Constructors
    Result(T val) : value_(std::move(val)) {} // Use T val by value for potential move
    Result(StorageError err) : error_(std::move(err)) {} // Use StorageError err by value
    
    // Copy and move constructors/assignments
    Result(const Result&) = default;
    Result(Result&&) = default;
    Result& operator=(const Result&) = default;
    Result& operator=(Result&&) = default;
    
    // Status checking
    bool hasValue() const { return value_.has_value(); }
    bool hasError() const { return error_.has_value(); }
    bool isOk() const { return hasValue(); }
    explicit operator bool() const { return isOk(); }
    
    // Value access
    const T& value() const& { // const lvalue ref overload
        if (!hasValue()) throw std::runtime_error("Result has no value (const access)");
        return *value_; 
    }
    
    T& value() & { // lvalue ref overload
        if (!hasValue()) throw std::runtime_error("Result has no value (non-const access)");
        return *value_; 
    }

    T&& value() && { // rvalue ref overload for moving out
        if (!hasValue()) throw std::runtime_error("Result has no value (rvalue access)");
        return std::move(*value_);
    }
    
    const T* operator->() const { 
        if (!hasValue()) throw std::runtime_error("Result has no value (const operator->)");
        return &(*value_); 
    }
    T* operator->() { 
        if (!hasValue()) throw std::runtime_error("Result has no value (operator->)");
        return &(*value_); 
    }

    const T& operator*() const& { return value(); } // const lvalue ref
    T& operator*() & { return value(); }            // lvalue ref
    T&& operator*() && { return std::move(value()); } // rvalue ref
    
    // Error access
    const StorageError& error() const& { // const lvalue ref
        if (!hasError()) throw std::runtime_error("Result has no error (const access)");
        return *error_;
    }
    StorageError& error() & { // lvalue ref
        if (!hasError()) throw std::runtime_error("Result has no error (non-const access)");
        return *error_;
    }
    StorageError&& error() && { // rvalue ref
        if (!hasError()) throw std::runtime_error("Result has no error (rvalue access)");
        return std::move(*error_);
    }
    
    // Convenience methods
    T valueOr(T default_value) const& { // Use T by value for default for potential move
        return hasValue() ? *value_ : std::move(default_value);
    }
    T valueOr(T default_value) && {
        return hasValue() ? std::move(*value_) : std::move(default_value);
    }
    
    // Map: if Ok, applies func to value; if Error, propagates error
    template<typename F>
    auto map(F&& func) & -> Result<decltype(func(std::declval<T&>()))> {
        if (hasValue()) {
            return Result<decltype(func(*value_))>(func(*value_));
        }
        return Result<decltype(func(*value_))>(*error_); // Propagate error_
    }
    template<typename F>
    auto map(F&& func) const& -> Result<decltype(func(std::declval<const T&>()))> {
        if (hasValue()) {
            return Result<decltype(func(*value_))>(func(*value_));
        }
        return Result<decltype(func(*value_))>(*error_);
    }
    template<typename F>
    auto map(F&& func) && -> Result<decltype(func(std::declval<T&&>()))> {
        if (hasValue()) {
            return Result<decltype(func(std::move(*value_)))>(func(std::move(*value_)));
        }
        return Result<decltype(func(std::move(*value_)))>(std::move(*error_));
    }
    
    // MapError: if Error, applies func to error; if Ok, propagates value
    template<typename F>
    Result<T> mapError(F&& func) & {
        if (hasError()) {
            return Result<T>(func(*error_));
        }
        return *this; // Return copy of Ok result
    }
    template<typename F>
    Result<T> mapError(F&& func) const& {
        if (hasError()) {
            return Result<T>(func(*error_));
        }
        return *this;
    }
     template<typename F>
    Result<T> mapError(F&& func) && {
        if (hasError()) {
            return Result<T>(func(std::move(*error_)));
        }
        return std::move(*this); // Move Ok result
    }
};

// Specialization for void
template<>
class Result<void> {
private:
    std::optional<StorageError> error_;

public:
    Result() = default; // Represents success
    Result(StorageError err) : error_(std::move(err)) {} // Use StorageError err by value
    
    Result(const Result&) = default;
    Result(Result&&) = default;
    Result& operator=(const Result&) = default;
    Result& operator=(Result&&) = default;

    bool hasError() const { return error_.has_value(); }
    bool isOk() const { return !hasError(); }
    explicit operator bool() const { return isOk(); }
    
    const StorageError& error() const& { // const lvalue ref
        if (!hasError()) throw std::runtime_error("Result<void> has no error (const access)");
        return *error_;
    }
    StorageError& error() & { // lvalue ref
        if (!hasError()) throw std::runtime_error("Result<void> has no error (non-const access)");
        return *error_;
    }
    StorageError&& error() && { // rvalue ref
        if (!hasError()) throw std::runtime_error("Result<void> has no error (rvalue access)");
        return std::move(*error_);
    }

    template<typename F>
    Result<void> mapError(F&& func) & {
        if (hasError()) {
            return Result<void>(func(*error_));
        }
        return *this;
    }
    template<typename F>
    Result<void> mapError(F&& func) const& {
        if (hasError()) {
            return Result<void>(func(*error_));
        }
        return *this;
    }
     template<typename F>
    Result<void> mapError(F&& func) && {
        if (hasError()) {
            return Result<void>(func(std::move(*error_)));
        }
        return std::move(*this);
    }
};

// Type alias for convenience for operations that don't return a value but can fail
using Status = Result<void>;

} // namespace storage