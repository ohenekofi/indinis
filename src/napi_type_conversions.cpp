// @src/napi_type_conversions.cpp
#include "napi_type_conversions.h"
#include "../include/debug_utils.h"
#include <iostream> // For logging
#include <string>
#include <vector>
#include <limits>
#include <stdexcept>

// --- Helper function to convert C++ ValueType to Napi::Value ---
Napi::Value ConvertValueToNapi(Napi::Env env, const ValueType& value) {
    try{
        LOG_TRACE("  [ConvertValueToNapi] Input C++ ValueType index: {}", value.index());
            return std::visit([&env](auto&& arg) -> Napi::Value {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, std::monostate>) {
                    return env.Null();
                } else if constexpr (std::is_same_v<T, bool>) {
                    return Napi::Boolean::New(env, arg);
                } else if constexpr (std::is_same_v<T, int64_t>) {
                    return Napi::Number::New(env, static_cast<double>(arg)); // JS numbers are doubles
                } else if constexpr (std::is_same_v<T, double>) {
                    return Napi::Number::New(env, arg);
                } else if constexpr (std::is_same_v<T, std::string>) {
                    return Napi::String::New(env, arg);
                } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                    return Napi::Buffer<uint8_t>::Copy(env, arg.data(), arg.size());
                }
            }, value);
    } catch (const std::bad_variant_access& e) {
        LOG_ERROR("  [ConvertValueToNapi] Error: Bad variant access. Input ValueType index: {}", value.index());
        Napi::Error::New(env, "Internal error: Failed to access C++ value variant: " + std::string(e.what())).ThrowAsJavaScriptException();
        return env.Null();
    } catch (const Napi::Error& e) {
         // Catch NAPI errors during value creation
         e.ThrowAsJavaScriptException();
        return env.Null();
    } catch (const std::exception& e) {
        Napi::Error::New(env, "Unknown C++ error during C++ value conversion: " + std::string(e.what())).ThrowAsJavaScriptException();
         return env.Null();
    } catch (...) {
        Napi::Error::New(env, "Unknown error during C++ value conversion").ThrowAsJavaScriptException();
        return env.Null();
    }
}

// --- Helper function to convert Napi::Value to C++ ValueType ---
std::optional<ValueType> ConvertNapiToValueType(Napi::Env env, const Napi::Value& jsValue) {
    if (jsValue.IsBoolean()) {
        return jsValue.As<Napi::Boolean>().Value();
    } else if (jsValue.IsNumber()) {
        double num = jsValue.As<Napi::Number>().DoubleValue();
        // Prefer int64_t if it fits without loss of precision
        if (num == static_cast<double>(static_cast<int64_t>(num))) {
             return static_cast<int64_t>(num);
        } else {
             return num;
        }
    } else if (jsValue.IsString()) {
        return jsValue.As<Napi::String>().Utf8Value();
    } else if (jsValue.IsBuffer()) {
        Napi::Buffer<uint8_t> buf = jsValue.As<Napi::Buffer<uint8_t>>();
        return std::vector<uint8_t>(buf.Data(), buf.Data() + buf.Length());
    } else if (jsValue.IsNull() || jsValue.IsUndefined()) {
        return std::monostate{};
    }

    // Unsupported JS type
    Napi::TypeError::New(env, "Unsupported JS type for database value.").ThrowAsJavaScriptException();
    return std::nullopt;
}