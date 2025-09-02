// @src/napi_type_conversions.h
#ifndef NAPI_TYPE_CONVERSIONS_H
#define NAPI_TYPE_CONVERSIONS_H

#include <napi.h>
#include <optional>
#include <variant>
#include "../include/indinis.h" // For ValueType

// --- Helper function declarations ---
Napi::Value ConvertValueToNapi(Napi::Env env, const ValueType& value);
std::optional<ValueType> ConvertNapiToValueType(Napi::Env env, const Napi::Value& jsValue);

#endif // NAPI_TYPE_CONVERSIONS_H