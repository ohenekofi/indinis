// @filename include/encoding_utils.h
#pragma once

#include <string>
#include <vector>
#include <cstdint> // For int32_t, uint32_t, int64_t, uint64_t
#include <cstring> // For std::memcpy
#include <algorithm> // For std::reverse if needed, or manual big-endian
#include <stdexcept> // For std::invalid_argument
#include <variant>   // If you define a Value variant type here
#include "types.h" 

// Your ValueType enum (from types.h or defined here if not already)
// enum class ValueStorageType { /* ... */ };
// using ValueVariant = std::variant<int64_t, double, std::string, std::vector<uint8_t>>;


namespace Indinis { // Optional namespace

// --- Order-Preserving Encoding for Signed Integers ---
inline std::string encodeInt32OrderPreserving(int32_t value) {
    // Flip the sign bit (MSB)
    // (int32_t)0x80000000 is -2147483648.
    // Casting to uint32_t first, then XORing with 0x80000000U is safer
    // to ensure the bitwise operation happens on the unsigned representation.
    uint32_t uvalue = static_cast<uint32_t>(value) ^ 0x80000000U;
    std::string encoded(4, '\0');
    // Big-endian serialization
    encoded[0] = static_cast<char>((uvalue >> 24) & 0xFF);
    encoded[1] = static_cast<char>((uvalue >> 16) & 0xFF);
    encoded[2] = static_cast<char>((uvalue >> 8) & 0xFF);
    encoded[3] = static_cast<char>(uvalue & 0xFF);
    return encoded;
}

inline std::string encodeInt64OrderPreserving(int64_t value) {
    // Flip the sign bit (MSB)
    uint64_t uvalue = static_cast<uint64_t>(value) ^ (1ULL << 63);
    std::string encoded(8, '\0');
    // Big-endian serialization
    for (int i = 0; i < 8; ++i) {
        encoded[i] = static_cast<char>((uvalue >> (56 - i * 8)) & 0xFF);
    }
    return encoded;
}

// --- Order-Preserving Encoding for Doubles ---
inline std::string encodeDoubleOrderPreserving(double value) {
    uint64_t bits;
    std::memcpy(&bits, &value, sizeof(double)); // Reinterpret double as uint64_t

    // If positive (sign bit is 0), flip the sign bit to make it 1.
    // This ensures positive numbers sort after negative numbers (which will start with 0 after their transformation).
    // If negative (sign bit is 1), flip all bits. This makes them start with 0 and reverses their order.
    if ((bits >> 63) == 0) { // Positive or +0.0
        bits |= (1ULL << 63);
    } else { // Negative or -0.0
        bits = ~bits;
    }

    std::string encoded(8, '\0');
    // Big-endian serialization
    for (int i = 0; i < 8; ++i) {
        encoded[i] = static_cast<char>((bits >> (56 - i * 8)) & 0xFF);
    }
    return encoded;
}

// --- Encoding for Strings (Handling Nulls for Composite Keys is separate) ---
// For direct storage as index key part, string itself is usually fine if BTree handles binary.
// However, if strings can contain arbitrary '\0' and they are part of a multi-segment key
// *before* the TxnId, they need escaping or length prefixing if a '\0' is used
// to separate those segments. The proposal rightly moves to fixed-size TxnId suffix
// to avoid this for the *final* separator.
inline std::string encodeStringOrderPreserving(const std::string& value) {
    // For BTree lexicographical comparison, raw string bytes usually work.
    // If we need to ensure no '\0' interferes with a *different* internal separator
    // for multi-field indexes (before TxnId), we'd escape here.
    // For now, assuming the `index_key` part of `createCompositeKey` is already a
    // single, potentially multi-segment, escaped/prefixed string if needed.
    // This function just returns the value as is.
    return value;
}

// --- Encoding for Binary Data ---
inline std::string encodeBinaryOrderPreserving(const std::vector<uint8_t>& value) {
    return std::string(reinterpret_cast<const char*>(value.data()), value.size());
}

inline std::string encodeValueToStringOrderPreserving(const ValueType& value) {
    return std::visit([](auto&& arg) -> std::string {
        using T = std::decay_t<decltype(arg)>;
        
        // Note: The specific type prefixes ('i', 'd', 's', etc.) that I mentioned
        // in an earlier design thought process are not strictly necessary here
        // as long as the binary representations do not overlap in a problematic way.
        // For example, an 8-byte int64 will not be confused with a string that has a
        // length prefix. The current encoding scheme is sufficient.

        if constexpr (std::is_same_v<T, std::monostate>) {
            // A special, single-byte representation for null that sorts first.
            return std::string(1, '\x00'); 
        } else if constexpr (std::is_same_v<T, int64_t>) {
            return encodeInt64OrderPreserving(arg);
        } else if constexpr (std::is_same_v<T, double>) {
            return encodeDoubleOrderPreserving(arg);
        } else if constexpr (std::is_same_v<T, std::string>) {
            return encodeStringOrderPreserving(arg);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            return encodeBinaryOrderPreserving(arg);
        } else if constexpr (std::is_same_v<T, bool>) {
            // Encode boolean as an order-preserving int64 (0 for false, 1 for true)
            return encodeInt64OrderPreserving(arg ? 1 : 0);
        } else {
            // This will cause a compile-time error if the ValueType variant is
            // ever extended with a type not handled here.
            // This is a good safety measure.
            // A static_assert(false, ...) is a common way to do this.
            throw std::logic_error("Unsupported type in ValueType variant for encoding");
        }
    }, value);
}

// You might want a generic dispatch function here if ValueType is from types.h
// For example (assuming ValueType and ValueVariant are defined in types.h and included):
/*
#include "types.h" // Assuming ValueType enum and ValueVariant (std::variant)

inline std::string encodeValueTypeOrderPreserving(const ValueVariant& value) {
    return std::visit([](auto&& arg) -> std::string {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, int64_t>) {
            // Decide if all integers are stored as int64_t in the variant,
            // or if you need to handle int32_t separately based on original type.
            // Assuming int64_t for now.
            return encodeInt64OrderPreserving(arg);
        } else if constexpr (std::is_same_v<T, double>) {
            return encodeDoubleOrderPreserving(arg);
        } else if constexpr (std::is_same_v<T, std::string>) {
            return encodeStringOrderPreserving(arg);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            return encodeBinaryOrderPreserving(arg);
        } else {
            // static_assert(false, "Unsupported type in ValueVariant for encoding");
            throw std::logic_error("Unsupported type in ValueVariant for encoding");
        }
    }, value);
}
*/

} // namespace Indinis