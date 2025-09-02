//src/serialization_utils.cpp
#include "../include/serialization_utils.h"
#include "../include/debug_utils.h" // For LOG_ERROR
#include <limits>

void SerializeString(std::ostream& out, const std::string& str) {
    if (str.length() > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("SerializeString: String length exceeds uint32_t max.");
    }
    uint32_t len = static_cast<uint32_t>(str.length());
    out.write(reinterpret_cast<const char*>(&len), sizeof(len));
    if (!out) {
        throw std::runtime_error("SerializeString: Failed to write string length.");
    }
    if (len > 0) {
        out.write(str.data(), len);
        if (!out) {
            throw std::runtime_error("SerializeString: Failed to write string data.");
        }
    }
}

std::string DeserializeString(std::istream& in) {
    uint32_t len;
    in.read(reinterpret_cast<char*>(&len), sizeof(len));
    if (in.gcount() != sizeof(len)) {
        if (in.eof() && in.gcount() == 0) return ""; // Clean EOF at start
        throw std::runtime_error("DeserializeString: Failed to read string length.");
    }

    // Sanity check to prevent allocating massive amounts of memory from corrupt data
    constexpr uint32_t MAX_SANE_STRING_LEN = 100 * 1024 * 1024; // 100MB
    if (len > MAX_SANE_STRING_LEN) {
        throw std::length_error("DeserializeString: String length in stream (" + std::to_string(len) + ") exceeds sanity limit.");
    }

    if (len == 0) return "";

    std::string str(len, '\0');
    in.read(&str[0], len);
    if (static_cast<uint32_t>(in.gcount()) != len) {
        throw std::runtime_error("DeserializeString: Failed to read full string data. Expected " + std::to_string(len) + " bytes.");
    }
    return str;
}