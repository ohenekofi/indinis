// include/serialization_utils.h
#pragma once

#include <string>
#include <ostream>
#include <istream>
#include <stdexcept> // For std::runtime_error, std::overflow_error

/**
 * @brief Serializes a string to an output stream with a 32-bit length prefix.
 * @param out The output stream.
 * @param str The string to serialize.
 * @throws std::overflow_error if the string is too long.
 * @throws std::runtime_error on stream write failure.
 */
void SerializeString(std::ostream& out, const std::string& str);

/**
 * @brief Deserializes a length-prefixed string from an input stream.
 * @param in The input stream.
 * @return The deserialized string.
 * @throws std::runtime_error on stream read failure or data corruption.
 * @throws std::length_error if the serialized length is unreasonably large.
 */
std::string DeserializeString(std::istream& in);