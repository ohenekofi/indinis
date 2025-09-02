// @filename: include/compression_utils.h (NEW FILE)
#pragma once
#include "types.h"

#include <vector>
#include <string>
#include <optional>
// #include "types.h" // If PageId is needed for logging, or CompressionType moves to types.h


class CompressionManager {
public:
    // Compresses data. Throws std::runtime_error on failure.
    // level: 0 for default, Zstd offers levels 1-22.
    static std::vector<uint8_t> compress(const uint8_t* uncompressed_data, size_t uncompressed_size,
                                         CompressionType type, int level = 0);

    // Decompresses data. Throws std::runtime_error on failure.
    // uncompressed_size_hint is crucial for pre-allocating buffer for decompression.
    static std::vector<uint8_t> decompress(const uint8_t* compressed_data, size_t compressed_size,
                                           size_t uncompressed_size_hint, CompressionType type);

    // Returns an estimated upper bound for compressed size. Useful for buffer allocation.
    static size_t get_max_compressed_size(size_t uncompressed_size, CompressionType type);
};