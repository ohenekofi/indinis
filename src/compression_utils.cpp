// @filename: src/compression_utils.cpp (MODIFIED for detailed logging)
#include "../include/compression_utils.h"
#include "../include/debug_utils.h"
#include "../../src/vendor/zstd/lib/zstd.h" // Include ZSTD header
#include "../../src/vendor/lz4/lz4.h" // Include LZ4 header
#include <stdexcept>
#include <cstring>

std::vector<uint8_t> CompressionManager::compress(const uint8_t* uncompressed_data, size_t uncompressed_size,
                                                CompressionType type, int level) {
    if (type == CompressionType::NONE || uncompressed_size == 0) {
        LOG_TRACE("[CompressionManager::compress] Type is NONE or size is 0. Skipping compression. Uncompressed size: {}", uncompressed_size);
        return std::vector<uint8_t>(uncompressed_data, uncompressed_data + uncompressed_size);
    }

    if (type == CompressionType::ZSTD) {
        size_t const cBuffSize = ZSTD_compressBound(uncompressed_size);
        std::vector<uint8_t> compressed_buffer(cBuffSize);
        int effective_level = (level == 0) ? ZSTD_CLEVEL_DEFAULT : level;

        LOG_TRACE("[CompressionManager::compress] Attempting ZSTD compression. Uncompressed size: {}, Level: {}", uncompressed_size, effective_level);
        size_t const cSize = ZSTD_compress(compressed_buffer.data(), cBuffSize,
                                           uncompressed_data, uncompressed_size,
                                           effective_level);
        if (ZSTD_isError(cSize)) {
            LOG_ERROR("[CompressionManager::compress] ZSTD_compress failed: {}", ZSTD_getErrorName(cSize));
            throw std::runtime_error(std::string("ZSTD_compress error: ") + ZSTD_getErrorName(cSize));
        }
        LOG_TRACE("[CompressionManager::compress] ZSTD compression successful. Original size: {}, Compressed size: {}", uncompressed_size, cSize);
        compressed_buffer.resize(cSize);
        return compressed_buffer;
    }else if (type == CompressionType::LZ4) {
        LOG_TRACE("[CompressionManager::compress] Attempting LZ4 compression. Uncompressed size: {}", uncompressed_size);
        int const max_dst_size = LZ4_compressBound(static_cast<int>(uncompressed_size));
        if (max_dst_size <= 0) {
             LOG_ERROR("[CompressionManager::compress] LZ4_compressBound failed or indicated uncompressible for size {}", uncompressed_size);
             throw std::runtime_error("LZ4_compressBound failed");
        }
        std::vector<uint8_t> compressed_buffer(max_dst_size);
        int const compressed_data_size = LZ4_compress_default(
            reinterpret_cast<const char*>(uncompressed_data),
            reinterpret_cast<char*>(compressed_buffer.data()),
            static_cast<int>(uncompressed_size),
            max_dst_size
        );
        if (compressed_data_size <= 0) {
            LOG_ERROR("[CompressionManager::compress] LZ4_compress_default failed.");
            throw std::runtime_error("LZ4_compress_default failed");
        }
        LOG_TRACE("[CompressionManager::compress] LZ4 compression successful. Original size: {}, Compressed size: {}", uncompressed_size, compressed_data_size);
        compressed_buffer.resize(compressed_data_size);
        return compressed_buffer;
    }
    LOG_WARN("[CompressionManager::compress] Unsupported compression type: {}. Returning uncompressed.", static_cast<int>(type));
    return std::vector<uint8_t>(uncompressed_data, uncompressed_data + uncompressed_size); // Fallback
}

std::vector<uint8_t> CompressionManager::decompress(const uint8_t* compressed_data, size_t compressed_size,
                                                  size_t uncompressed_size_hint, CompressionType type) {
    if (type == CompressionType::NONE || compressed_size == 0) {
        LOG_TRACE("[CompressionManager::decompress] Type is NONE or compressed size is 0. Skipping decompression. Output size will be: {}", compressed_size);
        return std::vector<uint8_t>(compressed_data, compressed_data + compressed_size);
    }

    if (type == CompressionType::ZSTD) {
        if (uncompressed_size_hint == 0) {
            LOG_ERROR("[CompressionManager::decompress] ZSTD decompress: uncompressed_size_hint is 0, which is required.");
            throw std::runtime_error("ZSTD decompress requires a non-zero uncompressed_size_hint.");
        }
        LOG_TRACE("[CompressionManager::decompress] Attempting ZSTD decompression. Compressed size: {}, Uncompressed hint: {}", compressed_size, uncompressed_size_hint);
        std::vector<uint8_t> decompressed_buffer(uncompressed_size_hint);
        size_t const dSize = ZSTD_decompress(decompressed_buffer.data(), uncompressed_size_hint,
                                             compressed_data, compressed_size);
        if (ZSTD_isError(dSize)) {
            LOG_ERROR("[CompressionManager::decompress] ZSTD_decompress failed: {}", ZSTD_getErrorName(dSize));
            throw std::runtime_error(std::string("ZSTD_decompress error: ") + ZSTD_getErrorName(dSize));
        }
        if (dSize != uncompressed_size_hint) {
            LOG_WARN("[CompressionManager::decompress] ZSTD_decompress: Actual decompressed size ({}) != hint ({}). Resizing output.", dSize, uncompressed_size_hint);
            decompressed_buffer.resize(dSize);
        }
        LOG_TRACE("[CompressionManager::decompress] ZSTD decompression successful. Compressed size: {}, Decompressed size: {}", compressed_size, dSize);
        return decompressed_buffer;
    }else if (type == CompressionType::LZ4) {
        if (uncompressed_size_hint == 0) {
            LOG_ERROR("[CompressionManager::decompress] LZ4 decompress: uncompressed_size_hint is 0, which is required.");
            throw std::runtime_error("LZ4 decompress requires a non-zero uncompressed_size_hint.");
        }
        LOG_TRACE("[CompressionManager::decompress] Attempting LZ4 decompression. Compressed size: {}, Uncompressed hint: {}", compressed_size, uncompressed_size_hint);
        std::vector<uint8_t> decompressed_buffer(uncompressed_size_hint);
        int const decompressed_size = LZ4_decompress_safe(
            reinterpret_cast<const char*>(compressed_data),
            reinterpret_cast<char*>(decompressed_buffer.data()),
            static_cast<int>(compressed_size),
            static_cast<int>(uncompressed_size_hint)
        );
        if (decompressed_size < 0) {
            LOG_ERROR("[CompressionManager::decompress] LZ4_decompress_safe failed with error code: {}", decompressed_size);
            throw std::runtime_error("LZ4_decompress_safe failed");
        }
        if (static_cast<size_t>(decompressed_size) != uncompressed_size_hint) {
             LOG_WARN("[CompressionManager::decompress] LZ4_decompress_safe: Actual decompressed size ({}) != hint ({}). Resizing output.", decompressed_size, uncompressed_size_hint);
             decompressed_buffer.resize(decompressed_size);
        }
        LOG_TRACE("[CompressionManager::decompress] LZ4 decompression successful. Compressed size: {}, Decompressed size: {}", compressed_size, decompressed_size);
        return decompressed_buffer;
    }
    LOG_WARN("[CompressionManager::decompress] Unsupported compression type: {}. Returning as is (likely error).", static_cast<int>(type));
    return std::vector<uint8_t>(compressed_data, compressed_data + compressed_size); // Fallback, likely problematic
}

// get_max_compressed_size remains the same
size_t CompressionManager::get_max_compressed_size(size_t uncompressed_size, CompressionType type) {
    if (type == CompressionType::NONE) {
        return uncompressed_size;
    }
    if (type == CompressionType::ZSTD) {
        return ZSTD_compressBound(uncompressed_size);
    }
    return uncompressed_size * 2; 
}