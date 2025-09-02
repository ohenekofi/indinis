// src/lsm/compressed_memtable.cpp

#include "../../include/lsm/compressed_memtable.h"
#include "../../include/compression_utils.h" // Our utility for LZ4/ZSTD
#include "../../include/debug_utils.h"
#include <stdexcept>

namespace engine {
namespace lsm {

// ============================================================================
//   CompressedMemTableIterator Implementation
// ============================================================================

CompressedMemTableIterator::CompressedMemTableIterator(std::unique_ptr<MemTableIterator> underlying_iterator)
    : underlying_iterator_(std::move(underlying_iterator)) {}

bool CompressedMemTableIterator::Valid() const {
    return underlying_iterator_ && underlying_iterator_->Valid();
}

void CompressedMemTableIterator::Next() {
    if (Valid()) {
        underlying_iterator_->Next();
    }
}

void CompressedMemTableIterator::Prev() {
    if (Valid()) {
        underlying_iterator_->Prev();
    }
}

void CompressedMemTableIterator::SeekToFirst() {
    if (underlying_iterator_) {
        underlying_iterator_->SeekToFirst();
    }
}

void CompressedMemTableIterator::SeekToLast() {
    if (underlying_iterator_) {
        underlying_iterator_->SeekToLast();
    }
}

void CompressedMemTableIterator::Seek(const std::string& key) {
    if (underlying_iterator_) {
        underlying_iterator_->Seek(key);
    }
}

Entry CompressedMemTableIterator::GetEntry() const {
    if (!Valid()) {
        throw std::runtime_error("CompressedMemTableIterator::GetEntry called when iterator is not valid.");
    }

    // Get the (potentially compressed) entry from the underlying iterator.
    Entry compressed_entry = underlying_iterator_->GetEntry();
    
    // Decompress the value if it's not a tombstone (empty).
    if (!compressed_entry.value.empty()) {
        try {
            // We assume the underlying value is a blob of bytes.
            std::vector<uint8_t> compressed_vec(compressed_entry.value.begin(), compressed_entry.value.end());
            
            // We need to know the original size. This is a design challenge.
            // A simple approach is to prepend the uncompressed size to the compressed data.
            if (compressed_vec.size() < sizeof(uint32_t)) {
                throw std::runtime_error("Compressed data is too small to contain size prefix.");
            }

            uint32_t uncompressed_size;
            std::memcpy(&uncompressed_size, compressed_vec.data(), sizeof(uint32_t));
            
            std::vector<uint8_t> decompressed_vec = CompressionManager::decompress(
                compressed_vec.data() + sizeof(uint32_t),
                compressed_vec.size() - sizeof(uint32_t),
                uncompressed_size,
                CompressionType::LZ4 // Assuming LZ4
            );
            
            compressed_entry.value = std::string(decompressed_vec.begin(), decompressed_vec.end());
        } catch (const std::exception& e) {
            // If decompression fails, the data is corrupt. Return the compressed data
            // and log an error.
            LOG_ERROR("[CompressedIterator] Decompression failed for key '{}': {}. Returning raw data.",
                      compressed_entry.key, e.what());
        }
    }
    
    return compressed_entry;
}


// ============================================================================
//   CompressedMemTable Implementation
// ============================================================================

CompressedMemTable::CompressedMemTable(std::unique_ptr<MemTableRep> underlying_memtable)
    : underlying_memtable_(std::move(underlying_memtable)) {
    if (!underlying_memtable_) {
        throw std::invalid_argument("Underlying memtable for CompressedMemTable cannot be null.");
    }
}

void CompressedMemTable::Add(const std::string& key, const std::string& value, uint64_t seq) {
    if (value.empty()) {
        // Handle empty string value, which might be a special case (not a tombstone).
        // We will store it uncompressed.
        underlying_memtable_->Add(key, value, seq);
        return;
    }
    
    // 1. Compress the value. We use LZ4 for its high speed.
    std::vector<uint8_t> uncompressed_vec(value.begin(), value.end());
    std::vector<uint8_t> compressed_vec = CompressionManager::compress(
        uncompressed_vec.data(), uncompressed_vec.size(), CompressionType::LZ4);
        
    // 2. Prepend the original (uncompressed) size to the data. This is crucial for decompression.
    uint32_t uncompressed_size = static_cast<uint32_t>(uncompressed_vec.size());
    std::vector<uint8_t> final_payload;
    final_payload.reserve(sizeof(uint32_t) + compressed_vec.size());
    
    const char* size_ptr = reinterpret_cast<const char*>(&uncompressed_size);
    final_payload.insert(final_payload.end(), size_ptr, size_ptr + sizeof(uint32_t));
    final_payload.insert(final_payload.end(), compressed_vec.begin(), compressed_vec.end());

    // 3. Store the size-prefixed compressed blob in the underlying memtable.
    underlying_memtable_->Add(key, std::string(final_payload.begin(), final_payload.end()), seq);
}

void CompressedMemTable::Delete(const std::string& key, uint64_t seq) {
    // Deletes (tombstones) have an empty value, so there's nothing to compress.
    // We pass the operation directly to the underlying memtable.
    underlying_memtable_->Delete(key, seq);
}

bool CompressedMemTable::Get(const std::string& key, std::string& value, uint64_t& seq) const {
    std::string compressed_value;
    if (underlying_memtable_->Get(key, compressed_value, seq)) {
        // A value was found. If it's empty, it could be a tombstone or an intentionally empty string.
        // The underlying Get already handles the tombstone check. So if we get here, it's a real value.
        if (compressed_value.empty()) {
            value = "";
            return true;
        }

        try {
            // Decompress the value using the prepended size.
            const std::vector<uint8_t> compressed_vec(compressed_value.begin(), compressed_value.end());
            
            if (compressed_vec.size() < sizeof(uint32_t)) {
                throw std::runtime_error("Compressed data is too small to contain size prefix.");
            }
            
            uint32_t uncompressed_size;
            std::memcpy(&uncompressed_size, compressed_vec.data(), sizeof(uint32_t));
            
            std::vector<uint8_t> decompressed_vec = CompressionManager::decompress(
                compressed_vec.data() + sizeof(uint32_t),
                compressed_vec.size() - sizeof(uint32_t),
                uncompressed_size,
                CompressionType::LZ4
            );
            
            value = std::string(decompressed_vec.begin(), decompressed_vec.end());
            return true;
        } catch (const std::exception& e) {
            LOG_ERROR("[CompressedMemTable] Decompression failed for key '{}': {}. Data may be corrupt.", key, e.what());
            // If decompression fails, we cannot return a valid value.
            return false;
        }
    }
    return false; // Key not found in underlying memtable.
}

std::unique_ptr<MemTableIterator> CompressedMemTable::NewIterator() const {
    // Create an iterator for the underlying memtable and wrap it in our decompressing iterator.
    return std::make_unique<CompressedMemTableIterator>(underlying_memtable_->NewIterator());
}

// --- Pass-through Methods ---

size_t CompressedMemTable::ApproximateMemoryUsage() const {
    return underlying_memtable_->ApproximateMemoryUsage();
}

size_t CompressedMemTable::Count() const {
    return underlying_memtable_->Count();
}

bool CompressedMemTable::Empty() const {
    return underlying_memtable_->Empty();
}

std::string CompressedMemTable::GetName() const {
    // Append "(Compressed)" to the name of the underlying implementation.
    return underlying_memtable_->GetName() + " (Compressed)";
}

} // namespace lsm
} // namespace engine