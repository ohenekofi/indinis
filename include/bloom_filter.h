// @include/bloom_filter.h (Let's assume this is the filename)
#pragma once // Add pragma once

#include <vector>
#include <string>
#include <cmath>
#include <fstream>
#include <iostream>
#include <functional>
// #include <bitset> // Not directly used, std::vector<bool> is okay
#include <mutex>
#include <memory>
#include <random>
#include <algorithm> // For std::fill

// Forward declaration for logging (optional)
// #include "debug_utils.h" // If you want to use LOG_TRACE etc.

class BloomFilter {
private:
    std::vector<bool> bitArray;
    size_t arraySize;
    size_t hashFunctionCount;
    size_t itemCount; // Number of items *added*, not current distinct items
    std::vector<uint64_t> hashSeeds;
    mutable std::mutex mutex; // For thread safety

    // MurmurHash3 implementation (unchanged from your provided code)
    uint64_t murmurHash3(const std::string& key, uint64_t seed) const {
        // ... (your MurmurHash3 implementation remains here) ...
        const uint64_t c1 = 0xff51afd7ed558ccd;
        const uint64_t c2 = 0xc4ceb9fe1a85ec53;
        
        uint64_t h1 = seed;
        uint64_t h2 = seed;
        
        const uint8_t* data = reinterpret_cast<const uint8_t*>(key.c_str());
        const int nblocks = key.size() / 16;
        
        const uint64_t* blocks = reinterpret_cast<const uint64_t*>(data);
        
        for (int i = 0; i < nblocks; i++) {
            uint64_t k1 = blocks[i*2];
            uint64_t k2 = blocks[i*2+1];
            
            k1 *= c1;
            k1 = (k1 << 31) | (k1 >> 33);
            k1 *= c2;
            h1 ^= k1;
            
            h1 = (h1 << 27) | (h1 >> 37);
            h1 += h2;
            h1 = h1*5 + 0x52dce729;
            
            k2 *= c2;
            k2 = (k2 << 33) | (k2 >> 31);
            k2 *= c1;
            h2 ^= k2;
            
            h2 = (h2 << 31) | (h2 >> 33);
            h2 += h1;
            h2 = h2*5 + 0x38495ab5;
        }
        
        const uint8_t* tail = data + nblocks*16;
        uint64_t k1_tail = 0; // Renamed to avoid conflict if k1 is used above
        uint64_t k2_tail = 0; // Renamed
        
        switch (key.size() & 15) {
            case 15: k2_tail ^= uint64_t(tail[14]) << 48; // Fall through
            case 14: k2_tail ^= uint64_t(tail[13]) << 40; // Fall through
            case 13: k2_tail ^= uint64_t(tail[12]) << 32; // Fall through
            case 12: k2_tail ^= uint64_t(tail[11]) << 24; // Fall through
            case 11: k2_tail ^= uint64_t(tail[10]) << 16; // Fall through
            case 10: k2_tail ^= uint64_t(tail[9]) << 8;   // Fall through
            case 9:  k2_tail ^= uint64_t(tail[8]) << 0;   // Fall through
                    k2_tail *= c2;
                    k2_tail = (k2_tail << 33) | (k2_tail >> 31);
                    k2_tail *= c1;
                    h2 ^= k2_tail;
            // No fall through from case 9 to case 8
            case 8:  k1_tail ^= uint64_t(tail[7]) << 56; // Fall through
            case 7:  k1_tail ^= uint64_t(tail[6]) << 48; // Fall through
            case 6:  k1_tail ^= uint64_t(tail[5]) << 40; // Fall through
            case 5:  k1_tail ^= uint64_t(tail[4]) << 32; // Fall through
            case 4:  k1_tail ^= uint64_t(tail[3]) << 24; // Fall through
            case 3:  k1_tail ^= uint64_t(tail[2]) << 16; // Fall through
            case 2:  k1_tail ^= uint64_t(tail[1]) << 8;  // Fall through
            case 1:  k1_tail ^= uint64_t(tail[0]) << 0;  // Fall through
                    k1_tail *= c1;
                    k1_tail = (k1_tail << 31) | (k1_tail >> 33);
                    k1_tail *= c2;
                    h1 ^= k1_tail;
        };
        
        h1 ^= key.size();
        h2 ^= key.size();
        
        h1 += h2;
        h2 += h1;
        
        // Final mix
        h1 ^= h1 >> 33;
        h1 *= 0xff51afd7ed558ccd;
        h1 ^= h1 >> 33;
        h1 *= 0xc4ceb9fe1a85ec53;
        h1 ^= h1 >> 33;
        
        h2 ^= h2 >> 33;
        h2 *= 0xff51afd7ed558ccd;
        h2 ^= h2 >> 33;
        h2 *= 0xc4ceb9fe1a85ec53;
        h2 ^= h2 >> 33;
        
        return h1 + h2;
    }
    
    std::vector<size_t> getHashedIndices(const std::string& item) const {
        std::vector<size_t> indices(hashFunctionCount);
        for (size_t i = 0; i < hashFunctionCount; ++i) {
            indices[i] = murmurHash3(item, hashSeeds[i]) % arraySize;
        }
        return indices;
    }

public:
    // Constructor with capacity and false positive rate
    BloomFilter(size_t expectedItems, double falsePositiveRate) 
        : itemCount(0) {
        if (expectedItems == 0) { // Handle case of 0 items to avoid division by zero
            arraySize = 1024; // A small default size
            hashFunctionCount = 3; // A small default hash count
        } else {
            arraySize = calculateOptimalSize(expectedItems, falsePositiveRate);
            hashFunctionCount = calculateOptimalHashFunctions(expectedItems, arraySize);
        }
        if (arraySize == 0) arraySize = 1; // Ensure arraySize is at least 1
        if (hashFunctionCount == 0) hashFunctionCount = 1; // Ensure hashFunctionCount is at least 1
        bitArray.resize(arraySize, false);
        initializeHashSeeds();
    }
    
    // Constructor with specified parameters (e.g., for deserialization)
    BloomFilter(size_t size, size_t numHashFunctions) 
        : arraySize(size), hashFunctionCount(numHashFunctions), itemCount(0) {
        if (arraySize == 0) arraySize = 1; 
        if (hashFunctionCount == 0) hashFunctionCount = 1;
        bitArray.resize(arraySize, false);
        initializeHashSeeds(); // Seeds will be overwritten during deserialization if loading
    }

    // Default constructor - needed for unique_ptr in SSTable if created later
    BloomFilter() : arraySize(1024), hashFunctionCount(3), itemCount(0) {
        bitArray.resize(arraySize, false);
        initializeHashSeeds();
    }
    
    // Copy constructor (deep copy)
    BloomFilter(const BloomFilter& other) {
        std::lock_guard<std::mutex> lock(other.mutex); // Lock other during copy
        arraySize = other.arraySize;
        hashFunctionCount = other.hashFunctionCount;
        itemCount = other.itemCount;
        hashSeeds = other.hashSeeds; // Copy seeds
        bitArray = other.bitArray;   // Copy bit array
    }
    
    // Move constructor
    BloomFilter(BloomFilter&& other) noexcept {
        std::lock_guard<std::mutex> lock(other.mutex); // Lock other during move
        bitArray = std::move(other.bitArray);
        arraySize = other.arraySize;
        hashFunctionCount = other.hashFunctionCount;
        itemCount = other.itemCount;
        hashSeeds = std::move(other.hashSeeds);
        
        // Reset other to a valid empty state
        other.arraySize = 0; // Or a small default
        other.hashFunctionCount = 0;
        other.itemCount = 0;
        // other.bitArray is already moved from
        // other.hashSeeds is already moved from
    }

    // Copy assignment
    BloomFilter& operator=(const BloomFilter& other) {
        if (this == &other) {
            return *this;
        }
        std::scoped_lock locks(mutex, other.mutex); // Lock both for assignment
        arraySize = other.arraySize;
        hashFunctionCount = other.hashFunctionCount;
        itemCount = other.itemCount;
        hashSeeds = other.hashSeeds;
        bitArray = other.bitArray;
        return *this;
    }

    // Move assignment
    BloomFilter& operator=(BloomFilter&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        std::scoped_lock locks(mutex, other.mutex); // Lock both for assignment
        bitArray = std::move(other.bitArray);
        arraySize = other.arraySize;
        hashFunctionCount = other.hashFunctionCount;
        itemCount = other.itemCount;
        hashSeeds = std::move(other.hashSeeds);

        other.arraySize = 0;
        other.hashFunctionCount = 0;
        other.itemCount = 0;
        return *this;
    }
    
    void initializeHashSeeds() {
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> dis;
        
        hashSeeds.resize(hashFunctionCount);
        for (size_t i = 0; i < hashFunctionCount; ++i) {
            hashSeeds[i] = dis(gen);
        }
    }
    
    static size_t calculateOptimalSize(size_t n, double p) {
        if (n == 0 || p <= 0.0 || p >= 1.0) return 1024; // Default for invalid inputs
        return static_cast<size_t>(std::abs(-static_cast<double>(n) * std::log(p) / (std::log(2.0) * std::log(2.0))));
    }
    
    static size_t calculateOptimalHashFunctions(size_t n, size_t m) {
        if (n == 0 || m == 0) return 3; // Default for invalid inputs
        return static_cast<size_t>(std::max(1.0, static_cast<double>(m) / n * std::log(2.0)));
    }
    
    void add(const std::string& item) {
        std::lock_guard<std::mutex> lock(mutex);
        std::vector<size_t> indices = getHashedIndices(item);
        for (size_t index : indices) {
            if (index < arraySize) { // Bounds check
                bitArray[index] = true;
            }
        }
        itemCount++;
    }
    
    bool mightContain(const std::string& item) const {
        std::lock_guard<std::mutex> lock(mutex);
        if (arraySize == 0) return false; // Or true, depending on desired behavior for empty filter
        std::vector<size_t> indices = getHashedIndices(item);
        for (size_t index : indices) {
            if (index >= arraySize || !bitArray[index]) { // Bounds check
                return false; 
            }
        }
        return true;
    }
    
    void merge(const BloomFilter& other) {
        std::scoped_lock locks(mutex, other.mutex);
        
        if (arraySize != other.arraySize || hashFunctionCount != other.hashFunctionCount) {
            // TODO: Decide on behavior or log an error/warning
            // For now, we'll throw as merging incompatible filters is problematic.
            throw std::invalid_argument("Cannot merge Bloom filters with different parameters (size/hash_count)");
        }
         // A more robust merge would also need to reconcile hashSeeds if they could differ
         // for filters with same size/hash_count. For now, assume seeds are compatible if params are.
        
        for (size_t i = 0; i < arraySize; ++i) {
            bitArray[i] = bitArray[i] || other.bitArray[i];
        }
        
        itemCount += other.itemCount; // This sum is an approximation of unique items.
    }
    
    void clear() {
        std::lock_guard<std::mutex> lock(mutex);
        std::fill(bitArray.begin(), bitArray.end(), false);
        itemCount = 0;
    }
    
    // --- NEW: Stream Serialization/Deserialization ---
    bool serializeToStream(std::ostream& out) const {
        std::lock_guard<std::mutex> lock(mutex);
        // Write metadata
        out.write(reinterpret_cast<const char*>(&arraySize), sizeof(arraySize));
        out.write(reinterpret_cast<const char*>(&hashFunctionCount), sizeof(hashFunctionCount));
        out.write(reinterpret_cast<const char*>(&itemCount), sizeof(itemCount)); // Persist itemCount
        
        // Write hash seeds
        for (uint64_t seed : hashSeeds) {
            out.write(reinterpret_cast<const char*>(&seed), sizeof(seed));
        }
        
        // Write bit array (pack 8 bits into each byte)
        size_t packedSize = (arraySize + 7) / 8;
        std::vector<char> packedBits(packedSize, 0); // Initialize with 0
        for (size_t i = 0; i < arraySize; ++i) {
            if (bitArray[i]) {
                packedBits[i / 8] |= (1 << (i % 8));
            }
        }
        if (packedSize > 0) { // Only write if there's data
            out.write(packedBits.data(), packedSize);
        }
        return out.good();
    }

    bool deserializeFromStream(std::istream& in) {
        std::lock_guard<std::mutex> lock(mutex);
        // Read metadata
        size_t tempArraySize, tempHashCount, tempItemCount;
        if (!in.read(reinterpret_cast<char*>(&tempArraySize), sizeof(tempArraySize))) return false;
        if (!in.read(reinterpret_cast<char*>(&tempHashCount), sizeof(tempHashCount))) return false;
        if (!in.read(reinterpret_cast<char*>(&tempItemCount), sizeof(tempItemCount))) return false;
        
        arraySize = tempArraySize;
        hashFunctionCount = tempHashCount;
        itemCount = tempItemCount;

        if (arraySize == 0) arraySize = 1; // Sanity check for loaded values
        if (hashFunctionCount == 0) hashFunctionCount = 1;

        // Read hash seeds
        hashSeeds.resize(hashFunctionCount);
        for (size_t i = 0; i < hashFunctionCount; ++i) {
            if (!in.read(reinterpret_cast<char*>(&hashSeeds[i]), sizeof(hashSeeds[i]))) return false;
        }
        
        // Read bit array
        bitArray.assign(arraySize, false); // Resize and initialize to false
        size_t packedSize = (arraySize + 7) / 8;
        if (packedSize > 0) { // Only read if there should be data
            std::vector<char> packedBits(packedSize);
            if (!in.read(packedBits.data(), packedSize)) return false;
            
            for (size_t i = 0; i < arraySize; ++i) {
                bitArray[i] = (packedBits[i / 8] & (1 << (i % 8))) != 0;
            }
        }
        return in.good();
    }
    // --- END NEW ---

    // Save to file (uses new serializeToStream)
    bool saveToFile(const std::string& filename) const {
        std::ofstream file(filename, std::ios::binary | std::ios::trunc);
        if (!file) {
            return false;
        }
        return serializeToStream(file);
    }
    
    // Load from file (uses new deserializeFromStream)
    bool loadFromFile(const std::string& filename) {
        std::ifstream file(filename, std::ios::binary);
        if (!file) {
            return false;
        }
        return deserializeFromStream(file);
    }
    
    double estimateFalsePositiveRate() const {
        std::lock_guard<std::mutex> lock(mutex);
        if (itemCount == 0 || arraySize == 0) return 0.0;
        
        double p_bit_zero = 1.0 - 1.0 / static_cast<double>(arraySize);
        double p_all_zero_after_inserts = std::pow(p_bit_zero, static_cast<double>(itemCount * hashFunctionCount));
        return std::pow(1.0 - p_all_zero_after_inserts, static_cast<double>(hashFunctionCount));
    }
    
    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex);
        return itemCount;
    }
    
    size_t getArraySize() const { // No lock needed, immutable after construction/deserialization
        return arraySize;
    }
    
    size_t getHashFunctionCount() const { // No lock needed
        return hashFunctionCount;
    }
    
    size_t getMemoryUsageBytes() const { // No lock needed
        return (arraySize + 7) / 8; 
    }
};