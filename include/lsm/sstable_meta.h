// include/lsm/sstable_meta.h
#pragma once

#include "../../include/types.h"      // For TxnId, Record, etc.
#include "../../include/bloom_filter.h" // For BloomFilter
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <limits>
#include <set>
#include <chrono>

// Forward-declare LSMTree to resolve a circular dependency if scanSSTableForPrefix needs it.
class LSMTree;

namespace engine {
namespace lsm {

 
#pragma pack(push, 1)
struct FilterPartitionIndexEntry {
    std::streampos partition_offset;
    uint32_t partition_size_bytes;
};
#pragma pack(pop)

#pragma pack(push, 1)
struct SSTablePostscript {
    uint64_t metadata_block_offset; // Offset from the beginning of the file to the start of the metadata block.
    uint32_t magic_number;          // Magic number to validate the footer.

    // Default constructor for initialization
    SSTablePostscript() : metadata_block_offset(0), magic_number(0) {}
};
#pragma pack(pop)
static_assert(sizeof(SSTablePostscript) == sizeof(uint64_t) + sizeof(uint32_t), "SSTablePostscript size mismatch.");
static constexpr size_t SSTABLE_POSTSCRIPT_SIZE = sizeof(SSTablePostscript);
static constexpr uint32_t SSTABLE_MAGIC_NUMBER = 0x53535442; // "SSTB" in ASCII


// This is now the ONE TRUE definition of SSTableMetadata.
struct SSTableMetadata {
    std::string filename;
    uint64_t sstable_id;
    int level;
    size_t size_bytes;
    TxnId min_txn_id;
    TxnId max_txn_id;
    std::string min_key;
    std::string max_key;
    
    // The in-memory L2 index of on-disk L1 index blocks.
    std::map<std::string, std::streampos> block_index;
    
    // The in-memory index of on-disk filter partitions.
    std::map<std::string, FilterPartitionIndexEntry> filter_partition_index;

    // A lazy-loading cache for filter partitions.
    mutable std::mutex filter_cache_mutex_;
    mutable std::map<std::string, std::shared_ptr<BloomFilter>> filter_cache_;

    std::chrono::system_clock::time_point creation_timestamp;
    uint64_t total_entries = 0;
    uint64_t tombstone_entries = 0;

    SSTableMetadata(uint64_t id = 0, int lvl = -1)
        : sstable_id(id), 
          level(lvl), 
          size_bytes(0),
          min_txn_id(std::numeric_limits<TxnId>::max()), 
          max_txn_id(0),
          creation_timestamp(std::chrono::system_clock::now()),
          total_entries(0),
          tombstone_entries(0)
    {}

    // Rule of 5
    SSTableMetadata(const SSTableMetadata&) = delete;
    SSTableMetadata& operator=(const SSTableMetadata&) = delete;
    SSTableMetadata(SSTableMetadata&&) = default;
    SSTableMetadata& operator=(SSTableMetadata&&) = default;
};

} // namespace lsm
} // namespace engine