// include/lsm/sstable_builder.h
#pragma once

#include "../types.h"
#include "sstable_meta.h"
#include <string>
#include <fstream>
#include <memory>
#include <map>
#include <vector>

namespace engine {
namespace lsm {

/**
 * @class SSTableBuilder
 * @brief A standalone utility for creating new SSTable files from sorted key-value pairs.
 */
class SSTableBuilder {
public:
    struct Options {
        CompressionType compression_type = CompressionType::NONE;
        int compression_level = 0;
        bool encryption_is_active = false;
        EncryptionScheme encryption_scheme = EncryptionScheme::NONE;
        const std::vector<unsigned char>* dek = nullptr;
        size_t target_block_size = 64 * 1024;
        size_t blocks_per_filter_partition = 64;
        double bloom_filter_fpr = 0.01;
    };

    SSTableBuilder(const std::string& filepath, const Options& options);
    ~SSTableBuilder();

    SSTableBuilder(const SSTableBuilder&) = delete;
    SSTableBuilder& operator=(const SSTableBuilder&) = delete;

    static constexpr size_t ENTRIES_PER_L1_INDEX_BLOCK = 256;
    void add(const Record& record);
    void finish();
    std::shared_ptr<SSTableMetadata> getFileMetadata() const;
    bool isFinished() const { return finished_; }
    uint64_t getRecordCount() const;
    std::shared_ptr<BloomFilter> getLivenessBloomFilter() const;


private:
    void writeDataBlock();
    void flushFilterPartition();
    void writeIndexAndFooter(const std::map<std::string, SSTableBlockIndexEntry>& l1_index);

    // Helpers moved from LSMTree
    void writeSSTableDataBlock(std::vector<uint8_t>& block_buffer, uint32_t num_records, SSTableBlockIndexEntry& out_entry);

    Options options_;
    std::ofstream output_file_;
    std::string filepath_;
    std::shared_ptr<SSTableMetadata> meta_;
    bool finished_ = false;

    std::vector<uint8_t> block_buffer_;
    std::string first_key_in_block_;
    uint32_t records_in_block_ = 0;

    std::unique_ptr<BloomFilter> current_filter_partition_;
    std::string first_key_in_filter_partition_;
    int blocks_in_filter_partition_ = 0;
    
    std::map<std::string, SSTableBlockIndexEntry> l1_block_index_temp_;
    std::shared_ptr<BloomFilter> liveness_bloom_filter_;
};

} // namespace lsm
} // namespace engine