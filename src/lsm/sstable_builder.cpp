// src/lsm/sstable_builder.cpp
#include "../../include/lsm/sstable_builder.h"
#include "../../include/lsm_tree.h" // For static helpers
#include "../../include/storage_error/storage_error.h"
#include "../../include/compression_utils.h"
#include "../../include/encryption_library.h"
#include "../../include/serialization_utils.h"
#include "../../include/debug_utils.h"

#include <filesystem>
#include <iomanip> 

namespace fs = std::filesystem;

namespace engine {
namespace lsm {

SSTableBuilder::SSTableBuilder(const std::string& filepath, const Options& options)
    : options_(options), filepath_(filepath)
{
    meta_ = std::make_shared<SSTableMetadata>();
    meta_->filename = filepath;

    size_t estimated_total_items = (options_.target_block_size / 256) * 100; // Heuristic
    liveness_bloom_filter_ = std::make_shared<BloomFilter>(estimated_total_items, options_.bloom_filter_fpr);

    output_file_.open(filepath, std::ios::binary | std::ios::trunc);
    if (!output_file_) {
        throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Failed to open SSTable file for builder.")
            .withFilePath(filepath);
    }
    
    size_t estimated_items = (options_.target_block_size / 256) * options_.blocks_per_filter_partition;
    current_filter_partition_ = std::make_unique<BloomFilter>(estimated_items > 100 ? estimated_items : 1000, options_.bloom_filter_fpr);
}

SSTableBuilder::~SSTableBuilder() {
    if (!finished_ && output_file_.is_open()) {
        output_file_.close();
        fs::remove(filepath_);
    }
}

uint64_t SSTableBuilder::getRecordCount() const {
    return meta_ ? meta_->total_entries : 0;
}

std::shared_ptr<BloomFilter> SSTableBuilder::getLivenessBloomFilter() const {
    if (!finished_) {
        // Or return nullptr, but throwing is safer for contract violations.
        throw std::runtime_error("Liveness filter is not available until SSTableBuilder is finished.");
    }
    return liveness_bloom_filter_;
}

void SSTableBuilder::add(const Record& record) {
    if (finished_) {
        throw std::runtime_error("Cannot add records to a finished SSTableBuilder.");
    }
    if (!meta_->max_key.empty() && record.key <= meta_->max_key) {
        throw storage::StorageError(storage::ErrorCode::INVALID_KEY, "Keys must be added in strictly increasing order.")
            .withDetails("Last key: '" + meta_->max_key + "', New key: '" + record.key + "'");
    }
    if (!record.deleted) {
        liveness_bloom_filter_->add(record.key);
    }
    current_filter_partition_->add(record.key);

    std::ostringstream record_stream(std::ios::binary);
    LSMTree::serializeRecord(record_stream, record);
    std::string serialized_record = record_stream.str();
    
    if (!block_buffer_.empty() && (block_buffer_.size() + serialized_record.length() > options_.target_block_size)) {
        writeDataBlock();
    }

    if (block_buffer_.empty()) {
        first_key_in_block_ = record.key;
    }

    block_buffer_.insert(block_buffer_.end(), serialized_record.begin(), serialized_record.end());
    records_in_block_++;

    if (meta_->min_key.empty()) meta_->min_key = record.key;
    meta_->max_key = record.key;
    meta_->total_entries++;
    if (record.deleted) meta_->tombstone_entries++;
    if (record.commit_txn_id != 0) {
        meta_->min_txn_id = std::min(meta_->min_txn_id, record.commit_txn_id);
        meta_->max_txn_id = std::max(meta_->max_txn_id, record.commit_txn_id);
    }
    current_filter_partition_->add(record.key);
    if (first_key_in_filter_partition_.empty()) {
        first_key_in_filter_partition_ = record.key;
    }
}

void SSTableBuilder::finish() {
    if (finished_) return;

    LOG_INFO("[SSTableBuilder] Finishing SSTable file: {}", filepath_);

    try {
        // --- Phase 1: Flush any remaining buffered data ---
        if (!block_buffer_.empty()) {
            LOG_TRACE("  [Builder Finish] Flushing final data block ({} records, {} bytes).", records_in_block_, block_buffer_.size());
            writeDataBlock();
        }
        if (blocks_in_filter_partition_ > 0) {
            LOG_TRACE("  [Builder Finish] Flushing final filter partition ({} blocks).", blocks_in_filter_partition_);
            flushFilterPartition();
        }
        
        // --- Phase 2: Write the main metadata block (if any data was added) ---
        if (meta_->total_entries > 0) {
            std::streampos metadata_start_offset = output_file_.tellp();
            LOG_TRACE("  [Builder Finish] Writing main metadata block at offset {}.", static_cast<long long>(metadata_start_offset));
            
            // The existing writeIndexAndFooter logic is now for the main metadata block
            writeIndexAndFooter(l1_block_index_temp_);
            
            // --- Phase 3: Write the Postscript ---
            SSTablePostscript postscript;
            postscript.metadata_block_offset = static_cast<uint64_t>(metadata_start_offset);
            postscript.magic_number = SSTABLE_MAGIC_NUMBER;
            
            std::streampos postscript_start_offset = output_file_.tellp();
            LOG_TRACE("  [Builder Finish] Writing postscript at offset {}. MetadataOffset={}, Magic={:#x}",
                      static_cast<long long>(postscript_start_offset), postscript.metadata_block_offset, postscript.magic_number);
            
            output_file_.write(reinterpret_cast<const char*>(&postscript), SSTABLE_POSTSCRIPT_SIZE);
            if (!output_file_) {
                throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Failed to write SSTable postscript.");
            }
        }

        // --- Phase 4: Finalize File ---
        output_file_.flush();
        output_file_.close();
        if (output_file_.fail()) {
            throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Stream error after closing SSTable file.");
        }

        if (meta_->total_entries > 0) {
            meta_->size_bytes = fs::file_size(filepath_);
            LOG_INFO("[SSTableBuilder] Finished successfully. Final file size: {} bytes, Total entries: {}", meta_->size_bytes, meta_->total_entries);
        } else {
            LOG_INFO("[SSTableBuilder] No records were added. Deleting empty file: {}", filepath_);
            fs::remove(filepath_);
            meta_->size_bytes = 0;
        }
        finished_ = true;

    } catch (const std::exception& e) {
        LOG_ERROR("[SSTableBuilder] CRITICAL FAILURE during finish() for '{}': {}. Cleaning up.", filepath_, e.what());
        if (output_file_.is_open()) output_file_.close();
        fs::remove(filepath_); // Ensure temp file is deleted on any failure
        finished_ = true; // Mark as finished to prevent dtor from trying again
        throw;
    }
}

std::shared_ptr<SSTableMetadata> SSTableBuilder::getFileMetadata() const {
    if (!finished_) {
        throw std::runtime_error("SSTable must be finished before getting metadata.");
    }
    return meta_;
}

// --- Private Helpers ---

void SSTableBuilder::writeDataBlock() {
    if (block_buffer_.empty()) return;

    SSTableBlockIndexEntry block_entry;
    writeSSTableDataBlock(block_buffer_, records_in_block_, block_entry);
    l1_block_index_temp_[first_key_in_block_] = block_entry;

    block_buffer_.clear();
    records_in_block_ = 0;
    
    blocks_in_filter_partition_++;
    if (blocks_in_filter_partition_ >= options_.blocks_per_filter_partition) {
        flushFilterPartition();
    }
}

void SSTableBuilder::flushFilterPartition() {
    if (blocks_in_filter_partition_ == 0) return;
    std::streampos offset = output_file_.tellp();
    current_filter_partition_->serializeToStream(output_file_);
    uint32_t size = static_cast<uint32_t>(static_cast<std::streampos>(output_file_.tellp()) - offset);
    meta_->filter_partition_index[first_key_in_filter_partition_] = {offset, size};

    size_t estimated_items = (options_.target_block_size / 256) * options_.blocks_per_filter_partition;
    current_filter_partition_ = std::make_unique<BloomFilter>(estimated_items, options_.bloom_filter_fpr);
    first_key_in_filter_partition_.clear();
    blocks_in_filter_partition_ = 0;
}

void SSTableBuilder::writeSSTableDataBlock(
    std::vector<uint8_t>& uncompressed_block_buffer,
    uint32_t num_records,
    SSTableBlockIndexEntry& out_entry)
{
    SSTableDataBlockHeader header;
    header.num_records_in_block = num_records;
    header.uncompressed_content_size_bytes = static_cast<uint32_t>(uncompressed_block_buffer.size());

    // --- Step 1: Compression ---
    std::vector<uint8_t> data_after_compression = uncompressed_block_buffer;
    CompressionType compression_used = CompressionType::NONE;
    if (options_.compression_type != CompressionType::NONE && !uncompressed_block_buffer.empty()) {
        try {
            std::vector<uint8_t> compressed_output = CompressionManager::compress(
                uncompressed_block_buffer.data(), uncompressed_block_buffer.size(),
                options_.compression_type, options_.compression_level);
            
            if (compressed_output.size() < uncompressed_block_buffer.size()) {
                data_after_compression = std::move(compressed_output);
                compression_used = options_.compression_type;
            }
        } catch (const std::exception& e) {
            LOG_WARN("[SSTableBuilder] Error compressing block for '{}': {}. Storing uncompressed.", filepath_, e.what());
        }
    }

    // --- Step 2: Encryption ---
    std::vector<uint8_t> final_payload_for_disk;
    EncryptionScheme encryption_used = EncryptionScheme::NONE;
    
    if (options_.encryption_is_active && options_.encryption_scheme != EncryptionScheme::NONE) {
        if (!options_.dek || options_.dek->empty()) {
            throw storage::StorageError(storage::ErrorCode::INVALID_CONFIGURATION, "DEK is missing or empty, cannot encrypt SSTable block.");
        }
        encryption_used = options_.encryption_scheme;

        try {
            auto block_iv = EncryptionLibrary::generateIV();
            std::memcpy(header.iv, block_iv.data(), std::min(block_iv.size(), sizeof(header.iv)));
            
            // Reconstruct AAD for GCM verification
            header.set_flags(compression_used, encryption_used);
            std::vector<uint8_t> aad_data;
            aad_data.push_back(header.flags);
            // ... (add other fields to AAD as needed) ...
            
            EncryptionLibrary::EncryptedData encrypted_out = EncryptionLibrary::encryptWithAES_GCM(
                data_after_compression, *options_.dek, block_iv, aad_data
            );
            
            std::memcpy(header.auth_tag, encrypted_out.tag.data(), sizeof(header.auth_tag));
            final_payload_for_disk = std::move(encrypted_out.data);

        } catch (const CryptoException& ce) {
            LOG_ERROR("[SSTableBuilder] CryptoException during block encryption for '{}': {}", filepath_, ce.what());
            throw;
        }
    } else {
        final_payload_for_disk = std::move(data_after_compression);
    }

    // --- Step 3 & 4: Finalize Header, Checksum, and Write ---
    header.set_flags(compression_used, encryption_used);
    header.compressed_payload_size_bytes = static_cast<uint32_t>(final_payload_for_disk.size());
    header.payload_checksum = calculate_payload_checksum(final_payload_for_disk.data(), final_payload_for_disk.size());

    out_entry.block_offset_in_file = output_file_.tellp();
    output_file_.write(reinterpret_cast<const char*>(&header), SSTABLE_DATA_BLOCK_HEADER_SIZE);
    
    if (!final_payload_for_disk.empty()) {
        output_file_.write(reinterpret_cast<const char*>(final_payload_for_disk.data()), final_payload_for_disk.size());
    }
    
    if (!output_file_) {
        throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Failed to write SSTable data block.")
            .withContext("sstable_file", filepath_);
    }
    
    // --- Step 5: Populate the output Index Entry ---
    out_entry.num_records = header.num_records_in_block;
    out_entry.compression_type = header.get_compression_type();
    out_entry.encryption_scheme = header.get_encryption_scheme();
    out_entry.uncompressed_payload_size_bytes = header.uncompressed_content_size_bytes;
    out_entry.compressed_payload_size_bytes = header.compressed_payload_size_bytes;
}

void SSTableBuilder::writeIndexAndFooter(const std::map<std::string, SSTableBlockIndexEntry>& l1_index) {
    if (!output_file_.is_open() || !output_file_.good()) {
        throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Output file stream not ready for writing builder index/footer.")
            .withFilePath(filepath_);
    }
    LOG_INFO("[SSTableBuilder] Writing index and footer for SSTable: {}", filepath_);

    // --- 1. Serialize and Write the Filter Partition Index ---
    const std::streampos filter_index_start_pos = output_file_.tellp();
    const uint32_t num_filter_partitions = static_cast<uint32_t>(meta_->filter_partition_index.size());
    
    LOG_TRACE("  [Footer Phase 1] Writing Filter Partition Index. Start Offset: {}, Num Partitions: {}",
              static_cast<long long>(filter_index_start_pos), num_filter_partitions);

    output_file_.write(reinterpret_cast<const char*>(&num_filter_partitions), sizeof(num_filter_partitions));
    
    for (const auto& [first_key, entry] : meta_->filter_partition_index) {
        SerializeString(output_file_, first_key);

        // --- FIX: Serialize struct members individually, not the whole struct ---
        // The previous code was: output_file_.write(reinterpret_cast<const char*>(&entry), sizeof(engine::lsm::FilterPartitionIndexEntry));
        
        // Convert std::streampos to a fixed-size, portable type (std::streamoff) for writing.
        std::streamoff offset_to_write = static_cast<std::streamoff>(entry.partition_offset);

        output_file_.write(reinterpret_cast<const char*>(&offset_to_write), sizeof(offset_to_write));
        output_file_.write(reinterpret_cast<const char*>(&entry.partition_size_bytes), sizeof(entry.partition_size_bytes));
        // --- END FIX ---

        LOG_TRACE("    - Filter Partition Index Entry: Key='{}', Offset={}, Size={}",
                format_key_for_print(first_key), static_cast<long long>(entry.partition_offset), entry.partition_size_bytes);
    }
    if (!output_file_) throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Stream error after writing filter partition index.").withFilePath(filepath_);

    // --- 2. Partition and Write L1 Block Indexes, while building the L2 Index ---
    std::map<std::string, std::streampos> l2_index_in_progress;
    auto it = l1_index.begin();
    int l1_block_count = 0;

    LOG_TRACE("  [Footer Phase 2] Partitioning and writing L1 index blocks...");
    while (it != l1_index.end()) {
        const std::string first_key_of_l1_block = it->first;
        const std::streampos l1_block_start_pos = output_file_.tellp();
        l2_index_in_progress[first_key_of_l1_block] = l1_block_start_pos;
        l1_block_count++;

        std::ostringstream l1_block_stream(std::ios::binary);
        size_t entries_in_this_block = 0;
        for (size_t i = 0; i < SSTableBuilder::ENTRIES_PER_L1_INDEX_BLOCK && it != l1_index.end(); ++i, ++it) {
            SerializeString(l1_block_stream, it->first);
            l1_block_stream.write(reinterpret_cast<const char*>(&it->second), sizeof(SSTableBlockIndexEntry));
            entries_in_this_block++;
        }
        
        const std::string l1_block_data = l1_block_stream.str();
        const uint32_t num_entries_header = static_cast<uint32_t>(entries_in_this_block);
        
        LOG_TRACE("    - Writing L1 Index Block #{}. Start Offset: {}, Entries: {}, Data Size: {} bytes",
                  l1_block_count, static_cast<long long>(l1_block_start_pos), num_entries_header, l1_block_data.size());

        output_file_.write(reinterpret_cast<const char*>(&num_entries_header), sizeof(num_entries_header));
        output_file_.write(l1_block_data.data(), l1_block_data.size());
    }
    if (!output_file_) throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Stream error after writing L1 index blocks.").withFilePath(filepath_);

    // --- 3. Serialize and Write the L2 Block Index ---
    const std::streampos l2_block_index_start_pos = output_file_.tellp();
    const uint32_t num_l2_entries = static_cast<uint32_t>(l2_index_in_progress.size());

    LOG_TRACE("  [Footer Phase 3] Writing L2 Block Index. Start Offset: {}, Entries: {}",
              static_cast<long long>(l2_block_index_start_pos), num_l2_entries);

    output_file_.write(reinterpret_cast<const char*>(&num_l2_entries), sizeof(num_l2_entries));
    for (const auto& [first_key, offset] : l2_index_in_progress) {
        SerializeString(output_file_, first_key);
        output_file_.write(reinterpret_cast<const char*>(&offset), sizeof(offset));
    }
    if (!output_file_) throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Stream error after writing L2 index.").withFilePath(filepath_);

    // --- 4. & 5. TxnID Range and Entry Counts ---
    const std::streampos txn_range_start_pos = output_file_.tellp();
    output_file_.write(reinterpret_cast<const char*>(&meta_->min_txn_id), sizeof(TxnId));
    output_file_.write(reinterpret_cast<const char*>(&meta_->max_txn_id), sizeof(TxnId));
    LOG_TRACE("  [Footer Phase 4] Writing TxnID Range. Start Offset: {}, MinTxn: {}, MaxTxn: {}",
              static_cast<long long>(txn_range_start_pos), meta_->min_txn_id, meta_->max_txn_id);

    const std::streampos entry_counts_start_pos = output_file_.tellp();
    output_file_.write(reinterpret_cast<const char*>(&meta_->total_entries), sizeof(uint64_t));
    output_file_.write(reinterpret_cast<const char*>(&meta_->tombstone_entries), sizeof(uint64_t));
    LOG_TRACE("  [Footer Phase 5] Writing Entry Counts. Start Offset: {}, Total: {}, Tombstones: {}",
              static_cast<long long>(entry_counts_start_pos), meta_->total_entries, meta_->tombstone_entries);
    if (!output_file_) throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Stream error after writing counts.").withFilePath(filepath_);

    // --- 6. Write the Final Footer (Pointers Section) ---
    const std::streampos footer_pointers_start_pos = output_file_.tellp();
    output_file_.write(reinterpret_cast<const char*>(&filter_index_start_pos), sizeof(std::streampos));
    output_file_.write(reinterpret_cast<const char*>(&l2_block_index_start_pos), sizeof(std::streampos));
    output_file_.write(reinterpret_cast<const char*>(&txn_range_start_pos), sizeof(std::streampos));
    output_file_.write(reinterpret_cast<const char*>(&entry_counts_start_pos), sizeof(std::streampos));
    LOG_TRACE("  [Footer Phase 6] Writing final footer pointers at offset {}.", static_cast<long long>(footer_pointers_start_pos));
    
    if (!output_file_) {
        throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Stream error while writing final footer pointers.")
            .withContext("sstable_file", filepath_);
    }
    
    // --- 7. Final Flush ---
    output_file_.flush();
    if (!output_file_) {
        throw storage::StorageError(storage::ErrorCode::IO_FLUSH_ERROR, "Failed to flush SSTable after writing all metadata.")
            .withContext("sstable_file", filepath_);
    }
    LOG_INFO("[SSTableBuilder] Successfully written and flushed all metadata for SSTable: {}", filepath_);
}

} // namespace lsm
} // namespace engine