// @src/lsm_tree.cpp
#include "../include/lsm_tree.h"
#include "../include/storage_engine.h"
#include "../include/debug_utils.h"
#include "../include/compression_utils.h"
#include "../include/encryption_library.h"
#include "../include/storage_error/storage_error.h"
#include "../include/storage_error/error_codes.h"
#include "../../include/lsm/compaction_manager.h"
#include "../../include/serialization_utils.h"
#include "../include/lsm/compaction_coordinator.h" 
#include "../include/lsm/sstable_reader.h" 
#include "../../include/lsm/sstable_builder.h"
#include "../../include/lsm/liveness_oracle.h"
#include "../../include/lsm/partitioned_memtable.h" 

// Include necessary implementation headers
#include <iostream>
#include <algorithm>
#include <thread>
#include <fstream>
#include <filesystem>
#include <system_error>
#include <cerrno>
#include <cstring>
#include <set>
#include <map>
#include <vector>
#include <string>
#include <iomanip> 
#include <sstream>
#include <stdexcept>
#include <iomanip> // For std::setw, std::setfill with numbers
#include <queue>
#include <cstdint>     // For uint64_t
#include <limits>      // For numeric_limits
#include <optional>
#include <ctime>  
#include <ostream>
#include <variant>
#include <chrono> 
#include <random>

namespace fs = std::filesystem; // Alias for convenience

static constexpr int BLOCKS_PER_FILTER_PARTITION = 64;
static constexpr size_t ENTRIES_PER_L1_INDEX_BLOCK = 256;

const size_t DEFAULT_COMPACTION_THRESHOLD = 4;
const std::chrono::milliseconds DEFAULT_COMPACTION_INTERVAL_MS(10000);
const std::chrono::seconds DEFAULT_BACKOFF_TIME_SECONDS(30);

namespace engine {
namespace lsm {
    // Forward declare if necessary, but includes are better
    class PartitionedMemTable;
    class SSTableBuilder;
    class MemTableIterator;
    struct Entry;
    struct VersionEdit;
}
}

namespace {
    // This anonymous namespace is a good place for static logging at startup
    struct LogDiskSizes {
        LogDiskSizes() {
            LOG_INFO("[DiskFormat] SSTablePostscript size: {} bytes", engine::lsm::SSTABLE_POSTSCRIPT_SIZE);
            LOG_INFO("[DiskFormat] SSTableDataBlockHeader size: {} bytes", SSTABLE_DATA_BLOCK_HEADER_SIZE);
            LOG_INFO("[DiskFormat] FilterPartitionIndexEntry size: {} bytes", sizeof(engine::lsm::FilterPartitionIndexEntry));
        }
    };
    // This object's constructor will run once when the program starts, logging the sizes.
    LogDiskSizes log_disk_sizes_on_startup;
}

static constexpr size_t SSTABLE_FOOTER_POINTERS_SECTION_SIZE =
    sizeof(std::streampos) /* bloom_filter_start_pos */ +
    sizeof(uint64_t)       /* bloom_filter_size_bytes_on_disk */ +
    sizeof(std::streampos) /* block_idx_start_pos */ +
    sizeof(std::streampos);/* txn_id_range_start_pos */

// Size of the TxnID range data itself
static constexpr size_t SSTABLE_TXN_ID_RANGE_DATA_SIZE =
    sizeof(TxnId) /* min_txn_id */ +
    sizeof(TxnId);/* max_txn_id */

std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> find_overlapping_sstables(
    const std::vector<std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>>& levels_snapshot,
    int target_level_idx,
    const std::string& min_key_range,
    const std::string& max_key_range
);

std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> LSMTree::execute_sub_compaction(
    const engine::lsm::SubCompactionTask& task,
    int target_level,
    TxnId oldest_active_txn)
{
    LOG_INFO("[SubCompaction] Starting task for key range ['{}', '{}'). Input files: {}.",
             task.start_key.empty() ? "-inf" : format_key_for_print(task.start_key),
             task.end_key.empty() ? "+inf" : format_key_for_print(task.end_key),
             task.input_files.size());

    // --- Phase 1: Setup Readers and Merge Heap ---
    CompactionState state;
    state.target_output_level = target_level;
    state.effective_gc_txn_id = oldest_active_txn;
    setupCompactionState(state, task.input_files, {});

    if (state.min_heap.empty()) {
        LOG_INFO("[SubCompaction] No valid records in any input files for this task. Task complete.");
        return {};
    }

    // --- Phase 2: Setup for Writing Output ---
    std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> new_files_metadata;
    
    engine::lsm::SSTableBuilder::Options builder_options;
    builder_options.compression_type = sstable_compression_type_;
    builder_options.compression_level = sstable_compression_level_;
    builder_options.encryption_is_active = sstable_encryption_is_active_;
    builder_options.encryption_scheme = sstable_default_encryption_scheme_;
    builder_options.dek = &database_dek_;
    builder_options.target_block_size = sstable_target_uncompressed_block_size_;
    
    std::unique_ptr<engine::lsm::SSTableBuilder> builder;
    uint64_t current_sstable_uncompressed_size = 0;

    // Helper lambda to finalize a builder and register its filter
    auto finalize_and_register_builder = [&]() {
        if (builder && builder->getRecordCount() > 0) {
            builder->finish();
            auto new_meta = builder->getFileMetadata();
            new_files_metadata.push_back(new_meta);

            if (storage_engine_ptr_ && storage_engine_ptr_->getLivenessOracle()) {
                auto liveness_filter = builder->getLivenessBloomFilter();
                if (liveness_filter) {
                    storage_engine_ptr_->getLivenessOracle()->addLivenessFilter(
                        new_meta->sstable_id, 
                        liveness_filter
                    );
                }
            }
        }
        builder.reset();
    };

    // --- Phase 3: Main Merge, GC, and Write Loop ---
    std::string current_processing_key;
    std::vector<Record> versions_for_current_key;

    while (!state.min_heap.empty()) {
        MergeHeapEntry top = state.min_heap.top();
        
        if (!task.end_key.empty() && top.record.key >= task.end_key) {
            break;
        }
        state.min_heap.pop();
        
        if (current_processing_key.empty()) {
            current_processing_key = top.record.key;
        }

        if (top.record.key != current_processing_key) {
            auto kept_records = processKeyVersionsAndGC(versions_for_current_key, oldest_active_txn, current_processing_key);
            for (const auto& rec : kept_records) {
                size_t record_size = estimateRecordSize(rec.key, rec);
                
                if (builder && current_sstable_uncompressed_size > 0 && 
                    current_sstable_uncompressed_size + record_size > SSTABLE_TARGET_MAX_SIZE_BYTES)
                {
                    finalize_and_register_builder();
                }
                
                if (!builder) {
                    uint64_t new_sstable_id = next_sstable_file_id_.fetch_add(1);
                    std::string filepath = generateSSTableFilename(target_level, new_sstable_id);
                    builder = std::make_unique<engine::lsm::SSTableBuilder>(filepath, builder_options);
                    current_sstable_uncompressed_size = 0;
                }
                
                builder->add(rec);
                current_sstable_uncompressed_size += record_size;
            }
            versions_for_current_key.clear();
            current_processing_key = top.record.key;
        }
        
        versions_for_current_key.push_back(top.record);

        size_t reader_idx = top.reader_index;
        if (state.readers[reader_idx]->advance() && state.readers[reader_idx]->peek().has_value()) {
            state.min_heap.push({*state.readers[reader_idx]->peek(), reader_idx});
        }
    }
    
    // --- Phase 4: Process the very last key's versions ---
    if (!versions_for_current_key.empty()) {
        auto kept_records = processKeyVersionsAndGC(versions_for_current_key, oldest_active_txn, current_processing_key);
        for (const auto& rec : kept_records) {
            size_t record_size = estimateRecordSize(rec.key, rec);
            if (builder && current_sstable_uncompressed_size > 0 && 
                current_sstable_uncompressed_size + record_size > SSTABLE_TARGET_MAX_SIZE_BYTES)
            {
                finalize_and_register_builder();
            }
            if (!builder) {
                uint64_t new_sstable_id = next_sstable_file_id_.fetch_add(1);
                std::string filepath = generateSSTableFilename(target_level, new_sstable_id);
                builder = std::make_unique<engine::lsm::SSTableBuilder>(filepath, builder_options);
                current_sstable_uncompressed_size = 0;
            }
            builder->add(rec);
            current_sstable_uncompressed_size += record_size;
        }
    }

    // --- Phase 5: Finalize the last SSTable file ---
    finalize_and_register_builder();

    LOG_INFO("[SubCompaction] Task for key range ['{}', '{}') finished. Created {} new SSTable(s).",
             format_key_for_print(task.start_key),
             task.end_key.empty() ? "+inf" : format_key_for_print(task.end_key),
             new_files_metadata.size());

    return new_files_metadata;
}

static void throw_sstable_footer_write_error(const std::string& context_message, const std::string& sstable_filename_context, std::ofstream& stream_ref) {
    // Check stream state before it's potentially closed by the caller or RAII
    bool eof = stream_ref.eof();
    bool fail = stream_ref.fail();
    bool bad = stream_ref.bad();

    std::string full_msg = "SSTableMetadata '" + sstable_filename_context + "' error: " + context_message +
                           ". Stream state - eof: " + std::to_string(eof) +
                           ", fail: " + std::to_string(fail) +
                           ", bad: " + std::to_string(bad);
    LOG_ERROR("[LSMTree Write Error] {}", full_msg);

    // It's generally the caller's responsibility to close the stream in a catch block
    // or via RAII if the stream object is managed that way.
    // Avoid closing the stream here as it might be closed again by the caller.
    throw std::runtime_error(full_msg);
}

static std::unique_ptr<engine::lsm::CompactionStrategy> CreateCompactionStrategy(
    engine::lsm::CompactionStrategyType type,
    LSMTree* lsm_tree_ptr,
    const engine::lsm::LeveledCompactionConfig& leveled_config,
    const engine::lsm::UniversalCompactionConfig& universal_config,
    const engine::lsm::FIFOCompactionConfig& fifo_config,
    const engine::lsm::HybridCompactionConfig& hybrid_config 
) {
    switch (type) {
        case engine::lsm::CompactionStrategyType::UNIVERSAL:
            return std::make_unique<engine::lsm::UniversalCompactionStrategy>(lsm_tree_ptr, universal_config);
        
        case engine::lsm::CompactionStrategyType::FIFO:
            return std::make_unique<engine::lsm::FIFOCompactionStrategy>(fifo_config);

        case engine::lsm::CompactionStrategyType::HYBRID: 
            return std::make_unique<engine::lsm::HybridCompactionStrategy>(lsm_tree_ptr, hybrid_config);

        case engine::lsm::CompactionStrategyType::LEVELED:
        default:
            return std::make_unique<engine::lsm::LeveledCompactionStrategy>(lsm_tree_ptr, leveled_config);
    }
}


// --- Helper Function: countPathSegments (Moved here) ---
static size_t countPathSegments(const std::string& path) {
    std::string path_to_split = path;
    // Debug Input
     // std::cout << "  [countPathSegments] Input Path: '" << path << "'" << std::endl;

    if (!path_to_split.empty() && path_to_split.back() == '/') {
        path_to_split.pop_back();
    }
    if (path_to_split.empty()) {
       // std::cout << "  [countPathSegments] Path empty after trailing slash removal. Count: 0" << std::endl;
        return 0;
    }

    size_t count = 0;
    std::stringstream ss(path_to_split);
    std::string segment;
    while (std::getline(ss, segment, '/')) {
        if (!segment.empty()) {
            count++;
        }
    }
    // Handle case where path has no slashes but is not empty
     if (count == 0 && !path_to_split.empty()) {
        count = 1;
     }

    // Debug Output
     // std::cout << "  [countPathSegments] Path: '" << path << "' -> Segment Count: " << count << std::endl;
    return count;
}

static constexpr size_t SSTABLE_FOOTER_SIZE = sizeof(std::streampos) /* key_idx_start_offset */ +
                                              sizeof(uint64_t)       /* bf_size_bytes */ +
     
                                              sizeof(std::streampos) /* bf_start_offset */;

/*
void engine::lsm::SSTableMetadata::scanSSTableForPrefix(
    const std::string& prefix,
    TxnId reader_txn_id,
    size_t limit,
    const std::string& start_key_exclusive,
    std::map<std::string, Record>& results,
    std::set<std::string>& deleted_keys,
    const LSMTree* lsm_tree_ptr) const
{
    if (!lsm_tree_ptr) {
        LOG_ERROR("[SSTableMetadata::scan] LSMTree instance pointer is null. Cannot proceed.");
        return;
    }

    // --- Phase 1: High-Level Pruning ---
    // Check if the SSTableMetadata's key range can possibly overlap with the prefix.
    if ((!this->min_key.empty() && this->max_key < prefix) ||
        (!this->max_key.empty() && this->min_key > prefix && this->min_key.rfind(prefix, 0) != 0)) {
        return; // This SSTableMetadata is entirely outside the prefix range.
    }

    // --- Phase 2: Open File and Find Starting Block ---
    std::ifstream sst_file_stream(this->filename, std::ios::binary);
    if (!sst_file_stream) {
        LOG_ERROR("[SSTableMetadata::scan] Failed to open SSTableMetadata file '{}'.", this->filename);
        return;
    }

    const std::string& start_scan_at = start_key_exclusive.empty() ? prefix : start_key_exclusive;
    auto block_it = this->block_index.upper_bound(start_scan_at);
    if (block_it != this->block_index.begin()) {
        --block_it;
    }

    // --- Phase 3: Iterate Through Relevant Blocks ---
    for (; block_it != this->block_index.end(); ++block_it) {
        if (limit > 0 && results.size() >= limit) break;

        const std::string& block_start_key = block_it->first;
        if (block_start_key > prefix && block_start_key.rfind(prefix, 0) != 0) {
            break; // This block and all subsequent ones are past our prefix.
        }

        std::vector<uint8_t> block_data = lsm_tree_ptr->readAndProcessSSTableBlock(
            sst_file_stream, block_it->second, this->filename);
        
        if (block_data.empty()) continue;

        // --- Phase 4: Iterate Records Within the Block ---
        const uint8_t* ptr = block_data.data();
        const uint8_t* end_ptr = ptr + block_data.size();
        
        while (ptr < end_ptr) {
            if (limit > 0 && results.size() >= limit) goto end_scan;

            std::optional<Record> rec_opt = LSMTree::deserializeRecordFromBuffer(ptr, end_ptr);
            if (!rec_opt) break; // End of block or corruption

            const Record& current_record = *rec_opt;
            
            // Check if we are past the prefix
            if (current_record.key.rfind(prefix, 0) != 0) {
                if (current_record.key > prefix) goto end_scan; // Optimization
                continue;
            }
            
            // Skip if already processed from a newer SSTableMetadata/memtable
            if (results.count(current_record.key) || deleted_keys.count(current_record.key)) {
                continue;
            }
            
            // MVCC Visibility Check
            if (current_record.commit_txn_id != 0 && current_record.commit_txn_id < reader_txn_id) {
                if (current_record.deleted) {
                    deleted_keys.insert(current_record.key);
                } else {
                    results[current_record.key] = current_record;
                }
            }
        }
    }

end_scan:
    return;
}
*/

void LSMTree::scanSSTableForPrefix(
    const std::shared_ptr<engine::lsm::SSTableMetadata>& sstable_meta,
    PrefixScanState& state) const
{
    // --- Phase 1: High-Level Pruning ---
    if ((!sstable_meta->min_key.empty() && sstable_meta->max_key < state.prefix) ||
        (!sstable_meta->max_key.empty() && sstable_meta->min_key > state.prefix && sstable_meta->min_key.rfind(state.prefix, 0) != 0)) {
        return; // This SSTable is entirely outside the prefix range.
    }

    // --- Phase 2: Open File ---
    std::ifstream sst_file_stream(sstable_meta->filename, std::ios::binary);
    if (!sst_file_stream) {
        LOG_ERROR("[LSM scanSSTableForPrefix] Failed to open SSTable file '{}'.", sstable_meta->filename);
        return;
    }

    const std::string& start_scan_at = state.start_key_exclusive.empty() ? state.prefix : state.start_key_exclusive;

    // --- Phase 3: Find Starting L1 Index Block using L2 Index ---
    if (sstable_meta->block_index.empty()) return;

    auto l2_it = sstable_meta->block_index.upper_bound(start_scan_at);
    if (l2_it != sstable_meta->block_index.begin()) {
        --l2_it;
    }

    // --- Phase 4: Iterate Through Relevant L1 Index Blocks ---
    for (; l2_it != sstable_meta->block_index.end(); ++l2_it) {
    // --------------
        if (state.isLimitReached()) break;

        if (l2_it->first > state.prefix && l2_it->first.rfind(state.prefix, 0) != 0) {
            break;
        }

        // Read and deserialize the L1 index block
        sst_file_stream.seekg(l2_it->second);
        uint32_t num_l1_entries;
        sst_file_stream.read(reinterpret_cast<char*>(&num_l1_entries), sizeof(num_l1_entries));
        
        std::map<std::string, SSTableBlockIndexEntry> l1_index_block;
        for (uint32_t i = 0; i < num_l1_entries; ++i) {
            auto l1_key_opt = deserializeKey(sst_file_stream);
            if (!l1_key_opt) {
                LOG_ERROR("[LSM scanSSTableForPrefix] Corrupt L1 index block in {}. Halting scan for this file.", sstable_meta->filename);
                return;
            }
            SSTableBlockIndexEntry entry;
            sst_file_stream.read(reinterpret_cast<char*>(&entry), sizeof(entry));
            l1_index_block[*l1_key_opt] = entry;
        }

        // Find starting data block within this L1 index
        auto block_it = l1_index_block.upper_bound(start_scan_at);
        if (block_it != l1_index_block.begin()) --block_it;

        // --- Phase 5: Iterate Through Relevant Data Blocks ---
        for (; block_it != l1_index_block.end(); ++block_it) {
            if (state.isLimitReached()) goto end_scan;
            if (block_it->first > state.prefix && block_it->first.rfind(state.prefix, 0) != 0) break;

            std::vector<uint8_t> block_data = readAndProcessSSTableBlock(sst_file_stream, block_it->second, sstable_meta->filename);
            if (block_data.empty()) continue;

            const uint8_t* ptr = block_data.data();
            const uint8_t* end_ptr = ptr + block_data.size();
            
            while (ptr < end_ptr) {
                if (state.isLimitReached()) goto end_scan;

                std::optional<Record> rec_opt = deserializeRecordFromBuffer(ptr, end_ptr);
                if (!rec_opt) break;

                const Record& current_record = *rec_opt;
                
                // Check if we are past the prefix
                if (current_record.key.rfind(state.prefix, 0) != 0) {
                    if (current_record.key > state.prefix) goto end_scan;
                    continue;
                }
                
                if (state.results_map.count(current_record.key) || state.deleted_keys_tracker.count(current_record.key)) {
                    continue;
                }
                
                if (current_record.commit_txn_id != 0 && current_record.commit_txn_id < state.reader_txn_id) {
                    if (current_record.deleted) {
                        state.deleted_keys_tracker.insert(current_record.key);
                    } else {
                        state.results_map[current_record.key] = current_record;
                    }
                }
            }
        }
    }

end_scan:
    return;
}

/*static*/ std::optional<Record> LSMTree::deserializeRecordFromBuffer(
    const uint8_t*& current_ptr, const uint8_t* end_ptr) 
{
    Record rec;
    // FIX: Use a consistent pointer type for arithmetic. Cast the uint8_t* to char*.
    const char* ptr = reinterpret_cast<const char*>(current_ptr);
    const char* const char_end_ptr = reinterpret_cast<const char*>(end_ptr);

    // Key
    if (static_cast<size_t>(char_end_ptr - ptr) < sizeof(uint32_t)) return std::nullopt;
    uint32_t key_len;
    std::memcpy(&key_len, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    if (static_cast<size_t>(char_end_ptr - ptr) < key_len) return std::nullopt;
    rec.key.assign(ptr, key_len);
    ptr += key_len;

    // Value (binary string)
    if (static_cast<size_t>(char_end_ptr - ptr) < sizeof(uint32_t)) return std::nullopt;
    uint32_t value_binary_len;
    std::memcpy(&value_binary_len, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    if (static_cast<size_t>(char_end_ptr - ptr) < value_binary_len) return std::nullopt;
    std::string value_binary(ptr, value_binary_len);
    ptr += value_binary_len;

    // Deserialize the binary value string back into a ValueType
    auto value_opt = LogRecord::deserializeValueTypeBinary(value_binary);
    if (!value_opt) return std::nullopt;
    rec.value = *value_opt;
    
    // Metadata
    if (static_cast<size_t>(char_end_ptr - ptr) < (sizeof(TxnId) * 2 + sizeof(time_t) + sizeof(bool))) return std::nullopt;
    std::memcpy(&rec.txn_id, ptr, sizeof(TxnId)); ptr += sizeof(TxnId);
    std::memcpy(&rec.commit_txn_id, ptr, sizeof(TxnId)); ptr += sizeof(TxnId);
    std::time_t ts_count;
    std::memcpy(&ts_count, ptr, sizeof(std::time_t)); ptr += sizeof(std::time_t);
    rec.timestamp = std::chrono::system_clock::from_time_t(ts_count);
    std::memcpy(&rec.deleted, ptr, sizeof(bool)); ptr += sizeof(bool);

    // FIX: Update the original uint8_t* reference before returning.
    current_ptr = reinterpret_cast<const uint8_t*>(ptr);
    return rec;
}

std::shared_ptr<engine::lsm::SSTableMetadata> LSMTree::validateAndLoadExternalSSTable(
    const std::string& filepath
) const {
    // This public method encapsulates the creation of the private SSTableFileInfo
    // struct and the call to the private loading helper.
    
    SSTableFileInfo dummy_info;
    dummy_info.full_path = filepath;
    dummy_info.level = -1;       // Not applicable for an external file yet
    dummy_info.sstable_id = 0;   // Will be assigned upon actual ingestion

    // The call to the private helper is now valid.
    return this->loadSingleSSTableMetadata(dummy_info);
}


/*
// --- Helper struct for the priority queue (Moved here) ---
void LSMTree::writeSSTableDataBlock(
    std::ofstream& file_stream,
    const std::vector<uint8_t>& uncompressed_block_content,
    uint32_t num_records_in_block_param,
    SSTableBlockIndexEntry& out_block_index_entry,
    const std::string& sstable_filename_for_context
) {
    if (!file_stream.is_open() || !file_stream.good()) {
        throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Output file stream is not ready for writing SSTable block.")
            .withContext("sstable_file", sstable_filename_for_context);
    }
    if (uncompressed_block_content.empty() && num_records_in_block_param > 0) {
        LOG_WARN("[LSMTree::writeSSTableDataBlock] For '", sstable_filename_for_context, 
                 "': Attempted to write block with ", num_records_in_block_param, " records but empty uncompressed data. Skipping block write.");
        out_block_index_entry = {}; // Zero out the entry
        return;
    }

    SSTableDataBlockHeader header_on_disk;
    header_on_disk.num_records_in_block = num_records_in_block_param;
    header_on_disk.uncompressed_content_size_bytes = static_cast<uint32_t>(uncompressed_block_content.size());

    std::vector<uint8_t> data_after_compression = uncompressed_block_content;
    CompressionType compression_used = CompressionType::NONE;

    // --- Step 1: Compression ---
    if (sstable_compression_type_ != CompressionType::NONE && !uncompressed_block_content.empty()) {
        try {
            std::vector<uint8_t> compressed_output = CompressionManager::compress(
                uncompressed_block_content.data(), uncompressed_block_content.size(),
                sstable_compression_type_, sstable_compression_level_);
            
            // Only use compression if it actually reduces the size.
            if (compressed_output.size() < uncompressed_block_content.size()) {
                data_after_compression = std::move(compressed_output);
                compression_used = sstable_compression_type_;
            }
        } catch (const std::exception& e) {
            LOG_WARN("[LSMTree] Error compressing SSTable block for '", sstable_filename_for_context, "': ", e.what(), ". Storing uncompressed.");
            data_after_compression = uncompressed_block_content; // Fallback to uncompressed
            compression_used = CompressionType::NONE;
        }
    }

    // --- Step 2: Encryption ---
    std::vector<uint8_t> final_payload_for_disk;
    EncryptionScheme encryption_used = EncryptionScheme::NONE;
    
    if (sstable_encryption_is_active_ && sstable_default_encryption_scheme_ != EncryptionScheme::NONE) {
        if (database_dek_.empty()) {
            throw storage::StorageError(storage::ErrorCode::STORAGE_NOT_INITIALIZED, "DEK is empty, cannot encrypt SSTable block.");
        }
        encryption_used = sstable_default_encryption_scheme_;
        if (encryption_used != EncryptionScheme::AES256_GCM_PBKDF2) {
            throw storage::StorageError(storage::ErrorCode::INVALID_CONFIGURATION, "LSMTree only supports AES-GCM for encryption.");
        }

        try {
            auto block_iv = EncryptionLibrary::generateIV();
            std::memcpy(header_on_disk.iv, block_iv.data(), std::min(block_iv.size(), sizeof(header_on_disk.iv)));
            
            // Construct Authenticated Associated Data (AAD) to protect metadata from tampering.
            std::vector<uint8_t> aad_data;
            header_on_disk.set_flags(compression_used, encryption_used);
            aad_data.push_back(header_on_disk.flags);
            const uint8_t* ucs_ptr = reinterpret_cast<const uint8_t*>(&header_on_disk.uncompressed_content_size_bytes);
            aad_data.insert(aad_data.end(), ucs_ptr, ucs_ptr + sizeof(header_on_disk.uncompressed_content_size_bytes));
            const uint8_t* nr_ptr = reinterpret_cast<const uint8_t*>(&header_on_disk.num_records_in_block);
            aad_data.insert(aad_data.end(), nr_ptr, nr_ptr + sizeof(header_on_disk.num_records_in_block));
            
            EncryptionLibrary::EncryptedData encrypted_out = EncryptionLibrary::encryptWithAES_GCM(
                data_after_compression, database_dek_, block_iv, aad_data
            );
            
            if(encrypted_out.tag.size() != sizeof(header_on_disk.auth_tag)) {
                throw storage::StorageError(storage::ErrorCode::INTERNAL_ERROR, "GCM encryption produced an authentication tag of unexpected size.");
            }
            std::memcpy(header_on_disk.auth_tag, encrypted_out.tag.data(), sizeof(header_on_disk.auth_tag));
            final_payload_for_disk = std::move(encrypted_out.data);

        } catch (const CryptoException& ce) {
            LOG_ERROR("[LSMTree] CryptoException during SSTable block encryption: ", ce.what());
            throw; // Re-throw as is
        }
    } else {
        final_payload_for_disk = std::move(data_after_compression);
    }

    // --- Step 3 & 4: Finalize Header, Checksum, and Write ---
    header_on_disk.set_flags(compression_used, encryption_used);
    header_on_disk.compressed_payload_size_bytes = static_cast<uint32_t>(final_payload_for_disk.size());
    header_on_disk.payload_checksum = calculate_payload_checksum(final_payload_for_disk.data(), final_payload_for_disk.size());

    out_block_index_entry.block_offset_in_file = file_stream.tellp();
    if (file_stream.fail()) {
        throw storage::StorageError(storage::ErrorCode::IO_SEEK_ERROR, "Failed to get stream position for block offset.")
            .withContext("sstable_file", sstable_filename_for_context);
    }
    
    file_stream.write(reinterpret_cast<const char*>(&header_on_disk), SSTABLE_DATA_BLOCK_HEADER_SIZE);
    if (!file_stream) {
        throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Failed to write SSTableDataBlockHeader.")
            .withContext("sstable_file", sstable_filename_for_context);
    }

    if (!final_payload_for_disk.empty()) {
        file_stream.write(reinterpret_cast<const char*>(final_payload_for_disk.data()), final_payload_for_disk.size());
        if (!file_stream) {
            throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Failed to write SSTable data block payload.")
                .withContext("sstable_file", sstable_filename_for_context);
        }
    }
    
    // --- Step 5: Populate Index Entry ---
    out_block_index_entry.num_records = header_on_disk.num_records_in_block;
    out_block_index_entry.compression_type = header_on_disk.get_compression_type();
    out_block_index_entry.encryption_scheme = header_on_disk.get_encryption_scheme();
    out_block_index_entry.uncompressed_payload_size_bytes = header_on_disk.uncompressed_content_size_bytes;
    out_block_index_entry.compressed_payload_size_bytes = header_on_disk.compressed_payload_size_bytes;

    LOG_TRACE("  SSTable Block (", num_records_in_block_param, " recs) written for '", sstable_filename_for_context,
              "'. Header Flags(E|C):", std::hex, std::showbase, static_cast<int>(header_on_disk.flags),
              std::dec, ", DiskPayloadSize:", header_on_disk.compressed_payload_size_bytes,
              ", UncompContentSize:", header_on_disk.uncompressed_content_size_bytes,
              ", Checksum:", std::hex, std::showbase, header_on_disk.payload_checksum,
              std::dec, ". IdxEntry Offset:", static_cast<long long>(out_block_index_entry.block_offset_in_file));
}
*/
/*static*/ std::string LSMTree::GetMemTableLogInfo(const std::shared_ptr<MemTable>& mt_ptr, const char* context_label) {
    // Gracefully handle the case where the shared_ptr is null.
    if (!mt_ptr) {
        return std::string(context_label) + ": [nullptr]";
    }

    std::ostringstream oss;
    oss << context_label << ": [Ptr=0x" << std::hex << reinterpret_cast<uintptr_t>(mt_ptr.get())
        << ", ApproxMem=" << mt_ptr->ApproximateMemoryUsage() << "B"
        << ", EstSize=" << mt_ptr->estimated_size_bytes_.load(std::memory_order_relaxed) << "B"
        << ", MinTxn=" << mt_ptr->min_txn_id.load(std::memory_order_relaxed)
        << ", MaxTxn=" << mt_ptr->max_txn_id.load(std::memory_order_relaxed)
        << "]";
    
    return oss.str();
}

LSMTree::LSMTree(
    const engine::lsm::PartitionedMemTable::Config& config,
    const std::string& data_dir_param,
    size_t memtable_max_size_param,
    size_t sstable_target_uncompressed_block_size_param,
    CompressionType sstable_compression_type_param,
    int sstable_compression_level_param,
    bool overall_encryption_enabled_passed,
    EncryptionScheme scheme_for_new_blocks_passed,
    const std::vector<unsigned char>& dek_passed,
    const std::vector<unsigned char>& global_salt_passed,
    size_t num_flush_threads_param,
    std::shared_ptr<engine::threading::ThreadPool> compaction_pool,
    std::shared_ptr<engine::lsm::WriteBufferManager> write_buffer_manager,
    std::shared_ptr<engine::threading::TokenBucketRateLimiter> rate_limiter,
    engine::lsm::CompactionStrategyType strategy_type,
    const engine::lsm::LeveledCompactionConfig& leveled_config,
    const engine::lsm::UniversalCompactionConfig& universal_config,
    const engine::lsm::FIFOCompactionConfig& fifo_config,
    const engine::lsm::HybridCompactionConfig& hybrid_config,
    std::shared_ptr<engine::lsm::CompactionFilter> compaction_filter
 
)
    : memtable_config_(config),
      data_dir_(data_dir_param),
      memtable_max_size_(memtable_max_size_param > 0 ? memtable_max_size_param : 4 * 1024 * 1024),
      sstable_target_uncompressed_block_size_(sstable_target_uncompressed_block_size_param > 0 ? sstable_target_uncompressed_block_size_param : 64 * 1024),
      sstable_compression_type_(sstable_compression_type_param),
      sstable_compression_level_(sstable_compression_level_param),
      sstable_encryption_is_active_(overall_encryption_enabled_passed),
      sstable_default_encryption_scheme_(scheme_for_new_blocks_passed),
      database_dek_(dek_passed),
      database_global_salt_(global_salt_passed),
      write_buffer_manager_(write_buffer_manager),
      compaction_rate_limiter_(rate_limiter), 
      versions_(std::make_unique<engine::lsm::VersionSet>()),
      next_sstable_file_id_(1),
      scheduler_running_(false),
      flush_workers_shutdown_(false),
      storage_engine_ptr_(nullptr),
      flush_count_(0),
      compaction_filter_(compaction_filter)
{
    LOG_INFO("[LSMTree Constructor] Initializing for data directory: {}", data_dir_);

    try {
        size_t min_memtable_size = 1 * 1024 * 1024; // 1 MB
        size_t max_memtable_size = 64 * 1024 * 1024; // 64 MB
        memtable_tuner_ = std::make_unique<engine::lsm::MemTableTuner>(
            memtable_max_size_, min_memtable_size, max_memtable_size);

        active_memtable_ = std::make_shared<engine::lsm::PartitionedMemTable>(memtable_config_);
        if (!active_memtable_) {
            throw storage::StorageError(storage::ErrorCode::OUT_OF_MEMORY, "Failed to allocate initial active PartitionedMemTable.");
        }

        if (write_buffer_manager_) {
            write_buffer_manager_->ReserveMemory(memtable_max_size_);
        }

        fs::create_directories(data_dir_);
        loadSSTablesMetadata();

        compaction_strategy_ = CreateCompactionStrategy(strategy_type, this, leveled_config, universal_config, fifo_config, hybrid_config);
        
        LOG_INFO("[LSMTree Constructor] Using '{}' compaction strategy.", 
                 magic_enum::enum_name(strategy_type));
        
        compaction_manager_ = std::make_unique<CompactionManagerForIndinisLSM>(this, compaction_pool);

        size_t actual_flush_threads = (num_flush_threads_param > 0) ? num_flush_threads_param : DEFAULT_FLUSH_WORKER_THREADS;
        flush_worker_threads_.reserve(actual_flush_threads);

        startCompactionAndFlushThreads();

    } catch (const std::exception& e) {
        LOG_FATAL("[LSMTree Constructor] CRITICAL FAILURE during initialization: {}. Attempting cleanup before re-throwing.", e.what());
        stopCompactionAndFlushThreads();
        if (write_buffer_manager_ && active_memtable_) {
            write_buffer_manager_->FreeMemory(memtable_max_size_);
        }
        throw;
    }

    LOG_INFO("[LSMTree Constructor] Initialization complete for store '{}'. Next SSTable ID: {}. Flush Threads: {}",
             data_dir_, next_sstable_file_id_.load(), flush_worker_threads_.size());
}

void LSMTree::scheduler_thread_loop() {
    LOG_INFO("[LSM SchedulerThread ThdID: {}] Started.", std::this_thread::get_id());
    while (scheduler_running_.load(std::memory_order_relaxed)) {
        {
            std::unique_lock<std::mutex> lock(scheduler_mutex_);
            if (scheduler_cv_.wait_for(lock, scheduler_interval_, [this] {
                return !scheduler_running_.load(std::memory_order_relaxed); // Stop if shutdown
            })) {
                if (!scheduler_running_.load(std::memory_order_relaxed)) break; // Woken by shutdown
            }
        } // lock released

        if (!scheduler_running_.load(std::memory_order_relaxed)) break;

        try {
            check_and_schedule_compactions();
        } catch (const std::exception& e) {
            LOG_ERROR("[LSM SchedulerThread ThdID: {}] Exception: {}", std::this_thread::get_id(), e.what());
            std::this_thread::sleep_for(std::chrono::seconds(10)); // Backoff on error
        }
    }
    LOG_INFO("[LSM SchedulerThread ThdID: {}] Stopped.", std::this_thread::get_id());
}

void LSMTree::setCompactionFilter(std::shared_ptr<engine::lsm::CompactionFilter> filter) {
    // This allows dynamically changing the filter logic on a running system.
    // We should use a mutex if this needs to be thread-safe with compactions.
    // For now, assuming it's called during quiescent periods.
    compaction_filter_ = filter;
    if (filter) {
        LOG_INFO("[LSMTree] Compaction filter set to: {}", filter->Name());
    } else {
        LOG_INFO("[LSMTree] Compaction filter has been cleared.");
    }
}

void LSMTree::check_and_schedule_compactions() {
    LOG_TRACE("[LSMTree CompactionScheduler] Checking for compactions...");
    if (!compaction_manager_ || !compaction_strategy_) return;

    // Get a snapshot of the current version state.
    engine::lsm::Version* current_v = versions_->GetCurrent();
    if (!current_v) return;
    engine::lsm::VersionUnreffer unreffer(current_v);
    const auto& levels_snapshot = current_v->GetLevels();
    
    // <<< DELEGATE SELECTION TO THE STRATEGY OBJECT >>>
    std::optional<CompactionJobForIndinis> job_to_schedule =
        compaction_strategy_->SelectCompaction(levels_snapshot, MAX_LEVELS);
    
    if (job_to_schedule) {
        compaction_manager_->schedule_job(std::move(*job_to_schedule));
    } else {
        LOG_TRACE("[LSMTree CompactionScheduler] No compactions needed at this time according to the strategy.");
    }
}


// --- Helper method implementations ---
std::pair<std::string, std::string> LSMTree::compute_key_range(
    const std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>& tables) const 
{
    // Handle the edge case of being given no tables.
    if (tables.empty()) {
        return {"", ""};
    }
    
    std::string global_min_key;
    std::string global_max_key;

    // Iterate through all tables to find the true min and max.
    // This approach is safer than initializing with tables[0] because the first
    // table might have empty min/max keys.
    for (const auto& table : tables) {
        // Find the overall minimum key.
        if (!table->min_key.empty()) {
            // If our current global min is empty, or if the table's min is smaller, update it.
            if (global_min_key.empty() || table->min_key < global_min_key) {
                global_min_key = table->min_key;
            }
        }
        
        // Find the overall maximum key.
        if (!table->max_key.empty()) {
            // If our current global max is empty, or if the table's max is larger, update it.
            if (global_max_key.empty() || table->max_key > global_max_key) {
                global_max_key = table->max_key;
            }
        }
    }
    
    return {global_min_key, global_max_key};
}

uint64_t LSMTree::calculate_level_size_bytes(const std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>& level_tables) const {
    uint64_t total_size = 0;
    for (const auto& sstable : level_tables) {
        total_size += sstable->size_bytes;
    }
    return total_size;
}

/*
uint64_t LSMTree::calculate_target_size_for_level(int level_idx) const {
    // This calculation might need levels_metadata_mutex if it were to access
    // other levels' current sizes, but here it's based on L0 config and multiplier.
    if (level_idx == 0) { // L0 has a count trigger, not a size target for initiating compaction
        return static_cast<uint64_t>(L0_SSTABLE_COUNT_TRIGGER * SSTABLE_TARGET_MAX_SIZE_BYTES);
    }
    if (level_idx < 0 || level_idx >= MAX_LEVELS) return 0;

    // Base target size for L1 (can be considered the "ideal" size output from many L0 flushes)
    uint64_t l1_target_size = static_cast<uint64_t>(L0_SSTABLE_COUNT_TRIGGER * SSTABLE_TARGET_MAX_SIZE_BYTES * 0.8); // Example: L1 aims to be sum of ~L0_TRIGGER tables
                                                                                                                    // The 0.8 is a heuristic to allow some room before triggering
    if (level_idx == 1) {
        return l1_target_size;
    }
    
    // Exponential growth for L2+
    return static_cast<uint64_t>(l1_target_size * std::pow(LEVEL_SIZE_MULTIPLIER, level_idx - 1));
}
*/
uint64_t LSMTree::getNextSSTableFileId() const {
    // Use memory_order_relaxed as the caller (compaction strategy) only needs an
    // approximate value for scoring and doesn't need strict synchronization.
    return next_sstable_file_id_.load(std::memory_order_relaxed);
}

/*
double LSMTree::calculate_compaction_score(const engine::lsm::SSTableMetadata& sstable) const {
    // Age score: higher for smaller (older) sstable_id
    // Normalize sstable_id; smaller IDs are older. next_sstable_file_id_ can be used as a rough upper bound.
    // Add 1 to avoid division by zero if sstable_id could be 0 (though typically starts at 1).
    double age_component = 1.0;
    uint64_t current_max_id = next_sstable_file_id_.load(std::memory_order_relaxed);
    if (current_max_id > sstable.sstable_id && sstable.sstable_id > 0) { // Ensure valid IDs
        // Older tables (smaller ID relative to current max) get higher score.
        // Example: (current_max_id - sstable.sstable_id) / current_max_id
        // This gives a value between 0 and 1, higher for older.
        age_component = static_cast<double>(current_max_id - sstable.sstable_id) / static_cast<double>(current_max_id -1); // -1 if min sstable_id is 1
    } else if (sstable.sstable_id == 0) {
        age_component = 0.0; // Lowest score for problematic ID
    }
    // Weight for age score, e.g. 0.3
    double age_score = age_component * 0.3;


    // Size score: higher for tables that are large relative to target OR very small (fragmentation)
    double size_deviation_score = 0.0;
    if (SSTABLE_TARGET_MAX_SIZE_BYTES > 0) {
        // Score for being too large
        if (sstable.size_bytes > SSTABLE_TARGET_MAX_SIZE_BYTES) {
            size_deviation_score += (static_cast<double>(sstable.size_bytes) / SSTABLE_TARGET_MAX_SIZE_BYTES) - 1.0;
        }
        // Score for being too small (fragmentation, potentially many small files)
        // This might be more relevant if we compact multiple small files together.
        // For picking a single file, being small isn't inherently bad unless there are many.
        // A factor like (1.0 - (sstable.size_bytes / SSTABLE_TARGET_MAX_SIZE_BYTES)) if sstable.size_bytes < threshold
    }
    // Weight for size score, e.g. 0.7
    double size_score = size_deviation_score * 0.7;
    
    // Placeholder: Add tombstone ratio score later if available
    // double tombstone_score = (sstable.tombstone_count / (double)sstable.total_entry_count) * 0.5;

    double final_score = age_score + size_score; // Combine weighted scores
    LOG_TRACE("      [CompactionScore] SSTableMetadata ID {}: AgeComp={:.3f} (raw_age_val={}), SizeDevComp={:.3f} (raw_size_bytes={}), FinalScore={:.3f}",
              sstable.sstable_id, age_score, age_component, size_score, sstable.size_bytes, final_score);
    return final_score;
}

bool LSMTree::is_compaction_worthwhile(
    const std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>& source_tables,
    const std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>& target_tables) const {
    
    if (source_tables.empty()) { // Cannot compact if no source tables
        LOG_TRACE("    [is_compaction_worthwhile] No source tables. Not worthwhile.");
        return false;
    }
    
    // Example: Don't compact a single small source table if it doesn't overlap with anything
    // and isn't particularly old or full of tombstones (score would be low).
    // This check is somewhat redundant if score already incorporates these.
    if (source_tables.size() == 1 && target_tables.empty()) {
        // Check if this single source table is "bad enough" to compact on its own
        // e.g., very old, or significantly oversized, or high tombstone ratio
        const auto& single_source = source_tables[0];
        if (single_source->size_bytes < (SSTABLE_TARGET_MAX_SIZE_BYTES / 2) && calculate_compaction_score(*single_source) < 0.5 ) { // Heuristic thresholds
             LOG_TRACE("    [is_compaction_worthwhile] Single source table ID {} (size {}B) with no target overlap is too small/healthy to compact alone. Score: {:.3f}",
                       single_source->sstable_id, single_source->size_bytes, calculate_compaction_score(*single_source));
            return false;
        }
    }
    
    uint64_t total_bytes_to_process = 0;
    for (const auto& table : source_tables) total_bytes_to_process += table->size_bytes;
    for (const auto& table : target_tables) total_bytes_to_process += table->size_bytes;
    
    // Minimum threshold for compaction (e.g., at least 25% of a target SSTableMetadata size)
    const uint64_t MIN_COMPACTION_BYTES_WORTHWHILE = SSTABLE_TARGET_MAX_SIZE_BYTES / 4;
    if (total_bytes_to_process < MIN_COMPACTION_BYTES_WORTHWHILE) {
        LOG_TRACE("    [is_compaction_worthwhile] Total bytes to process ({}B) is less than minimum worthwhile ({}B). Skipping.",
                  total_bytes_to_process, MIN_COMPACTION_BYTES_WORTHWHILE);
        return false;
    }
    
    LOG_TRACE("    [is_compaction_worthwhile] Compaction deemed worthwhile. Total bytes to process: {}B", total_bytes_to_process);
    return true;
}
*/
std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> LSMTree::find_overlapping_sstables(
    const std::vector<std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>>& levels_snapshot,
    int target_level_idx,
    const std::string& min_key_range,
    const std::string& max_key_range)
{
    std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> overlapping;
    // Bounds check the target level against the provided snapshot
    if (target_level_idx < 0 || static_cast<size_t>(target_level_idx) >= levels_snapshot.size() || 
        min_key_range.empty() || max_key_range.empty()) {
        return overlapping;
    }

    // FIX: Use the `levels_snapshot` parameter, not the member variable `levels_`.
    const auto& target_level_tables = levels_snapshot[target_level_idx];

    LOG_TRACE("  [FindOverlap] Searching L{} for overlap with range ['{}', '{}']. Table count: {}",
              target_level_idx, format_key_for_print(min_key_range), format_key_for_print(max_key_range), target_level_tables.size());

    for (const auto& sstable : target_level_tables) {
        bool sstable_max_before_range_min = !sstable->max_key.empty() && sstable->max_key < min_key_range;
        bool sstable_min_after_range_max = !sstable->min_key.empty() && sstable->min_key > max_key_range;
        
        if (!(sstable_max_before_range_min || sstable_min_after_range_max)) {
            overlapping.push_back(sstable);
            LOG_TRACE("    Overlap found: SSTable ID {} (L{}), Range ['{}', '{}']", 
                      sstable->sstable_id, sstable->level, format_key_for_print(sstable->min_key), format_key_for_print(sstable->max_key));
        }
    }
    return overlapping;
}

/**
 * @brief Executes a compaction job by merging input SSTables and writing new ones.
 *
 * This is the main orchestrator for the compaction process, called by a worker
 * thread from the shared compaction pool. It handles two primary scenarios:
 *
 * 1.  **FIFO Deletion**: If the job comes from a FIFO strategy and has no target
 *     files (sstables_L_plus_1 is empty), it's a deletion job. The method will
 *     atomically remove the expired SSTables from the metadata and delete them
 *     from disk without reading their contents.
 *
 * 2.  **Merging Compaction (Leveled/Universal)**: For standard compactions, it
 *     delegates the work to specialized helper methods for setup, a k-way merge
 *     loop (with MVCC garbage collection), and atomic finalization.
 *
 * @param level_L The source level for the primary input files.
 * @param sstables_L_input The files from level L to be compacted or deleted.
 * @param sstables_L_plus_1_input The overlapping files from level L+1 to be merged.
 * @return true if the compaction was successful, false otherwise.
 */
bool LSMTree::execute_compaction_job(
    int level_L,
    std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> files_L,
    std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> files_L_plus_1)
{
    // This method now acts as a coordinator, splitting the job and scheduling parallel tasks.

    // --- 1. Splitting Logic ---
    std::vector<engine::lsm::SubCompactionTask> sub_tasks;
    
    // Create the list of ALL input files from both levels. This is the only
    // declaration of `all_input_files` in this function scope.
    std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> all_input_files = files_L;
    all_input_files.insert(all_input_files.end(), files_L_plus_1.begin(), files_L_plus_1.end());

    if (files_L_plus_1.empty()) {
        // Simple case (e.g., L0->L1 with no overlaps): Create one sub-task for the whole range.
        sub_tasks.push_back({"", "", all_input_files});
    } else {
        // Complex case (e.g., L1->L2): Split the work based on the key ranges of the L+1 files.
        for (size_t i = 0; i < files_L_plus_1.size(); ++i) {
            const auto& target_file = files_L_plus_1[i];
            std::string start_key = target_file->min_key;
            std::string end_key = (i + 1 < files_L_plus_1.size()) ? files_L_plus_1[i+1]->min_key : "";

            engine::lsm::SubCompactionTask task;
            task.start_key = start_key;
            task.end_key = end_key;

            // Find all input files (from both levels) that overlap with this sub-task's range.
            for (const auto& input_file : all_input_files) {
                bool no_overlap = !input_file->max_key.empty() && input_file->max_key < start_key ||
                                  !end_key.empty() && !input_file->min_key.empty() && input_file->min_key >= end_key;
                if (!no_overlap) {
                    task.input_files.push_back(input_file);
                }
            }
            if (!task.input_files.empty()) {
                sub_tasks.push_back(std::move(task));
            }
        }
    }
    
    LOG_INFO("[LSMTree Compaction L{}] Split job into {} parallel sub-tasks.", level_L, sub_tasks.size());

    if (sub_tasks.empty()) {
        LOG_WARN("[LSMTree Compaction L{}] No sub-tasks generated, job is a no-op.", level_L);
        return true;
    }

    auto coordinator = std::make_shared<engine::lsm::CompactionJobCoordinator>(
        this, level_L, level_L + 1, sub_tasks.size(), files_L, files_L_plus_1
    );
    
    TxnId oldest_active = getOldestActiveTxnId();

    for (const auto& task_details : sub_tasks) {
        auto sub_task_lambda = [this, coordinator, task_details, oldest_active]() {
            if (coordinator->hasFailed()) {
                LOG_INFO("[SubCompactionTask] Skipping execution as job has already failed.");
            } else {
                try {
                    auto new_files = execute_sub_compaction(
                        task_details, 
                        coordinator->target_output_level_, 
                        oldest_active
                    );
                    coordinator->addSubTaskResult(std::move(new_files));
                } catch (const std::exception& e) {
                    LOG_ERROR("[SubCompactionTask] CRITICAL EXCEPTION: {}", e.what());
                    coordinator->reportTaskFailure();
                }
            }

            if (coordinator->taskCompleted() == coordinator->total_tasks_) {
                if (coordinator->hasFailed()) {
                    coordinator->abortJob();
                } else {
                    coordinator->finalizeJob();
                }
            }
        };
        compaction_manager_->schedule_job_on_pool(std::move(sub_task_lambda));
    }
    
    return true; // Main function returns immediately.
}



void LSMTree::setupCompactionState(
    CompactionState& state,
    const std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>& sstables_L,
    const std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>& sstables_L_plus_1)
{
    state.readers.reserve(sstables_L.size() + sstables_L_plus_1.size());

    auto initialize_readers = [&](const auto& files_metadata) {
        for (const auto& meta : files_metadata) {
            if (!meta) {
                LOG_WARN("[CompactionSetup] Encountered a null SSTableMetadata pointer. Skipping.");
                continue;
            }
            try {
                auto reader = std::make_unique<SSTableReader>(meta, this);
                if (reader->peek().has_value()) {
                    state.min_heap.push({*reader->peek(), state.readers.size()});
                }
                state.readers.push_back(std::move(reader));
            } catch (const std::exception& e) {
                LOG_ERROR("[CompactionSetup L{}] Failed to open reader for input file {}: {}",
                          state.level_L, meta->filename, e.what());
            }
        }
    };

    initialize_readers(sstables_L);
    initialize_readers(sstables_L_plus_1);
}

/**
 * @brief Initializes the CompactionState by creating readers for all input
 * SSTables and populating the min-heap with the first record from each reader.
 */
void LSMTree::finalizeCompactionJob(
    const CompactionState& state,
    const std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>& sstables_L,
    const std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>& sstables_L_plus_1)
{
    LOG_INFO("[CompactionFinalize L", state.level_L, "] Finalizing job. New files created: ", 
             state.new_output_files_metadata.size(), ". Old files to remove: ", 
             sstables_L.size() + sstables_L_plus_1.size());

    // --- 1. Create a VersionEdit to describe the atomic change ---
    engine::lsm::VersionEdit edit;

    // Add entries for the new files to be installed into the target level.
    for (const auto& new_meta : state.new_output_files_metadata) {
        if (!new_meta) {
            // This should not happen if the rest of the compaction logic is correct.
            LOG_ERROR("[CompactionFinalize L", state.level_L, "] Encountered a null metadata pointer for a new file. Aborting finalization to prevent corruption.");
            throw storage::StorageError(storage::ErrorCode::INTERNAL_ERROR, "Null metadata pointer for new file in finalizeCompactionJob.");
        }
        edit.AddFile(new_meta->level, new_meta);
        LOG_TRACE("  - Adding new file to VersionEdit: ", new_meta->filename, " (Level ", new_meta->level, ")");
    }

    // Add entries to mark the old input files for deletion from the metadata.
    for (const auto& old_meta : sstables_L) {
        edit.DeleteFile(old_meta->level, old_meta->sstable_id);
    }
    for (const auto& old_meta : sstables_L_plus_1) {
        edit.DeleteFile(old_meta->level, old_meta->sstable_id);
    }

    // --- 2. Atomically apply the metadata changes ---
    // This is the point of no return. Once this call succeeds, all new readers
    // will see the new files and will no longer see the old files.
    versions_->ApplyChanges(edit);
    LOG_INFO("[CompactionFinalize L", state.level_L, "] Metadata atomically updated via VersionSet. New version is now live.");

    // --- 3. Physically delete the old, now-unreferenced files from disk ---
    // This is safe to do now because no new read operation will ever be pointed to them.
    // Existing readers might still have a file handle if they hold an old Version,
    // but on POSIX systems, the file will be deleted after the last handle is closed.
    // On Windows, this might cause issues if handles are still open, but the VersionSet
    // model is designed to minimize this window.
    auto delete_file = [](const auto& meta) {
        try {
            if (fs::exists(meta->filename)) {
                fs::remove(meta->filename);
                LOG_TRACE("  - Deleted old file: ", meta->filename);
            }
        } catch (const fs::filesystem_error& e) {
            // Log an error but do not throw. A failed file deletion should not
            // crash the engine. The file can be cleaned up by a later startup scan.
            LOG_ERROR("[CompactionFinalize] Filesystem error while deleting old SSTable '", 
                      meta->filename, "': ", e.what());
        }
    };

    for (const auto& meta : sstables_L) {
        delete_file(meta);
    }
    for (const auto& meta : sstables_L_plus_1) {
        delete_file(meta);
    }
    LOG_INFO("[CompactionFinalize L", state.level_L, "] Old input files have been deleted from disk.");
}
// --- Phase 2: Merging and Writing ---

/**
 * @brief Starts a new temporary output file for the current compaction.
 * Populates the CompactionState with the new file stream and metadata object.
 */
void LSMTree::startNewCompactionOutputFile(CompactionState& state) {
    uint64_t new_sstable_id = next_sstable_file_id_.fetch_add(1);
    state.current_output_sstable_meta = std::make_shared<engine::lsm::SSTableMetadata>(new_sstable_id, state.target_output_level);
    state.current_output_sstable_meta->filename = generateSSTableFilename(state.target_output_level, new_sstable_id);
    
    // The main bloom_filter member is no longer used. The filter_partition_index map is used instead.
    
    state.temp_output_filename = state.current_output_sstable_meta->filename + ".tmp_compaction";
    state.output_file_stream.open(state.temp_output_filename, std::ios::binary | std::ios::trunc);
   if (!state.output_file_stream) {
        throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Failed to open temp output file for compaction.")
            .withFilePath(state.temp_output_filename);
    }

    state.current_sstable_uncompressed_size = 0;
    state.current_block_buffer.clear();
    state.records_in_current_block = 0;
    state.first_key_in_current_block.clear();
    
    // --- NEW: Initialize the first filter partition state ---
    state.current_filter_partition = std::make_unique<BloomFilter>(
        (sstable_target_uncompressed_block_size_ / 256) * BLOCKS_PER_FILTER_PARTITION, // Estimated items
        DEFAULT_BLOOM_FILTER_FPR
    );
    state.first_key_in_current_filter_partition.clear();
    state.blocks_in_current_filter_partition = 0;
}

/* 
void LSMTree::flushFilterPartition(
    std::ofstream& output_stream,
    std::shared_ptr<engine::lsm::SSTableMetadata>& sstable_meta,
    std::unique_ptr<BloomFilter>& current_partition,
    std::string& first_key_in_partition,
    int& blocks_in_partition)
{
    if (blocks_in_partition == 0 || !current_partition) {
        return;
    }

    std::streampos offset = output_stream.tellp();
    if (!current_partition->serializeToStream(output_stream)) {
        throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Failed to serialize filter partition.")
            .withContext("sstable_file", sstable_meta->filename);
    }
    
    uint32_t size = static_cast<uint32_t>(static_cast<std::streampos>(output_stream.tellp()) - offset);
    sstable_meta->filter_partition_index[first_key_in_partition] = {offset, size};
    
    LOG_TRACE("  - Flushed filter partition for key range starting at '{}'. Offset: {}, Size: {} bytes.",
              format_key_for_print(first_key_in_partition), static_cast<long long>(offset), size);

    // Reset state for the next partition by re-assigning the caller's variables.
    current_partition = std::make_unique<BloomFilter>(
        (sstable_target_uncompressed_block_size_ / 256) * BLOCKS_PER_FILTER_PARTITION,
        DEFAULT_BLOOM_FILTER_FPR
    );
    first_key_in_partition.clear();
    blocks_in_partition = 0;
}
*/
void LSMTree::ingestFile(
    std::shared_ptr<engine::lsm::SSTableMetadata> file_metadata,
    const std::string& file_path,
    bool move_file)
{
    // 1. Assign a new, unique file ID.
    uint64_t new_file_id = next_sstable_file_id_.fetch_add(1);
    
    // 2. Determine the target level. For V1, all ingested files go to L0.
    int target_level = 0;
    
    // 3. Update the metadata object with its new identity.
    file_metadata->sstable_id = new_file_id;
    file_metadata->level = target_level;
    file_metadata->filename = generateSSTableFilename(target_level, new_file_id);
    
    // 4. Check for key-range overlaps. For L0, overlaps are allowed.
    // An advanced version would check for overlaps if ingesting to L1+.

    // 5. Move or copy the file into the database directory with its new name.
    try {
        if (move_file) {
            fs::rename(file_path, file_metadata->filename);
            LOG_TRACE("[LSM Ingest] Moved external file from '{}' to '{}'.", file_path, file_metadata->filename);
        } else {
            fs::copy_file(file_path, file_metadata->filename);
            LOG_TRACE("[LSM Ingest] Copied external file from '{}' to '{}'.", file_path, file_metadata->filename);
        }
        file_metadata->size_bytes = fs::file_size(file_metadata->filename);
    } catch (const fs::filesystem_error& e) {
        throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Filesystem error during file ingestion.")
            .withDetails(e.what());
    }

    // 6. Atomically update the VersionSet to make the new file live.
    LOG_INFO("[LSM Ingest] Applying VersionEdit to add new SSTable ID {} to L{}.", new_file_id, target_level);
    engine::lsm::VersionEdit edit;
    edit.AddFile(target_level, file_metadata);
    versions_->ApplyChanges(edit);
    
    // 7. Notify the scheduler to check if this new file triggers a compaction.
    notifyScheduler();
}

void LSMTree::finalizeCurrentCompactionOutputFile(CompactionState& state) {
    // 1. Guard against finalizing a non-existent or already finalized file.
    if (!state.builder) {
        return;
    }

    // 2. Check if any data was actually written to the builder. If not, abort
    //    and clean up the empty temporary file without creating metadata.
    if (state.current_sstable_uncompressed_size == 0) {
        state.builder.reset(); // Destructor will delete the empty .tmp file.
        LOG_TRACE("[Compaction] Finalizing output file: No data was written, aborting file creation.");
        return;
    }

    try {
        // 3. Tell the builder to finish its work. This writes the last data block,
        //    all filter/block indexes, and the final footer.
        state.builder->finish();

        // 4. Retrieve the fully populated metadata from the now-finalized builder.
        std::shared_ptr<engine::lsm::SSTableMetadata> new_meta = state.builder->getFileMetadata();
        
        // 5. Add this completed metadata to the list of new files generated by this job.
        //    The coordinator will use this list to update the VersionSet.
        state.new_output_files_metadata.push_back(new_meta);
        
        LOG_INFO("[Compaction] Finalized new SSTable: {}. Size: {} bytes, Entries: {}",
                 new_meta->filename, new_meta->size_bytes, new_meta->total_entries);

    } catch (const std::exception& e) {
        LOG_ERROR("[Compaction] CRITICAL: Failed to finalize SSTable via builder for file '{}': {}",
                  state.builder->getFileMetadata()->filename, e.what());
        // Do not add to the list of new files. The builder's destructor will clean up the temp file.
    }

    // 6. Reset the builder unique_ptr. This prepares the state for the next
    //    potential output file in the same compaction job (if the data was split).
    state.builder.reset();
    state.current_sstable_uncompressed_size = 0;
}

/**
 * @brief Processes a single compacted record, adding it to the current output
 * block and flushing the block to disk if it becomes full.
 */
void LSMTree::writeCompactedRecord(CompactionState& state, const Record& record_to_write)
{
    // 1. Estimate the record's size to check against file and block limits.
    // Note: A more precise but slower method would be to serialize first.
    // estimateRecordSize is a fast heuristic sufficient for this purpose.
    size_t record_size = estimateRecordSize(record_to_write.key, record_to_write);

    // 2. Apply I/O throttling before proceeding.
    if (compaction_rate_limiter_) {
        compaction_rate_limiter_->consume(record_size);
    }

    // 3. Check if the current SSTable file is full and needs to be rolled over.
    // We must check this *before* adding the new record.
    if (state.current_sstable_uncompressed_size > 0 &&
        (state.current_sstable_uncompressed_size + record_size > SSTABLE_TARGET_MAX_SIZE_BYTES))
    {
        // The current SSTable is full. Finalize it.
        finalizeCurrentCompactionOutputFile(state);
        // Start a new one for the current record.
        startNewCompactionOutputFile(state);
    }

    // If there is no active builder (e.g., at the very start of the job), create one.
    if (!state.builder) {
        startNewCompactionOutputFile(state);
    }

    // 4. Add the record to the active SSTableBuilder. The builder handles all the
    //    internal details of block creation, filter partitioning, and metadata updates.
    state.builder->add(record_to_write);

    // 5. Update our running total for the current file's size.
    state.current_sstable_uncompressed_size += record_size;
}

/**
 * @brief The main merge loop that pulls records from the min-heap, processes
 * them for GC, and writes the surviving records to output files.
 */
void LSMTree::processCompactionMergeLoop(CompactionState& state) {
    std::string current_processing_key;
    std::vector<Record> versions_for_current_key;

    while (!state.min_heap.empty()) {
        MergeHeapEntry top = state.min_heap.top();
        state.min_heap.pop();

        if (top.reader_index >= state.readers.size()) {
            LOG_ERROR("[CompactionMerge] CRITICAL: Heap returned invalid reader_index ", top.reader_index,
                      " (out of bounds for readers size ", state.readers.size(), "). Aborting job.");
            throw storage::StorageError(storage::ErrorCode::LSM_COMPACTION_FAILED, "Invalid reader index from merge heap.");
        }
        
        if (current_processing_key.empty()) {
            current_processing_key = top.record.key;
        }

        if (top.record.key != current_processing_key) {
            std::vector<Record> kept_records = processKeyVersionsAndGC(versions_for_current_key, state.effective_gc_txn_id, current_processing_key);
            for (const auto& rec : kept_records) {
                writeCompactedRecord(state, rec);
            }
            versions_for_current_key.clear();
            current_processing_key = top.record.key;
        }
        
        versions_for_current_key.push_back(top.record);

        size_t reader_idx = top.reader_index;
        if (state.readers[reader_idx]->advance() && state.readers[reader_idx]->peek().has_value()) {
            state.min_heap.push({*state.readers[reader_idx]->peek(), reader_idx});
        }
    }
    
    if (!versions_for_current_key.empty()) {
        std::vector<Record> kept_records = processKeyVersionsAndGC(versions_for_current_key, state.effective_gc_txn_id, current_processing_key);
        for (const auto& rec : kept_records) {
            writeCompactedRecord(state, rec);
        }
    }
}

// --- Phase 3: Finalization ---

void LSMTree::setStorageEngine(StorageEngine* engine) {
    storage_engine_ptr_ = engine;
}

LSMTree::~LSMTree() {
    LOG_INFO("[LSMTree Destructor] Shutting down LSM-Tree for data directory '{}'...", data_dir_);

    // --- Step 1: Stop all background worker threads ---
    // This ensures no new work is scheduled and all loops will terminate.
    stopCompactionAndFlushThreads();
    LOG_INFO("  [LSMTree Destructor] All background threads have been stopped.");

    // --- Step 2: Gather all remaining in-memory data for a final flush ---
    std::vector<std::shared_ptr<engine::lsm::PartitionedMemTable>> all_memtables_to_flush;
    {
        // FIX: Use the correct mutex name 'memtable_data_mutex_'.
        std::unique_lock<std::shared_mutex> data_lock(memtable_data_mutex_);

        // Add the active memtable if it contains any data.
        if (active_memtable_ && !active_memtable_->Empty()) {
            LOG_INFO("  [LSMTree Destructor] Active memtable contains data. Adding to final flush list.");
            all_memtables_to_flush.push_back(std::move(active_memtable_));
        }
        active_memtable_.reset(); // Ensure the pointer is cleared.

        // Add all immutable memtables.
        if (!immutable_memtables_.empty()) {
            LOG_INFO("  [LSMTree Destructor] Found {} immutable memtables. Adding to final flush list.", immutable_memtables_.size());
            all_memtables_to_flush.insert(
                all_memtables_to_flush.end(),
                std::make_move_iterator(immutable_memtables_.begin()),
                std::make_move_iterator(immutable_memtables_.end())
            );
            immutable_memtables_.clear();
        }
    } // Mutex is released here.

    // --- Step 3: Perform final synchronous flushes and release memory ---
    if (!all_memtables_to_flush.empty()) {
        LOG_INFO("  [LSMTree Destructor] Performing final synchronous flush of {} memtable(s)...", all_memtables_to_flush.size());
        
        for (const auto& mt_to_flush : all_memtables_to_flush) {
            size_t mem_size = mt_to_flush->ApproximateMemoryUsage();
            try {
                // Use the dedicated flush helper which handles all file I/O.
                flushImmutableMemTableToFile(mt_to_flush);
            } catch (const std::exception& e) {
                LOG_ERROR("  [LSMTree Destructor] CRITICAL: Exception during final synchronous flush of memtable: {}. Data may not be persisted if WAL is disabled.", e.what());
            }
            
            // CRITICAL: Release the memory reservation for this memtable.
            // This happens regardless of flush success because the memtable object is being destroyed.
            if (write_buffer_manager_) {
                write_buffer_manager_->FreeMemory(mem_size > 0 ? mem_size : memtable_max_size_);
            }
        }
        LOG_INFO("  [LSMTree Destructor] Final synchronous flushes complete.");
    } else {
        // If there was an active memtable that was reserved for but empty, we still need to free its initial reservation.
        if (write_buffer_manager_) {
            // Note: This assumes one active memtable was always reserved for.
            write_buffer_manager_->FreeMemory(memtable_max_size_);
        }
        LOG_INFO("  [LSMTree Destructor] No pending in-memory data to flush.");
    }

    LOG_INFO("[LSMTree Destructor] Shutdown complete for data directory '{}'.", data_dir_);
}

void LSMTree::startCompactionAndFlushThreads() {
    // Start Compaction Scheduler Thread
    if (!scheduler_running_.exchange(true, std::memory_order_acq_rel)) {
        if (!compaction_manager_) {
            LOG_ERROR("[LSMTree] CompactionManager not initialized before starting compaction scheduler!");
            scheduler_running_.store(false, std::memory_order_release);
        } else {
            scheduler_thread_ = std::thread(&LSMTree::scheduler_thread_loop, this);
            LOG_INFO("[LSMTree] Compaction scheduler thread started.");
        }
    } else {
        LOG_WARN("[LSMTree] Compaction scheduler thread already running or start requested again.");
    }

    // Start Background Flush Worker Threads
    if (!flush_workers_shutdown_.load(std::memory_order_relaxed)) { // Check if not already running/shutting down
        size_t num_flush_threads_to_start = flush_worker_threads_.capacity(); // Use reserved capacity
        if (flush_worker_threads_.empty() && num_flush_threads_to_start > 0) {
             LOG_INFO("[LSMTree] Starting {} background flush worker thread(s)...", num_flush_threads_to_start);
            for (size_t i = 0; i < num_flush_threads_to_start; ++i) {
                flush_worker_threads_.emplace_back(&LSMTree::background_flush_worker_loop, this);
            }
        } else if (!flush_worker_threads_.empty()){
            LOG_WARN("[LSMTree] Background flush workers appear to be already started.");
        }
    }
}

void LSMTree::stopCompactionAndFlushThreads() {
    // Stop Compaction Scheduler and Workers
    bool comp_scheduler_was_running = scheduler_running_.exchange(false, std::memory_order_acq_rel);
    if (comp_scheduler_was_running) {
        LOG_INFO("[LSMTree] Stopping compaction scheduler thread...");
        { std::lock_guard<std::mutex> lock(scheduler_mutex_); scheduler_cv_.notify_one(); }
        if (scheduler_thread_.joinable()) scheduler_thread_.join();
        LOG_INFO("[LSMTree] Compaction scheduler thread stopped.");
    }
    if (compaction_manager_) {
        compaction_manager_.reset(); // Destructor of CompactionManagerForIndinisLSM handles its workers
        LOG_INFO("[LSMTree] CompactionManager reset and its worker threads shut down.");
    }

    // Stop Background Flush Workers
    bool flush_workers_were_active = !flush_workers_shutdown_.exchange(true, std::memory_order_acq_rel);
    if (flush_workers_were_active && !flush_worker_threads_.empty()) {
        LOG_INFO("[LSMTree] Stopping background flush worker threads...");
        { 
            std::unique_lock<std::shared_mutex> data_lock(memtable_data_mutex_);
            immutable_memtable_ready_cv_.notify_all(); }
        for (auto& worker : flush_worker_threads_) {
            if (worker.joinable()) worker.join();
        }
        flush_worker_threads_.clear();
        LOG_INFO("[LSMTree] Background flush worker threads stopped.");
    }
}

std::string LSMTree::generateSSTableFilename(int level, uint64_t id) {
    std::ostringstream oss;
    oss << "sstable_L" << level << "_"
        << std::setw(10) << std::setfill('0') << id << ".dat";
    return (fs::path(data_dir_) / oss.str()).string();
}



TxnId LSMTree::getOldestActiveTxnId() {
    if (storage_engine_ptr_) { // Check if pointer is set
        return storage_engine_ptr_->getOldestActiveTxnId();
    }
    
    // Fallback if storage_engine_ptr_ is not set (should not happen in normal operation)
    LOG_WARN("[LSMTree::getOldestActiveTxnId] storage_engine_ptr_ not set! Compaction will merge but not aggressively prune old versions.");
    return 0; // Conservative default: keep all versions if no info
}

// --- LSMTree::serializeRecord ---
/*static*/ void LSMTree::serializeRecord(std::ostream& stream, const Record& record) {
    // 1. Key Serialization (Length-prefixed)
    uint32_t key_len = static_cast<uint32_t>(record.key.size());
    stream.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
    if (key_len > 0) {
        stream.write(record.key.data(), key_len);
    }

    // 2. Value Serialization (using the new binary format)
    std::string value_binary = LogRecord::serializeValueTypeBinary(record.value);
    uint32_t value_len = static_cast<uint32_t>(value_binary.size());
    stream.write(reinterpret_cast<const char*>(&value_len), sizeof(value_len));
    if (value_len > 0) {
        stream.write(value_binary.data(), value_len);
    }
    
    // 3. Metadata Serialization
    stream.write(reinterpret_cast<const char*>(&record.txn_id), sizeof(record.txn_id));
    stream.write(reinterpret_cast<const char*>(&record.commit_txn_id), sizeof(record.commit_txn_id));
    auto ts_count = std::chrono::system_clock::to_time_t(record.timestamp);
    stream.write(reinterpret_cast<const char*>(&ts_count), sizeof(ts_count));
    stream.write(reinterpret_cast<const char*>(&record.deleted), sizeof(record.deleted));
}

// --- LSMTree::deserializeKey ---
std::optional<std::string> LSMTree::deserializeKey(std::ifstream& file) {
    uint32_t key_size32; // MODIFIED: Use uint32_t for key size
    std::streampos keySizePos = file.tellg();

    if (!file.read(reinterpret_cast<char*>(&key_size32), sizeof(key_size32))) { // MODIFIED: sizeof(key_size32)
        if (file.eof() && file.gcount() == 0) return std::nullopt;
        LOG_WARN("      [SSTableMetadata DeserializeKey] Failed to read key_size (uint32_t) at pos {}. EOF: {}. GCount: {}",
                 static_cast<long long>(keySizePos), file.eof(), file.gcount());
        return std::nullopt;
    }

    // Sanity check for key_size32. Max key size can be defined.
    const uint32_t MAX_REASONABLE_KEY_SIZE = 4096 * 10; // e.g., 40KB, adjust as needed
    if (key_size32 > MAX_REASONABLE_KEY_SIZE) {
         LOG_ERROR("      [SSTableMetadata DeserializeKey] Unreasonable key size (uint32_t) detected: {} at pos {}", key_size32, static_cast<long long>(keySizePos));
         throw std::runtime_error("DeserializeKey: Unreasonable key size detected: " + std::to_string(key_size32));
    }

    std::string key(key_size32, '\0');
    if (key_size32 > 0 && !file.read(&key[0], key_size32)) { // MODIFIED: use key_size32
         LOG_WARN("      [SSTableMetadata DeserializeKey] Failed to read key data (expected {} bytes) at pos {} after reading size. EOF: {}",
                  key_size32, static_cast<long long>(static_cast<std::streamoff>(keySizePos) + static_cast<std::streamoff>(sizeof(key_size32))), file.eof());
         if (file.eof()) return std::nullopt;
         else throw std::runtime_error("DeserializeKey: Failed to read key data");
    }
    LOG_TRACE("      [SSTableMetadata DeserializeKey] Read key: '{}' (size32 {})", format_key_for_print(key), key_size32);
    return key;
}

// --- LSMTree::deserializeRecord ---
/*static*/ std::optional<Record> LSMTree::deserializeRecord(std::ifstream& file) {
    Record record;
    std::streampos record_start_pos_in_file = file.tellg();
    if (record_start_pos_in_file == std::streampos(-1)) {
        LOG_ERROR("[DeserializeRecord File] Failed to get initial stream position. Stream state: {}", file.rdstate());
        file.clear(); // Attempt to clear for further ops if any, but this read fails
        return std::nullopt;
    }

    LOG_TRACE("  [DeserializeRecord File] Attempting to deserialize record starting at file offset {}.", static_cast<long long>(record_start_pos_in_file));

    // Helper for file reading
    auto read_from_file_checked = 
        [&file, &record, record_start_pos_in_file](void* dest, std::streamsize size_to_read, const char* field_name_for_log) -> bool {
        std::streampos field_read_pos = file.tellg();
        file.read(reinterpret_cast<char*>(dest), size_to_read);
        if (file.gcount() != size_to_read) {
            LOG_ERROR("  [DeserializeRecord File] Failed to read {} bytes for '{}' for key (if known) '{}' at file offset {}. Read {} bytes. EOF: {}, Fail: {}.",
                      size_to_read, field_name_for_log, format_key_for_print(record.key), static_cast<long long>(field_read_pos),
                      file.gcount(), file.eof(), file.fail());
            // Don't reset file stream here, let caller handle based on overall failure
            return false;
        }
        LOG_TRACE("    Read {} bytes for '{}' from file. Key (if known): '{}'. New file offset: {}.", 
                  size_to_read, field_name_for_log, format_key_for_print(record.key), static_cast<long long>(file.tellg()));
        return true;
    };


    // 1. Key
    uint32_t key_len_u32;
    if (!read_from_file_checked(&key_len_u32, sizeof(key_len_u32), "key_len")) {
        if (file.eof() && file.gcount() == 0 && record_start_pos_in_file == file.tellg()) return std::nullopt; // Clean EOF at very start
        return std::nullopt;
    }
    static constexpr uint32_t MAX_SANE_KEY_LEN_FILE = 16 * 1024;
    if (key_len_u32 > MAX_SANE_KEY_LEN_FILE) {
        LOG_ERROR("  [DeserializeRecord File] Unreasonable key length from file: {}. Max allowed: {}. Corruption likely.", key_len_u32, MAX_SANE_KEY_LEN_FILE);
        return std::nullopt;
    }
    record.key.resize(key_len_u32);
    if (key_len_u32 > 0) {
        if (!read_from_file_checked(&record.key[0], key_len_u32, "key_data")) return std::nullopt;
    }
    LOG_TRACE("    Read key from file: '{}' (len {})", format_key_for_print(record.key), key_len_u32);

    // 2. Value type index
    int value_type_index_read_int;
    if (!read_from_file_checked(&value_type_index_read_int, sizeof(value_type_index_read_int), "value_type_index")) return std::nullopt;
    LOG_TRACE("    Read value_type_index from file: {} for key '{}'.", value_type_index_read_int, format_key_for_print(record.key));

    // 3. Value data
    static constexpr uint32_t MAX_REASONABLE_STRING_OR_VECTOR_LENGTH_FROM_SSTABLE_FILE = 100 * 1024 * 1024;
    switch (value_type_index_read_int) {
        case 0: // std::monostate
            record.value = std::monostate{};
            LOG_TRACE("      Value type from file: std::monostate for key '{}'.", format_key_for_print(record.key));
            break;
        case 1: { // int64_t
            int64_t v_i64;
            if (!read_from_file_checked(&v_i64, sizeof(v_i64), "int64_t value")) return std::nullopt;
            record.value = v_i64;
            LOG_TRACE("      Value type from file: int64_t ({}) for key '{}'.", v_i64, format_key_for_print(record.key));
            break;
        }
        case 2: { // double
            double v_double;
            if (!read_from_file_checked(&v_double, sizeof(v_double), "double value")) return std::nullopt;
            record.value = v_double;
            LOG_TRACE("      Value type from file: double ({}) for key '{}'.", v_double, format_key_for_print(record.key));
            break;
        }
        case 3: { // std::string
            uint32_t str_len_u32;
            if (!read_from_file_checked(&str_len_u32, sizeof(str_len_u32), "string_len")) return std::nullopt;
            if (str_len_u32 > MAX_REASONABLE_STRING_OR_VECTOR_LENGTH_FROM_SSTABLE_FILE) {
                LOG_ERROR("  [DeserializeRecord File] Unreasonable string value length from file: {} for key '{}'. Max allowed: {}.",
                          str_len_u32, format_key_for_print(record.key), MAX_REASONABLE_STRING_OR_VECTOR_LENGTH_FROM_SSTABLE_FILE);
                return std::nullopt;
            }
            std::string s_val(str_len_u32, '\0');
            if (str_len_u32 > 0) {
                if (!read_from_file_checked(&s_val[0], str_len_u32, "string_data")) return std::nullopt;
            }
            record.value = s_val;
            LOG_TRACE("      Value type from file: std::string (len {}) for key '{}'. Preview: '{}'.",
                      str_len_u32, format_key_for_print(record.key), format_key_for_print(s_val.substr(0,50)));
            break;
        }
        case 4: { // std::vector<uint8_t>
            uint32_t vec_len_u32;
            if (!read_from_file_checked(&vec_len_u32, sizeof(vec_len_u32), "vector_len")) return std::nullopt;
            if (vec_len_u32 > MAX_REASONABLE_STRING_OR_VECTOR_LENGTH_FROM_SSTABLE_FILE) {
                LOG_ERROR("  [DeserializeRecord File] Unreasonable vector value length from file: {} for key '{}'. Max allowed: {}.",
                          vec_len_u32, format_key_for_print(record.key), MAX_REASONABLE_STRING_OR_VECTOR_LENGTH_FROM_SSTABLE_FILE);
                return std::nullopt;
            }
            std::vector<uint8_t> v_val(vec_len_u32);
            if (vec_len_u32 > 0) {
                if (!read_from_file_checked(v_val.data(), vec_len_u32, "vector_data")) return std::nullopt;
            }
            record.value = v_val;
            LOG_TRACE("      Value type from file: std::vector<uint8_t> (len {}) for key '{}'.",
                      vec_len_u32, format_key_for_print(record.key));
            break;
        }
        default:
            LOG_ERROR("  [DeserializeRecord File] Invalid value_type_index: {} encountered for key '{}'. Record deserialization failed.",
                      value_type_index_read_int, format_key_for_print(record.key));
            return std::nullopt;
    }

    // 4. Metadata
    std::time_t timestamp_raw;
    bool deleted_bool;
    if (!read_from_file_checked(&record.txn_id, sizeof(record.txn_id), "txn_id") ||
        !read_from_file_checked(&record.commit_txn_id, sizeof(record.commit_txn_id), "commit_txn_id") ||
        !read_from_file_checked(&timestamp_raw, sizeof(timestamp_raw), "timestamp_raw") ||
        !read_from_file_checked(&deleted_bool, sizeof(deleted_bool), "deleted_flag")) {
        LOG_ERROR("  [DeserializeRecord File] Failed reading one or more metadata fields for key '{}'.", format_key_for_print(record.key));
        return std::nullopt;
    }
    record.deleted = deleted_bool;
    record.timestamp = std::chrono::system_clock::from_time_t(timestamp_raw);

    LOG_TRACE("    Successfully deserialized record from file for key '{}': TxnID={}, CommitTxnID={}, Deleted={}. Record fully parsed. Current file offset: {}",
              format_key_for_print(record.key), record.txn_id, record.commit_txn_id, record.deleted, static_cast<long long>(file.tellg()));
    return record;
}

// --- LSMTree::skipRecordValueAndMeta ---
bool LSMTree::skipRecordValueAndMeta(std::ifstream& file) {
    int value_type_index;
    if (!file.read(reinterpret_cast<char*>(&value_type_index), sizeof(value_type_index))) return false;

    size_t skip_bytes = 0;
    size_t skip_bytes_for_value = 0; // Bytes for the value itself
    switch (value_type_index) {
        case 0: skip_bytes = sizeof(int64_t); break; // int64_t
        case 1: skip_bytes = sizeof(double); break; // double
        case 2: // string
        case 3: { // vector<uint8_t>
            uint32_t len32; // MODIFIED: Read uint32_t for length
            if (!file.read(reinterpret_cast<char*>(&len32), sizeof(len32))) return false;
            // Max length sanity check (optional but good for robustness)
            if (len32 > 20 * 1024 * 1024) { // e.g., 20MB limit for skipping
                 LOG_ERROR("      [SkipRecordValueAndMeta] Unreasonable length to skip: {}", len32);
                 throw std::runtime_error("SkipValue: Unreasonable length detected: " + std::to_string(len32));
            }
            skip_bytes_for_value = len32;
            break;
        }
        default: throw std::runtime_error("SkipValue: Invalid value type index: " + std::to_string(value_type_index));
    }

    // Add size of metadata
    //skip_bytes += sizeof(TxnId) + sizeof(TxnId) + sizeof(std::time_t) + sizeof(bool);
    size_t total_skip_bytes = skip_bytes_for_value +
                            sizeof(TxnId) +          // record.txn_id
                            sizeof(TxnId) +          // record.commit_txn_id
                            sizeof(std::time_t) +    // record.timestamp
                            sizeof(bool);            // record.deleted

    file.seekg(total_skip_bytes, std::ios::cur);
    if (!file.good()) {
        LOG_WARN("      [SkipRecordValueAndMeta] Seek failed or stream bad after skipping {} bytes. EOF: {}, Fail: {}",
                 total_skip_bytes, file.eof(), file.fail());
        return false;
    }
    return true;
}

// --- LSMTree::put ---
/**
 * @brief Inserts a record into the LSM-Tree's memory layer.
 *
 * This is the primary entry point for all writes. The method orchestrates a multi-step
 * process to ensure thread safety, apply backpressure, and manage the lifecycle of
 * in-memory tables (memtables).
 *
 * The process is as follows:
 * 1.  **Backpressure**: Waits if the queue of memtables to be flushed is full.
 * 2.  **Swap Check**: Checks if the current active memtable has reached its size limit. If so,
 *     it is moved to the immutable queue, and a new active memtable is created.
 * 3.  **Insertion**: The record is inserted into the active memtable's highly-concurrent SkipList.
 * 4.  **Notification**: If a swap occurred, a background worker thread is notified to begin
 *     flushing the newly immutable memtable to disk.
 *
 * @param key The key of the record.
 * @param record The Record object containing the value and transaction metadata.
 * @param oldest_active_txn_id The transaction ID of the oldest active transaction in the
 *        system, used for MVCC decisions within the VersionedRecord.
 */
void LSMTree::put(const std::string& key, const Record& record, TxnId oldest_active_txn_id) {
    // 1. Estimate the record's size to check against the memtable's limit.
    size_t estimated_size = estimateRecordSize(key, record);

    // 2. Apply backpressure if the system is low on memory resources.
    waitForImmutableSlot();

    // 3. Check if the active memtable is full and swap it if necessary.
    // This is a crucial step that may block if it needs to reserve memory from the
    // global WriteBufferManager.
    bool needs_flush_notification = false;
    if (active_memtable_->ApproximateMemoryUsage() + estimated_size >= memtable_max_size_ &&
        !active_memtable_->Empty())
    {
        // The swap logic is now correctly encapsulated in flushActiveMemTable.
        // We call the asynchronous version so this 'put' operation doesn't block for the I/O.
        flushActiveMemTable(false); // This internally calls swapActiveMemTable.
        needs_flush_notification = true; // The async flush was notified inside flushActiveMemTable.
    }

    // 4. Add the record to the (potentially new) active memtable.
    // This part is fast and highly concurrent.
    addToActiveMemTable(key, record, estimated_size, oldest_active_txn_id);

    // NOTE: The `notifyFlushWorker()` call is now handled by `flushActiveMemTable`
    // when a swap actually occurs, so we no longer need to call it here.
}

std::shared_ptr<engine::lsm::PartitionedMemTable> LSMTree::RebalanceAndSwapActiveMemTable(
    std::shared_ptr<engine::lsm::PartitionedMemTable> memtable_to_rebalance)
{
    LOG_INFO("[LSMTree Rebalance] Live rebalancing started for memtable {:p} ({} records, ~{} bytes).",
             static_cast<void*>(memtable_to_rebalance.get()),
             memtable_to_rebalance->Count(),
             memtable_to_rebalance->ApproximateMemoryUsage());

    // 1. Freeze the source memtable to prevent any new writes during migration.
    memtable_to_rebalance->FreezeAll();
    LOG_TRACE("  [Rebalance] Source memtable is now frozen.");

    // 2. Compute new range boundaries by sampling keys.
    std::vector<std::string> boundaries;
    size_t total_entries = memtable_to_rebalance->Count();
    size_t target_samples = memtable_config_.sample_size;

    if (total_entries > 0 && target_samples > 0 && memtable_config_.partition_count > 1) {
        std::vector<std::string> sampled_keys;
        sampled_keys.reserve(target_samples);

        size_t step = std::max<size_t>(1, total_entries / target_samples);
        size_t current_pos = 0;

        std::unique_ptr<engine::lsm::MemTableIterator> iter = memtable_to_rebalance->NewIterator();
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            if (current_pos % step == 0) {
                sampled_keys.push_back(iter->GetEntry().key);
            }
            if (sampled_keys.size() >= target_samples) {
                break; // Stop once we have enough samples
            }
            current_pos++;
        }
        
        if (sampled_keys.size() >= memtable_config_.partition_count) {
            std::sort(sampled_keys.begin(), sampled_keys.end());
            size_t num_partitions = memtable_config_.partition_count;
            for (size_t i = 1; i < num_partitions; ++i) {
                size_t idx = (i * sampled_keys.size()) / num_partitions;
                boundaries.push_back(sampled_keys[idx]);
            }
            LOG_INFO("  [Rebalance] Computed {} new split points from a sample of {} keys.",
                     boundaries.size(), sampled_keys.size());
        }
    }

    // 3. Check if rebalancing is possible and worthwhile.
    if (boundaries.empty()) {
        LOG_WARN("[LSMTree Rebalance] Failed to compute new boundaries. Aborting rebalance and performing a standard flush of the original memtable.");
        // Unfreeze is not strictly necessary as it's being flushed anyway, but it's good practice.
        // In a more complex system, we might unfreeze it here.
        return memtable_to_rebalance; // Return the original, skewed memtable.
    }

    // 4. Create the new, rebalanced memtable using the migration constructor.
    try {
        engine::lsm::PartitionedMemTable::Config new_config = memtable_config_;
        new_config.range_boundaries = boundaries; // Set the new ranges

        LOG_TRACE("  [Rebalance] Creating new rebalanced memtable via migration constructor...");
        auto rebalanced_memtable = std::make_shared<engine::lsm::PartitionedMemTable>(new_config, *memtable_to_rebalance);
        LOG_INFO("[LSMTree Rebalance] Live data migration complete. New balanced memtable {:p} created.",
                 static_cast<void*>(rebalanced_memtable.get()));
        
        return rebalanced_memtable; // Return the new, balanced table.

    } catch (const std::exception& e) {
        LOG_ERROR("[LSMTree Rebalance] CRITICAL: Exception during data migration: {}. Aborting rebalance.", e.what());
        // In case of failure, we must return the original memtable to ensure no data is lost.
        return memtable_to_rebalance;
    }
}
// --- Private Helper Method Implementations ---

/**
 * @brief Estimates the memory footprint of a record for memtable size tracking.
 * This is a heuristic used to decide when a memtable is full. It does not need to be exact
 * but should reasonably approximate the memory used by the key, value, and metadata.
 * @param key The record's key.
 * @param record The record's data.
 * @return An estimated size in bytes. This method is thread-safe and const.
 */
size_t LSMTree::estimateRecordSize(const std::string& key, const Record& record) const {
    size_t value_size = 0;
    if (std::holds_alternative<std::string>(record.value)) {
        value_size = std::get<std::string>(record.value).length();
    } else if (std::holds_alternative<std::vector<uint8_t>>(record.value)) {
        value_size = std::get<std::vector<uint8_t>>(record.value).size();
    } else {
        value_size = 64; // Overhead for other fixed-size types or empty values
    }
    // Total size = key + value + overhead for SkipList node pointers and VersionedRecord metadata.
    return key.length() + value_size + 128;
}

/**
 * @brief Blocks the calling thread if the immutable memtable queue is full.
 * This function waits on a condition variable until a slot becomes available in the
 * `immutable_memtables_` queue or until a shutdown is initiated.
 * @pre The caller must NOT hold the `memtable_data_mutex_`.
 * @post The calling thread may proceed, as a slot is guaranteed to be available (or the system is shutting down).
 */
void LSMTree::waitForImmutableSlot() {
    std::unique_lock<std::mutex> cv_lock(memtable_cv_mutex_);
    immutable_slot_available_cv_.wait(cv_lock, [this] {
        if (flush_workers_shutdown_.load()) {
            return true; // Don't block if we're shutting down.
        }
        // A shared lock is sufficient here as we are only reading the size.
        std::shared_lock<std::shared_mutex> data_lock(memtable_data_mutex_);
        return immutable_memtables_.size() < MAX_IMMUTABLE_MEMTABLES;
    });

    if (flush_workers_shutdown_.load()) {
        throw storage::StorageError(storage::ErrorCode::CANCELLED, "LSMTree put operation cancelled due to shutdown.");
    }
}

void LSMTree::TriggerFlush() {
    // This is a simple notification. The background worker will do the actual work.
    // This method is now the implementation of the IFlushable interface.
    std::unique_lock<std::mutex> cv_lock(memtable_cv_mutex_);
    immutable_memtable_ready_cv_.notify_one();
    LOG_INFO("[LSMTree] Flush triggered by external manager (e.g., WriteBufferManager).");
}

/**
 * @brief Swaps the active memtable, performing live rebalancing if necessary.
 *
 * This is the central control point for the memtable lifecycle. When a swap is triggered,
 * it checks if the outgoing memtable is imbalanced.
 *
 * - If IMBALANCED: It calls the `RebalanceAndSwapActiveMemTable` orchestrator to perform a
 *   live data migration, resulting in a new, balanced immutable memtable for the flush queue.
 *
 * - If BALANCED: It performs a simple swap, moving the existing active memtable directly
 *   to the immutable queue.
 *
 * In both cases, it creates a new, empty, hash-partitioned memtable to become the active one for
 * subsequent writes.
 *
 * @return The `PartitionedMemTable` to be flushed (either the original or the newly rebalanced one).
 *         Returns `nullptr` if no swap was necessary.
 */
std::shared_ptr<engine::lsm::PartitionedMemTable> LSMTree::swapActiveMemTable() {
    std::unique_lock<std::shared_mutex> data_lock(memtable_data_mutex_);
    
    if (!active_memtable_ || active_memtable_->Empty()) {
        return nullptr;
    }

    std::shared_ptr<engine::lsm::PartitionedMemTable> memtable_to_process = active_memtable_;
    
    flush_count_.fetch_add(1, std::memory_order_relaxed);

    // --- REBALANCING DECISION ---
    bool should_rebalance = memtable_to_process->IsImbalanced() &&
                            (memtable_config_.rebalance_frequency == 0 ||
                             flush_count_.load(std::memory_order_relaxed) % memtable_config_.rebalance_frequency == 0);
    
    if (should_rebalance) {
        // --- LIVE REBALANCING PATH ---
        // The orchestrator will return a new, balanced memtable containing the data.
        memtable_to_process = RebalanceAndSwapActiveMemTable(memtable_to_process);
    }
    // If not rebalancing, memtable_to_process remains the original, skewed-but-not-rebalanced table.

    // --- CREATE NEW ACTIVE MEMTABLE ---
    // The new active memtable *always* starts as a hash-partitioned table. It will only
    // become range-partitioned if it itself is rebalanced upon its next flush.
    try {
        engine::lsm::PartitionedMemTable::Config new_active_config = memtable_config_;
        // Ensure we don't carry over any range boundaries from a previous rebalance.
        new_active_config.range_boundaries.clear(); 
        
        active_memtable_ = std::make_shared<engine::lsm::PartitionedMemTable>(new_active_config);
    } catch (const std::exception& e) {
        LOG_FATAL("[LSMTree] Failed to create new active memtable during swap: {}. Reverting.", e.what());
        // Revert: put the table we were going to flush back as active.
        active_memtable_ = memtable_to_process; 
        throw;
    }

    // --- QUEUE THE TABLE FOR FLUSHING ---
    immutable_memtables_.push_back(memtable_to_process);
    
    // Coordinate with the global WriteBufferManager
    if (write_buffer_manager_) {
        // We always reserve memory for the *new* active memtable's maximum potential size.
        write_buffer_manager_->ReserveMemory(memtable_max_size_);
        // We register the table that is now in the immutable queue.
        write_buffer_manager_->RegisterImmutable(this, memtable_to_process->ApproximateMemoryUsage());
    }
    
    return memtable_to_process; // Return the table that was just added to the immutable queue.
}

void LSMTree::addToActiveMemTable(const std::string& key, const Record& record, size_t estimated_size, TxnId oldest_active_txn_id) {
    std::shared_ptr<engine::lsm::PartitionedMemTable> active_memtable_snapshot;
    {
        // A shared lock is sufficient because we are only reading the shared_ptr,
        // not changing which memtable is active.
        std::shared_lock<std::shared_mutex> data_lock(memtable_data_mutex_);
        active_memtable_snapshot = active_memtable_;
    }

    if (!active_memtable_snapshot) {
        // This should be an impossible state if the calling logic is correct.
        throw storage::StorageError(storage::ErrorCode::INTERNAL_ERROR, "Active memtable is null during insertion phase.");
    }
    
    // 1. Inform the adaptive tuner about the write.
    if (memtable_tuner_) {
        memtable_tuner_->RecordWrite(estimated_size);
    }
    
    // 2. Serialize the record's value for storage in the underlying partition.
    // The `PartitionedMemTable` interface works with raw strings for values.
    std::string value_str;
    bool is_tombstone = record.deleted;
    if (!is_tombstone) {
        value_str = LogRecord::serializeValueTypeBinary(record.value);
    }
    
    // 3. Delegate the write operation to the PartitionedMemTable.
    // It will handle hashing the key to the correct partition and performing the
    // concurrent write with fine-grained locking.
    if (is_tombstone) {
        // The commit_txn_id serves as the sequence number for the tombstone.
        active_memtable_snapshot->Delete(key, record.commit_txn_id);
    } else {
        active_memtable_snapshot->Add(key, value_str, record.commit_txn_id);
    }
}
/**
 * @brief Notifies one of the background flush worker threads that a new
 * immutable memtable is ready to be flushed.
 * @pre The caller must NOT hold any memtable-related locks to avoid deadlocks.
 */
void LSMTree::notifyFlushWorker() {
    std::unique_lock<std::mutex> cv_lock(memtable_cv_mutex_);
    immutable_memtable_ready_cv_.notify_one();
    LOG_TRACE("[LSMTree] Notified background flush worker.");
}

void LSMTree::BeginTxnRead(Transaction* tx_ptr) {
    std::shared_lock<std::shared_mutex> lock(memtable_data_mutex_);
    if (active_memtable_) {
        // This method needs to be added to PartitionedMemTable and plumbed down.
        active_memtable_->BeginTxnRead(tx_ptr);
    }
}
void LSMTree::EndTxnRead(Transaction* tx_ptr) {
    std::shared_lock<std::shared_mutex> lock(memtable_data_mutex_);
    if (active_memtable_) {
        active_memtable_->EndTxnRead(tx_ptr);
    }
}

std::shared_ptr<engine::lsm::PartitionedMemTable> LSMTree::getActivePartitionedMemTable() const {
    // === FIX: Use the correct, mutable mutex name ===
    std::shared_lock<std::shared_mutex> lock(memtable_data_mutex_);
    return active_memtable_;
}

bool LSMTree::flushImmutableMemTableToFile(std::shared_ptr<engine::lsm::PartitionedMemTable> memtable_to_flush) {
    if (!memtable_to_flush || memtable_to_flush->Empty()) {
        LOG_TRACE("[LSMFlush] Flush skipped: Memtable is null or empty.");
        return true; // This is not a failure.
    }

    // --- Phase 1: Preparation ---
    uint64_t new_sstable_id = next_sstable_file_id_.fetch_add(1);
    int target_level = 0; // Flushes from memory always create L0 files.
    std::string final_filepath = generateSSTableFilename(target_level, new_sstable_id);
    std::string temp_filepath = final_filepath + ".tmp_flush";

    LOG_INFO("[LSMFlush] Starting flush of memtable ({} records) to new L0 SSTable ID {}.",
             memtable_to_flush->Count(), new_sstable_id);

    // --- FIX --- Use the correct namespace for SSTableBuilder
    std::shared_ptr<engine::lsm::SSTableBuilder> builder;
    
    // --- Phase 2: Build the SSTable File ---
    try {
        // --- FIX --- Use the correct namespace for SSTableBuilder
        engine::lsm::SSTableBuilder::Options builder_options;
        builder_options.compression_type = sstable_compression_type_;
        builder_options.compression_level = sstable_compression_level_;
        builder_options.encryption_is_active = sstable_encryption_is_active_;
        builder_options.encryption_scheme = sstable_default_encryption_scheme_;
        builder_options.dek = &database_dek_;
        builder_options.target_block_size = sstable_target_uncompressed_block_size_;

        // --- FIX --- Use the correct namespace for SSTableBuilder
        builder = std::make_shared<engine::lsm::SSTableBuilder>(temp_filepath, builder_options);

        // --- FIX --- Use the correct namespace for MemTableIterator and Entry
        std::unique_ptr<engine::lsm::MemTableIterator> iter = memtable_to_flush->NewIterator();
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            engine::lsm::Entry entry = iter->GetEntry();
            
            Record record_to_add;
            record_to_add.key = entry.key;
            record_to_add.commit_txn_id = entry.sequence_number;
            record_to_add.txn_id = entry.sequence_number;
            
            if (entry.value.empty()) {
                record_to_add.deleted = true;
            } else {
                record_to_add.deleted = false;
                auto value_opt = LogRecord::deserializeValueTypeBinary(entry.value);
                if (!value_opt) {
                    LOG_WARN("[LSMFlush] Failed to deserialize value for key '{}' from memtable during flush. Skipping record.", entry.key);
                    continue;
                }
                record_to_add.value = *value_opt;
            }
            
            builder->add(record_to_add);
        }

        builder->finish();

    } catch (const std::exception& e) {
        LOG_ERROR("[LSMFlush] CRITICAL FAILURE during SSTable building for ID {}: {}. The flush operation has failed.",
                  new_sstable_id, e.what());
        return false;
    }

    // --- Phase 3: Finalization and Metadata Integration ---
    try {
        auto sstable_metadata = builder->getFileMetadata();
        
        if (sstable_metadata->total_entries == 0) {
            LOG_INFO("[LSMFlush] Flush for SSTable ID {} resulted in an empty file. No changes will be applied.", new_sstable_id);
            return true;
        }

        sstable_metadata->filename = final_filepath;
        sstable_metadata->sstable_id = new_sstable_id;
        sstable_metadata->level = target_level;

        fs::rename(temp_filepath, final_filepath);
        
        if (storage_engine_ptr_ && storage_engine_ptr_->getLivenessOracle()) {
            auto liveness_filter = builder->getLivenessBloomFilter();
            if (liveness_filter) {
                storage_engine_ptr_->getLivenessOracle()->addLivenessFilter(
                    sstable_metadata->sstable_id, 
                    liveness_filter
                );
                LOG_TRACE("[LSMFlush] Liveness filter for new SSTable ID {} registered with the oracle.", new_sstable_id);
            }
        }
        
        // --- FIX --- Use the correct namespace for VersionEdit
        engine::lsm::VersionEdit edit;
        edit.AddFile(target_level, sstable_metadata);
        versions_->ApplyChanges(edit);

        LOG_INFO("[LSMFlush] Finalized flush to L{} SSTable: {}. Size: {} bytes. New Version installed.",
                 target_level, final_filepath, sstable_metadata->size_bytes);
        
        return true;

    } catch (const std::exception& e) {
        LOG_ERROR("[LSMFlush] CRITICAL FAILURE during finalization for SSTable ID {}: {}. Cleaning up.",
                  new_sstable_id, e.what());
        fs::remove(temp_filepath);
        fs::remove(final_filepath);
        return false;
    }
}

std::shared_ptr<engine::lsm::SSTableMetadata> LSMTree::createSSTableMetadataForFlush(
    const std::shared_ptr<engine::lsm::PartitionedMemTable>& memtable_to_flush,
    int target_level)
{
    // 1. Atomically get a new ID for this SSTable.
    uint64_t flush_sstable_id = next_sstable_file_id_.fetch_add(1);

    // 2. Create the metadata object, which sets the ID, level, and timestamp.
    auto sstable_metadata = std::make_shared<engine::lsm::SSTableMetadata>(flush_sstable_id, target_level);
    
    // 3. Generate the final path for the file.
    sstable_metadata->filename = generateSSTableFilename(target_level, flush_sstable_id);
    
    // 4. Initialize min/max TxnID. These will be updated as records are written.
    sstable_metadata->min_txn_id = std::numeric_limits<TxnId>::max();
    sstable_metadata->max_txn_id = 0;

    // 5. Estimate item count for the Bloom filters. This is a key optimization.
    // The PartitionedMemTable's Count() is a fast, accurate source for this estimate.
    size_t num_records = memtable_to_flush->Count();
    
    // The Bloom filter itself is no longer created here. The partitioned filter
    // logic inside the write loop will create the smaller filter partitions as needed.
    // The `filter_partition_index` map in the metadata will be populated during the write.

    LOG_TRACE("[LSMFlush Prep] Created metadata for SSTable ID {}. Target Level: {}, Records to flush: {}.",
              flush_sstable_id, target_level, num_records);
              
    return sstable_metadata;
}

void LSMTree::writeMemTableToTempSSTable(
    const std::shared_ptr<engine::lsm::PartitionedMemTable>& memtable_to_flush,
    std::shared_ptr<engine::lsm::SSTableMetadata>& sstable_metadata,
    const std::string& temp_filename)
{
    // 1. Set up the builder options from the LSMTree's configuration.
    engine::lsm::SSTableBuilder::Options builder_options;
    builder_options.compression_type = sstable_compression_type_;
    builder_options.compression_level = sstable_compression_level_;
    builder_options.encryption_is_active = sstable_encryption_is_active_;
    builder_options.encryption_scheme = sstable_default_encryption_scheme_;
    builder_options.dek = &database_dek_;
    builder_options.target_block_size = sstable_target_uncompressed_block_size_;
    
    // 2. Create the SSTableBuilder instance. This opens the temporary file for writing.
    engine::lsm::SSTableBuilder builder(temp_filename, builder_options);

    // 3. Iterate through the sorted memtable and add each record to the builder.
    std::unique_ptr<engine::lsm::MemTableIterator> iter = memtable_to_flush->NewIterator();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        engine::lsm::Entry entry = iter->GetEntry();
        
        Record record_to_add;
        record_to_add.key = entry.key;
        record_to_add.commit_txn_id = entry.sequence_number;
        record_to_add.txn_id = entry.sequence_number; // In a flush, txn_id and commit_txn_id are the same
        
        if (entry.value.empty()) { // Memtable's convention for a tombstone
            record_to_add.deleted = true;
        } else {
            record_to_add.deleted = false;
            auto value_opt = LogRecord::deserializeValueTypeBinary(entry.value);
            if (!value_opt) {
                LOG_WARN("[LSMFlush] Failed to deserialize value for key '{}' from memtable. Skipping record.", entry.key);
                continue;
            }
            record_to_add.value = *value_opt;
        }
        
        // The builder handles all internal buffering, block creation, filter partitioning, etc.
        builder.add(record_to_add);
    }

    // 4. Finalize the SSTable file. This writes all metadata and closes the file.
    builder.finish();
    
    // 5. Get the fully populated metadata object from the builder.
    // This is a new shared_ptr, so we overwrite the one passed in.
    sstable_metadata = builder.getFileMetadata(); 
}


void LSMTree::finalizeSSTableFlush(
    std::shared_ptr<engine::lsm::SSTableMetadata>& sstable_metadata,
    const std::string& temp_filename)
{
    // === PHASE 1: Filesystem Operation ===
    try {
        fs::rename(temp_filename, sstable_metadata->filename);
        sstable_metadata->size_bytes = fs::file_size(sstable_metadata->filename);
    } catch (const fs::filesystem_error& e) {
        throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Failed to rename/finalize SSTable file.")
            .withContext("source", temp_filename)
            .withContext("destination", sstable_metadata->filename)
            .withDetails(e.what());
    }

    // === PHASE 2: Atomic Metadata Update via VersionSet ===
    engine::lsm::VersionEdit edit;
    edit.AddFile(sstable_metadata->level, sstable_metadata);
    versions_->ApplyChanges(edit);

    LOG_INFO("[LSMFlush] Finalized flush to L{} SSTable: {}. Size: {} bytes. New Version installed.",
             sstable_metadata->level, sstable_metadata->filename, sstable_metadata->size_bytes);
}

void LSMTree::background_flush_worker_loop() {
    LOG_INFO("[LSMFlushWorker ThdID: {}] Background flush worker thread started for LSMTree {:p}.",
             std::this_thread::get_id(), static_cast<void*>(this));

    while (!flush_workers_shutdown_.load(std::memory_order_acquire)) {
        std::shared_ptr<engine::lsm::PartitionedMemTable> memtable_to_flush = nullptr;

        // --- Phase 1: Wait for and retrieve an immutable memtable ---
        {
            std::unique_lock<std::mutex> cv_lock(memtable_cv_mutex_);
            immutable_memtable_ready_cv_.wait(cv_lock, [this] {
                if (flush_workers_shutdown_.load()) return true;
                std::shared_lock<std::shared_mutex> data_lock(memtable_data_mutex_);
                return !immutable_memtables_.empty();
            });

            if (flush_workers_shutdown_.load()) break;

            { // Scope for exclusive data lock to modify the queue
                std::unique_lock<std::shared_mutex> data_lock(memtable_data_mutex_);
                if (!immutable_memtables_.empty()) {
                    memtable_to_flush = std::move(immutable_memtables_.front());
                    immutable_memtables_.erase(immutable_memtables_.begin());
                }
            }

            if (memtable_to_flush) {
                 immutable_slot_available_cv_.notify_all();
            }
        }

        // --- Phase 2: Coordinate with Global Manager and Flush ---
        if (memtable_to_flush) {
            size_t flushed_mem_size = memtable_to_flush->ApproximateMemoryUsage();
            
            // Unregister from the global manager BEFORE flushing.
            if (write_buffer_manager_) {
                write_buffer_manager_->UnregisterImmutable(this, flushed_mem_size);
            }
            
            bool flush_success = false;
            try {
                flush_success = flushImmutableMemTableToFile(memtable_to_flush);
            } catch (const std::exception& e) {
                LOG_ERROR("[LSMFlushWorker ThdID: {}] Exception during flush: {}. Memtable data may be lost if not recovered from WAL.",
                          std::this_thread::get_id(), e.what());
            }

            // CRITICAL: Free Memory from the Global Manager, regardless of flush success.
            // The memory is no longer tracked by this LSMTree, so its reservation must be released.
            if (write_buffer_manager_) {
                write_buffer_manager_->FreeMemory(flushed_mem_size);
            }
            
            if (flush_success) {
                LOG_INFO("[LSMFlushWorker ThdID: {}] Successfully flushed memtable ({} records, ~{} bytes) to SSTableMetadata.",
                         std::this_thread::get_id(), memtable_to_flush->Count(), flushed_mem_size);
                
                // Notify the compaction scheduler that a new L0 file exists.
                {
                    std::lock_guard<std::mutex> comp_sched_lock(scheduler_mutex_);
                    scheduler_cv_.notify_one();
                }
            } else {
                LOG_ERROR("[LSMFlushWorker ThdID: {}] Failed to flush memtable ({} records).",
                          std::this_thread::get_id(), memtable_to_flush->Count());
            }
            
            memtable_to_flush.reset(); // Release the shared_ptr.
        }
    }
    LOG_INFO("[LSMFlushWorker ThdID: {}] Background flush worker thread terminating for LSMTree {:p}.",
             std::this_thread::get_id(), static_cast<void*>(this));
}

// Renamed public flush method
void LSMTree::flushActiveMemTable(bool synchronous /* = false */) {
    // Step 1: Safely swap the active memtable. This returns the memtable that needs flushing.
    std::shared_ptr<engine::lsm::PartitionedMemTable> memtable_to_flush = swapActiveMemTable();

    if (!memtable_to_flush) {
        LOG_INFO("[LSMTree::flushActiveMemTable] Active memtable was empty. No flush necessary.");
        return;
    }

    // Step 2: Decide whether to flush now or delegate to a background worker.
    if (synchronous) {
        LOG_INFO("[LSMTree::flushActiveMemTable] Performing synchronous flush for memtable...");
        
        bool flush_success = false;
        size_t mem_size = memtable_to_flush->ApproximateMemoryUsage();
        
        try {
            // Unregister from the global manager before flushing.
            if (write_buffer_manager_) {
                write_buffer_manager_->UnregisterImmutable(this, mem_size);
            }
            
            flush_success = flushImmutableMemTableToFile(memtable_to_flush);
            
            // Free the memory from the global manager.
            if (write_buffer_manager_) {
                write_buffer_manager_->FreeMemory(mem_size);
            }

        } catch (const std::exception& e) {
            LOG_ERROR("[LSMTree::flushActiveMemTable] Exception during synchronous flush: {}", e.what());
            // Even on failure, we must free the memory reservation.
            if (write_buffer_manager_) {
                write_buffer_manager_->FreeMemory(mem_size);
            }
        }

        if (flush_success) {
            // Remove the now-flushed memtable from the immutable list.
            std::unique_lock<std::shared_mutex> data_lock(memtable_data_mutex_);
            immutable_memtables_.erase(
                std::remove(immutable_memtables_.begin(), immutable_memtables_.end(), memtable_to_flush),
                immutable_memtables_.end()
            );
            
            // Notify compaction scheduler.
            std::lock_guard<std::mutex> sched_lock(scheduler_mutex_);
            scheduler_cv_.notify_one();
        } else {
            LOG_ERROR("[LSMTree::flushActiveMemTable] SYNCHRONOUS flush FAILED. Data is in WAL but not SSTableMetadata.");
        }

    } else { // Asynchronous
        LOG_INFO("[LSMTree::flushActiveMemTable] Asynchronous flush requested. Notifying background worker.");
        notifyFlushWorker();
    }
}

/**
 * @brief Retrieves the latest visible version of a record for a given key.
 *
 * This method orchestrates the search through the LSM-Tree's hierarchical layers,
 * adhering to Multi-Version Concurrency Control (MVCC) rules to provide snapshot isolation.
 * The search order is critical for correctness:
 * 1.  In-Memory Active MemTable
 * 2.  In-Memory Immutable MemTables (newest to oldest)
 * 3.  On-Disk Level 0 (L0) SSTables (newest to oldest)
 * 4.  On-Disk Level 1+ (L1+) SSTables
 *
 * The search stops as soon as the latest visible version (or a definitive tombstone) is found.
 *
 * @param key The key of the record to retrieve.
 * @param txn_id The transaction ID of the reader, which defines the snapshot in time.
 *        The method will only return data from transactions that committed *before* this ID.
 * @return An optional containing the visible Record if found. Returns std::nullopt if the key
 *         does not exist, has been deleted, or no version is visible to the given transaction.
 */
std::optional<Record> LSMTree::get(const std::string& key, TxnId txn_id) {
    // Stage 1: Search all in-memory layers using the refactored helper.
    LookupResult mem_result = searchMemTables(key, txn_id);

    if (mem_result.record.has_value()) {
        // A visible, non-deleted version was found in memory. This is the latest possible
        // version, so we can return it immediately without checking disk.
        return mem_result.record;
    }
    if (mem_result.found_definitive_tombstone) {
        // The latest visible version found in memory is a tombstone. This means the key
        // is considered deleted at this point in time, regardless of what older data
        // might exist on disk.
        return std::nullopt;
    }

    // Stage 2: Key was not found conclusively in memory. Proceed to search on-disk SSTables.
    // The existing SSTableMetadata search logic remains valid.
    return searchSSTables(key, txn_id);
}

/**
 * @brief Searches all in-memory memtables (active and immutable) for a key.
 * This is the first stage of any read operation. It takes a snapshot of the current
 * memtable pointers and then performs lock-free lookups.
 * @param key The key to search for.
 * @param reader_txn_id The transaction ID of the reader for MVCC visibility.
 * @return A LookupResult indicating the outcome of the search.
 * @pre The caller must NOT hold the `memtable_data_mutex_`.
 */
LSMTree::LookupResult LSMTree::searchMemTables(const std::string& key, TxnId reader_txn_id) {
    // Take a thread-safe snapshot of the memtable pointers.
    std::shared_ptr<engine::lsm::PartitionedMemTable> active_snapshot;
    std::vector<std::shared_ptr<engine::lsm::PartitionedMemTable>> immutable_snapshot;
    {
        std::unique_lock<std::shared_mutex> data_lock(memtable_data_mutex_);
        active_snapshot = active_memtable_;
        immutable_snapshot = immutable_memtables_;
    }

    // A helper lambda to process a lookup result from a single PartitionedMemTable.
    auto process_lookup = [&](const engine::lsm::PartitionedMemTable& memtable) -> std::optional<LookupResult> {
        std::string value_str;
        uint64_t seq_num;

        // The Get method on PartitionedMemTable is highly concurrent.
        if (memtable.Get(key, value_str, seq_num)) {
            // A version was found. Check if it's visible to our transaction.
            if (seq_num != 0 && seq_num < reader_txn_id) {
                // The version is visible. Check if it's a tombstone.
                // Our convention is that an empty value string signifies a tombstone.
                if (value_str.empty()) {
                    return LookupResult{std::nullopt, true}; // Definitive tombstone found.
                }

                // It's visible data. Reconstruct the Record object.
                Record rec;
                rec.key = key;
                rec.commit_txn_id = seq_num;
                rec.deleted = false;
                
                auto val_opt = LogRecord::deserializeValueTypeBinary(value_str);
                if (val_opt) {
                    rec.value = *val_opt;
                    return LookupResult{rec, false}; // Visible data found.
                } else {
                    LOG_WARN("[LSMTree] Failed to deserialize value for key '{}' from memtable. Treating as corrupt.", key);
                    // Treat corrupt data as if it wasn't found.
                }
            }
        }
        // No conclusive result (key not found, or version not visible).
        return std::nullopt;
    };

    // 1. Check the active memtable first, as it contains the newest data.
    if (active_snapshot) {
        if (auto result = process_lookup(*active_snapshot); result) {
            return *result;
        }
    }

    // 2. Check immutable memtables in reverse order (newest to oldest).
    for (auto it = immutable_snapshot.rbegin(); it != immutable_snapshot.rend(); ++it) {
        if (auto result = process_lookup(**it); result) {
            return *result;
        }
    }

    // No conclusive result found in any memtable. The search must continue to disk.
    return {std::nullopt, false};
}

/**
 * @brief Searches all on-disk SSTableMetadata levels for the latest visible version of a key.
 * This function orchestrates the search through L0 and then L1+.
 * @param key The key to search for.
 * @param reader_txn_id The transaction ID of the reader.
 * @return An optional Record if a visible version is found.
 * @pre The caller must NOT hold the `levels_metadata_mutex_`.
 */
std::optional<Record> LSMTree::searchSSTables(const std::string& key, TxnId reader_txn_id) {
    // === PHASE 1: Acquire an immutable snapshot of the current Version ===
    // This is an atomic pointer read, which is extremely fast and lock-free.
    engine::lsm::Version* current_version = versions_->GetCurrent();
    
    // Use a RAII guard to ensure Unref() is called on the Version when this function exits.
    // This is critical for preventing memory leaks.
    engine::lsm::VersionUnreffer unreffer(current_version);

    // Get the actual level metadata from the snapshot.
    const auto& levels_snapshot = current_version->GetLevels();

    LOG_TRACE("[LSMTree::searchSSTables] Acquired Version snapshot for key '{}'.", key);

    // === PHASE 2: Search Level 0 ===
    // L0 files overlap, so they must be checked newest-first.
    std::optional<Record> result = searchLevel0(levels_snapshot, key, reader_txn_id);
    if (result) {
        // Found the latest version on disk in L0. No need to search deeper (older) levels.
        return result;
    }

    // === PHASE 3: Search Level 1 and higher ===
    // L1+ files have non-overlapping key ranges, allowing for efficient binary search.
    for (size_t level_idx = 1; level_idx < levels_snapshot.size(); ++level_idx) {
        result = searchLevelN(levels_snapshot, level_idx, key, reader_txn_id);
        if (result) {
            // Found the key in this level. Since levels are searched in order (L1, L2, ...),
            // this is the latest version on disk.
            return result;
        }
    }

    // Key was not found in any SSTableMetadata on any level.
    return std::nullopt;
}

/**
 * @brief Searches all SSTables in Level 0 for a key.
 * @param levels_snapshot A snapshot of the current level metadata.
 * @param key The key to search for.
 * @param reader_txn_id The transaction ID of the reader.
 * @return An optional Record if a visible version is found.
 */
std::optional<Record> LSMTree::searchLevel0(
    const std::vector<std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>>& levels_snapshot,
    const std::string& key,
    TxnId reader_txn_id)
{
    if (levels_snapshot.empty()) return std::nullopt;

    // L0 files are sorted newest-first by their ID.
    for (const auto& sstable_meta : levels_snapshot[0]) {
        std::optional<Record> result = findRecordInSSTable(sstable_meta, key, reader_txn_id);
        if (result) {
            return result; // Found the latest version in L0.
        }
    }
    return std::nullopt;
}

/**
 * @brief Searches for a key in a sorted level (L1+).
 * @param levels_snapshot A snapshot of the current level metadata.
 * @param level_idx The index of the level to search (must be > 0).
 * @param key The key to search for.
 * @param reader_txn_id The transaction ID of the reader.
 * @return An optional Record if a visible version is found.
 */
std::optional<Record> LSMTree::searchLevelN(
    const std::vector<std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>>& levels_snapshot,
    int level_idx,
    const std::string& key,
    TxnId reader_txn_id)
{
    const auto& level_tables = levels_snapshot[level_idx];
    if (level_tables.empty()) return std::nullopt;

    // Binary search to find the candidate SSTableMetadata.
    auto it = std::lower_bound(level_tables.begin(), level_tables.end(), key,
        [](const std::shared_ptr<engine::lsm::SSTableMetadata>& sstable, const std::string& k) {
            return !sstable->max_key.empty() && sstable->max_key < k;
        });

    if (it != level_tables.end()) {
        const auto& sstable_meta = *it;
        // Verify the key is within the min/max range of the found SSTableMetadata.
        if ((sstable_meta->min_key.empty() || key >= sstable_meta->min_key)) {
            return findRecordInSSTable(sstable_meta, key, reader_txn_id);
        }
    }
    return std::nullopt;
}

/**
 * @brief Finds the latest visible record for a key within a single SSTableMetadata file.
 * @param sstable_meta The metadata of the SSTableMetadata to search.
 * @param key The key to search for.
 * @param reader_txn_id The transaction ID of the reader.
 * @return An optional Record containing the latest visible version found in this file.
 */
std::optional<Record> LSMTree::findRecordInSSTable(
    const std::shared_ptr<engine::lsm::SSTableMetadata>& sstable_meta,
    const std::string& key,
    TxnId reader_txn_id)
{
    // --- 1. Partitioned Filter Check ---
    if (!sstable_meta->filter_partition_index.empty()) {
        // Find the partition whose key range our target key falls into.
        auto it = sstable_meta->filter_partition_index.upper_bound(key);
        if (it != sstable_meta->filter_partition_index.begin()) {
            --it;
            const std::string& partition_key = it->first;
            
            // Check the lazy-loaded filter cache first.
            std::shared_ptr<BloomFilter> filter_partition;
            {
                std::lock_guard<std::mutex> lock(sstable_meta->filter_cache_mutex_);
                if (sstable_meta->filter_cache_.count(partition_key)) {
                    filter_partition = sstable_meta->filter_cache_[partition_key];
                }
            }

            // If not in cache, load it from disk.
            if (!filter_partition) {
                std::ifstream file_stream(sstable_meta->filename, std::ios::binary);
                if (file_stream) {
                    const auto& entry = it->second;
                    file_stream.seekg(entry.partition_offset);
                    auto new_filter = std::make_shared<BloomFilter>();
                    if (new_filter->deserializeFromStream(file_stream)) {
                        filter_partition = new_filter;
                        // Populate the cache for the next lookup.
                        std::lock_guard<std::mutex> lock(sstable_meta->filter_cache_mutex_);
                        sstable_meta->filter_cache_[partition_key] = filter_partition;
                    }
                }
            }

            // If we successfully loaded the filter, check it.
            if (filter_partition && !filter_partition->mightContain(key)) {
                return std::nullopt; // Filtered out! Key is definitely not in this SSTable.
            }
        }
    }
    
    // --- 2. Find Data Block using Two-Level Index ---
    if (sstable_meta->block_index.empty()) {
        return std::nullopt; // No data in this file.
    }

    std::ifstream file_stream(sstable_meta->filename, std::ios::binary);
    if (!file_stream) return std::nullopt;

    SSTableBlockIndexEntry data_block_entry;
    try {
        // Step 2a: Use in-memory L2 index to find the L1 index block on disk.
        auto l2_it = sstable_meta->block_index.upper_bound(key);
        if (l2_it == sstable_meta->block_index.begin()) {
            return std::nullopt; // Key is before the very first key in the file.
        }
        --l2_it;

        std::streampos l1_block_offset = l2_it->second;
        
        // Step 2b: Read and deserialize the L1 index block from disk.
        file_stream.seekg(l1_block_offset);
        uint32_t num_l1_entries;
        file_stream.read(reinterpret_cast<char*>(&num_l1_entries), sizeof(num_l1_entries));
        
        std::map<std::string, SSTableBlockIndexEntry> l1_index_block;
        for (uint32_t i = 0; i < num_l1_entries; ++i) {
            auto l1_key_opt = deserializeKey(file_stream);
            if (!l1_key_opt) throw std::runtime_error("Corrupt L1 index block.");
            SSTableBlockIndexEntry entry;
            file_stream.read(reinterpret_cast<char*>(&entry), sizeof(entry));
            l1_index_block[*l1_key_opt] = entry;
        }

        // Step 2c: Find the data block entry within the temporary L1 index map.
        auto block_it = l1_index_block.upper_bound(key);
        if (block_it == l1_index_block.begin()) {
            return std::nullopt;
        }
        --block_it;
        data_block_entry = block_it->second;

    } catch (const std::exception& e) {
        LOG_ERROR("[LSM findRecord] Error traversing multi-level index for key '{}' in {}: {}",
                  key, sstable_meta->filename, e.what());
        return std::nullopt;
    }
    
    // 3. Read and process the data block
    std::vector<uint8_t> block_data = readAndProcessSSTableBlock(file_stream, data_block_entry, sstable_meta->filename);
    if (block_data.empty()) return std::nullopt;

    // 4. Scan records within the block to find the latest visible version
    const uint8_t* ptr = block_data.data();
    const uint8_t* end_ptr = ptr + block_data.size();
    std::optional<Record> latest_visible_record;

    LOG_TRACE("    [findRecordInSSTable] Scanning block for key '{}'. Block size: {}",
            format_key_for_print(key), block_data.size()); // <<< ADD LOG

    while (ptr < end_ptr) {
        std::optional<Record> rec_opt = deserializeRecordFromBuffer(ptr, end_ptr);
        if (!rec_opt) break;

        // <<< ADD THIS DETAILED LOG >>>
        LOG_TRACE("      - Deserialized key: '{}', CommitTxn: {}",
                format_key_for_print(rec_opt->key), rec_opt->commit_txn_id);

        if (rec_opt->key == key) {
            LOG_TRACE("        -> KEY MATCH!"); // <<< ADD LOG
            if (rec_opt->commit_txn_id != 0 && rec_opt->commit_txn_id < reader_txn_id) {
                LOG_TRACE("          -> Visible version found."); // <<< ADD LOG
                latest_visible_record = rec_opt;
                break;
            }
        } else if (rec_opt->key > key && sstable_meta->level > 0) {
            LOG_TRACE("      - Scanned past target key. Stopping block scan."); // <<< ADD LOG
            break;
        }
    }

    if (latest_visible_record && latest_visible_record->deleted) {
        return std::nullopt; // The latest visible version is a tombstone.
    }
    
    return latest_visible_record;
}

std::vector<uint8_t> LSMTree::readAndProcessSSTableBlock(
    std::ifstream& file_stream,
    const SSTableBlockIndexEntry& block_meta,
    const std::string& sstable_filename_for_context) const
{
    try {
        // Phase 1: Read raw data and verify checksum
        SSTableDataBlockHeader block_header;
        std::vector<uint8_t> on_disk_payload = readBlockHeaderAndPayload(
            file_stream, block_meta, block_header, sstable_filename_for_context);

        // Phase 2: Decrypt if necessary
        std::vector<uint8_t> decrypted_payload = decryptBlockData(
            block_header, std::move(on_disk_payload));
            
        // Phase 3: Decompress if necessary
        std::vector<uint8_t> uncompressed_data = decompressBlockData(
            block_header, std::move(decrypted_payload));

        return uncompressed_data;

    } catch (const std::exception& e) {
        LOG_ERROR("[LSMTree::readAndProcessBlock] Failed to process block for SSTableMetadata '{}'. Error: {}",
                  sstable_filename_for_context, e.what());
        return {}; // Return empty vector on any failure
    }
}

/**
 * @brief Helper: Handles raw disk I/O for reading a block's header and payload.
 * It also performs the critical checksum verification.
 */
std::vector<uint8_t> LSMTree::readBlockHeaderAndPayload(
    std::ifstream& file_stream,
    const SSTableBlockIndexEntry& block_meta,
    SSTableDataBlockHeader& out_header,
    const std::string& sstable_filename_for_context) const
{
    if (!file_stream.is_open()) {
        throw std::runtime_error("File stream not open for reading.");
    }

    file_stream.seekg(block_meta.block_offset_in_file);
    if (!file_stream) {
        throw std::runtime_error("Failed to seek to block offset " + std::to_string(static_cast<long long>(block_meta.block_offset_in_file)));
    }

    file_stream.read(reinterpret_cast<char*>(&out_header), SSTABLE_DATA_BLOCK_HEADER_SIZE);
    if (!file_stream || file_stream.gcount() != static_cast<std::streamsize>(SSTABLE_DATA_BLOCK_HEADER_SIZE)) {
        throw std::runtime_error("Failed to read full SSTableDataBlockHeader.");
    }

    // Sanity check metadata consistency
    if (out_header.get_compression_type() != block_meta.compression_type ||
        out_header.get_encryption_scheme() != block_meta.encryption_scheme ||
        out_header.compressed_payload_size_bytes != block_meta.compressed_payload_size_bytes) {
        throw std::runtime_error("Mismatch between block index and on-disk header. Corruption suspected.");
    }

    std::vector<uint8_t> disk_payload_buffer(out_header.compressed_payload_size_bytes);
    if (out_header.compressed_payload_size_bytes > 0) {
        file_stream.read(reinterpret_cast<char*>(disk_payload_buffer.data()), out_header.compressed_payload_size_bytes);
        if (!file_stream) {
            throw std::runtime_error("Failed to read full on-disk payload.");
        }
    }

    uint32_t calculated_checksum = calculate_payload_checksum(disk_payload_buffer.data(), disk_payload_buffer.size());
    if (calculated_checksum != out_header.payload_checksum) {
        throw std::runtime_error("CHECKSUM MISMATCH! DiskHeader: " + std::to_string(out_header.payload_checksum) +
                                 ", Calculated: " + std::to_string(calculated_checksum));
    }

    return disk_payload_buffer;
}

/**
 * @brief Helper: Decrypts block data using metadata from the block's header.
 */
std::vector<uint8_t> LSMTree::decryptBlockData(
    const SSTableDataBlockHeader& block_header,
    std::vector<uint8_t>&& encrypted_payload) const
{
    EncryptionScheme scheme = block_header.get_encryption_scheme();
    if (scheme == EncryptionScheme::NONE) {
        return std::move(encrypted_payload);
    }

    if (!sstable_encryption_is_active_) {
        throw std::runtime_error("Block is encrypted but LSM encryption is disabled.");
    }
    if (database_dek_.empty()) {
        throw std::runtime_error("Encryption active but DEK missing.");
    }

    EncryptionLibrary::EncryptedData ed_from_block;
    ed_from_block.iv.assign(block_header.iv, block_header.iv + EncryptionLibrary::AES_GCM_IV_SIZE);
    ed_from_block.tag.assign(block_header.auth_tag, block_header.auth_tag + EncryptionLibrary::AES_GCM_TAG_SIZE);
    ed_from_block.data = std::move(encrypted_payload); // Move the payload

    // Reconstruct AAD for verification
    std::vector<uint8_t> aad_data;
    aad_data.push_back(block_header.flags);
    const uint8_t* ucs_ptr = reinterpret_cast<const uint8_t*>(&block_header.uncompressed_content_size_bytes);
    aad_data.insert(aad_data.end(), ucs_ptr, ucs_ptr + sizeof(block_header.uncompressed_content_size_bytes));
    const uint8_t* nr_ptr = reinterpret_cast<const uint8_t*>(&block_header.num_records_in_block);
    aad_data.insert(aad_data.end(), nr_ptr, nr_ptr + sizeof(block_header.num_records_in_block));

    return EncryptionLibrary::decryptWithAES_GCM_Binary(ed_from_block, database_dek_, aad_data);
}

/**
 * @brief Helper: Decompresses block data using metadata from the block's header.
 */
std::vector<uint8_t> LSMTree::decompressBlockData(
    const SSTableDataBlockHeader& block_header,
    std::vector<uint8_t>&& compressed_payload) const
{
    CompressionType comp_type = block_header.get_compression_type();
    if (comp_type == CompressionType::NONE) {
        return std::move(compressed_payload);
    }

    if (block_header.uncompressed_content_size_bytes == 0) {
        return {}; // Valid empty compressed block
    }

    return CompressionManager::decompress(
        compressed_payload.data(), compressed_payload.size(),
        block_header.uncompressed_content_size_bytes, comp_type);
}

void LSMTree::scanMemTablesForPrefix(PrefixScanState& state) {
    // Take a snapshot of the memtable pointers under a shared lock for concurrency.
    std::shared_ptr<engine::lsm::PartitionedMemTable> active_snapshot;
    std::vector<std::shared_ptr<engine::lsm::PartitionedMemTable>> immutable_snapshot;
    {
        std::shared_lock<std::shared_mutex> lock(memtable_data_mutex_);
        active_snapshot = active_memtable_;
        immutable_snapshot = immutable_memtables_;
    }

    // A helper lambda to scan a single PartitionedMemTable instance.
    // This avoids code duplication for scanning the active vs. immutable memtables.
    auto scan_single_memtable = [&](const engine::lsm::PartitionedMemTable& memtable) {
        if (state.isLimitReached()) return;

        const size_t prefix_segments = countPathSegments(state.prefix);
        const size_t expected_key_segments = prefix_segments + 1;

        std::unique_ptr<engine::lsm::MemTableIterator> iter = memtable.NewIterator();
        
        const std::string& start_key = state.start_key_exclusive.empty() ? state.prefix : state.start_key_exclusive;
        iter->Seek(start_key);

        if (!state.start_key_exclusive.empty() && iter->Valid() && iter->GetEntry().key == state.start_key_exclusive) {
            iter->Next();
        }

        while (iter->Valid()) {
            if (state.isLimitReached()) break;
            
            engine::lsm::Entry entry = iter->GetEntry();

            // Since the iterator is sorted, once we are past the prefix, we can stop.
            if (entry.key.rfind(state.prefix, 0) != 0) {
                break;
            }

            // Filter for direct children only.
            if (countPathSegments(entry.key) != expected_key_segments) {
                iter->Next();
                continue;
            }

            // If we've already found a result (or a tombstone) for this key in a newer
            // memtable, skip this older version.
            if (state.results_map.count(entry.key) || state.deleted_keys_tracker.count(entry.key)) {
                iter->Next();
                continue;
            }

            // Check MVCC visibility.
            if (entry.sequence_number != 0 && entry.sequence_number < state.reader_txn_id) {
                if (entry.value.empty()) { // Convention for tombstone.
                    state.deleted_keys_tracker.insert(entry.key);
                } else {
                    Record rec;
                    rec.key = entry.key;
                    rec.commit_txn_id = entry.sequence_number;
                    rec.deleted = false;
                    auto val_opt = LogRecord::deserializeValueTypeBinary(entry.value);
                    if (val_opt) {
                        rec.value = *val_opt;
                        state.results_map[entry.key] = rec;
                    }
                }
            }
            iter->Next();
        }
    };

    // 1. Scan the active memtable first.
    if (active_snapshot) {
        scan_single_memtable(*active_snapshot);
    }

    if (state.isLimitReached()) return;

    // 2. Scan immutable memtables in reverse order (newest to oldest).
    for (auto it = immutable_snapshot.rbegin(); it != immutable_snapshot.rend(); ++it) {
        if (state.isLimitReached()) break;
        if (*it) {
            scan_single_memtable(**it);
        }
    }
}

std::vector<Record> LSMTree::getPrefix(
    const std::string& prefix_param,
    TxnId reader_txn_id,
    size_t limit,
    const std::string& start_key_exclusive)
{
    // Normalize prefix to ensure it acts like a directory path.
    std::string effective_prefix = prefix_param;
    if (effective_prefix.empty() || effective_prefix.back() != '/') {
        effective_prefix += '/';
    }

    // Initialize the state object that will be passed through the scan helpers.
    PrefixScanState state = {
        effective_prefix,
        reader_txn_id,
        limit,
        start_key_exclusive
    };

    // --- Phase 1 & 2: Scan all in-memory layers ---
    scanMemTablesForPrefix(state);
    if (state.isLimitReached()) {
        goto finalize_results;
    }

    // --- Phase 3: Scan all on-disk layers ---
    scanSSTablesForPrefix(state);

finalize_results:
    // Convert the results map to a vector for the final return value.
    std::vector<Record> final_results_vec;
    final_results_vec.reserve(state.results_map.size());
    for (const auto& pair : state.results_map) {
        final_results_vec.push_back(pair.second);
    }

    LOG_INFO("[LSMTree::getPrefix] Scan for prefix '{}' complete. Returning {} visible records.",
             format_key_for_print(prefix_param), final_results_vec.size());
             
    return final_results_vec;
}

// --- Private Helper Method Implementations for getPrefix ---

/**
 * @brief Scans all on-disk SSTableMetadata levels for records matching a prefix.
 * @param state The current state of the prefix scan, which will be updated by this method.
 */
void LSMTree::scanSSTablesForPrefix(PrefixScanState& state) {
    // === PHASE 1: Acquire an immutable snapshot of the current Version ===
    engine::lsm::Version* current_version = versions_->GetCurrent();
    engine::lsm::VersionUnreffer unreffer(current_version); // RAII guard

    const auto& levels_snapshot = current_version->GetLevels();

    LOG_TRACE("[LSMTree::scanSSTablesForPrefix] Acquired Version snapshot for prefix '{}'.", state.prefix);

    // === PHASE 2: Scan L0 tables (newest to oldest) ===
    if (!levels_snapshot.empty()) {
        for (const auto& sstable_meta : levels_snapshot[0]) {
            if (state.isLimitReached()) return;
            
            // FIX: Remove the redundant 'if' check. The called function will perform
            // its own pruning. We can call it directly.
            this->scanSSTableForPrefix(sstable_meta, state);
        }
    }

    if (state.isLimitReached()) return;

    // === PHASE 3: Scan L1+ tables (sorted by key range) ===
    for (size_t level_idx = 1; level_idx < levels_snapshot.size(); ++level_idx) {
        if (state.isLimitReached()) return;
        
        const auto& level_tables = levels_snapshot[level_idx];
        if (level_tables.empty()) continue;

        // Find the starting SSTable for the scan using a binary search on max_key.
        const std::string& start_key = state.start_key_exclusive.empty() ? state.prefix : state.start_key_exclusive;
        auto sstable_it = std::lower_bound(level_tables.begin(), level_tables.end(), start_key,
            [](const std::shared_ptr<engine::lsm::SSTableMetadata>& sstable_ptr, const std::string& k) {
                return !sstable_ptr->max_key.empty() && sstable_ptr->max_key < k;
            });

        // Iterate through all potentially relevant SSTables in this level.
        for (; sstable_it != level_tables.end(); ++sstable_it) {
            if (state.isLimitReached()) break;
            
            const auto& sstable_meta = *sstable_it;
            
            // FIX: Add a specific optimization for sorted levels before the main call.
            // If this table's range starts AFTER the prefix (and doesn't begin with it), 
            // we can stop searching this entire level.
            if (!sstable_meta->min_key.empty() && sstable_meta->min_key > state.prefix && sstable_meta->min_key.rfind(state.prefix, 0) != 0) {
                break; 
            }
            
            this->scanSSTableForPrefix(sstable_meta, state);
        }
    }
}

// --- LSMTree::remove ---
// Remove is implemented by placing a tombstone via `put`.
// The caller (Transaction) should create the tombstone Record.
void LSMTree::remove(const std::string& key, TxnId txn_id) {
     // This method is somewhat redundant now as the tombstone creation happens
     // in the Transaction or StorageEngine layer before calling `put`.
     // We keep it for potential direct use cases, but it just logs a warning.
     std::cerr << "Warning: LSMTree::remove called directly. Use put() with a tombstone Record for MVCC." << std::endl;
     // If needed, it could fetch the latest version to include in the tombstone:
     // std::optional<Record> current = get(key, txn_id); // Problem: infinite recursion? Need specific 'get committed'
     // Record tombstone { key, ValueType{}, txn_id, 0, std::chrono::system_clock::now(), true };
     // put(key, tombstone, ???); // Need oldest_active_txn_id
}

// // Additional mutex for condition variables
//std::mutex memtable_cv_mutex_;

// --- LSMTree::flushMemTable ---
static void throw_sstable_write_error_simple(const std::string& msg, const std::string& filename, std::ofstream& stream_ref_for_logging_only) {
 // Note: stream_ref_for_logging_only might be closed if the error happened during close.
    // Its state is mostly for immediate post-write errors.
    std::string full_msg = "SSTableMetadata '" + filename + "': " + msg +
                           ". Stream state (may be post-close if error on close) - eof: " + std::to_string(stream_ref_for_logging_only.eof()) +
                           ", fail: " + std::to_string(stream_ref_for_logging_only.fail()) +
                           ", bad: " + std::to_string(stream_ref_for_logging_only.bad());
    LOG_ERROR("[LSMTree Helper] {}", full_msg);
    // The stream should be closed by its RAII owner or in the catch block of the caller.
    throw std::runtime_error(full_msg);
}


static void throw_sstable_write_error(const std::string& msg, const std::string& filename, std::ofstream& stream) {
    std::string full_msg = "SSTableMetadata '" + filename + "': " + msg +
                           ". Stream state - eof: " + std::to_string(stream.eof()) +
                           ", fail: " + std::to_string(stream.fail()) +
                           ", bad: " + std::to_string(stream.bad());
    LOG_ERROR("[LSMTree writeIndexAndFooter] {}", full_msg);
    stream.close(); // Attempt to close before throwing
    throw std::runtime_error(full_msg);
}
/*
void LSMTree::writeIndexAndFooter(
    std::ofstream& output_file,
    std::shared_ptr<engine::lsm::SSTableMetadata>& table_metadata,
    const std::map<std::string, SSTableBlockIndexEntry>& l1_block_index)
{
    if (!output_file.is_open() || !output_file.good()) {
        throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Output file stream not ready for writing index and footer.")
            .withContext("sstable_file", table_metadata->filename);
    }
    LOG_INFO("[LSMTree] Writing index and footer for SSTable: {}", table_metadata->filename);

    // --- 1. Serialize and Write the Filter Partition Index ---
    std::streampos filter_index_start_pos = output_file.tellp();
    uint32_t num_filter_partitions = static_cast<uint32_t>(table_metadata->filter_partition_index.size());
    output_file.write(reinterpret_cast<const char*>(&num_filter_partitions), sizeof(num_filter_partitions));
    
    for (const auto& [first_key, entry] : table_metadata->filter_partition_index) {
        SerializeString(output_file, first_key);
        output_file.write(reinterpret_cast<const char*>(&entry), sizeof(engine::lsm::FilterPartitionIndexEntry));
    }
    LOG_TRACE("  SSTable '{}': Filter Partition Index written. Partitions: {}", table_metadata->filename, num_filter_partitions);

    // --- 2. Partition and Write L1 Block Indexes, while building the L2 Index ---
    std::map<std::string, std::streampos> l2_index_in_progress;
    auto it = l1_block_index.begin();
    while (it != l1_block_index.end()) {
        std::string first_key_of_l1_block = it->first;
        std::streampos l1_block_start_pos = output_file.tellp();
        l2_index_in_progress[first_key_of_l1_block] = l1_block_start_pos;

        std::ostringstream l1_block_stream(std::ios::binary);
        size_t entries_in_this_block = 0;
        for (size_t i = 0; i < ENTRIES_PER_L1_INDEX_BLOCK && it != l1_block_index.end(); ++i, ++it) {
            SerializeString(l1_block_stream, it->first);
            l1_block_stream.write(reinterpret_cast<const char*>(&it->second), sizeof(SSTableBlockIndexEntry));
            entries_in_this_block++;
        }
        
        std::string l1_block_data = l1_block_stream.str();
        uint32_t num_entries_header = static_cast<uint32_t>(entries_in_this_block);
        output_file.write(reinterpret_cast<const char*>(&num_entries_header), sizeof(num_entries_header));
        output_file.write(l1_block_data.data(), l1_block_data.size());
    }
    LOG_TRACE("  SSTable '{}': All L1 index blocks written.", table_metadata->filename);

    // --- 3. Serialize and Write the L2 Block Index ---
    std::streampos l2_block_index_start_pos = output_file.tellp();
    uint32_t num_l2_entries = static_cast<uint32_t>(l2_index_in_progress.size());
    output_file.write(reinterpret_cast<const char*>(&num_l2_entries), sizeof(num_l2_entries));
    for (const auto& [first_key, offset] : l2_index_in_progress) {
        SerializeString(output_file, first_key);
        output_file.write(reinterpret_cast<const char*>(&offset), sizeof(offset));
    }
    LOG_TRACE("  SSTable '{}': L2 Block Index written. Entries: {}", table_metadata->filename, num_l2_entries);

    // --- 4. & 5. TxnID Range and Entry Counts ---
    std::streampos txn_range_start_pos = output_file.tellp();
    output_file.write(reinterpret_cast<const char*>(&table_metadata->min_txn_id), sizeof(TxnId));
    output_file.write(reinterpret_cast<const char*>(&table_metadata->max_txn_id), sizeof(TxnId));

    std::streampos entry_counts_start_pos = output_file.tellp();
    output_file.write(reinterpret_cast<const char*>(&table_metadata->total_entries), sizeof(uint64_t));
    output_file.write(reinterpret_cast<const char*>(&table_metadata->tombstone_entries), sizeof(uint64_t));

    // --- 6. Write the Final Footer (Pointers Section) ---
    output_file.write(reinterpret_cast<const char*>(&filter_index_start_pos), sizeof(std::streampos));
    output_file.write(reinterpret_cast<const char*>(&l2_block_index_start_pos), sizeof(std::streampos));
    output_file.write(reinterpret_cast<const char*>(&txn_range_start_pos), sizeof(std::streampos));
    output_file.write(reinterpret_cast<const char*>(&entry_counts_start_pos), sizeof(std::streampos));
    
    if (!output_file) {
        throw storage::StorageError(storage::ErrorCode::IO_WRITE_ERROR, "Stream error while writing final footer.")
            .withContext("sstable_file", table_metadata->filename);
    }
    
    // --- 7. Final Flush ---
    output_file.flush();
    if (!output_file) {
        throw storage::StorageError(storage::ErrorCode::IO_FLUSH_ERROR, "Failed to flush SSTable after writing footer.")
            .withContext("sstable_file", table_metadata->filename);
    }
    LOG_INFO("[LSMTree] Successfully written and flushed all metadata for SSTable: {}", table_metadata->filename);
}
*/
std::vector<Record> LSMTree::processKeyVersionsAndGC(
    const std::vector<Record>& versions_for_key,
    TxnId effective_oldest_active_txn_id_for_gc,
    const std::string& key)
{
    if (versions_for_key.empty()) {
        return {};
    }

    // 1. Sort all found versions for this key by commit_txn_id descending (newest first).
    std::vector<Record> sorted_versions = versions_for_key;
    std::sort(sorted_versions.begin(), sorted_versions.end(), [](const Record& a, const Record& b) {
        return a.commit_txn_id > b.commit_txn_id;
    });

    // --- MVCC Garbage Collection Logic ---

    // Find the single "historical anchor" record. This is the most recent version
    // that is *older* than the GC horizon (the oldest active transaction).
    // This version MUST be preserved to maintain correct snapshots for older transactions.
    std::optional<Record> historical_anchor;
    for (const auto& ver : sorted_versions) {
        if (ver.commit_txn_id != 0 && ver.commit_txn_id < effective_oldest_active_txn_id_for_gc) {
            historical_anchor = ver;
            break; 
        }
    }

    // The final version to keep is the newest version overall.
    // The historical anchor is only used to decide if older versions can be dropped.
    Record latest_version = sorted_versions.front();
    
    // Determine if the historical anchor itself can be garbage collected.
    // This is only possible if it's a tombstone AND it's the absolute latest version of the key.
    // If a newer version exists (even one not yet visible to all transactions),
    // this tombstone is still needed to hide the older data from those newer transactions.
    if (historical_anchor.has_value() && historical_anchor->deleted && historical_anchor->commit_txn_id == latest_version.commit_txn_id) {
        // The latest visible version is a tombstone older than any active transaction.
        // It has no purpose and can be completely removed.
        LOG_TRACE("  [GC for '{}'] Key fully garbage collected (latest version is an old tombstone).", key);
        return {}; // Return an empty vector, dropping the key.
    }
    
    // If we reach here, the `latest_version` is the one that must survive this compaction.
    // All other versions are superseded and implicitly dropped.
    
    // --- Compaction Filter Logic ---
    
    if (compaction_filter_) {
        // We only apply the filter to non-deleted records. Tombstones are purely for MVCC.
        if (!latest_version.deleted) {
            ValueType new_value;
            // Note: A more advanced implementation would pass the source level of the record.
            // For now, we pass 0 as a placeholder.
            engine::lsm::FilterDecision decision = compaction_filter_->Filter(
                0, // Placeholder for level
                latest_version.key,
                latest_version.value,
                &new_value
            );

            switch (decision) {
                case engine::lsm::FilterDecision::KEEP:
                    // Do nothing, proceed to keep the record.
                    break;
                case engine::lsm::FilterDecision::MODIFY:
                    // The filter wants to change the value.
                    latest_version.value = new_value;
                    LOG_TRACE("  [CompactionFilter] MODIFIED record for key '{}' via filter '{}'.", key, compaction_filter_->Name());
                    break;
                case engine::lsm::FilterDecision::DROP:
                    // The filter wants to delete this record.
                    LOG_TRACE("  [CompactionFilter] DROPPED record for key '{}' via filter '{}'.", key, compaction_filter_->Name());
                    return {}; // Return empty vector to drop it.
            }
        }
    }
    
    // Return a vector containing only the single, final, processed record.
    return { latest_version };
}

static std::vector<Record> gc_process_key_versions_for_merge(
    const std::vector<Record>& versions_for_key,
    TxnId effective_oldest_active_txn_id_for_gc,
    const std::string& key // For logging
) {
    if (versions_for_key.empty()) return {};

    std::vector<Record> versions_to_keep;
    // Sort versions by commit_txn_id descending (most recent first)
    std::vector<Record> sorted_versions = versions_for_key;
    std::sort(sorted_versions.begin(), sorted_versions.end(), [](const Record& a, const Record& b) {
        return a.commit_txn_id > b.commit_txn_id;
    });

    const Record* latest_version_older_than_gc_horizon = nullptr;
    for (const auto& ver : sorted_versions) {
        if (ver.commit_txn_id != 0 && ver.commit_txn_id < effective_oldest_active_txn_id_for_gc) {
            latest_version_older_than_gc_horizon = &ver;
            break; // Found the newest one that's older than horizon
        }
    }

    bool any_version_kept_newer_than_or_eq_gc_horizon = false;
    for (const auto& ver : sorted_versions) {
        bool should_keep_this_version = false;
        if (ver.commit_txn_id == 0) { // Should not happen for SSTableMetadata records
            LOG_WARN("[Compaction GC for '{}'] Record with commit_txn_id=0 found. Skipping.", key);
            continue;
        }

        if (ver.commit_txn_id >= effective_oldest_active_txn_id_for_gc) {
            should_keep_this_version = true;
            any_version_kept_newer_than_or_eq_gc_horizon = true;
        } else if (latest_version_older_than_gc_horizon == &ver) {
            // This is the latest version older than the horizon. Keep it unless it's a tombstone
            // AND no newer versions (>= horizon) were kept for this key.
            if (!(ver.deleted && !any_version_kept_newer_than_or_eq_gc_horizon)) {
                should_keep_this_version = true;
            }
        }
        // else: version is older than horizon and not the "latest older", so discard.

        if (should_keep_this_version) {
            versions_to_keep.push_back(ver);
        }
    }
    // The versions_to_keep might contain multiple versions if they are all >= horizon,
    // or one version if it's the latest older than horizon.
    // For writing to a new SSTableMetadata, typically only the *latest visible* version is written
    // from this set, unless the SSTableMetadata format supports multiple versions per key (which ours doesn't simply).
    // So, if versions_to_keep is not empty, we take the first one (which is the latest commit_txn_id).
    if (!versions_to_keep.empty()) {
        // Return only the latest surviving version.
        // If the latest surviving is a tombstone, and there are NO other surviving versions
        // newer than it, then this tombstone effectively makes the key non-existent for future reads
        // beyond this compaction's GC horizon.
        Record latest_surviving = versions_to_keep.front(); // Highest commit_txn_id among survivors
        LOG_TRACE("  [Compaction GC for '{}'] Kept version with CommitTxnID: {}, Deleted: {}",
                  key, latest_surviving.commit_txn_id, latest_surviving.deleted);
        return {latest_surviving}; // Return as a vector of one
    }

    LOG_TRACE("  [Compaction GC for '{}'] All versions garbage collected.", key);
    return {};
}

engine::lsm::MemTableTuner* LSMTree::getTuner() const {
    return memtable_tuner_.get();
}

LSMTree::MemTableStatsSnapshot LSMTree::getActiveMemTableStats() const {
    MemTableStatsSnapshot stats;
    std::shared_ptr<engine::lsm::PartitionedMemTable> active_snapshot;
    {
        std::shared_lock<std::shared_mutex> data_lock(memtable_data_mutex_);
        active_snapshot = active_memtable_;
    }
    
    if (active_snapshot) {
        stats.total_size_bytes = active_snapshot->ApproximateMemoryUsage();
        stats.record_count = active_snapshot->Count();
        
        // This requires PartitionedMemTable to expose its partitions or their stats
        // Let's assume PartitionedMemTable has a method `getPartitionStatsSnapshots()`
        // stats.partition_stats = active_snapshot->getPartitionStatsSnapshots();
        // If not, we'll implement a simplified version.
        // For now, let's return just the global stats.
    }
    return stats;
}

uint64_t LSMTree::getFlushCount() const {
    return flush_count_.load(std::memory_order_relaxed);
}

/**
 * @brief Loads metadata from all existing SSTableMetadata files in the data directory.
 * 
 * This is the main entry point for discovering the on-disk state of the LSM-Tree
 * on startup. It orchestrates file discovery, metadata parsing, and the final
 * organization of the loaded data into the VersionSet.
 */
void LSMTree::loadSSTablesMetadata() {
    LOG_INFO("[LSM LoadMeta] Starting SSTables metadata load from: {}", data_dir_);
    
    uint64_t max_sstable_id_found = 0;
    std::map<int, std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>> temp_levels_from_disk;

    try {
        // Phase 1: Scan the directory to find all valid SSTableMetadata files.
        std::vector<SSTableFileInfo> sstable_files = discoverSSTableFiles(max_sstable_id_found);

        if (sstable_files.empty()) {
            LOG_INFO("[LSM LoadMeta] No SSTableMetadata files found. Initializing with an empty VersionSet.");
        } else {
            // Phase 2: Iterate through discovered files and load the metadata from each.
            for (const auto& file_info : sstable_files) {
                std::shared_ptr<engine::lsm::SSTableMetadata> metadata = loadSingleSSTableMetadata(file_info);
                if (metadata) {
                    temp_levels_from_disk[metadata->level].push_back(metadata);
                }
            }
            
            // Phase 3: Sort the levels correctly.
            organizeLoadedSSTables(temp_levels_from_disk);
        }

        // Phase 4: Atomically initialize the VersionSet with the loaded and organized metadata.
        std::vector<std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>> initial_levels(MAX_LEVELS);
        for (auto const& [level_idx, sst_list] : temp_levels_from_disk) {
            if (level_idx >= 0 && level_idx < MAX_LEVELS) {
                initial_levels[level_idx] = sst_list;
            }
        }
        versions_->Initialize(std::move(initial_levels));
        
        // Update the atomic counter for the next SSTableMetadata ID.
        next_sstable_file_id_.store(max_sstable_id_found + 1);

        LOG_INFO("[LSM LoadMeta] Finished loading metadata. Next SSTableMetadata ID: {}. VersionSet initialized.",
                 next_sstable_file_id_.load());

    } catch (const std::exception& e) {
        LOG_FATAL("[LSM LoadMeta] A critical error occurred during metadata loading: {}. LSM-Tree may be in an inconsistent state.", e.what());
        // In a real system, this might trigger a more graceful shutdown or recovery mode.
        throw;
    }
}


void LSMTree::applyAndPersistVersionEdit(const engine::lsm::VersionEdit& edit) {
    if (!versions_) {
        throw storage::StorageError(storage::ErrorCode::STORAGE_NOT_INITIALIZED, 
            "Cannot apply version edit because VersionSet is not initialized.");
    }
    // This method now safely encapsulates the access to the private versions_ member.
    versions_->ApplyChanges(edit);
}

void LSMTree::notifyScheduler() {
    // Safely notifies the private condition variable.
    std::lock_guard<std::mutex> lock(scheduler_mutex_);
    scheduler_cv_.notify_one();
}

// --- Phase 1 Helper: File Discovery ---

/**
 * @brief Scans the data directory for files matching the SSTableMetadata pattern,
 *        deletes temporary files, and parses level/ID from valid filenames.
 * @param max_sstable_id_found (out) Updated with the highest sstable_id found.
 * @return A vector of SSTableFileInfo structs for all valid files found.
 */
std::vector<LSMTree::SSTableFileInfo> LSMTree::discoverSSTableFiles(uint64_t& max_sstable_id_found) const {
    std::vector<SSTableFileInfo> found_files;
    max_sstable_id_found = 0;

    if (!fs::exists(data_dir_) || !fs::is_directory(data_dir_)) {
        LOG_WARN("[LSM discoverSSTables] Data directory '{}' does not exist.", data_dir_);
        return found_files;
    }

    for (const auto& entry : fs::directory_iterator(data_dir_)) {
        if (!entry.is_regular_file()) continue;

        const std::string filename = entry.path().filename().string();

        if (filename.find(".tmp") != std::string::npos) {
            LOG_INFO("[LSM discoverSSTables] Found temporary file: {}. Deleting.", entry.path().string());
            fs::remove(entry.path());
            continue;
        }

        int level = -1;
        uint64_t sstable_id = 0;
        if (sscanf(filename.c_str(), "sstable_L%d_%llu.dat", &level, &sstable_id) == 2) {
            if (level >= 0 && level < MAX_LEVELS) {
                found_files.push_back({entry.path().string(), level, sstable_id});
                max_sstable_id_found = std::max(max_sstable_id_found, sstable_id);
            } else {
                LOG_WARN("[LSM discoverSSTables] Skipping file with invalid level {}: {}", level, filename);
            }
        }
    }
    return found_files;
}


// --- Phase 2 Helper: Single File Metadata Loading ---

/**
 * @brief Reads the footer of a single SSTableMetadata file to deserialize its metadata.
 * @param file_info Information about the file to load.
 * @return A shared_ptr to the loaded SSTableMetadata metadata, or nullptr on failure.
 */
std::shared_ptr<engine::lsm::SSTableMetadata> LSMTree::loadSingleSSTableMetadata(
    const SSTableFileInfo& file_info) const
{
    LOG_INFO("[LSM LoadMeta] Loading metadata for SSTable file: {}", file_info.full_path);
    auto metadata = std::make_shared<engine::lsm::SSTableMetadata>(file_info.sstable_id, file_info.level);
    metadata->filename = file_info.full_path;

    std::ifstream file_stream(file_info.full_path, std::ios::binary);
    if (!file_stream) {
        LOG_ERROR("  [LSM LoadMeta] Could not open file: {}", file_info.full_path);
        return nullptr;
    }

    try {
        metadata->size_bytes = fs::file_size(file_info.full_path);
        LOG_TRACE("  [LSM LoadMeta] File size: {} bytes.", metadata->size_bytes);
    } catch (const fs::filesystem_error& e) {
        LOG_ERROR("  [LSM LoadMeta] Could not get file size for {}: {}", file_info.full_path, e.what());
        return nullptr;
    }

    if (metadata->size_bytes < engine::lsm::SSTABLE_POSTSCRIPT_SIZE) {
        LOG_WARN("  [LSM LoadMeta] File {} is too small ({} bytes) to be a valid SSTable. Postscript requires {} bytes. Skipping.",
                 file_info.full_path, metadata->size_bytes, engine::lsm::SSTABLE_POSTSCRIPT_SIZE);
        return nullptr;
    }

    // --- Phase 1: Read the fixed-size Postscript from the end of the file ---
    engine::lsm::SSTablePostscript postscript;
    try {
        std::streamoff seek_pos = -static_cast<std::streamoff>(engine::lsm::SSTABLE_POSTSCRIPT_SIZE);
        file_stream.seekg(seek_pos, std::ios::end);
        if (!file_stream) throw std::runtime_error("Failed to seek for postscript.");

        file_stream.read(reinterpret_cast<char*>(&postscript), engine::lsm::SSTABLE_POSTSCRIPT_SIZE);
        if (!file_stream) throw std::runtime_error("Failed to read postscript data.");

        LOG_TRACE("  [LSM LoadMeta] Postscript read: Magic={:#x} (Expected: {:#x}), MetaOffset={}",
                  postscript.magic_number, engine::lsm::SSTABLE_MAGIC_NUMBER, postscript.metadata_block_offset);

        if (postscript.magic_number != engine::lsm::SSTABLE_MAGIC_NUMBER) {
            throw std::runtime_error("Invalid SSTable magic number.");
        }
        if (postscript.metadata_block_offset >= metadata->size_bytes) {
            throw std::runtime_error("Metadata block offset is out of file bounds.");
        }
    } catch (const std::exception& e) {
        LOG_ERROR("  [LSM LoadMeta] Error reading or validating postscript for {}: {}", file_info.full_path, e.what());
        return nullptr;
    }
    
    try {
        // --- Phase 2: Seek to the metadata block and read its sections ---
        file_stream.seekg(postscript.metadata_block_offset);
        if (!file_stream) throw std::runtime_error("Failed to seek to metadata block.");

        // Read Filter Partition Index
        uint32_t num_filter_partitions;
        file_stream.read(reinterpret_cast<char*>(&num_filter_partitions), sizeof(num_filter_partitions));
        for (uint32_t i = 0; i < num_filter_partitions; ++i) {
            std::string first_key = DeserializeString(file_stream);
            engine::lsm::FilterPartitionIndexEntry entry;
            
            // --- FIX: Deserialize struct members individually ---
            // The previous code was: file_stream.read(reinterpret_cast<char*>(&entry), sizeof(engine::lsm::FilterPartitionIndexEntry));

            std::streamoff offset_from_disk;
            file_stream.read(reinterpret_cast<char*>(&offset_from_disk), sizeof(offset_from_disk));
            file_stream.read(reinterpret_cast<char*>(&entry.partition_size_bytes), sizeof(entry.partition_size_bytes));

            if (!file_stream) {
                throw std::runtime_error("Failed to read FilterPartitionIndexEntry members for key '" + first_key + "'.");
            }
            // Convert the portable std::streamoff back to std::streampos.
            entry.partition_offset = static_cast<std::streampos>(offset_from_disk);
            // --- END FIX ---

            metadata->filter_partition_index[first_key] = entry;

            LOG_TRACE("      - Read Filter Entry: Key='{}', Offset={}, Size={}",
                    format_key_for_print(first_key), static_cast<long long>(entry.partition_offset), entry.partition_size_bytes);
        }
        LOG_TRACE("    - Read Filter Partition Index with {} entries.", num_filter_partitions);

        // Read L2 Block Index
        uint32_t num_l2_entries;
        file_stream.read(reinterpret_cast<char*>(&num_l2_entries), sizeof(num_l2_entries));
        for (uint32_t i = 0; i < num_l2_entries; ++i) {
            std::string first_key = DeserializeString(file_stream);
            std::streampos l1_offset;
            file_stream.read(reinterpret_cast<char*>(&l1_offset), sizeof(l1_offset));
            metadata->block_index[first_key] = l1_offset;
        }
        LOG_TRACE("    - Read L2 Block Index with {} entries.", num_l2_entries);

        // Read TxnID Range & Entry Counts from their now-known offsets (this was part of the original bug)
        // For simplicity, the builder writes them sequentially, so we read them sequentially here too.
        file_stream.read(reinterpret_cast<char*>(&metadata->min_txn_id), sizeof(TxnId));
        file_stream.read(reinterpret_cast<char*>(&metadata->max_txn_id), sizeof(TxnId));
        file_stream.read(reinterpret_cast<char*>(&metadata->total_entries), sizeof(uint64_t));
        file_stream.read(reinterpret_cast<char*>(&metadata->tombstone_entries), sizeof(uint64_t));
        LOG_TRACE("    - Read Txn Range [{}, {}] and Counts (Total: {}, Tombstones: {})",
                  metadata->min_txn_id, metadata->max_txn_id, metadata->total_entries, metadata->tombstone_entries);

        // --- Phase 3: Determine Min/Max Keys ---
        if (!metadata->block_index.empty()) {
            metadata->min_key = metadata->block_index.begin()->first;

            auto last_l2_entry_it = metadata->block_index.rbegin();
            std::streampos last_l1_block_offset = last_l2_entry_it->second;
            file_stream.seekg(last_l1_block_offset);
            
            uint32_t num_entries_in_last_l1_block;
            file_stream.read(reinterpret_cast<char*>(&num_entries_in_last_l1_block), sizeof(num_entries_in_last_l1_block));
            
            SSTableBlockIndexEntry last_data_block_meta;
            for (uint32_t i = 0; i < num_entries_in_last_l1_block; ++i) {
                (void)DeserializeString(file_stream);
                file_stream.read(reinterpret_cast<char*>(&last_data_block_meta), sizeof(last_data_block_meta));
            }

            auto last_block_data = readAndProcessSSTableBlock(file_stream, last_data_block_meta, file_info.full_path);
            if (!last_block_data.empty()) {
                const uint8_t* ptr = last_block_data.data();
                const uint8_t* end_ptr = ptr + last_block_data.size();
                std::string last_key_in_file;
                
                while(ptr < end_ptr) {
                    auto rec_opt = deserializeRecordFromBuffer(ptr, end_ptr);
                    if(rec_opt) {
                        last_key_in_file = rec_opt->key;
                    } else {
                        break;
                    }
                }
                metadata->max_key = last_key_in_file;
            } else {
                metadata->max_key = last_l2_entry_it->first;
            }
            LOG_TRACE("    - Min/Max keys determined: Min='{}', Max='{}'", 
                      format_key_for_print(metadata->min_key), format_key_for_print(metadata->max_key));
        }

        
        LOG_INFO("[LSM LoadMeta] Successfully loaded all metadata for {}", file_info.full_path);
        return metadata;

    } catch (const std::exception& e) {
        LOG_ERROR("  [LSM LoadMeta] Exception parsing metadata sections for {}: {}", file_info.full_path, e.what());
        return nullptr;
    }
}

/**
 * @brief Sorts the SSTableMetadata metadata within each level according to the LSM-Tree rules.
 * L0 is sorted by sstable_id descending (newest first).
 * L1+ are sorted by min_key ascending.
 */
void LSMTree::organizeLoadedSSTables(std::map<int, std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>>>& temp_levels_from_disk) const {
    for (auto& pair : temp_levels_from_disk) {
        int level = pair.first;
        auto& sst_list = pair.second;
        if (level == 0) {
            std::sort(sst_list.begin(), sst_list.end(),
                      [](const auto& a, const auto& b) { return a->sstable_id > b->sstable_id; });
        } else {
            std::sort(sst_list.begin(), sst_list.end(),
                      [](const auto& a, const auto& b) { return a->min_key < b->min_key; });
        }
    }
}

LSMTree::VersionStatsSnapshot LSMTree::getVersionStats() const {
    VersionStatsSnapshot stats;
    if (!versions_) {
        return stats;
    }

    stats.current_version_id = versions_->GetCurrentVersionIdForDebug();
    stats.live_versions_count = versions_->GetLiveVersionsCountForDebug();

    // Get the sstables per level from the current version snapshot
    engine::lsm::Version* current_v = versions_->GetCurrent();
    engine::lsm::VersionUnreffer unreffer(current_v);
    
    const auto& levels = current_v->GetLevels();
    stats.sstables_per_level.reserve(levels.size());
    for (const auto& level_vec : levels) {
        stats.sstables_per_level.push_back(level_vec.size());
    }
    
    return stats;
}
