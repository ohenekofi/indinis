// src/lsm/sstable_reader.cpp
#include "../../include/lsm/sstable_reader.h"
#include "../../include/lsm_tree.h"
#include "../../include/debug_utils.h"

SSTableReader::SSTableReader(
    const std::shared_ptr<engine::lsm::SSTableMetadata>& sstable_meta,
    const LSMTree* lsm_tree_ptr)
    : lsm_tree_ptr_(lsm_tree_ptr),
      filename_(sstable_meta->filename),
      sstable_id_(sstable_meta->sstable_id),
      eof_reached_(false),
      meta_snapshot_(sstable_meta)
{
    if (!lsm_tree_ptr_) {
        throw std::invalid_argument("SSTableReader requires a valid LSMTree pointer.");
    }

    file_stream_.open(filename_, std::ios::binary);
    if (!file_stream_) {
        eof_reached_ = true;
        throw std::runtime_error("SSTableReader: Failed to open file " + filename_);
    }
    
    if (meta_snapshot_->block_index.empty()) {
        eof_reached_ = true;
    } else {
        // Start by pointing to the first L1 index block.
        l2_block_iterator_ = meta_snapshot_->block_index.begin();
        // Prime the reader by loading the first L1 block and then the first data block.
        advance();
    }
}

SSTableReader::~SSTableReader() {
    if (file_stream_.is_open()) {
        file_stream_.close();
    }
}

bool SSTableReader::advance() {
    if (eof_reached_) {
        return false;
    }
    // Try to read the next record from the current data block.
    if (readNextRecordFromDataBlock()) {
        return true;
    }
    // If that fails (end of data block), try to load the next data block from the current L1 index.
    if (loadNextDataBlock()) {
        return readNextRecordFromDataBlock();
    }
    // If that fails (end of L1 index), try to load the next L1 index block.
    if (loadNextL1IndexBlock()) {
        // After loading a new L1 block, we must load its first data block.
        if (loadNextDataBlock()) {
            return readNextRecordFromDataBlock();
        }
    }
    
    // If all attempts fail, we have reached the end of the file.
    eof_reached_ = true;
    current_record_ = std::nullopt;
    return false;
}

bool SSTableReader::loadNextL1IndexBlock() {
    if (eof_reached_ || !file_stream_.is_open() || l2_block_iterator_ == meta_snapshot_->block_index.end()) {
        return false;
    }

    try {
        std::streampos l1_block_offset = l2_block_iterator_->second;
        file_stream_.seekg(l1_block_offset);

        uint32_t num_l1_entries;
        file_stream_.read(reinterpret_cast<char*>(&num_l1_entries), sizeof(num_l1_entries));
        
        current_l1_block_.clear();
        for (uint32_t i = 0; i < num_l1_entries; ++i) {
            auto key_opt = LSMTree::deserializeKey(file_stream_);
            if (!key_opt) throw std::runtime_error("Corrupt L1 index block: failed to read key.");
            SSTableBlockIndexEntry entry;
            file_stream_.read(reinterpret_cast<char*>(&entry), sizeof(entry));
            current_l1_block_[*key_opt] = entry;
        }
        l1_entry_iterator_ = current_l1_block_.begin();
    } catch (const std::exception& e) {
        LOG_ERROR("[SSTReader {}] Failed to load L1 index block: {}", filename_, e.what());
        return false;
    }

    l2_block_iterator_++;
    return true;
}

bool SSTableReader::loadNextDataBlock() {
    if (l1_entry_iterator_ == current_l1_block_.end()) {
        return false; // No more data blocks in this L1 index.
    }

    try {
        const SSTableBlockIndexEntry& data_block_meta = l1_entry_iterator_->second;
        current_data_block_buffer_ = lsm_tree_ptr_->readAndProcessSSTableBlock(
            file_stream_, data_block_meta, filename_
        );
        
        current_data_block_ptr_ = current_data_block_buffer_.data();
        current_data_block_end_ptr_ = current_data_block_ptr_ + current_data_block_buffer_.size();
    } catch (const std::exception& e) {
        LOG_ERROR("[SSTReader {}] Failed to load data block: {}", filename_, e.what());
        return false;
    }
    
    l1_entry_iterator_++;
    return true;
}

bool SSTableReader::readNextRecordFromDataBlock() {
    if (!current_data_block_ptr_ || current_data_block_ptr_ >= current_data_block_end_ptr_) {
        return false; // Current data block is exhausted or not loaded.
    }
    
    current_record_ = LSMTree::deserializeRecordFromBuffer(current_data_block_ptr_, current_data_block_end_ptr_);
    
    return current_record_.has_value();
}