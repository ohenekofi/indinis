// include/lsm/sstable_reader.h
#pragma once

#include "sstable_meta.h"
#include "../types.h"
#include <fstream>
#include <optional>
#include <vector>
#include <string>
#include <memory>
#include <cstdint>
#include <map>

class LSMTree;

class SSTableReader {
private:
    const LSMTree* lsm_tree_ptr_;
    std::ifstream file_stream_;
    std::string filename_;
    uint64_t sstable_id_;
    std::optional<Record> current_record_;
    bool eof_reached_ = false;

    // --- STATE FOR TWO-LEVEL INDEX TRAVERSAL ---
    std::shared_ptr<engine::lsm::SSTableMetadata> meta_snapshot_;
    
    // Iterator for the in-memory L2 index
    std::map<std::string, std::streampos>::const_iterator l2_block_iterator_;

    // The currently loaded L1 index block (temporary)
    std::map<std::string, SSTableBlockIndexEntry> current_l1_block_;
    
    // Iterator for the currently loaded L1 index block
    std::map<std::string, SSTableBlockIndexEntry>::const_iterator l1_entry_iterator_;

    // Buffer for the currently loaded DATA block
    std::vector<uint8_t> current_data_block_buffer_;
    const uint8_t* current_data_block_ptr_ = nullptr;
    const uint8_t* current_data_block_end_ptr_ = nullptr;
    // ---------------------------------------------

    bool loadNextL1IndexBlock();
    bool loadNextDataBlock();
    bool readNextRecordFromDataBlock();

public:
    SSTableReader(
        const std::shared_ptr<engine::lsm::SSTableMetadata>& sstable_meta,
        const LSMTree* lsm_tree_ptr
    );

    ~SSTableReader();
    // ... Rule of 5 ...
    SSTableReader(const SSTableReader&) = delete;
    SSTableReader& operator=(const SSTableReader&) = delete;
    SSTableReader(SSTableReader&&) noexcept = default;
    SSTableReader& operator=(SSTableReader&&) noexcept = default;
    
    const std::optional<Record>& peek() const { return current_record_; }
    bool advance();
    uint64_t getSSTableId() const { return sstable_id_; }
    const std::string& getFilename() const { return filename_; }
};