// include/columnar/columnar_file.h
#pragma once

#include "column_types.h"
#include "columnar_buffer.h"
#include "../../include/bloom_filter.h"
#include "../../include/types.h" // For CompressionType, EncryptionScheme

#include <string>
#include <fstream>
#include <vector>
#include <memory>
#include <limits>
// --- REMOVED --- No longer need mutex in header
// #include <mutex>

namespace engine {
namespace columnar {
    
// Forward declare for friendship
class ColumnarStore;
class ColumnarCompactionJobCoordinator; // --- NEW ---

// Metadata for a single compressed column chunk on disk.
struct ColumnChunkMetadata {
    uint32_t column_id;
    uint64_t offset_in_file;
    uint32_t size_on_disk;
    uint64_t row_count;
    ValueType min_value;
    ValueType max_value;
    CompressionType compression_type = CompressionType::NONE;
    uint32_t uncompressed_size = 0;
    EncryptionScheme encryption_scheme = EncryptionScheme::NONE;
};

// A collection of column chunks making up one "stripe" of data.
struct RowGroupMetadata {
    uint64_t row_count;
    std::vector<ColumnChunkMetadata> column_chunks;
};

// Represents a single, immutable, on-disk columnar file.
class ColumnarFile {
    friend class ColumnarStore;
    friend class ColumnarCompactionJobCoordinator; // --- NEW ---
public:
    // Writes a new columnar file from an in-memory buffer.
    struct private_key { explicit private_key() = default; };
    ColumnarFile(private_key, std::string path, uint64_t file_id);
    static std::unique_ptr<ColumnarFile> create(
        const std::string& path,
        uint64_t file_id,
        uint32_t schema_version,
        const std::vector<std::shared_ptr<ColumnarBuffer>>& buffers,
        CompressionType compression,
        int compression_level,
        bool encryption_active,
        EncryptionScheme encryption_scheme,
        const std::vector<unsigned char>& dek
    );

    const std::vector<std::string>& getPrimaryKeys() const;
    std::string getPrimaryKeyAt(size_t row_index) const;

    // Loads an existing file's metadata for reading.
    static std::unique_ptr<ColumnarFile> open(const std::string& path, uint64_t file_id);

    // --- MODIFIED --- This method is now fully thread-safe.
    std::unordered_map<uint32_t, ColumnVector> readColumns(
        const std::vector<uint32_t>& column_ids,
        const std::vector<int>& row_group_indices,
        bool encryption_active,
        const std::vector<unsigned char>& dek
    ) const;

    const std::string& getPath() const { return file_path_; }
    uint64_t getFileId() const { return file_id_; }
    uint32_t getSchemaVersion() const { return schema_version_; }
    const std::vector<RowGroupMetadata>& getRowGroups() const { return row_groups_; }
    const BloomFilter* getPrimaryKeyFilter() const { return pk_bloom_filter_.get(); }
    uint64_t getTotalRowCount() const;

private:
    // --- MODIFIED --- This constructor is now simpler
    ColumnarFile(std::string path, uint64_t file_id);
    
    // --- MODIFIED --- This method now uses a local stream and closes it
    void readFooterAndMetadata();

    std::string file_path_;
    uint64_t file_id_;
    uint32_t schema_version_;
    std::vector<RowGroupMetadata> row_groups_;
    std::unique_ptr<BloomFilter> pk_bloom_filter_;
    std::vector<std::string> primary_keys_;
    
    // --- REMOVED --- These members made reads serialized.
    // mutable std::ifstream reader_stream_;
    // mutable std::mutex reader_mutex_;
};

} // namespace columnar
} // namespace engine