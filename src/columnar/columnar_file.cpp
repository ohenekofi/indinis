// src/columnar/columnar_file.cpp

#include "../../include/columnar/columnar_file.h"
#include "../../include/debug_utils.h"
#include "../../include/compression_utils.h"
#include "../../include/encryption_library.h"
#include "../../include/storage_error/storage_error.h"

#include <nlohmann/json.hpp>
#include <stdexcept>
#include <algorithm> // For std::min_element, std::max_element

using json = nlohmann::json;

namespace engine {
namespace columnar {

// --- File Format Constants ---
constexpr uint32_t FILE_MAGIC_NUMBER = 0xC01C01A4; // "COLA"
constexpr uint16_t FILE_FORMAT_VERSION = 1;
constexpr uint32_t FOOTER_MAGIC_NUMBER = 0xF00DF00D;

// On-disk footer struct (fixed size)
#pragma pack(push, 1)
struct ColumnarFileFooter {
    uint64_t metadata_offset = 0;
    uint64_t metadata_size = 0;
    uint64_t bloom_filter_offset = 0;
    uint64_t bloom_filter_size = 0;
    uint32_t footer_magic = FOOTER_MAGIC_NUMBER;
};
#pragma pack(pop)

// --- JSON Serialization for Metadata Structs ---
// These live here as an implementation detail of the file format.

void to_json(json& j, const ColumnChunkMetadata& p) {
    // This needs to handle the ValueType variant.
    // For now, we will serialize it to a string for simplicity. A binary format like BSON/CBOR would be better.
    auto value_to_json = [](const ValueType& v) -> json {
        return std::visit([](auto&& arg) -> json {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, std::monostate>) return nullptr;
            else return arg;
        }, v);
    };

    j = json{
        {"column_id", p.column_id},
        {"offset_in_file", p.offset_in_file},
        {"size_on_disk", p.size_on_disk},
        {"row_count", p.row_count},
        {"min_value", value_to_json(p.min_value)},
        {"max_value", value_to_json(p.max_value)},
        {"compression_type", p.compression_type},
        {"uncompressed_size", p.uncompressed_size},
        {"encryption_scheme", p.encryption_scheme}
    };
}
void from_json(const json& j, ColumnChunkMetadata& p) {
    // Placeholder: a real implementation needs to convert json back to ValueType variant
    j.at("column_id").get_to(p.column_id);
    j.at("offset_in_file").get_to(p.offset_in_file);
    j.at("size_on_disk").get_to(p.size_on_disk);
    j.at("row_count").get_to(p.row_count);
    if (j.contains("compression_type")) {
        j.at("compression_type").get_to(p.compression_type);
    }
    if (j.contains("uncompressed_size")) {
        j.at("uncompressed_size").get_to(p.uncompressed_size);
    }
    if (j.contains("encryption_scheme")) {
        j.at("encryption_scheme").get_to(p.encryption_scheme);
    }
}

void to_json(json& j, const RowGroupMetadata& p) {
    j = json{{"row_count", p.row_count}, {"column_chunks", p.column_chunks}};
}
void from_json(const json& j, RowGroupMetadata& p) {
    j.at("row_count").get_to(p.row_count);
    j.at("column_chunks").get_to(p.column_chunks);
}


// --- Private Helper Functions for Serialization ---
namespace detail {
    // --- NEW DESERIALIZATION HELPER ---
        /**
     * @brief Serializes a vector of ValueTypes into a raw byte buffer.
     * 
     * This function iterates through a column vector, writing a type tag (the variant index)
     * followed by the data for each value. This creates a compact, self-describing
     * binary representation of the column's data.
     * 
     * @param vec The ColumnVector to serialize.
     * @return A vector of bytes representing the serialized data.
     */

    std::vector<uint8_t> serializeColumnVector(const ColumnVector& vec) {
        std::ostringstream oss(std::ios::binary);
        
        for (const auto& val : vec) {
            uint8_t index = static_cast<uint8_t>(val.index());
            oss.write(reinterpret_cast<const char*>(&index), sizeof(index));

            std::visit([&oss](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, std::monostate>) {
                    // No data needed.
                } else if constexpr (std::is_same_v<T, std::string>) {
                    uint32_t len = static_cast<uint32_t>(arg.length());
                    oss.write(reinterpret_cast<const char*>(&len), sizeof(len));
                    if (len > 0) oss.write(arg.data(), len);
                } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                    uint32_t len = static_cast<uint32_t>(arg.size());
                    oss.write(reinterpret_cast<const char*>(&len), sizeof(len));
                    if (len > 0) oss.write(reinterpret_cast<const char*>(arg.data()), len);
                } else { // For int64_t, double, bool
                    oss.write(reinterpret_cast<const char*>(&arg), sizeof(T));
                }
            }, val);
        }
        if (!oss) throw std::runtime_error("Failed to serialize column vector due to stream error.");
        std::string str_data = oss.str();
        return std::vector<uint8_t>(str_data.begin(), str_data.end());
    }


    // Deserializes a raw byte buffer back into a vector of ValueTypes.
    ColumnVector deserializeColumnVector(const std::vector<uint8_t>& buffer) {
        ColumnVector vec;
        if (buffer.empty()) return vec;

        const char* ptr = reinterpret_cast<const char*>(buffer.data());
        const char* const end_ptr = ptr + buffer.size();

        while (ptr < end_ptr) {
            // Check if there's at least one byte left for the type index
            if (ptr >= end_ptr) break;
            
            uint8_t index = static_cast<uint8_t>(*ptr++);

            // Check if there are enough bytes for the fixed-size data AFTER reading the index
            switch (index) {
                case 0: vec.emplace_back(std::monostate{}); break;
                case 1: { // int64_t
                    if (static_cast<size_t>(end_ptr - ptr) < sizeof(int64_t)) throw std::runtime_error("Buffer too short for int64_t.");
                    int64_t val; std::memcpy(&val, ptr, sizeof(val)); ptr += sizeof(val);
                    vec.emplace_back(val); break;
                }
                case 2: { // double
                    if (static_cast<size_t>(end_ptr - ptr) < sizeof(double)) throw std::runtime_error("Buffer too short for double.");
                    double val; std::memcpy(&val, ptr, sizeof(val)); ptr += sizeof(val);
                    vec.emplace_back(val); break;
                }
                case 3: { // std::string
                    if (static_cast<size_t>(end_ptr - ptr) < sizeof(uint32_t)) throw std::runtime_error("Buffer too short for string length.");
                    uint32_t len; std::memcpy(&len, ptr, sizeof(len)); ptr += sizeof(len);
                    if (static_cast<size_t>(end_ptr - ptr) < len) throw std::runtime_error("Buffer too short for string data.");
                    vec.emplace_back(std::string(ptr, len)); ptr += len; break;
                }
                case 4: { // std::vector<uint8_t>
                    if (static_cast<size_t>(end_ptr - ptr) < sizeof(uint32_t)) throw std::runtime_error("Buffer too short for vector length.");
                    uint32_t len; std::memcpy(&len, ptr, sizeof(len)); ptr += sizeof(len);
                    if (static_cast<size_t>(end_ptr - ptr) < len) throw std::runtime_error("Buffer too short for vector data.");
                    // FIX: Cast the char* to uint8_t* for the vector constructor
                    vec.emplace_back(std::vector<uint8_t>(reinterpret_cast<const uint8_t*>(ptr), reinterpret_cast<const uint8_t*>(ptr) + len));
                    ptr += len; break;
                }
                case 5: { // bool
                    if (static_cast<size_t>(end_ptr - ptr) < sizeof(bool)) throw std::runtime_error("Buffer too short for bool.");
                    bool val; std::memcpy(&val, ptr, sizeof(val)); ptr += sizeof(val);
                    vec.emplace_back(val); break;
                }
                default:
                    throw std::runtime_error("Unknown type index during column deserialization: " + std::to_string(index));
            }
        }
        return vec;
    }
}



// --- ColumnarFile Static Method Implementations ---
std::unique_ptr<ColumnarFile> ColumnarFile::create(
    const std::string& path, uint64_t file_id, uint32_t schema_version, 
    const std::vector<std::shared_ptr<ColumnarBuffer>>& buffers, // Changed parameter
    CompressionType compression, int compression_level, bool encryption_active,
    EncryptionScheme encryption_scheme, const std::vector<unsigned char>& dek) 
{
    if (buffers.empty()) {
        throw std::invalid_argument("Cannot create a columnar file from an empty list of buffers.");
    }
    LOG_INFO("Creating columnar file: {}. Row Groups: {}, Total Rows: {}", 
             path, buffers.size(), buffers[0]->getRowCount() * buffers.size()); // Approximate rows

    auto file = std::make_unique<ColumnarFile>(private_key{}, path, file_id);
    file->schema_version_ = schema_version;

    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("Failed to open file for writing: " + path);
    }

    // 1. Write File Header (as before)
    out.write(reinterpret_cast<const char*>(&FILE_MAGIC_NUMBER), sizeof(FILE_MAGIC_NUMBER));
    out.write(reinterpret_cast<const char*>(&FILE_FORMAT_VERSION), sizeof(FILE_FORMAT_VERSION));
    out.write(reinterpret_cast<const char*>(&schema_version), sizeof(schema_version));
    
    // --- 2. LOOP THROUGH BUFFERS TO WRITE MULTIPLE ROW GROUPS ---
    std::vector<std::string> all_primary_keys; // Aggregate all PKs for the file's bloom filter

    for (const auto& buffer : buffers) {
        RowGroupMetadata row_group_meta;
        row_group_meta.row_count = buffer->getRowCount();
        
        const auto& all_columns_data = buffer->getColumnsData();
        for (const auto& col_def : buffer->getSchema().columns) {
            if (all_columns_data.find(col_def.column_id) == all_columns_data.end()) {
                continue;
            }
            const auto& column_vector = all_columns_data.at(col_def.column_id);
            
            // a. Serialize the column vector to a byte buffer
            std::vector<uint8_t> processed_chunk = detail::serializeColumnVector(column_vector);
            
            ColumnChunkMetadata chunk_meta;
            chunk_meta.column_id = col_def.column_id;
            chunk_meta.row_count = column_vector.size();

            chunk_meta.uncompressed_size = processed_chunk.size();
            chunk_meta.compression_type = CompressionType::NONE; // Default

            // b. Calculate Min/Max Statistics on the unserialized data
            if (!column_vector.empty()) {
                std::vector<ValueType> non_null_values;
                std::copy_if(column_vector.begin(), column_vector.end(), std::back_inserter(non_null_values),
                    [](const ValueType& v){ return !std::holds_alternative<std::monostate>(v); });

                if (!non_null_values.empty()) {
                    chunk_meta.min_value = *std::min_element(non_null_values.begin(), non_null_values.end());
                    chunk_meta.max_value = *std::max_element(non_null_values.begin(), non_null_values.end());
                }
            }
            
            // c. Compress the serialized byte buffer
            if (compression != CompressionType::NONE && !processed_chunk.empty()) {
                processed_chunk = CompressionManager::compress(processed_chunk.data(), processed_chunk.size(), compression, compression_level);
                chunk_meta.compression_type = compression;
                // ------------------------------------------------
            }

            // d. Encrypt the (possibly compressed) byte buffer
            chunk_meta.encryption_scheme = EncryptionScheme::NONE; // Default
            if (encryption_active && encryption_scheme != EncryptionScheme::NONE && !processed_chunk.empty()) {
                try {
                    if (encryption_scheme != EncryptionScheme::AES256_GCM_PBKDF2) {
                        // For now, we only support GCM in the columnar store for its integrity features.
                        throw std::runtime_error("Unsupported encryption scheme for columnar store; only AES-GCM is implemented.");
                    }

                    auto iv = EncryptionLibrary::generateRandomBytes(EncryptionLibrary::AES_GCM_IV_SIZE);
                    std::string plaintext_str(processed_chunk.begin(), processed_chunk.end());

                    // --- Construct the AAD ---
                    std::vector<uint8_t> aad_data;
                    aad_data.reserve(sizeof(uint32_t) + sizeof(uint32_t)); // Reserve space for schema_version and column_id

                    // Add schema_version to AAD
                    const char* schema_ver_ptr = reinterpret_cast<const char*>(&schema_version);
                    aad_data.insert(aad_data.end(), schema_ver_ptr, schema_ver_ptr + sizeof(schema_version));

                    // Add column_id to AAD
                    const char* col_id_ptr = reinterpret_cast<const char*>(&col_def.column_id);
                    aad_data.insert(aad_data.end(), col_id_ptr, col_id_ptr + sizeof(col_def.column_id));
                    // ---  ---
                    
                    // Perform encryption, now passing the AAD
                    EncryptionLibrary::EncryptedData encrypted_result = EncryptionLibrary::encryptWithAES_GCM(
                        plaintext_str, dek, iv, aad_data // Pass the constructed AAD
                    );

                    // Prepend IV and Tag to the ciphertext for storage (as before)
                    processed_chunk.clear();
                    processed_chunk.insert(processed_chunk.end(), encrypted_result.iv.begin(), encrypted_result.iv.end());
                    processed_chunk.insert(processed_chunk.end(), encrypted_result.tag.begin(), encrypted_result.tag.end());
                    processed_chunk.insert(processed_chunk.end(), encrypted_result.data.begin(), encrypted_result.data.end());
                    
                    chunk_meta.encryption_scheme = encryption_scheme;
                }  catch (const std::exception& e) {
                    LOG_ERROR("Failed to encrypt column chunk for ID {}: {}", col_def.column_id, e.what());
                    // Fail the file creation entirely if encryption fails.
                    throw std::runtime_error("Columnar file creation failed due to encryption error.");
                }
            }

            // e. Write the final chunk to the file and record its metadata
            chunk_meta.offset_in_file = out.tellp();
            chunk_meta.size_on_disk = processed_chunk.size();
            out.write(reinterpret_cast<const char*>(processed_chunk.data()), processed_chunk.size());
            
            row_group_meta.column_chunks.push_back(chunk_meta);
        }
    
        file->row_groups_.push_back(row_group_meta); // Add this completed row group's metadata

        // Aggregate primary keys from this buffer
        const auto& pks_in_buffer = buffer->getPrimaryKeys();
        all_primary_keys.insert(all_primary_keys.end(), pks_in_buffer.begin(), pks_in_buffer.end());
    }
    // --- END OF NEW LOOP ---

    // 3. Write Primary Key Bloom Filter (using all aggregated PKs)
    ColumnarFileFooter footer;
    footer.bloom_filter_offset = out.tellp();
    file->pk_bloom_filter_ = std::make_unique<BloomFilter>(all_primary_keys.size(), 0.01);
    for (const auto& pk : all_primary_keys) {
        file->pk_bloom_filter_->add(pk);
    }
    // ... serialize bloom filter to stream ...
    if (!file->pk_bloom_filter_->serializeToStream(out)) {
        throw std::runtime_error("Failed to serialize bloom filter to stream for: " + path);
    }
    footer.bloom_filter_size = static_cast<uint64_t>(static_cast<std::streamoff>(out.tellp()) - footer.bloom_filter_offset);


    // 4. Write Metadata Section (now contains multiple row groups)
    footer.metadata_offset = out.tellp();
    json metadata_json;
    metadata_json["row_groups"] = file->row_groups_;
    metadata_json["primary_keys"] = all_primary_keys; // Store all PKs for potential full reconstruction
    std::string metadata_str = metadata_json.dump();
    out.write(metadata_str.data(), metadata_str.size());
    footer.metadata_size = metadata_str.size();

    // 5. Write Fixed-Size Footer
    out.write(reinterpret_cast<const char*>(&footer), sizeof(footer));

    out.close();
    if (out.fail()) {
        throw std::runtime_error("Stream error after closing file during create: " + path);
    }
    
    LOG_INFO("Successfully created columnar file: {}", path);
    
    // Populate the in-memory primary key cache for the new file object.
    file->primary_keys_ = all_primary_keys;

    return file;
}



const std::vector<std::string>& ColumnarFile::getPrimaryKeys() const {
    // This is a simple getter that returns a const reference to the member variable.
    // The `primary_keys_` member is populated when the file is opened by the `readFooter` method.
    return primary_keys_;
}

ColumnarFile::ColumnarFile(private_key, std::string path, uint64_t file_id)
    : ColumnarFile(std::move(path), file_id) // Delegate to the other private constructor
{
    // The private_key parameter is a tag used only to select this constructor,
    // so the body can be empty or delegate as shown.
}

std::unique_ptr<ColumnarFile> ColumnarFile::open(const std::string& path, uint64_t file_id) {
    auto file = std::unique_ptr<ColumnarFile>(new ColumnarFile(path, file_id));
    try {
        file->readFooterAndMetadata();
        return file;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to open columnar file {}: {}", path, e.what());
        return nullptr;
    }
}


// --- ColumnarFile Instance Method Implementations ---

ColumnarFile::ColumnarFile(std::string path, uint64_t file_id)
    : file_path_(std::move(path)), file_id_(file_id), schema_version_(0) {}




std::string ColumnarFile::getPrimaryKeyAt(size_t row_index) const {
    if (row_index >= primary_keys_.size()) {
        throw std::out_of_range("Row index out of bounds for primary key lookup.");
    }
    return primary_keys_[row_index];
}

uint64_t ColumnarFile::getTotalRowCount() const {
    uint64_t total = 0;
    for(const auto& group : row_groups_) {
        total += group.row_count;
    }
    return total;
}

void ColumnarFile::readFooterAndMetadata() {
    // This method is called during construction via the `open` factory, so it doesn't
    // need its own locking. It uses a local stream for all I/O.
    std::ifstream reader(file_path_, std::ios::binary);
    if (!reader) {
        throw storage::StorageError(storage::ErrorCode::FILE_NOT_FOUND, 
            "Could not open file for reading metadata: " + file_path_);
    }

    // 1. Locate and Read the Footer struct
    reader.seekg(0, std::ios::end);
    std::streampos file_end_pos = reader.tellg();
    if (file_end_pos < static_cast<std::streamoff>(sizeof(ColumnarFileFooter))) {
        throw storage::StorageError(storage::ErrorCode::STORAGE_CORRUPTION, 
            "File is too small to contain a valid footer: " + file_path_);
    }

    reader.seekg(-static_cast<std::streamoff>(sizeof(ColumnarFileFooter)), std::ios::end);
    ColumnarFileFooter footer;
    reader.read(reinterpret_cast<char*>(&footer), sizeof(footer));
    if (!reader) {
        throw storage::StorageError(storage::ErrorCode::IO_READ_ERROR, 
            "Could not read footer from: " + file_path_);
    }

    // 2. Validate Footer and Metadata Offsets
    if (footer.footer_magic != FOOTER_MAGIC_NUMBER) {
        throw storage::StorageError(storage::ErrorCode::STORAGE_CORRUPTION, 
            "Invalid footer magic number in file: " + file_path_);
    }
    if (footer.metadata_offset + footer.metadata_size > static_cast<uint64_t>(file_end_pos)) {
        throw storage::StorageError(storage::ErrorCode::STORAGE_CORRUPTION, 
            "Metadata section is out of file bounds in: " + file_path_);
    }

    // 3. Read and Parse the Main Metadata Block (JSON)
    std::string metadata_json_str(footer.metadata_size, '\0');
    reader.seekg(footer.metadata_offset);
    reader.read(&metadata_json_str[0], footer.metadata_size);
    if (!reader) {
        throw storage::StorageError(storage::ErrorCode::IO_READ_ERROR, 
            "Failed to read metadata block from: " + file_path_);
    }
    
    try {
        json metadata_json = json::parse(metadata_json_str);
        row_groups_ = metadata_json.at("row_groups").get<std::vector<RowGroupMetadata>>();
        primary_keys_ = metadata_json.at("primary_keys").get<std::vector<std::string>>();
    } catch (const json::exception& e) {
        throw storage::StorageError(storage::ErrorCode::STORAGE_CORRUPTION, 
            "Failed to parse metadata JSON in " + file_path_ + ": " + e.what());
    }

    // 4. Read and Deserialize the Bloom Filter
    if (footer.bloom_filter_offset + footer.bloom_filter_size > static_cast<uint64_t>(file_end_pos)) {
        throw storage::StorageError(storage::ErrorCode::STORAGE_CORRUPTION, 
            "Bloom filter section is out of file bounds in: " + file_path_);
    }
    pk_bloom_filter_ = std::make_unique<BloomFilter>();
    reader.seekg(footer.bloom_filter_offset);
    if (!pk_bloom_filter_->deserializeFromStream(reader)) {
        throw storage::StorageError(storage::ErrorCode::STORAGE_CORRUPTION, 
            "Failed to deserialize bloom filter from: " + file_path_);
    }

    // 5. Read and Validate the File Header
    reader.seekg(0);
    uint32_t magic;
    uint16_t version;
    reader.read(reinterpret_cast<char*>(&magic), sizeof(magic));
    reader.read(reinterpret_cast<char*>(&version), sizeof(version));
    reader.read(reinterpret_cast<char*>(&schema_version_), sizeof(schema_version_));

    if (!reader || magic != FILE_MAGIC_NUMBER || version != FILE_FORMAT_VERSION) {
        throw storage::StorageError(storage::ErrorCode::STORAGE_CORRUPTION, 
            "Invalid file header magic number or version in: " + file_path_);
    }

    LOG_TRACE("Successfully read all metadata for columnar file: {}", file_path_);
    // The local 'reader' stream is closed automatically when it goes out of scope.
}

std::unordered_map<uint32_t, ColumnVector> ColumnarFile::readColumns(
    const std::vector<uint32_t>& column_ids,
    const std::vector<int>& row_group_indices,
    bool encryption_active,
    const std::vector<unsigned char>& dek
) const {
    // Create a local ifstream for this read operation, making the method thread-safe.
    std::ifstream reader(file_path_, std::ios::binary);
    if (!reader) {
        throw storage::StorageError(storage::ErrorCode::FILE_NOT_FOUND, 
            "File stream could not be opened for reading column data: " + file_path_);
    }

    std::unordered_map<uint32_t, ColumnVector> result_data;
    if (row_groups_.empty() || column_ids.empty() || row_group_indices.empty()) {
        return result_data;
    }

    // Pre-initialize result vectors to ensure caller gets a vector for every requested column.
    for (uint32_t col_id : column_ids) {
        result_data[col_id] = ColumnVector();
    }

    // Loop over only the specific row groups requested by the query planner.
    for (int rg_idx : row_group_indices) {
        if (rg_idx < 0 || static_cast<size_t>(rg_idx) >= row_groups_.size()) {
            LOG_WARN("Requested row group index {} is out of bounds for file {}. Skipping.", rg_idx, file_path_);
            continue;
        }
        
        const auto& row_group = row_groups_[rg_idx];
        for (const auto& chunk_meta : row_group.column_chunks) {
            // Check if this specific column chunk is one we need to read.
            if (std::find(column_ids.begin(), column_ids.end(), chunk_meta.column_id) == column_ids.end()) {
                continue;
            }
            
            std::vector<uint8_t> processed_chunk;
            // 1. Read the raw chunk data from disk.
            if (chunk_meta.size_on_disk > 0) {
                processed_chunk.resize(chunk_meta.size_on_disk);
                reader.seekg(chunk_meta.offset_in_file);
                if (!reader) {
                     throw storage::StorageError(storage::ErrorCode::IO_SEEK_ERROR, 
                        "Failed to seek to column chunk for ID " + std::to_string(chunk_meta.column_id));
                }
                reader.read(reinterpret_cast<char*>(processed_chunk.data()), chunk_meta.size_on_disk);
                if (!reader) {
                    throw storage::StorageError(storage::ErrorCode::IO_READ_ERROR, 
                        "Failed to read column chunk for ID " + std::to_string(chunk_meta.column_id));
                }
            }

            // 2. Decrypt if necessary, using the scheme stored in the chunk's metadata.
            if (chunk_meta.encryption_scheme != EncryptionScheme::NONE) {
                if (processed_chunk.empty()) {
                    // Nothing to decrypt.
                } else if (chunk_meta.encryption_scheme == EncryptionScheme::AES256_GCM_PBKDF2) {
                    if (processed_chunk.size() < (EncryptionLibrary::AES_GCM_IV_SIZE + EncryptionLibrary::AES_GCM_TAG_SIZE)) {
                        throw storage::StorageError(storage::ErrorCode::STORAGE_CORRUPTION, "Encrypted chunk is too small to contain IV and Tag.");
                    }
                    
                    EncryptionLibrary::EncryptedData data_to_decrypt;
                    const uint8_t* ptr = processed_chunk.data();
                    
                    data_to_decrypt.iv.assign(ptr, ptr + EncryptionLibrary::AES_GCM_IV_SIZE);
                    ptr += EncryptionLibrary::AES_GCM_IV_SIZE;
                    data_to_decrypt.tag.assign(ptr, ptr + EncryptionLibrary::AES_GCM_TAG_SIZE);
                    ptr += EncryptionLibrary::AES_GCM_TAG_SIZE;
                    data_to_decrypt.data.assign(processed_chunk.begin() + (ptr - processed_chunk.data()), processed_chunk.end());

                    // Reconstruct AAD for verification.
                    std::vector<uint8_t> aad_data;
                    aad_data.reserve(sizeof(uint32_t) + sizeof(uint32_t));
                    const auto* schema_ver_ptr = reinterpret_cast<const uint8_t*>(&this->schema_version_);
                    aad_data.insert(aad_data.end(), schema_ver_ptr, schema_ver_ptr + sizeof(this->schema_version_));
                    const auto* col_id_ptr = reinterpret_cast<const uint8_t*>(&chunk_meta.column_id);
                    aad_data.insert(aad_data.end(), col_id_ptr, col_id_ptr + sizeof(chunk_meta.column_id));

                    // Perform decryption.
                    processed_chunk = EncryptionLibrary::decryptWithAES_GCM_Binary(data_to_decrypt, dek, aad_data);
                } else {
                    throw storage::StorageError(storage::ErrorCode::NOT_IMPLEMENTED, "Unsupported encryption scheme encountered during column read.");
                }
            }

            // 3. Decompress if necessary.
            if (chunk_meta.compression_type != CompressionType::NONE) {
                if (chunk_meta.uncompressed_size > 0 && !processed_chunk.empty()) {
                    processed_chunk = CompressionManager::decompress(
                        processed_chunk.data(),
                        processed_chunk.size(),
                        chunk_meta.uncompressed_size,
                        chunk_meta.compression_type
                    );
                } else if (chunk_meta.uncompressed_size == 0 && processed_chunk.empty()) {
                    // Valid case of an empty compressed chunk.
                } else {
                    throw storage::StorageError(storage::ErrorCode::COMPRESSION_ERROR, "Compressed chunk has inconsistent size metadata.");
                }
            }
            
            // 4. Deserialize the final byte buffer and append to the result map.
            ColumnVector chunk_vector = detail::deserializeColumnVector(processed_chunk);
            auto& target_vector = result_data.at(chunk_meta.column_id);
            target_vector.insert(target_vector.end(), std::make_move_iterator(chunk_vector.begin()), std::make_move_iterator(chunk_vector.end()));
        }
    }
    
    return result_data;
}

} // namespace columnar
} // namespace engine