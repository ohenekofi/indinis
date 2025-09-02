// @src/types.cpp
#include "../include/types.h"
#include "../include/debug_utils.h" // For LOG_ macros if used in toString or other helpers
#include "../vendor/zlib/zlib.h"  // For crc32 (used in LogRecord::serialize)

#include <vector>
#include <cstring>   // For std::memcpy
#include <stdexcept> // For std::runtime_error, std::overflow_error
#include <sstream>   // For std::ostringstream, std::istringstream
#include <iomanip>   // For std::setw, std::setfill
#include <algorithm> // For std::min, std::max (not directly used here but good include for .cpp)


namespace {
    // Serializes a string with a 32-bit length prefix.
    void serialize_string_to_binary_buffer(std::vector<uint8_t>& buffer, const std::string& str) {
        if (str.length() > std::numeric_limits<uint32_t>::max()) {
            throw std::overflow_error("String length exceeds uint32_t max during serialization.");
        }
        uint32_t len = static_cast<uint32_t>(str.length());
        const char* len_ptr = reinterpret_cast<const char*>(&len);
        buffer.insert(buffer.end(), len_ptr, len_ptr + sizeof(len));
        if (len > 0) {
            buffer.insert(buffer.end(), reinterpret_cast<const uint8_t*>(str.data()), reinterpret_cast<const uint8_t*>(str.data()) + len);
        }
    }

    // Serializes a vector<uint8_t> with a 32-bit length prefix.
    void serialize_vector_to_binary_buffer(std::vector<uint8_t>& buffer, const std::vector<uint8_t>& vec) {
        if (vec.size() > std::numeric_limits<uint32_t>::max()) {
            throw std::overflow_error("Vector size exceeds uint32_t max during serialization.");
        }
        uint32_t len = static_cast<uint32_t>(vec.size());
        const char* len_ptr = reinterpret_cast<const char*>(&len);
        buffer.insert(buffer.end(), len_ptr, len_ptr + sizeof(len));
        if (len > 0) {
            buffer.insert(buffer.end(), vec.begin(), vec.end());
        }
    }

    // Deserializes a length-prefixed string.
    std::string deserialize_string_from_binary_data(const char*& ptr, const char* end_ptr) {
        if (static_cast<size_t>(end_ptr - ptr) < sizeof(uint32_t)) {
            throw std::runtime_error("ValueType deserialize: Buffer too short for string length prefix.");
        }
        uint32_t len;
        std::memcpy(&len, ptr, sizeof(uint32_t));
        ptr += sizeof(uint32_t);

        if (static_cast<size_t>(end_ptr - ptr) < len) {
            throw std::runtime_error("ValueType deserialize: Buffer too short for string data (len: " + std::to_string(len) + ").");
        }
        std::string str(ptr, len);
        ptr += len;
        return str;
    }

    // Deserializes a length-prefixed vector<uint8_t>.
    std::vector<uint8_t> deserialize_vector_from_binary_data(const char*& ptr, const char* end_ptr) {
        if (static_cast<size_t>(end_ptr - ptr) < sizeof(uint32_t)) {
            throw std::runtime_error("ValueType deserialize: Buffer too short for vector length prefix.");
        }
        uint32_t len;
        std::memcpy(&len, ptr, sizeof(uint32_t));
        ptr += sizeof(uint32_t);

        if (static_cast<size_t>(end_ptr - ptr) < len) {
            throw std::runtime_error("ValueType deserialize: Buffer too short for vector data (len: " + std::to_string(len) + ").");
        }
        // FIX: Cast the char* to a const uint8_t* for the vector constructor
        const uint8_t* vec_start = reinterpret_cast<const uint8_t*>(ptr);
        std::vector<uint8_t> vec(vec_start, vec_start + len);
        ptr += len;
        return vec;
    }
} // end anonymous namespace


// --- LogRecord Method Implementations ---

void LogRecord::serialize(std::ostream& out) const {
    // 1. Serialize the core fields into a temporary buffer to calculate CRC
    std::ostringstream payload_content_stream(std::ios::binary);
    payload_content_stream.write(reinterpret_cast<const char*>(&lsn), sizeof(lsn));
    payload_content_stream.write(reinterpret_cast<const char*>(&txn_id), sizeof(txn_id));
    payload_content_stream.write(reinterpret_cast<const char*>(&type), sizeof(type));

    // Serialize key with its length
    if (key.length() > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("LogRecord::serialize: Key length exceeds uint32_t max.");
    }
    uint32_t key_len_32 = static_cast<uint32_t>(key.size());
    payload_content_stream.write(reinterpret_cast<const char*>(&key_len_32), sizeof(key_len_32));
    if (key_len_32 > 0) {
        payload_content_stream.write(key.data(), key_len_32);
    }

    // --- START OF  ---
    // The 'value' member is ALREADY a serialized binary string from serializeValueTypeBinary.
    // We just need to write its length and its raw content.
    if (value.length() > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("LogRecord::serialize: Value (binary string) length exceeds uint32_t max.");
    }
    uint32_t value_binary_len_32 = static_cast<uint32_t>(value.size());
    payload_content_stream.write(reinterpret_cast<const char*>(&value_binary_len_32), sizeof(value_binary_len_32));
    if (value_binary_len_32 > 0) {
        payload_content_stream.write(value.data(), value_binary_len_32);
    }
    // --- END OF  ---

    if (!payload_content_stream) {
        throw std::runtime_error("LogRecord::serialize: Stream error during payload content serialization.");
    }
    
    std::string payload_content_data_str = payload_content_stream.str();
    uint32_t payload_content_data_len = static_cast<uint32_t>(payload_content_data_str.length());

    // 2. Calculate CRC32 of the payload content
    uLong crc = crc32(0L, Z_NULL, 0);
    if (payload_content_data_len > 0) {
        crc = crc32(crc, reinterpret_cast<const Bytef*>(payload_content_data_str.data()), payload_content_data_len);
    }
    uint32_t crc_val_32 = static_cast<uint32_t>(crc);

    // 3. Write the framing to the final output stream
    out.write(reinterpret_cast<const char*>(&payload_content_data_len), sizeof(payload_content_data_len));
    if (!out) throw std::runtime_error("LogRecord::serialize: Failed to write payload_content_data_len.");

    if (payload_content_data_len > 0) {
        out.write(payload_content_data_str.data(), payload_content_data_len);
        if (!out) throw std::runtime_error("LogRecord::serialize: Failed to write payload_content_data.");
    }

    out.write(reinterpret_cast<const char*>(&crc_val_32), sizeof(crc_val_32));
    if (!out) throw std::runtime_error("LogRecord::serialize: Failed to write CRC.");
}

bool LogRecord::deserialize(std::istream& in) {
    uint32_t payload_content_data_len;
    if (!in.read(reinterpret_cast<char*>(&payload_content_data_len), sizeof(payload_content_data_len))) {
        if (in.eof() && in.gcount() == 0) return false; // Clean EOF before reading anything for this record
        LOG_TRACE("[LogRecord Deserialize] Failed to read payload_content_data_len. EOF: {}, GCount: {}", in.eof(), in.gcount());
        return false;
    }

    constexpr uint32_t MAX_LOG_RECORD_PAYLOAD_SIZE = 128 * 1024 * 1024; // e.g., 128MB
    if (payload_content_data_len > MAX_LOG_RECORD_PAYLOAD_SIZE) {
        LOG_ERROR("[LogRecord Deserialize] Excessive payload_content_data_len: {}. Record likely corrupt.", payload_content_data_len);
        in.setstate(std::ios_base::failbit);
        return false;
    }

    std::vector<char> payload_content_buffer(payload_content_data_len);
    if (payload_content_data_len > 0) {
        if (!in.read(payload_content_buffer.data(), payload_content_data_len)) {
            LOG_WARN("[LogRecord Deserialize] Failed to read payload_content_data (expected {} bytes). EOF: {}, GCount: {}", payload_content_data_len, in.eof(), in.gcount());
            return false;
        }
    }

    uint32_t stored_crc_val_32;
    if (!in.read(reinterpret_cast<char*>(&stored_crc_val_32), sizeof(stored_crc_val_32))) {
        LOG_WARN("[LogRecord Deserialize] Failed to read stored CRC. EOF: {}, GCount: {}", in.eof(), in.gcount());
        return false;
    }

    uLong calculated_crc_long = crc32(0L, Z_NULL, 0);
    if (payload_content_data_len > 0) {
        calculated_crc_long = crc32(calculated_crc_long, reinterpret_cast<const Bytef*>(payload_content_buffer.data()), payload_content_data_len);
    }
    uint32_t calculated_crc_val_32 = static_cast<uint32_t>(calculated_crc_long);

    if (calculated_crc_val_32 != stored_crc_val_32) {
        LOG_ERROR("[LogRecord Deserialize] CRC mismatch! Stored: {:#010x}, Calculated: {:#010x}. PayloadLen: {}.",
                  stored_crc_val_32, calculated_crc_val_32, payload_content_data_len);
        return false;
    }

    try {
        std::istringstream payload_content_stream(std::string(payload_content_buffer.data(), payload_content_data_len), std::ios::binary);
        if (!payload_content_stream.read(reinterpret_cast<char*>(&lsn), sizeof(lsn))) throw std::runtime_error("payload_stream: read lsn");
        if (!payload_content_stream.read(reinterpret_cast<char*>(&txn_id), sizeof(txn_id))) throw std::runtime_error("payload_stream: read txn_id");
        if (!payload_content_stream.read(reinterpret_cast<char*>(&type), sizeof(type))) throw std::runtime_error("payload_stream: read type");

        uint32_t key_len_32;
        if (!payload_content_stream.read(reinterpret_cast<char*>(&key_len_32), sizeof(key_len_32))) throw std::runtime_error("payload_stream: read key_len");
        key.resize(key_len_32);
        if (key_len_32 > 0) { if (!payload_content_stream.read(&key[0], key_len_32)) throw std::runtime_error("payload_stream: read key_data"); }

        uint32_t value_binary_len_32;
        if (!payload_content_stream.read(reinterpret_cast<char*>(&value_binary_len_32), sizeof(value_binary_len_32))) throw std::runtime_error("payload_stream: read value_binary_len");
        value.resize(value_binary_len_32); // 'value' member stores the binary string
        if (value_binary_len_32 > 0) { if (!payload_content_stream.read(&value[0], value_binary_len_32)) throw std::runtime_error("payload_stream: read value_binary_data"); }

        // Check if all payload content was consumed
        payload_content_stream.peek(); // Attempt to read one more byte to check EOF
        if (!payload_content_stream.eof()) {
             std::streamoff remaining_bytes = -1; // Can't easily get remaining without reading more
             if(payload_content_stream.good()){ // Try to get current pos
                std::streamoff current_pos_in_payload = payload_content_stream.tellg();
                if(current_pos_in_payload != -1){
                    remaining_bytes = payload_content_data_len - current_pos_in_payload;
                }
             }
            LOG_WARN("[LogRecord Deserialize] Payload content stream for LSN {} had {} unconsumed bytes. Expected total payload content len: {}.",
                     lsn, (remaining_bytes != -1 ? std::to_string(remaining_bytes) : "unknown"), payload_content_data_len);
        }

    } catch (const std::exception& e) {
        LOG_ERROR("[LogRecord Deserialize] Exception during payload content deserialization (LSN if known from successful prev field: {}): {}", (lsn > 0 ? std::to_string(lsn) : "unknown"), e.what());
        return false;
    }
    return true;
}


uint32_t LogRecord::getDiskSize() const {
    std::ostringstream temp_payload_content_stream(std::ios::binary);
    temp_payload_content_stream.write(reinterpret_cast<const char*>(&lsn), sizeof(lsn));
    temp_payload_content_stream.write(reinterpret_cast<const char*>(&txn_id), sizeof(txn_id));
    temp_payload_content_stream.write(reinterpret_cast<const char*>(&type), sizeof(type));
    uint32_t key_len_32 = static_cast<uint32_t>(key.size());
    temp_payload_content_stream.write(reinterpret_cast<const char*>(&key_len_32), sizeof(key_len_32));
    if (key_len_32 > 0) temp_payload_content_stream.write(key.data(), key_len_32);
    uint32_t value_binary_len_32 = static_cast<uint32_t>(value.size());
    temp_payload_content_stream.write(reinterpret_cast<const char*>(&value_binary_len_32), sizeof(value_binary_len_32));
    if (value_binary_len_32 > 0) temp_payload_content_stream.write(value.data(), value_binary_len_32);

    if (!temp_payload_content_stream) {
        LOG_ERROR("[LogRecord::getDiskSize] Stream error occurred while calculating payload content length for LSN {}.", lsn);
        throw std::runtime_error("LogRecord::getDiskSize: ostringstream error during size calculation.");
    }
    uint32_t actual_payload_content_len = static_cast<uint32_t>(temp_payload_content_stream.str().length());

    // On disk: [payload_content_len (4B)] [payload_content_data (actual_payload_content_len B)] [CRC (4B)]
    return sizeof(uint32_t) + actual_payload_content_len + sizeof(uint32_t);
}

/*static*/ std::string LogRecord::serializeValueTypeBinary(const ValueType& value) {
    std::vector<uint8_t> buffer;
    buffer.reserve(256); // Pre-allocate a reasonable starting size

    // Write the variant index as a 1-byte type tag.
    uint8_t type_index = static_cast<uint8_t>(value.index());
    buffer.push_back(type_index);

    std::visit([&buffer](auto&& arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            // No data for null/monostate.
        } else if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, double> || std::is_same_v<T, bool>) {
            // For fixed-size types (int64, double, bool), write the raw bytes.
            const char* ptr = reinterpret_cast<const char*>(&arg);
            buffer.insert(buffer.end(), ptr, ptr + sizeof(T));
        } else if constexpr (std::is_same_v<T, std::string>) {
            serialize_string_to_binary_buffer(buffer, arg);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            serialize_vector_to_binary_buffer(buffer, arg);
        }
    }, value);

    return std::string(reinterpret_cast<const char*>(buffer.data()), buffer.size());
}

/*static*/ std::optional<ValueType> LogRecord::deserializeValueTypeBinary(const std::string& data_str) {
    if (data_str.empty()) {
        return std::nullopt;
    }
    
    const char* ptr = data_str.data();
    const char* end_ptr = ptr + data_str.size();

    uint8_t type_index = static_cast<uint8_t>(*ptr++);

    try {
        switch (type_index) {
            case 0: return std::monostate{};
            case 1: { // int64_t
                if (static_cast<size_t>(end_ptr - ptr) < sizeof(int64_t)) throw std::runtime_error("Data too short for int64_t.");
                int64_t val; std::memcpy(&val, ptr, sizeof(int64_t)); return val;
            }
            case 2: { // double
                if (static_cast<size_t>(end_ptr - ptr) < sizeof(double)) throw std::runtime_error("Data too short for double.");
                double val; std::memcpy(&val, ptr, sizeof(double)); return val;
            }
            case 3: { // std::string
                return deserialize_string_from_binary_data(ptr, end_ptr);
            }
            case 4: { // std::vector<uint8_t>
                return deserialize_vector_from_binary_data(ptr, end_ptr);
            }
            case 5: { // bool
                if (static_cast<size_t>(end_ptr - ptr) < sizeof(bool)) throw std::runtime_error("Data too short for bool.");
                bool val; std::memcpy(&val, ptr, sizeof(bool)); return val;
            }
            default:
                LOG_ERROR("[DeserializeValueTypeBinary] Unknown type index: {}", static_cast<int>(type_index));
                return std::nullopt;
        }
    } catch (const std::exception& e) {
        LOG_ERROR("[DeserializeValueTypeBinary] Error: {}. Type index: {}", e.what(), static_cast<int>(type_index));
        return std::nullopt;
    }
}

// --- VersionedRecord Method Implementations ---
VersionedRecord::VersionedRecord() = default;

VersionedRecord::VersionedRecord(const Record& record) : current(record) {}

std::optional<Record> VersionedRecord::getVisibleVersion(TxnId reader_txn_id) const {
    std::shared_lock<std::shared_mutex> lock(const_cast<std::shared_mutex&>(mutex)); // const_cast for mutable mutex
    // 1. Check current version
    if (current.commit_txn_id != 0 && current.commit_txn_id < reader_txn_id) {
        return current.deleted ? std::nullopt : std::optional<Record>(current);
    }
    // 2. Check historical versions
    for (auto it = versions.rbegin(); it != versions.rend(); ++it) {
         const auto& version = *it;
         if (version.commit_txn_id != 0 && version.commit_txn_id < reader_txn_id) {
             return version.deleted ? std::nullopt : std::optional<Record>(version);
         }
    }
    return std::nullopt;
}

void VersionedRecord::addVersion(const Record& record, TxnId oldest_active_txn_id) {
    std::unique_lock<std::shared_mutex> lock(mutex);

    // Determine if 'current' holds meaningful data or if there's history
    bool current_is_meaningful_or_history_exists =
        (!current.key.empty() || current.commit_txn_id != 0 || !versions.empty() ||
         (current.value.index() != std::variant_npos && // Ensure current.value is not valueless
          !std::holds_alternative<std::monostate>(current.value))); // Or if you use monostate for empty


    if (current_is_meaningful_or_history_exists) {
        versions.push_back(current);
    }
    current = record; // Set the new record as current

    // Simple pruning: keep a max number of versions for now
    // TODO: Implement GC based on oldest_active_txn_id for more advanced pruning
    const size_t MAX_MEMTABLE_VERSIONS_TO_KEEP = 10; // Configurable
    if (versions.size() > MAX_MEMTABLE_VERSIONS_TO_KEEP) {
        // Erase oldest versions to maintain the limit
        versions.erase(versions.begin(), versions.begin() + (versions.size() - MAX_MEMTABLE_VERSIONS_TO_KEEP));
    }
}


// --- CheckpointMetadata Method Implementations ---
void CheckpointMetadata::serialize(std::ostream& out) const {
    const uint32_t VERSION = 1; // Simple versioning
    out.write(reinterpret_cast<const char*>(&VERSION), sizeof(VERSION));
    out.write(reinterpret_cast<const char*>(&checkpoint_id), sizeof(checkpoint_id));
    out.write(reinterpret_cast<const char*>(&checkpoint_begin_lsn), sizeof(checkpoint_begin_lsn));
    auto ts_count = timestamp.time_since_epoch().count(); // Get count for serialization
    out.write(reinterpret_cast<const char*>(&ts_count), sizeof(ts_count));

    if (active_txns_at_checkpoint_begin.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("Too many active transactions to serialize count as uint32_t.");
    }
    uint32_t active_txn_count = static_cast<uint32_t>(active_txns_at_checkpoint_begin.size());
    out.write(reinterpret_cast<const char*>(&active_txn_count), sizeof(active_txn_count));
    for (const TxnId& txn_id : active_txns_at_checkpoint_begin) {
        out.write(reinterpret_cast<const char*>(&txn_id), sizeof(txn_id));
    }
    if(!out) { // Check stream state after all writes
        throw std::runtime_error("CheckpointMetadata::serialize encountered a stream error after writing data.");
    }
}

bool CheckpointMetadata::deserialize(std::istream& in) {
    uint32_t version;
    if (!in.read(reinterpret_cast<char*>(&version), sizeof(version))) return false;

    if (version == 1) {
        if (!in.read(reinterpret_cast<char*>(&checkpoint_id), sizeof(checkpoint_id))) return false;
        if (!in.read(reinterpret_cast<char*>(&checkpoint_begin_lsn), sizeof(checkpoint_begin_lsn))) return false;

        std::chrono::system_clock::rep ts_count;
        if (!in.read(reinterpret_cast<char*>(&ts_count), sizeof(ts_count))) return false;
        timestamp = std::chrono::system_clock::time_point(std::chrono::system_clock::duration(ts_count));

        uint32_t active_txn_count;
        if (!in.read(reinterpret_cast<char*>(&active_txn_count), sizeof(active_txn_count))) return false;

        active_txns_at_checkpoint_begin.clear();
        for (uint32_t i = 0; i < active_txn_count; ++i) {
            TxnId txn_id;
            if (!in.read(reinterpret_cast<char*>(&txn_id), sizeof(txn_id))) return false;
            active_txns_at_checkpoint_begin.insert(txn_id);
        }
        return true;
    }
    // Handle other versions or log error for unknown version
    LOG_WARN("CheckpointMetadata::deserialize found unknown version: {}", version);
    return false;
}

std::string CheckpointMetadata::toString() const {
    std::ostringstream oss;
    oss << "CheckpointMetadata{id=" << checkpoint_id
        << ", begin_lsn=" << checkpoint_begin_lsn
        // << ", timestamp=" << std::chrono::system_clock::to_time_t(timestamp) // Example way to print time
        << ", active_txns_count=" << active_txns_at_checkpoint_begin.size()
        << "}";
    return oss.str();
}

// --- CheckpointInfo Method Implementations ---
std::string CheckpointInfo::toString() const {
    std::ostringstream oss;
    std::string status_str;
    switch (status) {
        case CheckpointStatus::NOT_STARTED: status_str = "NOT_STARTED"; break;
        case CheckpointStatus::IN_PROGRESS: status_str = "IN_PROGRESS"; break;
        case CheckpointStatus::COMPLETED:   status_str = "COMPLETED"; break;
        case CheckpointStatus::FAILED:      status_str = "FAILED"; break;
        default: status_str = "UNKNOWN";
    }
    oss << "CheckpointInfo[id=" << checkpoint_id << ", status=" << status_str;
    if (status != CheckpointStatus::NOT_STARTED) { // Only show LSNs if applicable
            oss << ", begin_lsn=" << checkpoint_begin_wal_lsn;
    }
    if (status == CheckpointStatus::COMPLETED || status == CheckpointStatus::FAILED) {
        if (start_time.time_since_epoch().count() != 0 && end_time.time_since_epoch().count() != 0 && end_time >= start_time) {
            auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
            oss << ", end_lsn=" << checkpoint_end_wal_lsn << ", duration=" << duration_ms << "ms";
        } else {
            oss << ", end_lsn=" << checkpoint_end_wal_lsn << ", duration=N/A";
        }
    }
    if (status == CheckpointStatus::FAILED && !error_msg.empty()) {
        oss << ", Error: " << error_msg;
    }
    if (!active_txns_at_begin.empty()) {
        oss << ", active_txns_at_begin_count=" << active_txns_at_begin.size();
    }
    oss << "]";
    return oss.str();
}