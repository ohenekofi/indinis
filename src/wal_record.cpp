// @src/wal_record.cpp
#include "../include/wal_record.h"
#include "../include/serialization_utils.h"
#include <sstream>   // For std::ostringstream in getPayloadSize implementations
#include <limits>    // For std::numeric_limits
#include <cstring>   // For std::memcpy (though often not strictly needed with modern stream I/O)
#include <stdexcept> // For std::runtime_error

// --- Serialization Helpers (Implementations) ---

/*
void SerializeString(std::ostream& out, const std::string& str) {
    uint32_t len = static_cast<uint32_t>(str.length());
    if (str.length() > std::numeric_limits<uint32_t>::max()) {
        LOG_ERROR("SerializeString: String length ", str.length(), " exceeds uint32_t max.");
        throw std::overflow_error("String too long for WAL serialization (exceeds uint32_t_max)");
    }
    out.write(reinterpret_cast<const char*>(&len), sizeof(len));
    if (!out) throw std::runtime_error("WAL SerializeString: Failed to write string length.");
    if (len > 0) {
        out.write(str.data(), len);
        if (!out) throw std::runtime_error("WAL SerializeString: Failed to write string data.");
    }
}

std::string DeserializeString(std::istream& in) {
    uint32_t len;
    in.read(reinterpret_cast<char*>(&len), sizeof(len));
    if (in.gcount() != sizeof(len)) {
        if (in.eof() && in.gcount() == 0) return ""; // Graceful EOF if at exact end before reading length
        if (in.eof()) throw std::runtime_error("WAL DeserializeString: EOF while reading string length.");
        throw std::runtime_error("WAL DeserializeString: Failed to read string length.");
    }
    if (len == 0) return "";

    // Sanity check for excessively large strings to prevent massive memory allocation
    // Adjust this limit based on expected maximum string sizes. 100MB is generous.
    if (len > (100 * 1024 * 1024)) { 
        LOG_ERROR("WAL DeserializeString: Attempting to deserialize string of unreasonable length: ", len);
        throw std::length_error("WAL DeserializeString: String length in WAL exceeds sanity limit.");
    }

    std::string str(len, '\0');
    in.read(&str[0], len); // Read directly into string's buffer
    if (static_cast<uint32_t>(in.gcount()) != len) {
        if (in.eof()) throw std::runtime_error("WAL DeserializeString: EOF while reading string data, expected " + std::to_string(len) + " bytes.");
        throw std::runtime_error("WAL DeserializeString: Failed to read string data, expected " + std::to_string(len) + " bytes.");
    }
    return str;
}
*/
void SerializeRecordToWal(std::ostream& out, const Record& record) {
    SerializeString(out, record.key);

    uint8_t value_type_index = static_cast<uint8_t>(record.value.index());
    out.write(reinterpret_cast<const char*>(&value_type_index), sizeof(value_type_index));
    if (!out) throw std::runtime_error("WAL SerializeRecordToWal: Failed to write value_type_index.");

    std::visit([&out](auto&& arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, double>) {
            out.write(reinterpret_cast<const char*>(&arg), sizeof(T));
        } else if constexpr (std::is_same_v<T, std::string>) {
            SerializeString(out, arg); // Uses the helper
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            uint32_t len = static_cast<uint32_t>(arg.size());
             if (arg.size() > std::numeric_limits<uint32_t>::max()) {
                 throw std::overflow_error("WAL SerializeRecordToWal: Binary vector too long for serialization.");
             }
            out.write(reinterpret_cast<const char*>(&len), sizeof(len));
            if (!out) throw std::runtime_error("WAL SerializeRecordToWal: Failed to write vector length.");
            if (len > 0) {
                out.write(reinterpret_cast<const char*>(arg.data()), len);
            }
        }
        if (!out) throw std::runtime_error("WAL SerializeRecordToWal: Failed during value variant serialization.");
    }, record.value);

    out.write(reinterpret_cast<const char*>(&record.commit_txn_id), sizeof(record.commit_txn_id));
    std::time_t timestamp_raw = std::chrono::system_clock::to_time_t(record.timestamp);
    out.write(reinterpret_cast<const char*>(&timestamp_raw), sizeof(timestamp_raw));
    out.write(reinterpret_cast<const char*>(&record.deleted), sizeof(record.deleted));
    if (!out) throw std::runtime_error("WAL SerializeRecordToWal: Failed to write Record metadata.");
}

Record DeserializeRecordFromWal(std::istream& in) {
    Record record;
    record.key = DeserializeString(in); // Uses helper

    uint8_t value_type_index;
    in.read(reinterpret_cast<char*>(&value_type_index), sizeof(value_type_index));
    if (in.gcount() != sizeof(value_type_index)) {
        if (in.eof()) throw std::runtime_error("WAL DeserializeRecordFromWal: EOF while reading value_type_index for key '" + record.key + "'.");
        throw std::runtime_error("WAL DeserializeRecordFromWal: Failed to read value_type_index for key '" + record.key + "'.");
    }

    switch (value_type_index) {
        case 0: { // int64_t
            int64_t v; 
            in.read(reinterpret_cast<char*>(&v), sizeof(v)); 
            if (in.gcount()!=sizeof(v)) throw std::runtime_error("WAL DeserializeRecordFromWal: Failed to read int64_t value for key '" + record.key + "'.");
            record.value = v; 
            break; 
        }
        case 1: { // double
            double v;  
            in.read(reinterpret_cast<char*>(&v), sizeof(v));  
            if (in.gcount()!=sizeof(v)) throw std::runtime_error("WAL DeserializeRecordFromWal: Failed to read double value for key '" + record.key + "'.");
            record.value = v; 
            break; 
        }
        case 2: { // std::string
            record.value = DeserializeString(in); // Uses helper
            break; 
        }
        case 3: { // std::vector<uint8_t>
            uint32_t len;
            in.read(reinterpret_cast<char*>(&len), sizeof(len));
            if (in.gcount()!=sizeof(len)) throw std::runtime_error("WAL DeserializeRecordFromWal: Failed to read vector length for key '" + record.key + "'.");
            std::vector<uint8_t> v(len);
            if (len > 0) {
                in.read(reinterpret_cast<char*>(v.data()), len);
                if (static_cast<uint32_t>(in.gcount())!=len) throw std::runtime_error("WAL DeserializeRecordFromWal: Failed to read vector data for key '" + record.key + "'.");
            }
            record.value = v;
            break;
        }
        default: 
            throw std::runtime_error("WAL DeserializeRecordFromWal: Invalid value_type_index " + std::to_string(value_type_index) + " for key '" + record.key + "'.");
    }

    in.read(reinterpret_cast<char*>(&record.commit_txn_id), sizeof(record.commit_txn_id));
    std::time_t timestamp_raw;
    in.read(reinterpret_cast<char*>(&timestamp_raw), sizeof(timestamp_raw));
    in.read(reinterpret_cast<char*>(&record.deleted), sizeof(record.deleted));
    
    if (in.fail()) { // Check stream state after all reads for metadata
        if (in.eof()) throw std::runtime_error("WAL DeserializeRecordFromWal: EOF while reading Record metadata for key '" + record.key + "'.");
        throw std::runtime_error("WAL DeserializeRecordFromWal: Failed to read Record metadata for key '" + record.key + "'.");
    }
    record.timestamp = std::chrono::system_clock::from_time_t(timestamp_raw);

    return record;
}


// --- Derived WalRecord Implementations ---

// BeginTransactionWalRecord
BeginTransactionWalRecord::BeginTransactionWalRecord(TxnId txn_id) : BaseWalRecord(txn_id) {}
WalRecordType BeginTransactionWalRecord::getType() const { return TYPE; }
uint32_t BeginTransactionWalRecord::getPayloadSize() const { return 0; }
void BeginTransactionWalRecord::serializePayload(std::ostream& out) const {
    // No specific payload for this record type beyond what's in the disk header
}
std::unique_ptr<BeginTransactionWalRecord> BeginTransactionWalRecord::deserializePayload(std::istream& in, TxnId txn_id_from_header) {
    return std::make_unique<BeginTransactionWalRecord>(txn_id_from_header);
}

// CommitTransactionWalRecord
CommitTransactionWalRecord::CommitTransactionWalRecord(TxnId txn_id) : BaseWalRecord(txn_id) {}
WalRecordType CommitTransactionWalRecord::getType() const { return TYPE; }
uint32_t CommitTransactionWalRecord::getPayloadSize() const { return 0; }
void CommitTransactionWalRecord::serializePayload(std::ostream& out) const { /* No payload */ }
std::unique_ptr<CommitTransactionWalRecord> CommitTransactionWalRecord::deserializePayload(std::istream& in, TxnId txn_id_from_header) {
    return std::make_unique<CommitTransactionWalRecord>(txn_id_from_header);
}

// AbortTransactionWalRecord
AbortTransactionWalRecord::AbortTransactionWalRecord(TxnId txn_id) : BaseWalRecord(txn_id) {}
WalRecordType AbortTransactionWalRecord::getType() const { return TYPE; }
uint32_t AbortTransactionWalRecord::getPayloadSize() const { return 0; }
void AbortTransactionWalRecord::serializePayload(std::ostream& out) const { /* No payload */ }
std::unique_ptr<AbortTransactionWalRecord> AbortTransactionWalRecord::deserializePayload(std::istream& in, TxnId txn_id_from_header) {
    return std::make_unique<AbortTransactionWalRecord>(txn_id_from_header);
}

// LsmPutWalRecord
LsmPutWalRecord::LsmPutWalRecord(TxnId txn_id, Record record_data)
    : BaseWalRecord(txn_id), record_data_(std::move(record_data)) {}
WalRecordType LsmPutWalRecord::getType() const { return TYPE; }
uint32_t LsmPutWalRecord::getPayloadSize() const {
    // Calculate size by serializing to a temporary stream
    std::ostringstream temp_stream(std::ios::binary);
    SerializeRecordToWal(temp_stream, record_data_);
    return static_cast<uint32_t>(temp_stream.str().length());
}
void LsmPutWalRecord::serializePayload(std::ostream& out) const {
    SerializeRecordToWal(out, record_data_);
}
std::unique_ptr<LsmPutWalRecord> LsmPutWalRecord::deserializePayload(std::istream& in, TxnId txn_id_from_header) {
    Record r = DeserializeRecordFromWal(in);
    // The Record struct itself has a txn_id field. For an LsmPutWalRecord,
    // this field in the deserialized Record should reflect the creator txn_id.
    // The WalDiskRecordHeader's txn_id is authoritative for this WAL entry.
    r.txn_id = txn_id_from_header; 
    return std::make_unique<LsmPutWalRecord>(txn_id_from_header, std::move(r));
}

// BTreeInsertEntryWalRecord
BTreeInsertEntryWalRecord::BTreeInsertEntryWalRecord(TxnId txn_id, std::string index_name, std::string composite_key, std::string value)
    : BaseWalRecord(txn_id), index_name_(std::move(index_name)),
      composite_key_(std::move(composite_key)), value_(std::move(value)) {}
WalRecordType BTreeInsertEntryWalRecord::getType() const { return TYPE; }
uint32_t BTreeInsertEntryWalRecord::getPayloadSize() const {
    std::ostringstream temp_stream(std::ios::binary);
    SerializeString(temp_stream, index_name_);
    SerializeString(temp_stream, composite_key_);
    SerializeString(temp_stream, value_);
    return static_cast<uint32_t>(temp_stream.str().length());
}
void BTreeInsertEntryWalRecord::serializePayload(std::ostream& out) const {
    SerializeString(out, index_name_);
    SerializeString(out, composite_key_);
    SerializeString(out, value_);
}
std::unique_ptr<BTreeInsertEntryWalRecord> BTreeInsertEntryWalRecord::deserializePayload(std::istream& in, TxnId txn_id_from_header) {
    std::string index_name = DeserializeString(in);
    std::string composite_key = DeserializeString(in);
    std::string value = DeserializeString(in);
    return std::make_unique<BTreeInsertEntryWalRecord>(txn_id_from_header, std::move(index_name), std::move(composite_key), std::move(value));
}

// BTreeRemoveKeyPhysicalWalRecord
BTreeRemoveKeyPhysicalWalRecord::BTreeRemoveKeyPhysicalWalRecord(TxnId txn_id, std::string index_name, std::string composite_key)
    : BaseWalRecord(txn_id), index_name_(std::move(index_name)),
      composite_key_(std::move(composite_key)) {}
WalRecordType BTreeRemoveKeyPhysicalWalRecord::getType() const { return TYPE; }
uint32_t BTreeRemoveKeyPhysicalWalRecord::getPayloadSize() const {
    std::ostringstream temp_stream(std::ios::binary);
    SerializeString(temp_stream, index_name_);
    SerializeString(temp_stream, composite_key_);
    return static_cast<uint32_t>(temp_stream.str().length());
}
void BTreeRemoveKeyPhysicalWalRecord::serializePayload(std::ostream& out) const {
    SerializeString(out, index_name_);
    SerializeString(out, composite_key_);
}
std::unique_ptr<BTreeRemoveKeyPhysicalWalRecord> BTreeRemoveKeyPhysicalWalRecord::deserializePayload(std::istream& in, TxnId txn_id_from_header) {
    std::string index_name = DeserializeString(in);
    std::string composite_key = DeserializeString(in);
    return std::make_unique<BTreeRemoveKeyPhysicalWalRecord>(txn_id_from_header, std::move(index_name), std::move(composite_key));
}

// UpdateBTreeRootWalRecord
UpdateBTreeRootWalRecord::UpdateBTreeRootWalRecord(TxnId txn_id, std::string index_name, PageId new_root_page_id)
    : BaseWalRecord(txn_id), index_name_(std::move(index_name)), new_root_page_id_(new_root_page_id) {}
WalRecordType UpdateBTreeRootWalRecord::getType() const { return TYPE; }
uint32_t UpdateBTreeRootWalRecord::getPayloadSize() const {
    std::ostringstream temp_stream(std::ios::binary);
    SerializeString(temp_stream, index_name_);
    temp_stream.write(reinterpret_cast<const char*>(&new_root_page_id_), sizeof(new_root_page_id_));
    return static_cast<uint32_t>(temp_stream.str().length());
}
void UpdateBTreeRootWalRecord::serializePayload(std::ostream& out) const {
    SerializeString(out, index_name_);
    out.write(reinterpret_cast<const char*>(&new_root_page_id_), sizeof(new_root_page_id_));
    if (!out) throw std::runtime_error("WAL UpdateBTreeRootWalRecord: Failed to write payload.");
}
std::unique_ptr<UpdateBTreeRootWalRecord> UpdateBTreeRootWalRecord::deserializePayload(std::istream& in, TxnId txn_id_from_header) {
    std::string index_name = DeserializeString(in);
    PageId new_root_id;
    in.read(reinterpret_cast<char*>(&new_root_id), sizeof(new_root_id));
    if(in.gcount() != sizeof(new_root_id)) {
        if (in.eof()) throw std::runtime_error("WAL UpdateBTreeRootWalRecord: EOF while reading new_root_page_id for index '" + index_name + "'.");
        throw std::runtime_error("WAL UpdateBTreeRootWalRecord: Failed to read new_root_page_id for index '" + index_name + "'.");
    }
    return std::make_unique<UpdateBTreeRootWalRecord>(txn_id_from_header, std::move(index_name), new_root_id);
}

// EndOfLogWalRecord
EndOfLogWalRecord::EndOfLogWalRecord() : BaseWalRecord(INVALID_TXN_ID) {}
WalRecordType EndOfLogWalRecord::getType() const { return TYPE; }
uint32_t EndOfLogWalRecord::getPayloadSize() const { return 0; }
void EndOfLogWalRecord::serializePayload(std::ostream& out) const { /* No payload */ }
std::unique_ptr<EndOfLogWalRecord> EndOfLogWalRecord::deserializePayload(std::istream& in, TxnId txn_id_from_header) {
    return std::make_unique<EndOfLogWalRecord>();
}