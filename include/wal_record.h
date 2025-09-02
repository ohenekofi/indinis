// @include/wal_record.h
#pragma once

#include "types.h"       // For TxnId, PageId, Record, ValueType, INVALID_TXN_ID
#include "debug_utils.h" // For LOG_ERROR etc. (though not directly used in this header's declarations)

#include <string>
#include <vector>
#include <variant>
#include <memory>       // For std::unique_ptr
#include <ostream>      // For std::ostream
#include <istream>      // For std::istream
#include <stdexcept>    // For std::runtime_error
#include <limits>       // For std::numeric_limits (used by INVALID_TXN_ID if defined differently)

// Log Sequence Number (LSN) - Often a monotonically increasing number or file offset.
using Lsn = uint64_t;
static constexpr Lsn INVALID_LSN = 0; // Or std::numeric_limits<uint64_t>::max();

enum class WalRecordType : uint8_t {
    INVALID = 0,
    BEGIN_TRANSACTION,
    COMMIT_TRANSACTION,
    ABORT_TRANSACTION,
    LSM_PUT,                  // Logs the full Record for LSM-Tree data
    BTREE_INSERT_ENTRY,       // Logs IndexName, CompositeKey, Value (which is the Primary Key for the data record)
    BTREE_REMOVE_KEY_PHYSICAL,// Logs IndexName, CompositeKey (this implies a tombstone for MVCC or physical delete)
    UPDATE_BTREE_ROOT,        // Logs IndexName, NewRootPageId for a specific B-Tree
    END_OF_LOG                // Optional marker for the end of a WAL file, useful for parsing truncated logs
};

// --- Forward Declarations for Serialization Helpers (defined in wal_record.cpp) ---
void SerializeString(std::ostream& out, const std::string& str);
std::string DeserializeString(std::istream& in);
void SerializeRecordToWal(std::ostream& out, const Record& record); // For LsmPutWalRecord
Record DeserializeRecordFromWal(std::istream& in);                 // For LsmPutWalRecord

// --- Base WAL Record Structure ---
// All specific WAL records inherit from this.
struct BaseWalRecord {
    TxnId txn_id_ = INVALID_TXN_ID; // Most records are associated with a transaction.
                                    // INVALID_TXN_ID can be used for system records (e.g., EndOfLog).

    BaseWalRecord() = default;
    explicit BaseWalRecord(TxnId txn_id) : txn_id_(txn_id) {}
    virtual ~BaseWalRecord() = default; // Ensure virtual destructor for polymorphism

    virtual WalRecordType getType() const = 0; // Pure virtual: must be implemented by derived classes
    
    // Calculates size of *payload specific to this record type* (excluding common header fields like type, txn_id, payload_length, CRC).
    virtual uint32_t getPayloadSize() const = 0; 
    
    // Serializes only the specific payload of the derived record type.
    virtual void serializePayload(std::ostream& out) const = 0; 
};

// --- Specific Log Record Structures ---
// Each derived class:
// - Defines its static constexpr WalRecordType TYPE.
// - Implements getType(), getPayloadSize(), serializePayload().
// - Provides a static deserializePayload(...) factory method that takes an istream 
//   and the txn_id (already read from the disk record header) to reconstruct the object.

struct BeginTransactionWalRecord : public BaseWalRecord {
    static constexpr WalRecordType TYPE = WalRecordType::BEGIN_TRANSACTION;
    explicit BeginTransactionWalRecord(TxnId txn_id);
    WalRecordType getType() const override;
    uint32_t getPayloadSize() const override;
    void serializePayload(std::ostream& out) const override;
    static std::unique_ptr<BeginTransactionWalRecord> deserializePayload(std::istream& in, TxnId txn_id_from_header);
};

struct CommitTransactionWalRecord : public BaseWalRecord {
    static constexpr WalRecordType TYPE = WalRecordType::COMMIT_TRANSACTION;
    explicit CommitTransactionWalRecord(TxnId txn_id);
    WalRecordType getType() const override;
    uint32_t getPayloadSize() const override;
    void serializePayload(std::ostream& out) const override;
    static std::unique_ptr<CommitTransactionWalRecord> deserializePayload(std::istream& in, TxnId txn_id_from_header);
};

struct AbortTransactionWalRecord : public BaseWalRecord {
    static constexpr WalRecordType TYPE = WalRecordType::ABORT_TRANSACTION;
    explicit AbortTransactionWalRecord(TxnId txn_id);
    WalRecordType getType() const override;
    uint32_t getPayloadSize() const override;
    void serializePayload(std::ostream& out) const override;
    static std::unique_ptr<AbortTransactionWalRecord> deserializePayload(std::istream& in, TxnId txn_id_from_header);
};

struct LsmPutWalRecord : public BaseWalRecord {
    static constexpr WalRecordType TYPE = WalRecordType::LSM_PUT;
    Record record_data_; // The full Record structure to be put into LSM tree

    LsmPutWalRecord(TxnId txn_id, Record record_data);
    WalRecordType getType() const override;
    uint32_t getPayloadSize() const override;
    void serializePayload(std::ostream& out) const override;
    static std::unique_ptr<LsmPutWalRecord> deserializePayload(std::istream& in, TxnId txn_id_from_header);
};

struct BTreeInsertEntryWalRecord : public BaseWalRecord {
    static constexpr WalRecordType TYPE = WalRecordType::BTREE_INSERT_ENTRY;
    std::string index_name_;    // Name of the B-Tree index being modified
    std::string composite_key_; // The full composite key (field_part + txn_id_encoded)
    std::string value_;         // The value stored in the B-Tree (typically the primary key of the main data record, or BTree::INDEX_TOMBSTONE)

    BTreeInsertEntryWalRecord(TxnId txn_id, std::string index_name, std::string composite_key, std::string value);
    WalRecordType getType() const override;
    uint32_t getPayloadSize() const override;
    void serializePayload(std::ostream& out) const override;
    static std::unique_ptr<BTreeInsertEntryWalRecord> deserializePayload(std::istream& in, TxnId txn_id_from_header);
};

struct BTreeRemoveKeyPhysicalWalRecord : public BaseWalRecord {
    static constexpr WalRecordType TYPE = WalRecordType::BTREE_REMOVE_KEY_PHYSICAL;
    std::string index_name_;
    std::string composite_key_; // The composite key to be removed (or more accurately, for which a tombstone of a higher TxnId might be written)

    BTreeRemoveKeyPhysicalWalRecord(TxnId txn_id, std::string index_name, std::string composite_key);
    WalRecordType getType() const override;
    uint32_t getPayloadSize() const override;
    void serializePayload(std::ostream& out) const override;
    static std::unique_ptr<BTreeRemoveKeyPhysicalWalRecord> deserializePayload(std::istream& in, TxnId txn_id_from_header);
};

struct UpdateBTreeRootWalRecord : public BaseWalRecord {
    static constexpr WalRecordType TYPE = WalRecordType::UPDATE_BTREE_ROOT;
    std::string index_name_;
    PageId new_root_page_id_; // The new root PageId for this B-Tree

    UpdateBTreeRootWalRecord(TxnId txn_id, std::string index_name, PageId new_root_page_id);
    WalRecordType getType() const override;
    uint32_t getPayloadSize() const override;
    void serializePayload(std::ostream& out) const override;
    static std::unique_ptr<UpdateBTreeRootWalRecord> deserializePayload(std::istream& in, TxnId txn_id_from_header);
};

struct EndOfLogWalRecord : public BaseWalRecord {
    static constexpr WalRecordType TYPE = WalRecordType::END_OF_LOG;
    EndOfLogWalRecord(); // Not tied to a specific transaction, uses INVALID_TXN_ID
    WalRecordType getType() const override;
    uint32_t getPayloadSize() const override;
    void serializePayload(std::ostream& out) const override;
    static std::unique_ptr<EndOfLogWalRecord> deserializePayload(std::istream& in, TxnId txn_id_from_header);
};


// --- WalRecordVariant and Disk Header ---
// Variant to hold any WAL record type for convenience, particularly for recovery processing.
using WalRecordVariant = std::variant<
    std::unique_ptr<BeginTransactionWalRecord>,
    std::unique_ptr<CommitTransactionWalRecord>,
    std::unique_ptr<AbortTransactionWalRecord>,
    std::unique_ptr<LsmPutWalRecord>,
    std::unique_ptr<BTreeInsertEntryWalRecord>,
    std::unique_ptr<BTreeRemoveKeyPhysicalWalRecord>,
    std::unique_ptr<UpdateBTreeRootWalRecord>,
    std::unique_ptr<EndOfLogWalRecord>
>;

// Header structure for each WAL record as written to disk.
// Format on disk: [WalDiskRecordHeader Fields] [Payload (PayloadLength bytes)] [CRC32 (4 bytes of header+payload)]
struct WalDiskRecordHeader {
    WalRecordType type;         // Type of the WAL record
    TxnId txn_id;               // Transaction ID this record belongs to (or INVALID_TXN_ID for system records)
    uint32_t payload_length;    // Length of the specific payload that follows this header
};

// Size of the CRC32 checksum stored at the end of each disk record.
static constexpr size_t WAL_DISK_CRC_SIZE = sizeof(uint32_t);