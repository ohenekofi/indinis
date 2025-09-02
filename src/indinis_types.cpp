// @src/indinis_types.cpp

#include "../include/indinis_types.h" // Include the corresponding header

#include <iostream> // For std::cout in addVersion (debugging)
#include <algorithm> // For std::min/max (though not directly used in these methods)
#include <utility>   // For std::move in IndexField constructor

// --- VersionedRecord Method Implementations ---

// Default constructor (can be omitted if default member initialization is sufficient)
VersionedRecord::VersionedRecord() = default;

// Constructor initializing with a record
VersionedRecord::VersionedRecord(const Record& record) : current(record) {}

// Gets the version visible to the reader transaction (Snapshot Isolation)
std::optional<Record> VersionedRecord::getVisibleVersion(TxnId reader_txn_id) const {
    // Acquire shared lock for reading versions
    std::shared_lock<std::shared_mutex> lock(const_cast<std::shared_mutex&>(mutex));

    // 1. Check the 'current' version first.
    // It's visible if it's committed (commit_txn_id != 0) AND
    // its commit ID is less than the reader's transaction ID.
    if (current.commit_txn_id != 0 && current.commit_txn_id < reader_txn_id) {
        // If the latest visible version is a tombstone, return nullopt (key doesn't exist at snapshot)
        // Otherwise, return the record data.
        return current.deleted ? std::nullopt : std::optional<Record>(current);
    }

    // 2. If 'current' is not visible, check the historical versions in reverse order (newest first).
    for (auto it = versions.rbegin(); it != versions.rend(); ++it) {
         const auto& version = *it;
         // Check visibility condition for historical versions
         if (version.commit_txn_id != 0 && version.commit_txn_id < reader_txn_id) {
             // Found the most recent committed version visible to the reader.
             // Return nullopt if it's a tombstone, otherwise return the record.
             return version.deleted ? std::nullopt : std::optional<Record>(version);
         }
    }

    // 3. If no committed version (current or historical) is visible to the reader,
    // the key effectively does not exist at this snapshot time.
    return std::nullopt;
}

// Adds a new version (usually uncommitted initially) and prunes old versions
void VersionedRecord::addVersion(const Record& record, TxnId /* oldest_active_txn_id */) {
    // Acquire exclusive lock to modify the version chain
    std::unique_lock<std::shared_mutex> lock(mutex);

    // Check if the current 'current' record holds meaningful data (has a key or was committed)
    // or if there are already historical versions. This prevents pushing an initial default state.
    bool current_is_valid_or_history_exists = (!current.key.empty() || current.commit_txn_id != 0 || !versions.empty());

    // If the current version is valid or there's history, move the current 'current'
    // record into the historical versions vector.
    if (current_is_valid_or_history_exists) {
        versions.push_back(current); // Add the previous 'current' to history
        // Optional Debugging:
        // std::cout << "  [addVersion] Key '" << record.key << "' pushed old current (CommitTxn: "
        //           << current.commit_txn_id << ", Del: " << current.deleted << ") to history. History size: "
        //           << versions.size() << std::endl;
    }

    // Set the new record as the 'current' version.
    current = record;
    // Optional Debugging:
    // std::cout << "  [addVersion] Key '" << record.key << "' set new current (Txn: " << record.txn_id
    //           << ", CommitTxn: " << record.commit_txn_id << ", Del: " << record.deleted << ")" << std::endl;


    // --- Pruning Logic ---
    // TODO: Implement proper GC based on oldest_active_txn_id if needed.
    // The current simple fallback keeps a limited number of versions.

    const size_t MAX_VERSIONS_FALLBACK = 10; // Keep last N versions
    if (versions.size() > MAX_VERSIONS_FALLBACK) {
        // Remove the oldest historical version if the history exceeds the limit.
        // std::cout << "  [addVersion] Pruning oldest version for Key '" << record.key << "'" << std::endl; // Optional Debug
        versions.erase(versions.begin());
    }
}

// --- IndexField Constructor ---
IndexField::IndexField(std::string n, IndexSortOrder o)
    : name(std::move(n)), // Use std::move for efficiency if n is an rvalue
      order(o)
{}

// --- IndexDefinition Method Implementation ---
bool IndexDefinition::isValid() const {
    // An index definition is valid if it has a non-empty name,
    // at least one field defined, and a non-empty store path.
    return !name.empty() && !fields.empty() && !storePath.empty();
}