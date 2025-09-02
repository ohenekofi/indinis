// @include/wal_manager.h
#pragma once

#include "types.h" // For LSN, TxnId, LogRecord, LogRecordType
#include <string>
#include <fstream>
#include <mutex>
#include <vector> // For deserializeAllRecords (testing)

class SimpleWALManager {
public:
    SimpleWALManager(const std::string& wal_directory, const std::string& wal_file_name = "wal.log");
    ~SimpleWALManager();

    // Appends a record. Does not guarantee immediate flush unless specified by policy (not yet).
    // Returns the LSN of the appended record, or 0 on failure.
    LSN appendLogRecord(LogRecord& record);

    // Appends a record AND ensures it is flushed to disk.
    // Critical for COMMIT_TXN records.
    // Returns the LSN of the appended record, or 0 on failure.
    LSN appendAndFlushLogRecord(LogRecord& record);

    // Forces a flush of any buffered log data to disk.
    void flushLog();

    // For testing/debugging: Reads all records from the current WAL file.
    std::vector<LogRecord> getAllRecordsForTesting() const;


private:
    std::string wal_file_path_;
    std::ofstream wal_file_stream_;
    mutable std::mutex log_mutex_; // Protects access to wal_file_stream_ and next_lsn_
    LSN next_lsn_;

    bool openLogFile();
    LSN recoverNextLSN(); // Helper to find next LSN on startup from existing log
};