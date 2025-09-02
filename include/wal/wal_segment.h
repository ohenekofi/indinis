// @include/wal/wal_segment.h
#pragma once

// #include "types.h" // For LSN, LogRecord // KEEP THIS - LogRecord is from types.h
// Remove the internal LogRecord if it was here, types.h has the one we need.
#include "types.h" // Ensure LogRecord from types.h is included

#include <string>
#include <fstream>
#include <vector>
#include <mutex>
#include <cstdint> // For uint32_t

// Magic number for segment file validation
constexpr uint32_t WAL_SEGMENT_MAGIC = 0xDEADBEEF;
// Version for segment file format, if needed in the future
constexpr uint16_t WAL_SEGMENT_VERSION = 1;

#pragma pack(push, 1)
struct WALSegmentTrailer {
    uint32_t trailer_magic = 0xBEEFFEED; // Different magic for trailer
    uint32_t data_checksum; // Checksum of all LogRecord data (not headers) in this segment
    LSN      last_lsn_in_segment; // Explicitly store last LSN again for quick check
    uint64_t total_data_bytes_in_segment; // Store total LogRecord payload bytes
};
#pragma pack(pop)
static constexpr size_t WAL_SEGMENT_TRAILER_SIZE = sizeof(WALSegmentTrailer);


struct WALSegmentHeader {
    uint32_t magic_number = WAL_SEGMENT_MAGIC;
    uint16_t version = WAL_SEGMENT_VERSION;
    uint32_t segment_id = 0;
    LSN first_lsn_in_segment = 0;
    LSN last_lsn_in_segment_on_disk = 0; // LSN of last *valid complete* record known to be on disk
    uint64_t data_size_bytes = 0;        // Size of *valid complete* LogRecord data (excluding this header and trailer)
    uint32_t header_checksum = 0;        // *** NEW: Checksum for the header itself ***
};
static constexpr size_t WAL_SEGMENT_HEADER_SIZE = sizeof(WALSegmentHeader);


class WALSegment {
public:
    WALSegment(const std::string& file_path, uint32_t segment_id, LSN first_lsn_expected);
    ~WALSegment();

    bool open(bool create_new = false);
    void close();

    // Changed to use LogRecord from types.h
    bool appendRecord(const LogRecord& record); 

    bool flush();
    bool sync(); 

    LSN getFirstLSN() const { return header_.first_lsn_in_segment; }
    LSN getLastLSN() const; // Will read from header_ or scan if needed
    uint32_t getSegmentId() const { return header_.segment_id; }
    const std::string& getFilePath() const { return file_path_; }
    size_t getCurrentSize() const; // Current physical size of the file on disk
    bool isOpen() const;
    bool isHealthy() const { return is_healthy_; }

    // Changed to use LogRecord from types.h
    std::vector<LogRecord> readAllRecords() const;
    std::vector<LogRecord> readRecordsStrictlyAfter(LSN start_lsn_exclusive) const;

    WALSegment(const WALSegment&) = delete;
    WALSegment& operator=(const WALSegment&) = delete;
    WALSegmentHeader header_; // Store the header as a member
    bool writeHeader();
    

private:

    bool verifyHeaderAndScan(); // Renamed to also imply scanning for last LSN

    std::string file_path_;

    
    std::fstream file_stream_;
    mutable std::mutex file_mutex_; 
    bool is_healthy_;
    size_t current_physical_size_on_disk_; // Physical size of file
    LSN last_lsn_buffered_or_written_; // Tracks LSN of last record passed to appendRecord
                                       // might not be on disk yet.

    uint32_t calculateHeaderChecksum() const; // New private helper
    bool writeTrailer();                      // New private helper
    bool verifyTrailer();   
    bool verifyTrailerAndDataChecksum();
    uint32_t running_data_checksum_; 
};