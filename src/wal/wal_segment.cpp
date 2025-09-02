// @src/wal/wal_segment.cpp
#include "../include/wal/wal_segment.h"
#include "../include/debug_utils.h" // For LOG_ macros
#include <iostream> // For std::cerr in case of errors
#include <algorithm> // For std::max
#include <filesystem>
#include <vector> 

// Constructor remains the same
WALSegment::WALSegment(const std::string& file_path, uint32_t segment_id, LSN first_lsn_expected)
    : file_path_(file_path),
      is_healthy_(false),
      current_physical_size_on_disk_(0), // Will be updated in open()
      last_lsn_buffered_or_written_(0),
      running_data_checksum_(crc32(0L, Z_NULL, 0)) 
{
    header_.segment_id = segment_id;
    header_.first_lsn_in_segment = first_lsn_expected;
    header_.last_lsn_in_segment_on_disk = 0; // Initialized to 0, scan will update
    header_.data_size_bytes = 0;             // Initialized to 0, scan will update
    header_.header_checksum = 0;             // Will be calculated before writing

    if (first_lsn_expected > 0) {
        last_lsn_buffered_or_written_ = first_lsn_expected - 1;
    } else {
        // If first_lsn_expected is 0, it means this segment might be the very first one overall,
        // or its starting LSN isn't known yet by the caller.
        // It will be set by the first record written if data_size_bytes is 0 in appendRecord.
        last_lsn_buffered_or_written_ = 0;
    }
    LOG_TRACE("[WALSegment ID {}] Constructed for path '{}'. Expected first LSN in header: {}. Initial last_lsn_buffered: {}", 
        header_.segment_id, file_path_, header_.first_lsn_in_segment, last_lsn_buffered_or_written_);
}


// Destructor remains the same
WALSegment::~WALSegment() {
    if (file_stream_.is_open()) {
        LOG_WARN("[WALSegment ID {} Path {}] Destructor called on open segment. Closing.", header_.segment_id, file_path_);
        close(); // Ensure header is updated on close if data was written
    }
}

uint32_t WALSegment::calculateHeaderChecksum() const {
    // Checksum covers all fields *except* header_checksum itself
    WALSegmentHeader temp_header_for_checksum = header_;
    temp_header_for_checksum.header_checksum = 0; // Zero out checksum field for calculation
    return calculate_payload_checksum(reinterpret_cast<const uint8_t*>(&temp_header_for_checksum),
                                      WAL_SEGMENT_HEADER_SIZE);
}

// writeHeader remains the same
bool WALSegment::writeHeader() {
    // Assumes file_mutex_ is HELD by the caller
    if (!file_stream_.is_open()) {
        LOG_ERROR("[WALSegment ID {} Path {}] writeHeader: File stream not open.", header_.segment_id, file_path_);
        return false;
    }
    
    std::streampos original_pos = file_stream_.tellp(); // Get current position
    if(original_pos == std::streampos(-1)) { // tellp can fail
        LOG_ERROR("[WALSegment ID {} Path {}] writeHeader: Failed to get current stream position before writing header.", header_.segment_id, file_path_);
        file_stream_.clear(); // Clear error flags
        // Decide on a fallback or return false. Forcing seek to 0 is an option if original_pos invalid.
        original_pos = 0; // Assume we want to write at the start if tellp failed.
    }

    header_.header_checksum = calculateHeaderChecksum(); // Calculate and set checksum

    file_stream_.seekp(0, std::ios::beg); // Always write header at the beginning
    if (!file_stream_) {
        LOG_ERROR("[WALSegment ID {} Path {}] writeHeader: Failed to seekp(0) for writing header.", header_.segment_id, file_path_);
        is_healthy_ = false; // Mark unhealthy as we can't trust segment state
        // Attempt to restore original position if it was valid
        if (original_pos != std::streampos(-1) && original_pos != std::streampos(0) && file_stream_.good()) {
             file_stream_.seekp(original_pos);
        }
        return false;
    }

    file_stream_.write(reinterpret_cast<const char*>(&header_), WAL_SEGMENT_HEADER_SIZE);
    if (!file_stream_) {
        LOG_ERROR("[WALSegment ID {} Path {}] writeHeader: Failed to write segment header data. System error: {}", 
                  header_.segment_id, file_path_, strerror(errno));
        is_healthy_ = false;
        // Attempt to restore, stream might be bad
        if (original_pos != std::streampos(-1) && original_pos != std::streampos(0) && file_stream_.good()) {
             file_stream_.seekp(original_pos);
        }
        return false;
    }

    file_stream_.flush(); // Ensure header is flushed to OS buffers
    if (!file_stream_) {
        LOG_ERROR("[WALSegment ID {} Path {}] writeHeader: Failed to flush after writing header. System error: {}", 
                  header_.segment_id, file_path_, strerror(errno));
        is_healthy_ = false;
        // Attempt to restore
        if (original_pos != std::streampos(-1) && original_pos != std::streampos(0) && file_stream_.good()) {
             file_stream_.seekp(original_pos);
        }
        return false;
    }

    // After writing/flushing the header, decide where the put pointer should be.
    // If original_pos indicates we were past the header (e.g., updating after a sync), restore it.
    // Otherwise (initial write), it should be right after the header.
    if (original_pos != std::streampos(-1) && original_pos > static_cast<std::streamoff>(WAL_SEGMENT_HEADER_SIZE)) {
        file_stream_.seekp(original_pos);
    } else {
        file_stream_.seekp(WAL_SEGMENT_HEADER_SIZE, std::ios::beg);
    }
    if (!file_stream_) {
        LOG_ERROR("[WALSegment ID {} Path {}] writeHeader: Failed to seekp after writing header. Original pos: {}, Target was just after header or original_pos.",
                  header_.segment_id, file_path_, static_cast<long long>(original_pos));
        is_healthy_ = false;
        return false;
    }

    LOG_TRACE("[WALSegment ID {} Path {}] Header (checksum {:#010x}) written/flushed. FirstLSN: {}, LastLSNOnDisk: {}, DataSize: {}. Stream at: {}",
        header_.segment_id, file_path_, header_.header_checksum, header_.first_lsn_in_segment, 
        header_.last_lsn_in_segment_on_disk, header_.data_size_bytes, static_cast<long long>(file_stream_.tellp()));
    return true;
}

bool WALSegment::verifyHeaderAndScan() {
    // This method assumes the caller (WALSegment::open) holds the file_mutex_.
    LOG_TRACE("[WALSegment Scan ID {}] Starting header verification and record scan for '{}'.",
              header_.segment_id, file_path_);

    if (!file_stream_.is_open()) {
        LOG_ERROR("  [WALSegment Scan ID {}] Cannot verify/scan: file stream not open.", header_.segment_id);
        is_healthy_ = false;
        return false;
    }

    // --- 1. Read and Validate the On-Disk Header ---

    WALSegmentHeader header_from_disk;
    file_stream_.seekg(0, std::ios::beg);
    file_stream_.read(reinterpret_cast<char*>(&header_from_disk), WAL_SEGMENT_HEADER_SIZE);

    if (file_stream_.gcount() != static_cast<std::streamsize>(WAL_SEGMENT_HEADER_SIZE)) {
        LOG_ERROR("  [WALSegment Scan ID {}] Failed to read full header (read {} bytes). File is likely too small or corrupt.",
                  header_.segment_id, file_stream_.gcount());
        is_healthy_ = false;
        return false;
    }
    file_stream_.clear(); // Clear EOF if exactly header size was read.

    uint32_t stored_header_checksum = header_from_disk.header_checksum;
    
    // --- FIX: Correct call to calculateHeaderChecksum ---
    // We want to calculate the checksum of the header we just read from disk.
    // So, we temporarily assign it to our member variable to use the `const` method.
    // A more elegant solution might be a static helper, but this is correct and safe.
    WALSegmentHeader old_member_header = this->header_; // Save current member state
    this->header_ = header_from_disk;                   // Temporarily set member to disk version
    uint32_t calculated_header_checksum = calculateHeaderChecksum(); // Now this call is correct (no args)
    this->header_ = old_member_header;                  // Restore original member state
    // --- END FIX ---

    if (stored_header_checksum != calculated_header_checksum) {
        LOG_ERROR("  [WALSegment Scan ID {}] HEADER CHECKSUM MISMATCH! Stored: {:#010x}, Calculated: {:#010x}. Header corrupt. Segment is UNHEALTHY.",
                  header_.segment_id, stored_header_checksum, calculated_header_checksum);
        is_healthy_ = false;
        return false;
    }
    header_from_disk.header_checksum = stored_header_checksum; // Restore for adoption

    if (header_from_disk.magic_number != WAL_SEGMENT_MAGIC || header_from_disk.version != WAL_SEGMENT_VERSION) {
        LOG_ERROR("  [WALSegment Scan ID {}] Header magic/version mismatch. Magic: {:#x}, Version: {}. Segment is UNHEALTHY.",
                  header_.segment_id, header_from_disk.magic_number, header_from_disk.version);
        is_healthy_ = false;
        return false;
    }

    this->header_ = header_from_disk;
    last_lsn_buffered_or_written_ = this->header_.last_lsn_in_segment_on_disk;

    LOG_INFO("  [WALSegment Scan ID {}] Header verified. SegID: {}, FirstLSN: {}, LastLSNInHeader: {}, DataSizeInHeader: {}",
             header_.segment_id, header_.first_lsn_in_segment,
             header_.last_lsn_in_segment_on_disk, header_.data_size_bytes);

    // --- 2. Scan All Records ---

    file_stream_.seekg(WAL_SEGMENT_HEADER_SIZE, std::ios::beg);
    if (!file_stream_) {
         LOG_ERROR("  [WALSegment Scan ID {}] Failed to seek past header for record scan.", header_.segment_id);
         is_healthy_ = false; return false;
    }

    running_data_checksum_ = crc32(0L, Z_NULL, 0);
    LSN actual_scanned_max_lsn = (header_.first_lsn_in_segment > 0) ? header_.first_lsn_in_segment - 1 : 0;
    uint64_t actual_scanned_data_bytes = 0;
    std::streampos last_known_good_record_end_pos = WAL_SEGMENT_HEADER_SIZE;
    
    // --- FIX: Use LogRecord, not Record ---
    LogRecord log_record; 
    // --- END FIX ---

    LOG_TRACE("    Scanning records from offset {} to find true end of data...", static_cast<long long>(last_known_good_record_end_pos));
    
    while (file_stream_.good() && file_stream_.peek() != EOF) {
        std::streampos current_record_start_pos = file_stream_.tellg();
        
        // --- FIX: Use LogRecord::deserialize ---
        if (!log_record.deserialize(file_stream_)) {
            LOG_WARN("    [WALSegment Scan] Deserialize failed or hit end of valid records at offset {}. This is expected after a crash.",
                     static_cast<long long>(current_record_start_pos));
            file_stream_.clear();
            break;
        }
        
        std::streampos current_pos_after_read = file_stream_.tellg();
        size_t record_disk_size = static_cast<size_t>(current_pos_after_read - current_record_start_pos);
        
        // Update checksum with the raw bytes of the record we just read.
        std::vector<char> record_raw_bytes(record_disk_size);
        file_stream_.seekg(current_record_start_pos);
        file_stream_.read(record_raw_bytes.data(), record_disk_size);
        file_stream_.seekg(current_pos_after_read);
        
        uLong current_crc = static_cast<uLong>(running_data_checksum_);
        uLong new_crc = crc32(current_crc, 
                              reinterpret_cast<const Bytef*>(record_raw_bytes.data()),
                              static_cast<uInt>(record_raw_bytes.size()));
        running_data_checksum_ = static_cast<uint32_t>(new_crc);

        // --- FIX: Use LogRecord::lsn ---
        actual_scanned_max_lsn = std::max(actual_scanned_max_lsn, log_record.lsn);
        actual_scanned_data_bytes += record_disk_size;
        last_known_good_record_end_pos = current_pos_after_read;

        LOG_TRACE("      Scanned LSN {}, SizeOnDisk {}, Running CRC {:#010x}",
                  log_record.lsn, record_disk_size, running_data_checksum_);
        // --- END FIX ---
    }

    // --- 3. Finalize State and Verify Trailer (no changes needed here) ---
    // ... (rest of the function is unchanged and correct) ...

    bool header_was_updated = false;
    if (actual_scanned_data_bytes != header_.data_size_bytes) {
        LOG_WARN("    [WALSegment Scan] Data size mismatch. Header: {}, Scanned: {}. Updating header.",
                 header_.data_size_bytes, actual_scanned_data_bytes);
        header_.data_size_bytes = actual_scanned_data_bytes;
        header_was_updated = true;
    }
    if (actual_scanned_max_lsn != header_.last_lsn_in_segment_on_disk) {
        LOG_WARN("    [WALSegment Scan] Last LSN mismatch. Header: {}, Scanned: {}. Updating header.",
                 header_.last_lsn_in_segment_on_disk, actual_scanned_max_lsn);
        header_.last_lsn_in_segment_on_disk = actual_scanned_max_lsn;
        header_was_updated = true;
    }

    is_healthy_ = true;

    file_stream_.seekg(last_known_good_record_end_pos);
    if (!verifyTrailerAndDataChecksum()) {
        header_was_updated = true;
    }

    if (header_was_updated && is_healthy_) {
        LOG_INFO("    [WALSegment Scan] Header values were updated by scan. Writing corrected header to disk.");
        if (!writeHeader()) {
             LOG_ERROR("    [WALSegment Scan] CRITICAL: Failed to write updated header after scan. Marking segment UNHEALTHY.", header_.segment_id);
             is_healthy_ = false;
        }
    }

    file_stream_.seekp(last_known_good_record_end_pos);
    if (!file_stream_) {
        LOG_ERROR("    [WALSegment Scan] CRITICAL: Failed to seek to end of valid data (offset {}). Segment UNHEALTHY.",
                  static_cast<long long>(last_known_good_record_end_pos));
        is_healthy_ = false;
        return false;
    }
    
    last_lsn_buffered_or_written_ = header_.last_lsn_in_segment_on_disk;
    current_physical_size_on_disk_ = static_cast<size_t>(last_known_good_record_end_pos);

    LOG_INFO("[WALSegment Scan ID {}] Verification/scan finished. Healthy: {}. Effective Last LSN: {}. Next append offset: {}",
             header_.segment_id, is_healthy_, last_lsn_buffered_or_written_, static_cast<long long>(file_stream_.tellp()));
             
    return is_healthy_;
}

// open() method remains the same
bool WALSegment::open(bool create_new) {
    std::lock_guard<std::mutex> lock(file_mutex_); 

    if (file_stream_.is_open()) {
        LOG_WARN("[WALSegment ID {} Path {}] Open: Already open. Health: {}", 
                 header_.segment_id, file_path_, is_healthy_);
        return is_healthy_;
    }

    LOG_INFO("[WALSegment ID {} Path {}] Open: Attempting. create_new: {}", 
             header_.segment_id, file_path_, create_new);

    std::ios_base::openmode mode = std::ios::binary | std::ios::in | std::ios::out;
    bool file_existed_before_open = std::filesystem::exists(file_path_);

    if (create_new) {
        mode |= std::ios::trunc; 
        LOG_INFO("[WALSegment ID {} Path {}] Open: Using create_new mode (truncates if exists).", header_.segment_id, file_path_);
    } else {
        if (!file_existed_before_open) {
            LOG_INFO("[WALSegment ID {} Path {}] Open: File DNE and create_new=false. Cannot open.", header_.segment_id, file_path_);
            is_healthy_ = false;
            return false; 
        }
        LOG_INFO("[WALSegment ID {} Path {}] Open: Opening existing file.", header_.segment_id, file_path_);
    }

    file_stream_.open(file_path_, mode);
    
    if (!file_stream_.is_open()) {
        LOG_ERROR("[WALSegment ID {} Path {}] Open: Failed to open fstream. System error: {}", 
                  header_.segment_id, file_path_, strerror(errno));
        is_healthy_ = false;
        return false;
    }

    if (create_new || !file_existed_before_open) {
        // New file or truncated existing one.
        header_.last_lsn_in_segment_on_disk = 0; 
        header_.data_size_bytes = 0;             
        // header_.first_lsn_in_segment is already set from constructor.
        
        LOG_INFO("[WALSegment ID {} Path {}] Open (new/trunc): Initializing header. FirstLSN in header: {}.", 
                 header_.segment_id, file_path_, header_.first_lsn_in_segment);

        if (!writeHeader()) { // writeHeader calculates checksum, writes, flushes header, and seeks to end of header
            LOG_ERROR("[WALSegment ID {} Path {}] Open (new/trunc): Failed to write initial header.", header_.segment_id, file_path_);
            is_healthy_ = false;
            file_stream_.close(); 
            return false;
        }
        // After writeHeader, fstream's put pointer should be at WAL_SEGMENT_HEADER_SIZE.
        // Let's verify and set current_physical_size_on_disk_ accurately.
        file_stream_.seekp(0, std::ios::end); // Go to actual physical end of file
        if (!file_stream_) {
            LOG_ERROR("[WALSegment ID {} Path {}] Open (new/trunc): Failed to seek to EOF after header write to determine size.", header_.segment_id, file_path_);
            is_healthy_ = false; file_stream_.close(); return false;
        }
        current_physical_size_on_disk_ = static_cast<size_t>(file_stream_.tellp());
        
        if (current_physical_size_on_disk_ < WAL_SEGMENT_HEADER_SIZE) {
            LOG_ERROR("[WALSegment ID {} Path {}] Open (new/trunc): CRITICAL - Physical file size ({}) is less than header size ({}) after writing header. File system issue or truncation problem.", 
                      header_.segment_id, file_path_, current_physical_size_on_disk_, WAL_SEGMENT_HEADER_SIZE);
            // This is a severe issue. Attempt to pad or error out.
            // For now, error out. A more robust system might try to write zeros up to header size.
            is_healthy_ = false; file_stream_.close(); return false;
        }
        
        // Position for the first data record explicitly *after* the header.
        // This is where the *next* write (first LogRecord) should occur.
        file_stream_.seekp(WAL_SEGMENT_HEADER_SIZE, std::ios::beg);
        if (!file_stream_) {
            LOG_ERROR("[WALSegment ID {} Path {}] Open (new/trunc): Failed to seekp to start of data area (offset {}).", 
                      header_.segment_id, file_path_, WAL_SEGMENT_HEADER_SIZE);
            is_healthy_ = false; file_stream_.close(); return false;
        }
        // current_physical_size_on_disk_ still reflects total file size.
        // header_.data_size_bytes is 0, so expected append pos is WAL_SEGMENT_HEADER_SIZE.

        // last_lsn_buffered_or_written_ initialization (from constructor) is okay.
        is_healthy_ = true;
        LOG_INFO("[WALSegment ID {} Path {}] Open (new/trunc): Initialized. Healthy. Physical Size: {}, Expected Append Pos: {}, LastBufferedLSN: {}",
                 header_.segment_id, file_path_, current_physical_size_on_disk_, 
                 static_cast<long long>(file_stream_.tellp()), last_lsn_buffered_or_written_);

    } else { // Opening an existing file that was not truncated
        LOG_INFO("[WALSegment ID {} Path {}] Open (existing): Verifying header and scanning records...", 
                 header_.segment_id, file_path_);
        if (!verifyHeaderAndScan()) { 
            // is_healthy_ is set by verifyHeaderAndScan
            if(file_stream_.is_open()) file_stream_.close(); 
            LOG_ERROR("[WALSegment ID {} Path {}] Open (existing): Verification/scan failed. Segment unhealthy.", header_.segment_id, file_path_);
        } else {
            LOG_INFO("[WALSegment ID {} Path {}] Open (existing): Verified/scanned. Healthy: {}. Effective Last LSN: {}. Physical Size: {}. Append Pos: {}",
                     header_.segment_id, file_path_, is_healthy_, last_lsn_buffered_or_written_, 
                     current_physical_size_on_disk_, static_cast<long long>(file_stream_.tellp()));
        }
    }
    
    if (!is_healthy_ && file_stream_.is_open()) {
        file_stream_.close();
    } else if (is_healthy_ && !file_stream_.is_open()) {
        LOG_ERROR("[WALSegment ID {} Path {}] Open: CRITICAL - Healthy but stream not open post-logic!", header_.segment_id, file_path_);
        is_healthy_ = false; 
    }
    
    return is_healthy_;
}
bool WALSegment::writeTrailer() {
    // Assumes file_mutex_ is held, stream is open and positioned at end of data
    if (!is_healthy_ || !file_stream_.is_open()) {
        LOG_ERROR("[WALSegment ID {} Path {}] Cannot write trailer, segment unhealthy or not open.", header_.segment_id, file_path_);
        return false;
    }

    std::streampos expected_trailer_write_pos = static_cast<std::streampos>(WAL_SEGMENT_HEADER_SIZE + header_.data_size_bytes);
    file_stream_.seekp(expected_trailer_write_pos); // Ensure we are at the correct position
    if (!file_stream_ || file_stream_.tellp() != expected_trailer_write_pos) {
        LOG_ERROR("[WALSegment ID {} Path {}] writeTrailer: Stream not at expected end of data or seek failed. Current: {}, Expected: {}",
                  header_.segment_id, file_path_, static_cast<long long>(file_stream_.tellp()),
                  static_cast<long long>(expected_trailer_write_pos));
        return false;
    }

    WALSegmentTrailer trailer; // Magic is set by default constructor
    trailer.last_lsn_in_segment = header_.last_lsn_in_segment_on_disk;
    trailer.total_data_bytes_in_segment = header_.data_size_bytes;

    // Robust data_checksum calculation:
    // Re-read the entire data section and checksum it. This is safest but can be slow.
    // A running checksum updated during appendRecord would be more performant.
    // For now, a simplified checksum of key metadata as a placeholder.
    // ** THIS IS A WEAK CHECKSUM AND SHOULD BE REPLACED IN PRODUCTION **
    std::vector<uint8_t> data_to_checksum_trailer;
    data_to_checksum_trailer.reserve(sizeof(LSN) + sizeof(uint64_t));
    const char* ptr = reinterpret_cast<const char*>(&trailer.last_lsn_in_segment);
    data_to_checksum_trailer.insert(data_to_checksum_trailer.end(), ptr, ptr + sizeof(LSN));
    ptr = reinterpret_cast<const char*>(&trailer.total_data_bytes_in_segment);
    data_to_checksum_trailer.insert(data_to_checksum_trailer.end(), ptr, ptr + sizeof(uint64_t));
    // trailer.data_checksum = calculate_payload_checksum(data_to_checksum_trailer.data(), data_to_checksum_trailer.size());
    trailer.data_checksum = running_data_checksum_;
    LOG_WARN("[WALSegment ID {} Path {}] Using  checksum for trailer.data_checksum.", header_.segment_id, file_path_);


    file_stream_.write(reinterpret_cast<const char*>(&trailer), WAL_SEGMENT_TRAILER_SIZE);
    if (!file_stream_) {
        LOG_ERROR("[WALSegment ID {} Path {}] Failed to write segment trailer.", header_.segment_id, file_path_);
        // Attempt to truncate the partially written trailer to avoid confusion on next open.
        // This is tricky and platform-dependent if fstream doesn't expose ftruncate easily.
        // For now, just log. The stream is in a bad state.
        return false;
    }
    // file_stream_.flush(); // Handled by sync() or final close() in ExtWALManager usually.
    current_physical_size_on_disk_ += WAL_SEGMENT_TRAILER_SIZE;
    LOG_INFO("[WALSegment ID {} Path {}] Segment trailer written. LastLSN: {}, TotalDataBytes: {}, TrailerChecksum(Weak): {:#010x}",
              header_.segment_id, file_path_, trailer.last_lsn_in_segment, trailer.total_data_bytes_in_segment, trailer.data_checksum);
    return true;
}

bool WALSegment::verifyTrailerAndDataChecksum() {
    // This method assumes the caller (verifyHeaderAndScan) holds the file_mutex_ and has
    // positioned the stream at the expected start of the trailer.

    if (!is_healthy_ || !file_stream_.is_open()) {
        LOG_TRACE("[WALSegment ID {}] Skipping trailer verification: segment is not healthy or open.", header_.segment_id);
        return false;
    }

    // --- 1. Read the Trailer Struct from Disk ---

    WALSegmentTrailer trailer_from_file;
    std::streampos trailer_read_start_pos = file_stream_.tellg();

    file_stream_.read(reinterpret_cast<char*>(&trailer_from_file), WAL_SEGMENT_TRAILER_SIZE);

    if (file_stream_.gcount() != static_cast<std::streamsize>(WAL_SEGMENT_TRAILER_SIZE)) {
        // This is not necessarily an error; it's the expected state for a file
        // that crashed before the trailer was written.
        LOG_INFO("  [WALSegment Scan ID {}] Trailer not found or incomplete at offset {}. Read {} bytes. Segment was not closed cleanly.",
                 header_.segment_id, static_cast<long long>(trailer_read_start_pos), file_stream_.gcount());
        
        file_stream_.clear(); // Clear EOF/fail flags
        // We must seek back to the position before our failed read attempt.
        file_stream_.seekg(trailer_read_start_pos);
        return false;
    }
    file_stream_.clear(); // Clear EOF if we read exactly to the end of the file

    // --- 2. Perform Verifications ---

    // Check 2a: Validate the magic number to detect corruption within the trailer itself.
    if (trailer_from_file.trailer_magic != 0xBEEFFEED) {
        LOG_WARN("  [WALSegment Scan ID {}] Trailer magic number mismatch. Read: {:#x}, Expected: {:#x}. Trailer is corrupt.",
                 header_.segment_id, trailer_from_file.trailer_magic, 0xBEEFFEED);
        return false;
    }
    LOG_TRACE("    [WALSegment Scan] Trailer magic number verified.");

    // Check 2b: CRITICAL DATA INTEGRITY CHECK. Compare the checksum from the trailer
    // against the checksum calculated by scanning all records in the file.
    if (trailer_from_file.data_checksum != running_data_checksum_) {
        LOG_ERROR("  [WALSegment Scan ID {}] CRITICAL: DATA CHECKSUM MISMATCH! Trailer checksum: {:#010x}, Calculated from scanned records: {:#010x}. Segment data is CORRUPT.",
                 header_.segment_id, trailer_from_file.data_checksum, running_data_checksum_);
        // Even if the trailer is structurally valid, a data checksum mismatch means the
        // content of the WAL records has been altered since it was written.
        is_healthy_ = false; // Mark the entire segment as unhealthy.
        return false;
    }
    LOG_INFO("  [WALSegment Scan ID {}] Data checksum VERIFIED ({:#010x}). Data integrity is intact.", 
             this->header_.segment_id, trailer_from_file.data_checksum);

    // Check 2c: Consistency checks between trailer and (scan-updated) header.
    // These are sanity checks; a mismatch is a warning, not a fatal error, as the
    // recovery scan is the ultimate source of truth.
    if (trailer_from_file.last_lsn_in_segment != header_.last_lsn_in_segment_on_disk) {
         LOG_WARN("  [WALSegment Scan ID {}] Trailer/Header LSN mismatch. Trailer: {}, Header (from scan): {}. Using scanned value.",
                  header_.segment_id, trailer_from_file.last_lsn_in_segment, header_.last_lsn_in_segment_on_disk);
    }
     if (trailer_from_file.total_data_bytes_in_segment != header_.data_size_bytes) {
         LOG_WARN("  [WALSegment Scan ID {}] Trailer/Header data size mismatch. Trailer: {}, Header (from scan): {}. Using scanned value.",
                  header_.segment_id, trailer_from_file.total_data_bytes_in_segment, header_.data_size_bytes);
    }

    // If we passed all checks, the segment was closed cleanly and the data is valid.
    return true;
}

bool WALSegment::verifyTrailer() {
    // Assumes file_mutex_ is held, stream is open, and positioned at the start of where trailer should be.
    if (!is_healthy_ || !file_stream_.is_open()) return false;

    WALSegmentTrailer trailer_from_file;
    std::streampos trailer_read_start_pos = file_stream_.tellg();
    file_stream_.read(reinterpret_cast<char*>(&trailer_from_file), WAL_SEGMENT_TRAILER_SIZE);

    if (file_stream_.gcount() != static_cast<std::streamsize>(WAL_SEGMENT_TRAILER_SIZE)) {
        LOG_WARN("[WALSegment ID {} Path {}] verifyTrailer: Failed to read full trailer (read {} bytes from pos {}). EOF: {}",
                 header_.segment_id, file_path_, file_stream_.gcount(), static_cast<long long>(trailer_read_start_pos), file_stream_.eof());
        file_stream_.clear(); // Clear stream errors
        file_stream_.seekg(trailer_read_start_pos); // Reset position
        return false;
    }
    file_stream_.clear(); // Clear EOF if exact read

    if (trailer_from_file.trailer_magic != 0xBEEFFEED) { // Use actual magic value
        LOG_WARN("[WALSegment ID {} Path {}] verifyTrailer: Trailer magic mismatch. Read: {:#x}, Expected: {:#x}",
                 header_.segment_id, file_path_, trailer_from_file.trailer_magic, 0xBEEFFEED);
        return false;
    }

    // Verify the (weak) data_checksum
    std::vector<uint8_t> data_to_checksum_trailer_verify;
    data_to_checksum_trailer_verify.reserve(sizeof(LSN) + sizeof(uint64_t));
    const char* ptr = reinterpret_cast<const char*>(&trailer_from_file.last_lsn_in_segment);
    data_to_checksum_trailer_verify.insert(data_to_checksum_trailer_verify.end(), ptr, ptr + sizeof(LSN));
    ptr = reinterpret_cast<const char*>(&trailer_from_file.total_data_bytes_in_segment);
    data_to_checksum_trailer_verify.insert(data_to_checksum_trailer_verify.end(), ptr, ptr + sizeof(uint64_t));
    uint32_t calculated_data_checksum = calculate_payload_checksum(data_to_checksum_trailer_verify.data(), data_to_checksum_trailer_verify.size());

    if (trailer_from_file.data_checksum != calculated_data_checksum) {
        LOG_WARN("[WALSegment ID {} Path {}] verifyTrailer: Trailer data_checksum (WEAK) mismatch. Stored: {:#010x}, Calculated: {:#010x}",
                 header_.segment_id, file_path_, trailer_from_file.data_checksum, calculated_data_checksum);
        // return false; // For a weak checksum, mismatch might not be fatal if other fields align.
    }
    if (trailer_from_file.last_lsn_in_segment != header_.last_lsn_in_segment_on_disk) {
         LOG_WARN("[WALSegment ID {} Path {}] verifyTrailer: Trailer last_lsn ({}) mismatch with current header's last_lsn ({}). Header might have been updated by scan.",
                  header_.segment_id, file_path_, trailer_from_file.last_lsn_in_segment, header_.last_lsn_in_segment_on_disk);
        // This is acceptable if the scan corrected the header.
    }
     if (trailer_from_file.total_data_bytes_in_segment != header_.data_size_bytes) {
         LOG_WARN("[WALSegment ID {} Path {}] verifyTrailer: Trailer total_data_bytes ({}) mismatch with current header's data_size ({}). Header might have been updated by scan.",
                  header_.segment_id, file_path_, trailer_from_file.total_data_bytes_in_segment, header_.data_size_bytes);
        // This is acceptable.
    }
    LOG_TRACE("[WALSegment ID {} Path {}] Trailer seems structurally valid (Magic OK). LSN in trailer: {}, DataSize in trailer: {}",
              header_.segment_id, file_path_, trailer_from_file.last_lsn_in_segment, trailer_from_file.total_data_bytes_in_segment);
    return true;
}

// close() method remains the same
void WALSegment::close() {
    std::lock_guard<std::mutex> lock(file_mutex_);
    if (file_stream_.is_open()) {
        LOG_INFO("[WALSegment ID {} Path {}] Closing segment. Current last LSN written/buffered: {}. Data size in header: {}",
                 header_.segment_id, file_path_, last_lsn_buffered_or_written_, header_.data_size_bytes);

        if (is_healthy_) {
            // Update header with the most current information known before closing
            if (last_lsn_buffered_or_written_ >= header_.first_lsn_in_segment ||
                (header_.first_lsn_in_segment == 0 && last_lsn_buffered_or_written_ == 0) ) {
                 // Only update if last_lsn_buffered is valid for this segment
                 header_.last_lsn_in_segment_on_disk = last_lsn_buffered_or_written_;
            }
            // header_.data_size_bytes should have been updated by appendRecord calls.

            LOG_TRACE("  Finalizing header before close. LastLSNOnDisk: {}, DataSize: {}",
                      header_.last_lsn_in_segment_on_disk, header_.data_size_bytes);
            if (!writeHeader()) { // writeHeader flushes its own write
                 LOG_ERROR("  [WALSegment Close ID {}] Failed to write final header on close. Data on disk may not reflect header.", header_.segment_id);
            }

            // Attempt to write trailer for clean shutdown indication
            if (!writeTrailer()) {
                 LOG_WARN("  [WALSegment Close ID {}] Failed to write segment trailer. Segment may appear as crashed on next open.", header_.segment_id);
            }
        } else {
            LOG_WARN("  [WALSegment Close ID {}] Segment marked unhealthy. Skipping final header/trailer update.", header_.segment_id);
        }

        try {
            file_stream_.flush(); // Ensure all OS buffers are flushed.
            if (!file_stream_) {
                LOG_ERROR("  [WALSegment Close ID {}] Stream error during final flush for {}. State: {}", header_.segment_id, file_path_, file_stream_.rdstate());
            }
        } catch (const std::ios_base::failure& e) {
            LOG_ERROR("  [WALSegment Close ID {}] Exception during final flush for {}: {}", header_.segment_id, file_path_, e.what());
        }

        file_stream_.close();
        if (file_stream_.fail()) { // Check failbit *after* close
             LOG_ERROR("  [WALSegment Close ID {}] Stream error reported AFTER closing file {}. Path: {}", header_.segment_id, file_path_, file_stream_.rdstate());
        }
        LOG_INFO("[WALSegment ID {} Path {}] Segment file stream closed.", header_.segment_id, file_path_);
    } else {
        LOG_TRACE("[WALSegment ID {} Path {}] Close called, but file stream was not open.", header_.segment_id, file_path_);
    }
    is_healthy_ = false; // Mark as not actively usable once closed.
}

bool WALSegment::appendRecord(const LogRecord& record) {
    // A single mutex lock protects the entire operation, ensuring thread safety
    // for all file I/O and updates to member variables.
    std::lock_guard<std::mutex> lock(file_mutex_);

    // --- 1. Pre-computation and Validation ---

    // Immediately fail if the segment is not in a healthy, open state.
    if (!is_healthy_ || !file_stream_.is_open()) {
        LOG_ERROR("[WALSegment ID {}] Append failed for LSN {}: Segment is unhealthy or not open.",
                  header_.segment_id, record.lsn);
        return false;
    }

    // LSN 0 is invalid and indicates a likely programming error.
    if (record.lsn == 0) {
        LOG_ERROR("[WALSegment ID {}] Append failed: Attempted to write a record with an invalid LSN of 0.", header_.segment_id);
        return false;
    }
    
    // Enforce strict sequential LSN order within the segment.
    if (record.lsn <= last_lsn_buffered_or_written_) {
        LOG_ERROR("[WALSegment ID {}] Append failed for LSN {}: LSN is not greater than the last written/buffered LSN {}.",
                  header_.segment_id, record.lsn, last_lsn_buffered_or_written_);
        return false; 
    }

    // --- 2. Handle First Record Logic ---

    // If this is the very first record in a newly created segment, its LSN
    // defines the starting point for this segment file.
    if (header_.data_size_bytes == 0) {
        // If the header's expected first LSN was 0, adopt this record's LSN.
        if (header_.first_lsn_in_segment == 0) {
            header_.first_lsn_in_segment = record.lsn;
            // The last LSN is now the one just before this first record.
            last_lsn_buffered_or_written_ = record.lsn - 1; 
            LOG_INFO("[WALSegment ID {}] Append: First record. Setting header.first_lsn_in_segment to {}.",
                     header_.segment_id, record.lsn);
            
            // Persist the updated header immediately.
            if (!writeHeader()) {
                LOG_ERROR("[WALSegment ID {}] Append failed: Could not write updated header for first LSN {}.", header_.segment_id, record.lsn);
                is_healthy_ = false;
                return false;
            }
        } 
        // If the header already had an expected first LSN, validate it.
        else if (record.lsn != header_.first_lsn_in_segment) {
            LOG_ERROR("[WALSegment ID {}] Append failed: First record LSN {} MISMATCH with expected first LSN {}.",
                      header_.segment_id, record.lsn, header_.first_lsn_in_segment);
            is_healthy_ = false; 
            return false;
        }
    }

    // --- 3. Serialize to Buffer for Checksum and Write ---
    
    std::string serialized_record_data;
    try {
        // Serialize the record into a temporary in-memory buffer first.
        // This is crucial for calculating the checksum on the *exact* bytes
        // that will be written to disk.
        std::ostringstream temp_record_stream(std::ios::binary);
        record.serialize(temp_record_stream);
        serialized_record_data = temp_record_stream.str();
    } catch (const std::exception& e) {
        LOG_ERROR("[WALSegment ID {}] Append failed for LSN {}: Serialization error: {}",
                  header_.segment_id, record.lsn, e.what());
        return false;
    }
    
    if (serialized_record_data.empty()) {
        LOG_WARN("[WALSegment ID {}] Serialization of LSN {} resulted in empty data. Skipping write.",
                 header_.segment_id, record.lsn);
        return true; // Not a failure, but nothing to do.
    }

    // --- 4. File I/O ---

    // Ensure the stream's write pointer is at the correct position (end of valid data).
    // This protects against corruption from previous failed writes.
    std::streampos expected_write_pos = static_cast<std::streampos>(WAL_SEGMENT_HEADER_SIZE + header_.data_size_bytes);
    if (file_stream_.tellp() != expected_write_pos) {
        LOG_WARN("[WALSegment ID {}] Stream position mismatch before append. Current: {}, Expected: {}. Reseeking.",
                  header_.segment_id, static_cast<long long>(file_stream_.tellp()), static_cast<long long>(expected_write_pos));
        file_stream_.seekp(expected_write_pos);
        if (!file_stream_) {
            LOG_ERROR("[WALSegment ID {}] Append failed for LSN {}: Could not seek to correct write position {}.",
                      header_.segment_id, record.lsn, static_cast<long long>(expected_write_pos));
            is_healthy_ = false;
            return false;
        }
    }
    
    // Write the pre-serialized data to the file.
    file_stream_.write(serialized_record_data.data(), serialized_record_data.length());
    
    if (!file_stream_) {
        LOG_ERROR("[WALSegment ID {}] Append failed for LSN {}: Stream error after writing record data.",
                  header_.segment_id, record.lsn);
        is_healthy_ = false;
        file_stream_.clear(); // Attempt to clear error flags
        return false;
    }
    
    // --- 5. Update State and Checksum (only on successful write) ---

    // Update the running CRC32 checksum with the bytes just written.
    uLong current_crc = static_cast<uLong>(running_data_checksum_);
    uLong new_crc = crc32(current_crc, 
                          reinterpret_cast<const Bytef*>(serialized_record_data.data()),
                          static_cast<uInt>(serialized_record_data.length()));
    running_data_checksum_ = static_cast<uint32_t>(new_crc);

    // Update metadata
    last_lsn_buffered_or_written_ = record.lsn;
    uint32_t record_disk_size = static_cast<uint32_t>(serialized_record_data.length());
    header_.data_size_bytes += record_disk_size;
    current_physical_size_on_disk_ += record_disk_size;

    LOG_TRACE("[WALSegment ID {}] Appended LSN {}. DiskSize: {}, New TotalData: {}, Running CRC32: {:#010x}",
              header_.segment_id, record.lsn, record_disk_size, header_.data_size_bytes, running_data_checksum_);
              
    return true;
}

// flush() method remains the same
bool WALSegment::flush() {
    std::lock_guard<std::mutex> lock(file_mutex_);
    if (!is_healthy_ || !file_stream_.is_open()) return false;
    file_stream_.flush();
    if (!file_stream_) {
        LOG_ERROR("[WALSegment ID {} Path {}] Stream error during flush.", header_.segment_id, file_path_);
        is_healthy_ = false;
        return false;
    }
    return true;
}

// sync() method: update header after successful sync
bool WALSegment::sync() {
    std::lock_guard<std::mutex> lock(file_mutex_);
    if (!is_healthy_ || !file_stream_.is_open()) {
         LOG_WARN("[WALSegment ID {} Path {}] Sync called but segment not healthy or open.", header_.segment_id, file_path_);
        return false;
    }
    
    file_stream_.flush();
    if (!file_stream_) {
        LOG_ERROR("[WALSegment ID {} Path {}] Stream error during pre-sync flush.", header_.segment_id, file_path_);
        is_healthy_ = false;
        return false;
    }

    // Platform-specific fsync would go here.
    LOG_TRACE("[WALSegment ID {} Path {}] Sync called (OS fsync would be here). Current data size: {}, Last LSN written/buffered: {}", 
              header_.segment_id, file_path_, header_.data_size_bytes, last_lsn_buffered_or_written_);
    
    // After a successful sync, update the header on disk with the latest LSN and data size
    // for records *actually persisted* in this segment.
    if (last_lsn_buffered_or_written_ > header_.last_lsn_in_segment_on_disk || 
        (header_.data_size_bytes > 0 && header_.last_lsn_in_segment_on_disk == 0 && last_lsn_buffered_or_written_ >= header_.first_lsn_in_segment) ) { 
        // Update if new LSNs written OR if it's the first sync of a non-empty segment
        
        header_.last_lsn_in_segment_on_disk = last_lsn_buffered_or_written_;
        // header_.data_size_bytes is already updated by appendRecord.
        
        std::streampos current_append_pos = file_stream_.tellp(); // Save current append position
        
        if (!writeHeader()) { 
            LOG_ERROR("[WALSegment ID {} Path {}] Failed to update header on disk after sync. LastLSNOnDisk was to be set to {}.", 
                      header_.segment_id, file_path_, header_.last_lsn_in_segment_on_disk);
            is_healthy_ = false;
            // Attempt to restore append position
            if(current_append_pos != -1) file_stream_.seekp(current_append_pos);
            return false;
        }
        // Restore seek position for appending
        file_stream_.seekp(current_append_pos);
         if (!file_stream_) {
            LOG_ERROR("[WALSegment ID {} Path {}] Failed to restore seek position after header update during sync.", header_.segment_id, file_path_);
            is_healthy_ = false;
            return false;
        }
        LOG_INFO("[WALSegment ID {} Path {}] Header updated on disk after sync. LastLSNOnDisk: {}, DataSize: {}",
                 header_.segment_id, file_path_, header_.last_lsn_in_segment_on_disk, header_.data_size_bytes);
    }
    return true;
}


// getLastLSN() method remains the same
LSN WALSegment::getLastLSN() const {
    std::lock_guard<std::mutex> lock(file_mutex_); 
    return header_.last_lsn_in_segment_on_disk;
}

// getCurrentSize() method remains the same
size_t WALSegment::getCurrentSize() const {
    std::lock_guard<std::mutex> lock(file_mutex_);
    if (file_stream_.is_open()) {
        return current_physical_size_on_disk_;
    }
    return 0; 
}

// isOpen() method remains the same
bool WALSegment::isOpen() const {
    std::lock_guard<std::mutex> lock(file_mutex_);
    return file_stream_.is_open();
}

// readAllRecords() method remains the same (using LogRecord::deserialize and getDiskSize)
std::vector<LogRecord> WALSegment::readAllRecords() const {
    std::lock_guard<std::mutex> lock(file_mutex_); // Ensure consistent read if segment is being modified
    std::vector<LogRecord> records;
    // It's safer to read from a closed segment or one explicitly opened for read-only.
    // Here, we assume this is for recovery/testing and main stream might be open for append.
    // So, we open a new stream for reading.
    if (!is_healthy_ && header_.data_size_bytes == 0) { // If not healthy and no data, likely unusable
         LOG_WARN("[WALSegment ID {} Path {}] Cannot readAllRecords, segment marked not healthy and no data size reported.", header_.segment_id, file_path_);
        return records;
    }


    std::ifstream reader_stream(file_path_, std::ios::binary);
    if (!reader_stream.is_open()) {
        LOG_ERROR("[WALSegment ID {} Path {}] Failed to open temporary reader stream for readAllRecords.", header_.segment_id, file_path_);
        return records;
    }

    reader_stream.seekg(WAL_SEGMENT_HEADER_SIZE);
    if (!reader_stream) {
        LOG_ERROR("[WALSegment ID {} Path {}] Failed to seek past header in temporary reader.", header_.segment_id, file_path_);
        return records;
    }

    LogRecord temp_record;
    uint64_t bytes_read_from_data_section = 0;
    const uint64_t expected_data_bytes = header_.data_size_bytes; // Read from header

    LOG_TRACE("[WALSegment ID {} Path {}] Reading all records. Header reports data_size_bytes: {}", header_.segment_id, file_path_, expected_data_bytes);

    while (reader_stream.good() && reader_stream.peek() != EOF) {
        if (expected_data_bytes > 0 && bytes_read_from_data_section >= expected_data_bytes) {
            LOG_TRACE("[WALSegment ID {} Path {}] Reached reported data_size_bytes ({}). Stopping read.", header_.segment_id, file_path_, expected_data_bytes);
            break;
        }
        std::streampos record_start_pos = reader_stream.tellg();
        // *** USE CORRECTED SIZE METHOD ***
        uint32_t record_disk_size_before_deserialize = 0; // We don't know size before deserialize
                                                      // but deserialize tells us if it worked.

        if (!temp_record.deserialize(reader_stream)) { 
            if (reader_stream.eof() && bytes_read_from_data_section < expected_data_bytes && expected_data_bytes > 0) {
                 LOG_WARN("[WALSegment ID {} Path {}] readAllRecords: EOF reached prematurely. Expected {} data bytes, read {}. Last LSN: {}", 
                          header_.segment_id, file_path_, expected_data_bytes, bytes_read_from_data_section, (records.empty() ? 0 : records.back().lsn) );
            } else if (!reader_stream.eof()){ // deserialize failed not due to clean EOF
                 LOG_WARN("[WALSegment ID {} Path {}] readAllRecords: Failed to deserialize record at offset {}. Stopping read. Last LSN {}.", 
                          header_.segment_id, file_path_, static_cast<long long>(record_start_pos), (records.empty() ? 0 : records.back().lsn) );
            }
            break;
        }
        records.push_back(temp_record);
        record_disk_size_before_deserialize = temp_record.getDiskSize(); // Get size of successfully deserialized record
        bytes_read_from_data_section += record_disk_size_before_deserialize;
    }
    LOG_INFO("[WALSegment ID {} Path {}] readAllRecords read {} records. Total bytes processed from data section: {}", 
              header_.segment_id, file_path_, records.size(), bytes_read_from_data_section);
    if (expected_data_bytes > 0 && bytes_read_from_data_section != expected_data_bytes) {
        LOG_WARN("[WALSegment ID {} Path {}] Discrepancy: header_.data_size_bytes ({}) != scanned_data_bytes ({}). Check for corruption or incomplete final record.",
                 header_.segment_id, file_path_, expected_data_bytes, bytes_read_from_data_section);
    }
    return records;
}

// readRecordsStrictlyAfter() method remains the same (using LogRecord::deserialize and getDiskSize)
std::vector<LogRecord> WALSegment::readRecordsStrictlyAfter(LSN start_lsn_exclusive) const {
    std::lock_guard<std::mutex> lock(file_mutex_);
    std::vector<LogRecord> records;
    // If segment's max known LSN is not even greater than start_lsn_exclusive, no need to scan
    if (!is_healthy_ || !file_stream_.is_open() || header_.last_lsn_in_segment_on_disk <= start_lsn_exclusive) {
        return records;
    }

    std::ifstream reader_stream(file_path_, std::ios::binary);
    if (!reader_stream.is_open()) return records;

    reader_stream.seekg(WAL_SEGMENT_HEADER_SIZE);
    if (!reader_stream) return records;

    LogRecord temp_record;
    uint64_t bytes_read_from_data_section = 0;
    const uint64_t expected_data_bytes = header_.data_size_bytes;

    while (reader_stream.good() && reader_stream.peek() != EOF) {
         if (expected_data_bytes > 0 && bytes_read_from_data_section >= expected_data_bytes) break;
        // *** USE CORRECTED SIZE METHOD ***
        if (!temp_record.deserialize(reader_stream)) break;
        
        bytes_read_from_data_section += temp_record.getDiskSize();
        if (temp_record.lsn > start_lsn_exclusive) {
            records.push_back(temp_record);
        }
        // Optimization: if we've read up to the last known LSN from header, no need to read further.
        if (temp_record.lsn >= header_.last_lsn_in_segment_on_disk && header_.last_lsn_in_segment_on_disk != 0) {
             break;
         }
    }
    return records;
}