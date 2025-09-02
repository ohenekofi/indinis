// @filename: src/buffer_pool_manager.cpp

#include "../include/buffer_pool_manager.h"
// #include "../include/wal.h" // For ExtWALManager (used by setWALManager and potentially by flush methods)
#include "../include/wal/sharded_wal_manager.h" 
#include "../include/debug_utils.h"
#include "../include/compression_utils.h" // For CompressionManager, CompressionType, OnDiskPageHeader
#include "../include/encryption_library.h" // Your EncryptionLibrary
#include "../include/storage_error/storage_error.h" // Provides full definition of storage::StorageError
#include "../include/storage_error/error_codes.h"   // Provides definition of storage::ErrorCode
// types.h is included via buffer_pool_manager.h, which gives access to calculate_payload_checksum

#include <stdexcept>
#include <iostream>
#include <vector>  // For temporary buffer during fetch/flush
#include <cstring> // For memcpy

// Assume this represents the size of the B-Tree's logical uncompressed page content.
// This must be <= SimpleDiskManager::PAGE_SIZE - ON_DISK_PAGE_HEADER_SIZE
// The Page objects in the BPM will manage data buffers of this size.
static constexpr size_t BPM_LOGICAL_PAGE_CONTENT_SIZE = SimpleDiskManager::PAGE_SIZE - ON_DISK_PAGE_HEADER_SIZE;


SimpleBufferPoolManager::SimpleBufferPoolManager(
    size_t pool_size,
    SimpleDiskManager& disk_manager_ref,
    // New Compression Params
    bool overall_compression_enabled,
    CompressionType compression_type_for_pages,
    int compression_level_for_pages,
    // Existing Encryption Params
    bool overall_encryption_enabled_param, // Renamed to avoid shadowing
    EncryptionScheme scheme_for_new_pages,
    const std::vector<unsigned char>& dek_passed,
    const std::vector<unsigned char>& global_salt_passed)
    : disk_manager_(disk_manager_ref),
      pool_size_(pool_size),
      pages_(pool_size),
      page_table_latches_(NUM_PAGE_TABLE_SHARDS),
      recently_referenced_(pool_size, false),
      victim_pinned_(pool_size, false),
      clock_hand_(0),
      wal_manager_(nullptr),
      // --- INITIALIZE NEW MEMBERS ---
      compression_is_active_(overall_compression_enabled),
      compression_type_(compression_type_for_pages),
      compression_level_(compression_level_for_pages),
      // ----------------------------
      encryption_is_active_(overall_encryption_enabled_param),
      default_encryption_scheme_(scheme_for_new_pages),
      database_dek_(dek_passed),
      database_global_salt_(global_salt_passed)
{

    if (pool_size_ == 0) {
        LOG_FATAL("[BPM] Buffer pool size cannot be zero.");
        throw std::invalid_argument("Buffer pool size cannot be zero.");
    }
    if (BPM_LOGICAL_PAGE_CONTENT_SIZE <= 0) {
        LOG_FATAL("[BPM] Logical page content size is invalid (<=0). Check PAGE_SIZE and ON_DISK_PAGE_HEADER_SIZE.");
        throw std::logic_error("Invalid logical page content size calculation for BPM.");
    }

    for (size_t i = 0; i < pool_size_; ++i) {
        free_list_.push_back(i);
    }

    if (encryption_is_active_) {
        if (database_dek_.empty()) {
            LOG_ERROR("[BPM Constructor] Encryption is ACTIVE, but Database DEK is EMPTY. This is a critical error. Disabling BPM encryption features for safety.");
            encryption_is_active_ = false; 
            // Consider throwing std::runtime_error here to halt initialization if this state is unacceptable.
        }
        if (database_global_salt_.empty() && encryption_is_active_) { // Check only if encryption is active
             LOG_WARN("[BPM Constructor] Encryption is ACTIVE, but Database Global Salt is EMPTY. Some operations might fail if salt is expected.");
             // Depending on scheme, this might also be a critical error.
        }
    }

    LOG_INFO("[BPM] Initialized. Pool size: {}. Logical Page Content Size: {}. Encryption Active in BPM: {}", 
             pool_size_, BPM_LOGICAL_PAGE_CONTENT_SIZE, encryption_is_active_);
}

SimpleBufferPoolManager::~SimpleBufferPoolManager() {
    LOG_INFO("[BPM] Shutting down...");
    try {
        flushAllPages();
        LOG_INFO("[BPM] All dirty pages flushed.");
    } catch (const std::exception& e) {
        LOG_ERROR("[BPM] Error flushing pages during destruction: {}", e.what());
    } catch (...) {
        LOG_ERROR("[BPM] Unknown error flushing pages during destruction.");
    }
    LOG_INFO("[BPM] Shutdown complete.");
}

size_t SimpleBufferPoolManager::getDirtyPageCount() const {
    std::lock_guard<std::mutex> flr_lock(free_list_replacer_latch_);
    size_t dirty_count = 0;
    for (const auto& page : pages_) {
        if (page.isDirty() && page.getPageId() != INVALID_PAGE_ID) {
            dirty_count++;
        }
    }
    return dirty_count;
}

std::optional<size_t> SimpleBufferPoolManager::findVictimFrame() {
    // Assumes free_list_replacer_latch_ is HELD by the caller
    if (pool_size_ == 0) return std::nullopt;

    size_t frames_checked = 0;
    while (frames_checked < 2 * pool_size_) {
        size_t current_frame_idx = clock_hand_;
        clock_hand_ = (clock_hand_ + 1) % pool_size_;

        if (!victim_pinned_[current_frame_idx]) {
            if (recently_referenced_[current_frame_idx]) {
                recently_referenced_[current_frame_idx] = false;
            } else {
                LOG_TRACE("  [BPM Clock] Found victim frame: {}", current_frame_idx);
                return current_frame_idx;
            }
        }
        frames_checked++;
    }
    return std::nullopt;
}

Page* SimpleBufferPoolManager::fetchPage(PageId page_id) {
    if (page_id == INVALID_PAGE_ID) {
        LOG_ERROR("[BPM fetchPage {}] Called with INVALID_PAGE_ID.", page_id);
        // Or: throw storage::StorageError(storage::ErrorCode::INVALID_KEY, "Attempt to fetch INVALID_PAGE_ID");
        return nullptr;
    }

    size_t shard_idx = getPageTableShardIndex(page_id);
    Page* result_page_ptr = nullptr;

    // 1. Check if page is already in buffer pool
    {
        std::lock_guard<std::mutex> pt_lock(page_table_latches_[shard_idx]);
        auto table_it = page_table_.find(page_id);
        if (table_it != page_table_.end()) {
            size_t frame_idx = table_it->second;
            result_page_ptr = &pages_[frame_idx];
            result_page_ptr->incrementPinCount();
            { // Scope for free_list_replacer_latch_
                std::lock_guard<std::mutex> flr_lock(free_list_replacer_latch_);
                recently_referenced_[frame_idx] = true;
                victim_pinned_[frame_idx] = true; // Mark as pinned for replacer
            }
            LOG_INFO("[BPM fetchPage {}] Found in frame {}. Pin count now: {}", page_id, frame_idx, result_page_ptr->getPinCount());
            return result_page_ptr;
        }
    } // pt_lock released
    LOG_INFO("[BPM fetchPage {}] Not in buffer pool. Finding frame...", page_id);

    // 2. Page not in buffer. Acquire a frame (either free or victim).
    std::unique_lock<std::mutex> flr_lock(free_list_replacer_latch_); // Lock for free_list_ and replacer state
    size_t target_frame_idx;

    if (!free_list_.empty()) {
        target_frame_idx = free_list_.front();
        free_list_.pop_front();
        LOG_TRACE("  [BPM fetchPage {}] Using free frame: {}", page_id, target_frame_idx);
    } else {
        LOG_TRACE("  [BPM fetchPage {}] No free frames, finding victim...", page_id);
        std::optional<size_t> victim_idx_opt = findVictimFrame(); // Assumes flr_lock is held
        if (!victim_idx_opt) {
            LOG_ERROR("[BPM fetchPage {}] Cannot fetch. Pool full and no victim frame could be found (all pinned?).", page_id);
            // flr_lock is still held, will be released on return
            throw storage::StorageError(storage::ErrorCode::STORAGE_FULL, "Buffer pool exhausted, no victim page available.")
                .withContext("page_id_requested", std::to_string(page_id));
        }
        target_frame_idx = *victim_idx_opt;
        Page& victim_page_ref = pages_[target_frame_idx]; // Page object of the victim
        PageId victim_page_id_val = victim_page_ref.getPageId(); // ID of the page being evicted

        LOG_INFO("  [BPM fetchPage {}] Found victim frame: {} (was page {}).", page_id, target_frame_idx, victim_page_id_val);

        if (victim_page_ref.isDirty()) {
            // Temporarily release flr_lock to call flushPage, which might acquire other locks
            flr_lock.unlock();
            LOG_INFO("    Victim page {} in frame {} is dirty. Flushing before eviction...", victim_page_id_val, target_frame_idx);
            bool flush_ok = false;
            try {
                flush_ok = flushPage(victim_page_id_val); // This now enforces ARIES and calculates checksum on write
            } catch (const storage::StorageError& e) {
                LOG_ERROR("    [BPM fetchPage {}] StorageError flushing dirty victim page {}: {}. Cannot proceed with fetch.", page_id, victim_page_id_val, e.toDetailedString());
                // Re-acquire flr_lock if needed for consistent state before throwing/returning
                // flr_lock.lock(); // Not strictly needed if rethrowing, but good if we were to continue
                throw; // Rethrow the critical flush error
            } catch (const std::exception& e) {
                 LOG_ERROR("    [BPM fetchPage {}] Std::exception flushing dirty victim page {}: {}. Cannot proceed with fetch.", page_id, victim_page_id_val, e.what());
                 throw storage::StorageError(storage::ErrorCode::IO_FLUSH_ERROR, "Failed to flush dirty victim page during fetch: " + std::string(e.what()))
                    .withContext("victim_page_id", std::to_string(victim_page_id_val));
            }

            flr_lock.lock(); // Re-acquire flr_lock
            if (!flush_ok) {
                LOG_ERROR("  [BPM fetchPage {}] Failed to flush dirty victim page {}. Cannot proceed with current fetch.", page_id, victim_page_id_val);
                // The victim page is still in the buffer, pinned (implicitly by replacer logic), and dirty.
                // This is a problematic state. For now, we can't use this frame.
                // We might need to try finding another victim, or fail the fetch.
                // For simplicity, fail the fetch.
                throw storage::StorageError(storage::ErrorCode::IO_FLUSH_ERROR, "Failed to flush dirty victim page, cannot fetch new page into this frame.")
                    .withContext("victim_page_id", std::to_string(victim_page_id_val));
            }
            LOG_INFO("    Victim page {} flushed successfully.", victim_page_id_val);
        }

        // Remove victim from page_table_
        if (victim_page_id_val != INVALID_PAGE_ID) {
            flr_lock.unlock(); // Release flr_lock to acquire page_table_latch for victim
            size_t victim_shard_idx = getPageTableShardIndex(victim_page_id_val);
            {
                std::lock_guard<std::mutex> victim_pt_lock(page_table_latches_[victim_shard_idx]);
                page_table_.erase(victim_page_id_val);
                LOG_TRACE("    Victim page {} removed from page table (shard {}).", victim_page_id_val, victim_shard_idx);
            }
            flr_lock.lock(); // Re-acquire flr_lock
        }
        // Reset victim's Page object metadata (already under flr_lock)
        victim_page_ref.WLatch(); // W-latch to modify its state safely
        victim_page_ref.resetMemory();
        victim_page_ref.setPageId(INVALID_PAGE_ID);
        victim_page_ref.setDirty(false);
        victim_page_ref.pin_count_.store(0, std::memory_order_relaxed);
        victim_page_ref.setPageLSN(0);
        victim_page_ref.WUnlock();
    }
    // flr_lock is HELD at this point if we took a victim or came from free_list_

    // 3. Load requested page data from disk, decrypt, decompress into target_frame_idx
    Page& target_page_in_bpm = pages_[target_frame_idx];
    std::vector<char> raw_physical_disk_page_buffer(SimpleDiskManager::PAGE_SIZE);

    flr_lock.unlock(); // Release flr_lock BEFORE disk I/O and significant CPU work

    try {
        disk_manager_.readPage(page_id, raw_physical_disk_page_buffer.data());
        LOG_TRACE("  [BPM fetchPage {}] Physical page data read from disk.", page_id);
    } catch (const storage::StorageError& e) {
        LOG_ERROR("  [BPM fetchPage {}] Failed reading physical page from disk: {}", page_id, e.toDetailedString());
        // Cleanup frame before throwing: add it back to free list
        flr_lock.lock(); // Re-acquire for cleanup
        target_page_in_bpm.WLatch();
        target_page_in_bpm.setPageId(INVALID_PAGE_ID); // Ensure it's marked invalid
        target_page_in_bpm.pin_count_.store(0);
        target_page_in_bpm.WUnlock();
        free_list_.push_front(target_frame_idx);
        recently_referenced_[target_frame_idx] = false;
        victim_pinned_[target_frame_idx] = false;
        // flr_lock.unlock(); // Lock will be released by destructor if we throw
        throw; // Rethrow the caught storage error
    } catch (const std::exception& e) { // Catch other potential errors from readPage
         LOG_ERROR("  [BPM fetchPage {}] Std::exception reading physical page from disk: {}", page_id, e.what());
         flr_lock.lock();
         target_page_in_bpm.WLatch(); target_page_in_bpm.setPageId(INVALID_PAGE_ID); target_page_in_bpm.pin_count_.store(0); target_page_in_bpm.WUnlock();
         free_list_.push_front(target_frame_idx); recently_referenced_[target_frame_idx] = false; victim_pinned_[target_frame_idx] = false;
         throw storage::StorageError(storage::ErrorCode::IO_READ_ERROR, "Failed to read page from disk: " + std::string(e.what()))
            .withContext("page_id", std::to_string(page_id));
    }


    const OnDiskPageHeader* disk_header =
        reinterpret_cast<const OnDiskPageHeader*>(raw_physical_disk_page_buffer.data());

    LOG_INFO("  [BPM fetchPage {}] OnDiskHeader - Flags(E{:#x}|C{:#x}):{:#04x}, DiskPayloadSize:{}, UncompLogicalSize:{}, PageLSN:{}, DiskChecksum:{:#010x}",
              page_id, (disk_header->flags >> 4) & 0x0F, disk_header->flags & 0x0F, disk_header->flags,
              disk_header->payload_size_on_disk, disk_header->uncompressed_logical_size, disk_header->page_lsn, disk_header->payload_checksum);

    // *** Verify payload_checksum ***
    const uint8_t* disk_payload_ptr_for_checksum =
        reinterpret_cast<const uint8_t*>(raw_physical_disk_page_buffer.data() + ON_DISK_PAGE_HEADER_SIZE);
    size_t disk_payload_size_for_checksum = disk_header->payload_size_on_disk;
    uint32_t calculated_checksum_on_read = 0;

    if (disk_payload_size_for_checksum > 0) {
        if (ON_DISK_PAGE_HEADER_SIZE + disk_payload_size_for_checksum > SimpleDiskManager::PAGE_SIZE) {
             LOG_ERROR("  [BPM fetchPage {}] Header reports payload_size_on_disk {} which exceeds page boundary. Corruption likely.", page_id, disk_payload_size_for_checksum);
             // Cleanup frame and throw
             flr_lock.lock();
             target_page_in_bpm.WLatch(); target_page_in_bpm.setPageId(INVALID_PAGE_ID); target_page_in_bpm.pin_count_.store(0); target_page_in_bpm.WUnlock();
             free_list_.push_front(target_frame_idx); recently_referenced_[target_frame_idx] = false; victim_pinned_[target_frame_idx] = false;
             throw storage::StorageError(storage::ErrorCode::STORAGE_CORRUPTION, "Page header payload size exceeds physical page limits on fetch.")
                 .withContext("page_id", std::to_string(page_id));
        }
        calculated_checksum_on_read = calculate_payload_checksum(disk_payload_ptr_for_checksum, disk_payload_size_for_checksum);
    } else {
        calculated_checksum_on_read = calculate_payload_checksum(nullptr, 0); // Checksum for empty payload
    }

    if (calculated_checksum_on_read != disk_header->payload_checksum) {
        LOG_ERROR("  [BPM fetchPage {}] CHECKSUM MISMATCH! DiskHeaderChecksum: {:#010x}, CalculatedOnRead: {:#010x}. Page data corruption suspected.",
                  page_id, disk_header->payload_checksum, calculated_checksum_on_read);
        flr_lock.lock();
        target_page_in_bpm.WLatch(); target_page_in_bpm.setPageId(INVALID_PAGE_ID); target_page_in_bpm.pin_count_.store(0); target_page_in_bpm.WUnlock();
        free_list_.push_front(target_frame_idx); recently_referenced_[target_frame_idx] = false; victim_pinned_[target_frame_idx] = false;
        throw storage::StorageError(storage::ErrorCode::CHECKSUM_MISMATCH, "Page content checksum mismatch on read.")
            .withContext("page_id", std::to_string(page_id))
            .withContext("expected_checksum", std::to_string(disk_header->payload_checksum))
            .withContext("actual_checksum", std::to_string(calculated_checksum_on_read));
    }
    LOG_INFO("  [BPM fetchPage {}] Payload checksum VERIFIED ({:#010x}).", page_id, calculated_checksum_on_read);


    // Page object setup (W-latch needed for modifying its internal state)
    target_page_in_bpm.WLatch();
    try {
        target_page_in_bpm.resetMemory(); // Zero out buffer before filling
        target_page_in_bpm.setPageId(page_id);
        target_page_in_bpm.setDirty(false); // Freshly fetched page is not dirty
        target_page_in_bpm.pin_count_.store(1, std::memory_order_relaxed); // Pin count is 1 now
        target_page_in_bpm.setPageLSN(disk_header->page_lsn);

        const uint8_t* disk_payload_ptr = disk_payload_ptr_for_checksum; // Same pointer
        size_t disk_payload_size = disk_payload_size_for_checksum;     // Same size

        std::vector<uint8_t> data_after_decryption; // To hold data if decryption happens
        char* bpm_page_logical_data_buffer = target_page_in_bpm.getData(); // Where final data goes

        // --- Step A: Decryption ---
        EncryptionScheme scheme_on_disk = disk_header->get_encryption_scheme();
        
        // Check if decryption is required for this page.
        if (scheme_on_disk != EncryptionScheme::NONE) {
            // Decryption is needed. First, check if the engine is configured for it.
            if (!this->encryption_is_active_) {
                LOG_ERROR("  [BPM fetchPage {}] Page on disk is encrypted (Scheme: {}) but BPM encryption is disabled. Cannot decrypt. Aborting fetch.",
                          page_id, static_cast<int>(scheme_on_disk));
                throw storage::StorageError(storage::ErrorCode::STORAGE_CORRUPTION, "Cannot read encrypted page because encryption is disabled.");
            }
            if (this->database_dek_.empty()) {
                throw storage::StorageError(storage::ErrorCode::STORAGE_NOT_INITIALIZED, "DEK not available for decryption of page " + std::to_string(page_id));
            }
            if (disk_payload_size == 0) {
                 // Encrypted payload should not be empty if there's uncompressed data.
                 // If uncompressed_logical_size is also 0, this is a valid empty page.
                 if (disk_header->uncompressed_logical_size > 0) {
                    throw storage::StorageError(storage::ErrorCode::STORAGE_CORRUPTION, "Encrypted payload on disk is empty but uncompressed size > 0 for page " + std::to_string(page_id));
                 }
                 // Otherwise, it's an empty encrypted page, so we just use the empty buffer.
                 data_after_decryption.clear();
            } else {
                LOG_TRACE("    [BPM fetchPage {}] Decrypting (DiskScheme: {}). Ciphertext size: {}.",
                          page_id, static_cast<int>(scheme_on_disk), disk_payload_size);
                
                try {
                    // We now only support GCM for new writes, so we only need to implement GCM decryption.
                    if (scheme_on_disk == EncryptionScheme::AES256_GCM_PBKDF2) {
                        EncryptionLibrary::EncryptedData ed_from_disk;
                        ed_from_disk.iv.assign(disk_header->iv, disk_header->iv + EncryptionLibrary::AES_GCM_IV_SIZE);
                        ed_from_disk.tag.assign(disk_header->auth_tag, disk_header->auth_tag + EncryptionLibrary::AES_GCM_TAG_SIZE);
                        ed_from_disk.data.assign(disk_payload_ptr, disk_payload_ptr + disk_payload_size);

                        // Reconstruct the AAD *exactly* as it was created during the flush operation.
                        // For pages, the AAD was just the flags byte.
                        std::vector<uint8_t> aad_data_for_verification;
                        aad_data_for_verification.push_back(disk_header->flags);
                        // If other fields (like page_id, LSN) were added to AAD during encryption, add them here too.

                        std::string decrypted_str = EncryptionLibrary::decryptWithAES_GCM(
                            ed_from_disk, this->database_dek_, aad_data_for_verification
                        );
                        data_after_decryption.assign(decrypted_str.begin(), decrypted_str.end());

                        LOG_TRACE("    [BPM fetchPage {}] GCM decryption successful. Size after decryption: {}.", page_id, data_after_decryption.size());
                    } else {
                        // If you need to support reading old CBC-encrypted pages, the logic would go here.
                        // For now, we treat it as an error.
                        throw storage::StorageError(storage::ErrorCode::STORAGE_CORRUPTION, "Unsupported or legacy encryption scheme on disk: " + std::to_string(static_cast<int>(scheme_on_disk)));
                    }
                } catch (const CryptoException& ce) {
                    // This is the most likely failure point for a wrong password, as the GCM tag will not match.
                    LOG_ERROR("  [BPM fetchPage {}] Decryption failed (likely auth tag mismatch or wrong key): {}", page_id, ce.what());
                    throw storage::StorageError(storage::ErrorCode::STORAGE_CORRUPTION, "Page decryption failed. Data may be corrupt, or the password may be incorrect.")
                        .withContext("page_id", std::to_string(page_id));
                }
            }
        } else { // No decryption performed
            data_after_decryption.assign(disk_payload_ptr, disk_payload_ptr + disk_payload_size);
        }

        // --- Step B: Decompression ---
        CompressionType compr_type_on_disk = disk_header->get_compression_type();
        if (compr_type_on_disk == CompressionType::NONE) {
            if (data_after_decryption.size() != disk_header->uncompressed_logical_size) {
                throw storage::StorageError(storage::ErrorCode::STORAGE_CORRUPTION, "Size mismatch for NONE compression on fetch.")
                    .withContext("page_id", std::to_string(page_id))
                    .withContext("data_after_dec_size", std::to_string(data_after_decryption.size()))
                    .withContext("header_uncomp_size", std::to_string(disk_header->uncompressed_logical_size));
            }
            if (disk_header->uncompressed_logical_size > Page::LOGICAL_CONTENT_SIZE) {
                 throw storage::StorageError(storage::ErrorCode::STORAGE_CORRUPTION, "Uncompressed logical size too large for BPM page buffer on fetch.")
                    .withContext("page_id", std::to_string(page_id));
            }
            if (disk_header->uncompressed_logical_size > 0) {
                std::memcpy(bpm_page_logical_data_buffer, data_after_decryption.data(), disk_header->uncompressed_logical_size);
            }
        } else { // Decompression needed
            if (disk_header->uncompressed_logical_size == 0 && data_after_decryption.size() > 0) {
                throw storage::StorageError(storage::ErrorCode::STORAGE_CORRUPTION, "Corrupt header: uncompressed_logical_size is 0, but data present for decompression.")
                    .withContext("page_id", std::to_string(page_id));
            }
            if (disk_header->uncompressed_logical_size == 0 && data_after_decryption.empty()){
                 // Valid empty compressed page. bpm_page_logical_data_buffer is already zeroed.
            } else {
                std::vector<uint8_t> fully_processed_data = CompressionManager::decompress(
                    data_after_decryption.data(), data_after_decryption.size(),
                    disk_header->uncompressed_logical_size, compr_type_on_disk);

                if (fully_processed_data.size() != disk_header->uncompressed_logical_size) {
                    throw storage::StorageError(storage::ErrorCode::COMPRESSION_ERROR, "Decompressed size mismatch with header on fetch.")
                        .withContext("page_id", std::to_string(page_id));
                }
                if (fully_processed_data.size() > Page::LOGICAL_CONTENT_SIZE) {
                    throw storage::StorageError(storage::ErrorCode::BUFFER_OVERFLOW, "Final processed data size too large for BPM page buffer on fetch.")
                        .withContext("page_id", std::to_string(page_id));
                }
                if (!fully_processed_data.empty()) {
                    std::memcpy(bpm_page_logical_data_buffer, fully_processed_data.data(), fully_processed_data.size());
                }
            }
        }
        LOG_TRACE("    [BPM fetchPage {}] Decompression successful (if any). Final logical data placed in buffer.", page_id);

    }  catch (const CryptoException& ce) { // Most specific first
        LOG_ERROR("  [BPM fetchPage {}] CryptoException processing page content: {}. Cleaning up frame.", page_id, ce.what());
        target_page_in_bpm.pin_count_.store(0); target_page_in_bpm.setPageId(INVALID_PAGE_ID);
        target_page_in_bpm.WUnlock();
        flr_lock.lock();
        free_list_.push_front(target_frame_idx); recently_referenced_[target_frame_idx] = false; victim_pinned_[target_frame_idx] = false;
        throw storage::StorageError(storage::ErrorCode::STORAGE_CORRUPTION, "Cryptography error during page fetch: " + std::string(ce.what()))
            .withContext("page_id", std::to_string(page_id));
    }
    catch (const storage::StorageError& e) { // Next specific error type
        LOG_ERROR("  [BPM fetchPage {}] StorageError processing page content: {}. Cleaning up frame.", page_id, e.toDetailedString());
        target_page_in_bpm.pin_count_.store(0); target_page_in_bpm.setPageId(INVALID_PAGE_ID);
        target_page_in_bpm.WUnlock();
        flr_lock.lock(); 
        free_list_.push_front(target_frame_idx);
        recently_referenced_[target_frame_idx] = false; victim_pinned_[target_frame_idx] = false;
        throw; // Re-throw the original, detailed StorageError
    } 
    catch (const std::exception& e) { // General std::exception (catches runtime_error, etc.)
        LOG_ERROR("  [BPM fetchPage {}] Std::exception processing page content: {}. Cleaning up frame.", page_id, e.what());
        target_page_in_bpm.pin_count_.store(0); target_page_in_bpm.setPageId(INVALID_PAGE_ID);
        target_page_in_bpm.WUnlock();
        flr_lock.lock();
        free_list_.push_front(target_frame_idx); recently_referenced_[target_frame_idx] = false; victim_pinned_[target_frame_idx] = false;
        throw storage::StorageError(storage::ErrorCode::INTERNAL_ERROR, "Generic error processing page content: " + std::string(e.what()))
            .withContext("page_id", std::to_string(page_id));
    }    
    target_page_in_bpm.WUnlock(); // Release Page's W-latch after successful content setup

    // 4. Update page_table_ and replacer state
    { // Scope for pt_lock
        std::lock_guard<std::mutex> pt_lock(page_table_latches_[shard_idx]);
        page_table_[page_id] = target_frame_idx;
    }
    { // Scope for final_flr_lock
        std::lock_guard<std::mutex> final_flr_lock(free_list_replacer_latch_);
        recently_referenced_[target_frame_idx] = true;
        victim_pinned_[target_frame_idx] = true; // Is now pinned
    }

    LOG_INFO("[BPM fetchPage {}] Successfully loaded and processed (DiskEncScheme:{}, DiskCompType:{}) into frame {}. PinCount:1",
              page_id, static_cast<int>(disk_header->get_encryption_scheme()), static_cast<int>(disk_header->get_compression_type()), target_frame_idx);
    return &target_page_in_bpm;
}

Page* SimpleBufferPoolManager::newPage(PageId& new_page_id_out /* out */) {
    new_page_id_out = INVALID_PAGE_ID;
    
    std::unique_lock<std::mutex> flr_lock(free_list_replacer_latch_);
    LOG_TRACE("[BPM] newPage() called.");
    size_t target_frame_idx;

    if (!free_list_.empty()) {
        target_frame_idx = free_list_.front();
        free_list_.pop_front();
        LOG_TRACE("  Using free frame: {}", target_frame_idx);
    } else {
        LOG_TRACE("  No free frames, finding victim...");
        std::optional<size_t> victim_idx_opt = this->findVictimFrame();
        if (!victim_idx_opt) {
            LOG_ERROR("[BPM] Cannot create new page. Pool full and no victim found!");
            return nullptr;
        }
        target_frame_idx = *victim_idx_opt;
        Page& victim_page_ref = pages_[target_frame_idx];
        PageId victim_page_id_val = victim_page_ref.getPageId();
        
        LOG_TRACE("  Found victim frame: {} for page {}", target_frame_idx, victim_page_id_val);
        
        if (victim_page_ref.isDirty()) {
            flr_lock.unlock(); // Release before calling flushPage
            bool flush_ok = flushPage(victim_page_id_val); // flushPage handles compression
            flr_lock.lock();   // Re-acquire
            if (!flush_ok) {
                LOG_ERROR("  [BPM newPage] Failed to flush dirty victim page {}. Cannot create new page.", victim_page_id_val);
                return nullptr;
            }
        }
        
        if (victim_page_id_val != INVALID_PAGE_ID) {
            flr_lock.unlock();
            size_t victim_shard_idx = getPageTableShardIndex(victim_page_id_val);
            { std::lock_guard<std::mutex> pt_lock(page_table_latches_[victim_shard_idx]); page_table_.erase(victim_page_id_val); }
            flr_lock.lock();
        }
        victim_page_ref.setPageId(INVALID_PAGE_ID);
        victim_page_ref.setPageLSN(0);
        victim_page_ref.pin_count_.store(0);
        victim_page_ref.setDirty(false);
    }
    // flr_lock is held

    // 2. Allocate page on disk. Release flr_lock before this I/O.
    flr_lock.unlock();
    PageId allocated_disk_page_id = INVALID_PAGE_ID;
    try {
        allocated_disk_page_id = disk_manager_.allocatePage();
        if (allocated_disk_page_id == INVALID_PAGE_ID) {
            LOG_ERROR("  Disk manager failed to allocate a new page ID during newPage.");
            flr_lock.lock(); // Re-acquire for cleanup
            free_list_.push_front(target_frame_idx); 
            recently_referenced_[target_frame_idx] = false;
            victim_pinned_[target_frame_idx] = false;
            return nullptr;
        }
        new_page_id_out = allocated_disk_page_id;
        LOG_TRACE("  Disk manager allocated new Page ID: {}", new_page_id_out);
    } catch (const std::exception& e) {
        LOG_ERROR("  Exception during disk_manager_.allocatePage: {}", e.what());
        flr_lock.lock(); // Re-acquire for cleanup
        free_list_.push_front(target_frame_idx);
        recently_referenced_[target_frame_idx] = false;
        victim_pinned_[target_frame_idx] = false;
        return nullptr;
    }

    // 3. Setup frame metadata (in-memory Page object)
    Page& new_page_frame = pages_[target_frame_idx];
    new_page_frame.resetMemory(); // Zero out the logical content buffer
    new_page_frame.setPageId(new_page_id_out);
    new_page_frame.setDirty(true); // New page is dirty, needs to be written (with header+data)
    new_page_frame.pin_count_.store(1, std::memory_order_relaxed);
    new_page_frame.setPageLSN(0); // Initial LSN; will be updated by first op that modifies it.

    // 4. Update buffer pool metadata (page_table and replacer state)
    size_t new_page_shard_idx = getPageTableShardIndex(new_page_id_out);
    {
        std::lock_guard<std::mutex> pt_lock(page_table_latches_[new_page_shard_idx]);
        page_table_[new_page_id_out] = target_frame_idx;
    }
    {
        std::lock_guard<std::mutex> final_flr_lock(free_list_replacer_latch_);
        recently_referenced_[target_frame_idx] = true;
        victim_pinned_[target_frame_idx] = true;
    }

    LOG_TRACE("  New page {} created in frame {}. Pin count: 1. It will be compressed on first flush.", new_page_id_out, target_frame_idx);
    return &new_page_frame;
}

bool SimpleBufferPoolManager::unpinPage(PageId page_id, bool is_dirty, LSN page_op_lsn) {
    if (page_id == INVALID_PAGE_ID) return false;
    
    size_t shard_idx = getPageTableShardIndex(page_id);
    size_t frame_idx_local = static_cast<size_t>(-1);
    Page* page_ptr = nullptr;

    {
        std::lock_guard<std::mutex> pt_lock(page_table_latches_[shard_idx]);
        auto table_it = page_table_.find(page_id);
        if (table_it == page_table_.end()) {
            LOG_WARN("[BPM] unpinPage: Page {} not found in page_table.", page_id);
            return false; 
        }
        frame_idx_local = table_it->second;
        page_ptr = &pages_[frame_idx_local];

        if (page_ptr->getPinCount() <= 0) {
            LOG_WARN("[BPM] unpinPage: Page {} called with pin count <= 0 (is {}).", page_id, page_ptr->getPinCount());
            if(page_ptr->getPinCount() < 0) page_ptr->pin_count_.store(0, std::memory_order_relaxed); 
            if(page_ptr->getPinCount() == 0) { // If it was exactly 0, ensure replacer knows
                 std::lock_guard<std::mutex> flr_lock(free_list_replacer_latch_);
                 if (frame_idx_local < victim_pinned_.size()) victim_pinned_[frame_idx_local] = false;
            }
            return false; 
        }
        
        if (is_dirty) {
            page_ptr->setDirty(true);
            // PageLSN should track the LSN of the *latest* log record describing the change to this page.
            if (page_ptr->getPageLSN() < page_op_lsn) { // Only update if current op's LSN is newer
                 page_ptr->setPageLSN(page_op_lsn);
            }
            LOG_TRACE("  Page {} marked dirty by unpin. PageLSN set to/kept at: {}", page_id, page_ptr->getPageLSN());
            
            // Update/Add to DPT (Dirty Page Table)
            std::lock_guard<std::mutex> flr_lock(free_list_replacer_latch_);
            if (dirty_page_table_.find(page_id) == dirty_page_table_.end()) {
                // If not in DPT, this page_op_lsn is the first LSN that made it dirty since last flush.
                dirty_page_table_[page_id] = page_op_lsn;
                LOG_TRACE("  Page {} added to DPT with recoveryLSN: {}", page_id, page_op_lsn);
            }
            // If already in DPT, its DPT recLSN (oldest dirtying LSN since last flush) should not be overwritten by a newer LSN.
        }
    } // pt_lock released

    if (page_ptr->decrementPinCount() == 0) {
        std::lock_guard<std::mutex> flr_lock(free_list_replacer_latch_);
        if (frame_idx_local < victim_pinned_.size()) { // Bounds check
             victim_pinned_[frame_idx_local] = false; // Mark as unpinned for replacer
             LOG_TRACE("[BPM] Page {} pin count now 0. Frame {} marked unpinned for replacer.", page_id, frame_idx_local);
        } else {
            LOG_ERROR("[BPM] Frame index {} out of bounds for victim_pinned_ in unpinPage. Pool size: {}", frame_idx_local, pool_size_);
        }
    } else {
        LOG_TRACE("[BPM] Page {} pin count now {}.", page_id, page_ptr->getPinCount());
    }
    return true;
}

bool SimpleBufferPoolManager::deletePage(PageId page_id) {
    if (page_id == INVALID_PAGE_ID) return false;
    
    LOG_TRACE("[BPM] deletePage({}) called.", page_id);
    size_t shard_idx = getPageTableShardIndex(page_id);
    size_t frame_idx_to_free = static_cast<size_t>(-1);

    {
        std::lock_guard<std::mutex> pt_lock(page_table_latches_[shard_idx]);
        auto table_it = page_table_.find(page_id);
        if (table_it != page_table_.end()) { 
            frame_idx_to_free = table_it->second;
            Page& page_in_buffer = pages_[frame_idx_to_free];
            if (page_in_buffer.getPinCount() > 0) {
                LOG_ERROR("  [BPM deletePage] Cannot delete page {} while pinned (count={})", page_id, page_in_buffer.getPinCount());
                return false;
            }
            page_table_.erase(table_it);
        }
    }

    if (frame_idx_to_free != static_cast<size_t>(-1)) {
        std::lock_guard<std::mutex> flr_lock(free_list_replacer_latch_);
        Page& page_obj = pages_[frame_idx_to_free];
        
        dirty_page_table_.erase(page_id); 
        
        page_obj.resetMemory();
        page_obj.setPageId(INVALID_PAGE_ID);
        page_obj.setDirty(false);
        page_obj.setPageLSN(0);
        page_obj.pin_count_.store(0); // Should already be 0

        free_list_.push_front(frame_idx_to_free); 
        if (frame_idx_to_free < recently_referenced_.size()) {
            recently_referenced_[frame_idx_to_free] = false;
            victim_pinned_[frame_idx_to_free] = false;
        }
        LOG_TRACE("  Page {} removed from buffer pool. Frame {} added to free list.", page_id, frame_idx_to_free);
    } else {
        LOG_TRACE("  Page {} not found in buffer pool for deletion.", page_id);
    }

    try {
        disk_manager_.deallocatePage(page_id);
        LOG_TRACE("  Disk manager deallocatePage called for page {}.", page_id);
    } catch (const std::exception& e) {
        LOG_ERROR("  Exception during disk_manager_.deallocatePage for {}: {}", page_id, e.what());
        return false; 
    }
    return true;
}

// @filename: src/buffer_pool_manager.cpp

#include "../include/buffer_pool_manager.h"
#include "../include/wal.h" // For ExtWALManager
#include "../include/debug_utils.h"
#include "../include/compression_utils.h"
#include "../include/encryption_library.h"
#include "../include/storage_error/storage_error.h" // For storage::StorageError
#include "../include/storage_error/error_codes.h"   // For storage::ErrorCode
// types.h is included via buffer_pool_manager.h, which gives access to calculate_payload_checksum

#include <stdexcept>
#include <iostream>
#include <vector>
#include <cstring> // For memcpy, memset
#include <algorithm> // For std::min

// Assumes calculate_payload_checksum is available (e.g., from types.h or a utility header)
// extern uint32_t calculate_payload_checksum(const uint8_t* data, size_t len); // If defined elsewhere


bool SimpleBufferPoolManager::flushPage(PageId page_id) {
    if (page_id == INVALID_PAGE_ID) {
        LOG_WARN("[BPM flushPage {}] Called with INVALID_PAGE_ID.", page_id);
        return false;
    }
    LOG_INFO("[BPM flushPage {}] Initiating flush sequence.", page_id);

    try {
        // Step 1: Get data from the buffer pool frame.
        auto flush_data_opt = getPageDataForFlush(page_id);
        if (!flush_data_opt || !flush_data_opt->is_dirty) {
            LOG_TRACE("  [BPM flushPage {}] Page not dirty or not in pool. Flush is a no-op.", page_id);
            return true; // Not an error, just nothing to do.
        }
        
        // Step 2: Enforce ARIES WAL Rule using our new method.
        if (wal_manager_ && flush_data_opt->page_lsn > 0) {
            LOG_INFO("  [BPM flushPage {}] ARIES Rule: Syncing WAL up to LSN {}.", page_id, flush_data_opt->page_lsn);
            wal_manager_->forceSyncToLSN(flush_data_opt->page_lsn); // CORRECTED CALL
            LOG_INFO("  [BPM flushPage {}] ARIES Rule: WAL sync complete.", page_id);
        }

        // Step 3: Prepare the data for disk (compress, encrypt, checksum).
        PreparedPagePayload prepared_page = preparePagePayloadForDisk(flush_data_opt->logical_data, flush_data_opt->page_lsn);

        // Step 4 & 5: Write to disk and update metadata.
        writePayloadToDiskAndUpdateMetadata(page_id, prepared_page);

    } catch (const storage::StorageError& e) {
        LOG_ERROR("[BPM flushPage {}] Flush failed due to StorageError: {}", page_id, e.toDetailedString());
        return false;
    } catch (const std::exception& e) {
        LOG_ERROR("[BPM flushPage {}] Flush failed due to std::exception: {}", page_id, e.what());
        return false;
    }

    LOG_INFO("[BPM flushPage {}] Flush sequence completed successfully.", page_id);
    return true;
}

// --- NEW PRIVATE HELPER IMPLEMENTATIONS ---

std::optional<SimpleBufferPoolManager::PageFlushData> SimpleBufferPoolManager::getPageDataForFlush(PageId page_id) {
    size_t shard_idx = getPageTableShardIndex(page_id);
    std::unique_lock<std::mutex> pt_lock(page_table_latches_[shard_idx]);
    
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return std::nullopt; // Page not in buffer pool.
    }

    Page& page = pages_[it->second];
    page.RLatch(); // Use R-Latch for reading content.
    pt_lock.unlock(); // Release table lock once we have page reference.

    PageFlushData data;
    data.is_dirty = page.isDirty();
    if (data.is_dirty) {
        data.page_lsn = page.getPageLSN();
        data.logical_data.assign(page.getData(), page.getData() + Page::LOGICAL_CONTENT_SIZE);
    }
    
    page.RUnlock();
    return data;
}

SimpleBufferPoolManager::PreparedPagePayload SimpleBufferPoolManager::preparePagePayloadForDisk(
    const std::vector<uint8_t>& logical_data, LSN page_lsn) {
    
    PreparedPagePayload result;
    result.header.page_lsn = page_lsn;
    result.header.uncompressed_logical_size = static_cast<uint32_t>(logical_data.size());

    // --- Step 1: Compression ---
    std::vector<uint8_t> data_after_compression = logical_data;
    CompressionType compression_used = CompressionType::NONE;

    // CORRECTED: Use the BPM's own compression settings
    if (this->compression_is_active_ && this->compression_type_ != CompressionType::NONE && !logical_data.empty()) {
        try {
            std::vector<uint8_t> compressed_output = CompressionManager::compress(
                logical_data.data(), logical_data.size(),
                this->compression_type_, this->compression_level_);
            
            if (compressed_output.size() < logical_data.size()) {
                data_after_compression = std::move(compressed_output);
                compression_used = this->compression_type_;
            }
        } catch (const std::exception& e) {
            LOG_WARN("[BPM preparePayload] Compression failed for page (LSN {}): {}. Storing uncompressed.", page_lsn, e.what());
            data_after_compression = logical_data;
            compression_used = CompressionType::NONE;
        }
    }
    
    // --- Step 2: Encryption ---
    EncryptionScheme encryption_used = EncryptionScheme::NONE;
    std::memset(result.header.iv, 0, sizeof(result.header.iv));
    // CORRECTED: Use the correct field name `auth_tag`
    std::memset(result.header.auth_tag, 0, sizeof(result.header.auth_tag));

    if (this->encryption_is_active_ && this->default_encryption_scheme_ != EncryptionScheme::NONE) {
        if (this->database_dek_.empty()) {
            throw storage::StorageError(storage::ErrorCode::STORAGE_NOT_INITIALIZED, "DEK not available for page encryption.");
        }
        encryption_used = this->default_encryption_scheme_;

        if (encryption_used != EncryptionScheme::AES256_GCM_PBKDF2) {
            throw CryptoException("BPM currently only supports AES-GCM for page encryption.");
        }
        
        try {
            auto page_iv = EncryptionLibrary::generateIV();
            std::memcpy(result.header.iv, page_iv.data(), std::min(page_iv.size(), sizeof(result.header.iv)));
            
            result.header.set_flags(compression_used, encryption_used);
            std::vector<uint8_t> aad_data;
            aad_data.push_back(result.header.flags);

            EncryptionLibrary::EncryptedData encrypted_out = EncryptionLibrary::encryptWithAES_GCM(
                data_after_compression, this->database_dek_, page_iv, aad_data
            );
            
            // CORRECTED: Use the correct field name `auth_tag`
            if(encrypted_out.tag.size() != sizeof(result.header.auth_tag)) {
                throw CryptoException("GCM encryption produced an authentication tag of unexpected size.");
            }
            std::memcpy(result.header.auth_tag, encrypted_out.tag.data(), sizeof(result.header.auth_tag));
            
            result.payload = std::move(encrypted_out.data);

        } catch (const CryptoException& ce) {
            LOG_ERROR("[BPM preparePayload] CryptoException during page encryption (LSN {}): {}", page_lsn, ce.what());
            throw;
        }
    } else {
        result.payload = std::move(data_after_compression);
        encryption_used = EncryptionScheme::NONE;
    }

    // --- Step 3 & 4: Finalize Header and Checksum ---
    result.header.set_flags(compression_used, encryption_used);
    result.header.payload_size_on_disk = static_cast<uint32_t>(result.payload.size());
    result.header.payload_checksum = calculate_payload_checksum(result.payload.data(), result.payload.size());
    
    LOG_TRACE("  [BPM preparePayload] Page (LSN {}) processed. Final on-disk payload size: {}, Comp: {}, Enc: {}, Checksum: {:#010x}",
              page_lsn, result.header.payload_size_on_disk, static_cast<int>(compression_used),
              static_cast<int>(encryption_used), result.header.payload_checksum);
              
    return result;
}

void SimpleBufferPoolManager::writePayloadToDiskAndUpdateMetadata(
    PageId page_id, const PreparedPagePayload& prepared_page) {

    // 1. Assemble the full 4KB physical page buffer.
    std::vector<char> physical_disk_page_buffer(SimpleDiskManager::PAGE_SIZE, 0);
    std::memcpy(physical_disk_page_buffer.data(), &prepared_page.header, ON_DISK_PAGE_HEADER_SIZE);
    if (ON_DISK_PAGE_HEADER_SIZE + prepared_page.payload.size() > SimpleDiskManager::PAGE_SIZE) {
        throw storage::StorageError(storage::ErrorCode::BUFFER_OVERFLOW, "Page content exceeds physical page size after processing.")
            .withContext("page_id", std::to_string(page_id));
    }
    if (!prepared_page.payload.empty()) {
        std::memcpy(physical_disk_page_buffer.data() + ON_DISK_PAGE_HEADER_SIZE, prepared_page.payload.data(), prepared_page.payload.size());
    }

    // 2. Write to disk.
    disk_manager_.writePage(page_id, physical_disk_page_buffer.data());

    // 3. Update BPM's in-memory metadata for the frame.
    size_t shard_idx = getPageTableShardIndex(page_id);
    std::unique_lock<std::mutex> pt_lock(page_table_latches_[shard_idx]);
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        Page& page = pages_[it->second];
        page.WLatch(); // W-Latch to safely modify dirty status
        pt_lock.unlock(); // Can unlock table lock now

        // Atomically check if the page has been re-dirtied since we copied its data.
        if (page.isDirty() && page.getPageLSN() <= prepared_page.header.page_lsn) {
            page.setDirty(false);
            LOG_TRACE("  [BPM flushPage {}] Cleared dirty flag in BPM.", page_id);
            // Remove from Dirty Page Table (DPT)
            std::lock_guard<std::mutex> flr_lock(free_list_replacer_latch_);
            dirty_page_table_.erase(page_id);
        } else if (page.isDirty()) {
            LOG_WARN("  [BPM flushPage {}] Page was re-dirtied (New LSN: {}) after data was copied for flush. Dirty flag remains set.", page_id, page.getPageLSN());
        }
        page.WUnlock();
    }
    // If page was evicted between our data copy and now, it's okay. The DPT entry will
    // be handled by the next checkpoint or a subsequent flush of the re-loaded page.
}

bool SimpleBufferPoolManager::flushAllPages() {
    LOG_INFO("[BPM] flushAllPages() called.");
    bool all_success = true;
    
    std::vector<PageId> page_ids_to_try_flush;
    page_ids_to_try_flush.reserve(pool_size_); 

    for (size_t i = 0; i < NUM_PAGE_TABLE_SHARDS; ++i) {
        std::lock_guard<std::mutex> pt_lock(page_table_latches_[i]);
        for (auto const& [page_id_in_map, frame_idx_unused] : page_table_) {
            if (getPageTableShardIndex(page_id_in_map) == i) {
                page_ids_to_try_flush.push_back(page_id_in_map);
            }
        }
    }
    LOG_TRACE("  [BPM flushAllPages] Collected {} page IDs to attempt flushing.", page_ids_to_try_flush.size());

    for (PageId pid_to_flush : page_ids_to_try_flush) {
        if (!flushPage(pid_to_flush)) {
            LOG_ERROR("  [BPM flushAllPages] Failed to flush page {} during flushAllPages.", pid_to_flush);
            all_success = false;
        }
    }
    LOG_INFO("[BPM] flushAllPages() finished. Overall success: {}", all_success);
    return all_success;
}

void SimpleBufferPoolManager::setWALManager(ShardedWALManager* wal_mgr) {
    wal_manager_ = wal_mgr;
    LOG_INFO("[BPM] ShardedWALManager instance has been set.");
}