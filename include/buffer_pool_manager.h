// @filename: include/buffer_pool_manager.h

#pragma once

#include "disk_manager.h" // For SimpleDiskManager::PAGE_SIZE
#include "types.h"        // For PageId, LSN, INVALID_PAGE_ID, OnDiskPageHeader (which includes CompressionType, EncryptionScheme)
#include <vector>
#include <unordered_map>
#include <list>
#include <mutex>          // For std::mutex, std::lock_guard, std::unique_lock
#include <shared_mutex>   // For std::shared_mutex (used in Page class)
#include <condition_variable>
#include <atomic>
#include <optional> 
#include <cstring>        // For std::memset
#include <iostream>       // For logging (optional, if not using debug_utils.h directly here)
#include <functional>     // For std::hash in sharded latch
#include "../include/debug_utils.h" // For LOG_ macros

// Forward declaration for WALManager if BPM uses it directly
//class ExtWALManager; 
class ShardedWALManager; 
// --- Page Class ---
// Represents a page in the buffer pool.
// Its internal data_ buffer ALWAYS holds UNCOMPRESSED, UNENCRYPTED logical page content.
class Page {
    friend class SimpleBufferPoolManager; // Allows BPM to access private members like page_id_ directly
    friend class BTree; // If BTree needs direct access to Page internals not via BPM (generally avoid)

public:
    // The size of the logical data this page object holds.
    // This must be SimpleDiskManager::PAGE_SIZE - ON_DISK_PAGE_HEADER_SIZE.
    static constexpr size_t LOGICAL_CONTENT_SIZE = SimpleDiskManager::PAGE_SIZE - ON_DISK_PAGE_HEADER_SIZE;

private:
    char data_[LOGICAL_CONTENT_SIZE]; // Buffer for uncompressed, unencrypted logical page data
    PageId page_id_ = INVALID_PAGE_ID;
    std::atomic<int> pin_count_{0};
    bool is_dirty_ = false;
    mutable std::shared_mutex page_latch_; // Per-page latch for R/W access to its content
    LSN page_lsn_ = 0;                  // LSN of the last log record that modified this page in memory

public:
    Page() { 
        resetMemory(); 
    }

    // Access to page data (uncompressed, unencrypted)
    char* getData() { return data_; }
    const char* getData() const { return data_; }

    PageId getPageId() const { return page_id_; }
    void setPageId(PageId id) { page_id_ = id; }

    bool isDirty() const { return is_dirty_; }
    void setDirty(bool dirty) { is_dirty_ = dirty; }
    
    int getPinCount() const { return pin_count_.load(std::memory_order_relaxed); }
    
    // Page Latching (protects access to page's content and metadata like is_dirty_, page_lsn_)
    void RLatch() const { page_latch_.lock_shared(); } // Made const as it protects readable state
    void RUnlock() const { page_latch_.unlock_shared(); } // Made const
    void WLatch() { page_latch_.lock(); }
    void WUnlock() { page_latch_.unlock(); }
    
    // Pin Counting (atomic, protects page from eviction)
    int incrementPinCount() { return pin_count_.fetch_add(1, std::memory_order_acq_rel) + 1; }
    int decrementPinCount() {
         int prev = pin_count_.fetch_sub(1, std::memory_order_acq_rel);
         if (prev <= 0) { // Should not be < 0 if logic is correct
            if (page_id_ != INVALID_PAGE_ID && prev < 0) {
                LOG_WARN("[Page {}] Pin count decremented below zero! (was {})", page_id_, prev - 1);
            }
            pin_count_.store(0, std::memory_order_release); // Correct to 0 if it went negative
            return 0;
         }
         return prev - 1;
    }

    LSN getPageLSN() const { return page_lsn_; }
    void setPageLSN(LSN lsn) { page_lsn_ = lsn; }

    void resetMemory() { std::memset(data_, 0, LOGICAL_CONTENT_SIZE); }
};


// --- SimpleBufferPoolManager Class ---
class SimpleBufferPoolManager {
public: 
    SimpleDiskManager& disk_manager_; 

private:
    size_t pool_size_;
    std::vector<Page> pages_; // Each Page object's data_ buffer holds UNCOMPRESSED LOGICAL data
    
    static constexpr size_t NUM_PAGE_TABLE_SHARDS = 16; 
    std::vector<std::mutex> page_table_latches_; // Protects shards of page_table_
    mutable std::mutex free_list_replacer_latch_; // Protects free_list_, clock_hand_, recently_referenced_, victim_pinned_, dirty_page_table_

    std::unordered_map<PageId, size_t> page_table_; // Maps PageId to frame index in pages_ vector
    std::list<size_t> free_list_;                   // List of free frame indices
    std::vector<bool> recently_referenced_;         // For Clock replacer (one per frame)
    std::vector<bool> victim_pinned_;               // For Clock replacer (true if frame's page_pin_count > 0)
    size_t clock_hand_;                             // For Clock replacer

    ShardedWALManager* wal_manager_ = nullptr;             // Pointer to WAL Manager (not owning)
    std::unordered_map<PageId, LSN> dirty_page_table_; // PageID -> LSN of first log record that made it dirty (RecoveryLSN)

    // --- NEW: Compression Related Members ---
    bool compression_is_active_ = false;
    CompressionType compression_type_ = CompressionType::NONE;
    int compression_level_ = 0;

    // --- Encryption Related Members ---
    bool encryption_is_active_ = false;                // Is database-level encryption active?
    std::vector<unsigned char> database_dek_;        // Copy of the Data Encryption Key
    std::vector<unsigned char> database_global_salt_;  // Copy of the global salt (e.g., for PBKDF2)
    EncryptionScheme default_encryption_scheme_ = EncryptionScheme::NONE; // NEW: Scheme for NEW pages


    // --- Private Helper Methods ---
    size_t getPageTableShardIndex(PageId page_id) const {
        return std::hash<PageId>{}(page_id) % NUM_PAGE_TABLE_SHARDS;
    }
    std::optional<size_t> findVictimFrame(); // Clock replacement algorithm helper

        // --- NEW PRIVATE HELPER METHODS (Refactored from flushPage) ---
    
    // Step 1: Safely read the page's dirty status, LSN, and logical data.
    struct PageFlushData {
        bool is_dirty;
        LSN page_lsn;
        std::vector<uint8_t> logical_data;
    };
    std::optional<PageFlushData> getPageDataForFlush(PageId page_id);

    // Step 2 & 3: Process the logical data (compress, encrypt) into a final disk-ready payload.
    struct PreparedPagePayload {
        std::vector<uint8_t> payload;
        OnDiskPageHeader header;
    };
    PreparedPagePayload preparePagePayloadForDisk(const std::vector<uint8_t>& logical_data, LSN page_lsn);

    // Step 4 & 5: Assemble the full physical page and update BPM metadata after a successful write.
    void writePayloadToDiskAndUpdateMetadata(PageId page_id, const PreparedPagePayload& prepared_page);

public:
    // Constructor updated to accept encryption parameters
 SimpleBufferPoolManager(size_t pool_size,
                            SimpleDiskManager& disk_manager_ref,
                            // --- NEW Compression Parameters ---
                            bool page_compression_enabled,
                            CompressionType compression_type_for_pages,
                            int compression_level_for_pages,
                            // --- Existing Encryption Parameters ---
                            bool overall_encryption_enabled,
                            EncryptionScheme scheme_for_new_pages,
                            const std::vector<unsigned char>& dek_passed,
                            const std::vector<unsigned char>& global_salt_passed);

    ~SimpleBufferPoolManager();

    // Deleted copy/move constructors and assignment operators
    SimpleBufferPoolManager(const SimpleBufferPoolManager&) = delete;
    SimpleBufferPoolManager& operator=(const SimpleBufferPoolManager&) = delete;
    SimpleBufferPoolManager(SimpleBufferPoolManager&&) = delete;
    SimpleBufferPoolManager& operator=(SimpleBufferPoolManager&&) = delete;

    size_t getDirtyPageCount() const; // Counts pages marked as dirty in the pool

    // Main BPM operations
    Page* fetchPage(PageId page_id); // Fetches a page, handles disk read, decryption, decompression
    bool unpinPage(PageId page_id, bool is_dirty, LSN page_op_lsn); // Unpins a page, marks dirty if needed
    Page* newPage(PageId& new_page_id /* out */); // Allocates a new page on disk and in buffer pool
    bool deletePage(PageId page_id); // Deallocates page on disk and removes from buffer pool

    // Flushing operations
    bool flushPage(PageId page_id);   // Flushes a specific page (handles compression, encryption, disk write)
    bool flushAllPages();           // Flushes all dirty pages in the buffer pool

    void setWALManager(ShardedWALManager* wal_mgr); // Sets the WAL manager instance
};