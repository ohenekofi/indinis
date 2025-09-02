// @FILENAME include/disk_manager.h
#pragma once

#include "types.h" // For PageId, INVALID_PAGE_ID
#include "storage_error/storage_error.h"
#include "debug_utils.h"
#include <string>
#include <fstream>
#include <vector>
#include <mutex>
#include <stdexcept> // Base for custom errors if not using StorageError directly
#include <map>
#include <set>       // For getFreePages
#include <functional>// For DefragmentationCallback

// Forward declaration for StorageError if used in method signatures
namespace storage { class StorageError; }

class SimpleDiskManager {
public:
    static constexpr size_t PAGE_SIZE = 4096;
    static constexpr size_t METADATA_FIXED_AREA_SIZE = 4096; // 1 page for metadata

    #pragma pack(push, 1)
    struct FreePageListData {
        PageId next_free_page_id = INVALID_PAGE_ID;
    };
    #pragma pack(pop)
    static_assert(sizeof(FreePageListData) <= PAGE_SIZE, "FreePageListData must fit in a page.");

    struct DefragmentationStats {
        size_t total_pages = 0;
        size_t allocated_pages = 0;
        size_t free_pages = 0;
        size_t fragmentation_gaps = 0;
        size_t largest_gap = 0;
        size_t potential_pages_saved = 0;
        double fragmentation_ratio = 0.0;
        // Removed file_size_bytes from here
    };

    struct DefragmentationProgress {
        double progress_percentage;
        std::string status_message;
        DefragmentationStats current_stats;
    };

    enum class DefragmentationMode {
        CONSERVATIVE,
        AGGRESSIVE
    };

    using DefragmentationCallback = std::function<void(const DefragmentationProgress&)>;

    // **NEW: Define DatabaseStats struct publicly within SimpleDiskManager**
    struct DatabaseStats {
        size_t file_size_bytes = 0;
        PageId total_pages = 0;
        PageId allocated_pages = 0;
        PageId free_pages = 0;
        PageId next_page_id = 0; // Next ID to be allocated if freelist is empty
        size_t num_btrees = 0;
        double utilization_ratio = 0.0; // allocated_pages / total_pages
    };

private:
    std::string db_filename_;
    std::fstream db_file_;
    std::mutex file_mutex_;

    struct Metadata {
        PageId next_page_id_to_allocate_if_freelist_empty_ = 1;
        PageId freelist_head_page_id_ = INVALID_PAGE_ID;
        std::map<std::string, PageId> btree_roots_;
        Metadata() : next_page_id_to_allocate_if_freelist_empty_(1), freelist_head_page_id_(INVALID_PAGE_ID) {}
    };

    void writeMetadataInternal(const Metadata& meta);
    Metadata readMetadataInternal();
    std::set<PageId> getFreePagesInternal();
    std::vector<PageId> getAllocatedPagesInternal();

public:
    explicit SimpleDiskManager(const std::string& db_file);
    ~SimpleDiskManager();

    SimpleDiskManager(const SimpleDiskManager&) = delete;
    SimpleDiskManager& operator=(const SimpleDiskManager&) = delete;
    SimpleDiskManager(SimpleDiskManager&&) = delete;
    SimpleDiskManager& operator=(SimpleDiskManager&&) = delete;

    void readPage(PageId page_id, char* page_data);
    void writePage(PageId page_id, const char* page_data);
    PageId allocatePage();
    void deallocatePage(PageId page_id);

    void setBTreeRoot(const std::string& btree_name, PageId root_page_id);
    PageId getBTreeRoot(const std::string& btree_name);
    bool removeBTreeRoot(const std::string& btree_name);
    std::vector<std::string> listBTrees();

    DefragmentationStats analyzeFragmentation();
    bool defragment(DefragmentationMode mode = DefragmentationMode::CONSERVATIVE,
                    DefragmentationCallback callback = nullptr);

    void flushToDisk();
    DatabaseStats getDatabaseStats(); // **Return type is now correctly defined**
    bool verifyFreeList();
};