// @include/btree.h
#pragma once

// Include dependencies FIRST
#include "types.h" // Provides PageId, INVALID_PAGE_ID, TxnId, etc.
#include "buffer_pool_manager.h" // Provides Page, SimpleBufferPoolManager

#include <string>
#include <vector>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <utility>
#include <stdexcept>
#include <map>
#include <cstring>
#include <algorithm>
#include <iostream>
#include <limits>
#include <functional>
#include <cmath> // For std::ceil

// In btree.h (before class BTree)

// Forward declare Page if not already visible or move guards after Page definition
// class Page; // If Page is defined later or in another header not yet included.
// Since buffer_pool_manager.h (which defines Page) is included by btree.h, this is fine.

class PageWriteLatchGuard {
public:
    explicit PageWriteLatchGuard(Page* page) : page_(page), latched_(false) {
        if (page_) {
            page_->WLatch();
            latched_ = true;
        }
    }
    ~PageWriteLatchGuard() {
        if (latched_ && page_) {
            // It's good practice to log if a latch is released, similar to acquisition
            // LOG_TRACE("Page ", page_->getPageId(), " WUnlock() via guard destructor.");
            page_->WUnlock();
        }
    }
    PageWriteLatchGuard(const PageWriteLatchGuard&) = delete;
    PageWriteLatchGuard& operator=(const PageWriteLatchGuard&) = delete;

    PageWriteLatchGuard(PageWriteLatchGuard&& other) noexcept : page_(other.page_), latched_(other.latched_) {
        other.page_ = nullptr;
        other.latched_ = false;
    }
    PageWriteLatchGuard& operator=(PageWriteLatchGuard&& other) noexcept {
        if (this != &other) {
            if (latched_ && page_) {
                    // LOG_TRACE("Page ", page_->getPageId(), " WUnlock() via guard move assignment.");
                    page_->WUnlock();
            }
            page_ = other.page_;
            latched_ = other.latched_;
            other.page_ = nullptr;
            other.latched_ = false;
        }
        return *this;
    }

    void release() { // Allow manual release before destruction if needed
        if (latched_ && page_) {
            // LOG_TRACE("Page ", page_->getPageId(), " WUnlock() via guard release().");
            page_->WUnlock();
        }
        latched_ = false;
        // page_ = nullptr; // Keep page_ to indicate what it *was* guarding, but don't unlock again
    }

    Page* get() const { return page_; }
    bool isLatched() const { return latched_; }

private:
    Page* page_;
    bool latched_;
};

class PageReadLatchGuard {
public:
    explicit PageReadLatchGuard(Page* page) : page_(page), latched_(false) {
        if (page_) {
            page_->RLatch();
            latched_ = true;
        }
    }
    ~PageReadLatchGuard() {
        if (latched_ && page_) {
            // LOG_TRACE("Page ", page_->getPageId(), " RUnlock() via guard destructor.");
            page_->RUnlock();
        }
    }
    PageReadLatchGuard(const PageReadLatchGuard&) = delete;
    PageReadLatchGuard& operator=(const PageReadLatchGuard&) = delete;

    PageReadLatchGuard(PageReadLatchGuard&& other) noexcept : page_(other.page_), latched_(other.latched_) {
        other.page_ = nullptr;
        other.latched_ = false;
    }
    PageReadLatchGuard& operator=(PageReadLatchGuard&& other) noexcept {
        if (this != &other) {
            if (latched_ && page_) {
                // LOG_TRACE("Page ", page_->getPageId(), " RUnlock() via guard move assignment.");
                page_->RUnlock();
            }
            page_ = other.page_;
            latched_ = other.latched_;
            other.page_ = nullptr;
            other.latched_ = false;
        }
        return *this;
    }

    void release() {
        if (latched_ && page_) {
            // LOG_TRACE("Page ", page_->getPageId(), " RUnlock() via guard release().");
            page_->RUnlock();
        }
        latched_ = false;
        // page_ = nullptr;
    }

    const Page* get() const { return page_; } // Const access to page
    bool isLatched() const { return latched_; }


private:
    Page* page_;
    bool latched_;
};

// Now, the BTree class definition follows...
// B+ Tree implementation using BufferPoolManager
class BTree {
public:
    // --- Configuration & Types ---
    static constexpr size_t PAGE_LOGICAL_SIZE = SimpleDiskManager::PAGE_SIZE - ON_DISK_PAGE_HEADER_SIZE; // Adjusted
    static constexpr int MAX_LEAF_KEYS = 32;         // Definition here
    static constexpr int MAX_INTERNAL_KEYS = 48;     // Definition here
    // Calculate minimums based on standard B+ Tree definition (ceiling) using integer math
    static constexpr int MIN_LEAF_KEYS = (MAX_LEAF_KEYS + 1) / 2;
    static constexpr int MIN_INTERNAL_KEYS = (MAX_INTERNAL_KEYS + 1) / 2;

    static constexpr int MAX_INTERNAL_CHILDREN = MAX_INTERNAL_KEYS + 1;
    static constexpr size_t PAGE_SIZE = SimpleDiskManager::PAGE_SIZE;
    static constexpr int MAX_COALESCE_ATTEMPTS = 3;
    
    //static constexpr char const* INDEX_TOMBSTONE = ""; // Defined here
    bool isLeaf(const Page* page) const { return getHeader(page)->is_leaf; }
    int findKeyIndex(const Page* page, const std::string& key) const;
    int getKeyCount(const Page* page) const { return getHeader(page)->key_count; }
    std::string getLeafKey(const Page* page, int index) const;
    void unpinPage(PageId page_id, bool is_dirty, LSN operation_lsn); // NEW
    bool leafHasSpace(const Page* page, size_t key_len, size_t val_len) const;
    void insertLeafEntry(Page* page, int index, const std::string& key, const std::string& value);
    Page* splitLeaf(Page* leaf_page);
    PageId getInternalChildId(const Page* page, int index) const;
    Page* fetchPage(PageId page_id);
    bool isFull(const Page* page) const;
    Page* splitInternal(Page* internal_page, std::string& middle_key /* out */);
    bool internalHasSpace(const Page* page, size_t key_len) const; // Add this if it was missing
    bool coalesceOrRedistribute(PageId underflow_node_id, bool node_is_actually_underflowed); // NEW
    //Page* findLeafPage(const std::string& key); // This is the non-path-locking one
    Page* findLeafPageWithReadPathLocking(const std::string& key, std::vector<PageReadLatchGuard>& path_latches);
    Page *findLeftmostLeaf() const;

    std::pair<Page *, int> findStartPosition(const std::string &start_key_part, bool start_inclusive) const;

    struct NodeHeader {
        bool is_leaf = false;
        int key_count = 0;
        PageId parent_page_id = INVALID_PAGE_ID;
        PageId next_page_or_sibling_id = INVALID_PAGE_ID;
        uint16_t free_space_pointer = 0;
    };

    std::vector<std::pair<std::string, std::string>> paginatedScan(
        const std::vector<OrderByClause>& orderBy,
        const std::optional<std::vector<ValueType>>& startCursor,
        bool startExclusive,
        const std::optional<std::vector<ValueType>>& endCursor,
        bool endExclusive,
        TxnId reader_txn_id
    );

    std::optional<std::pair<std::string, std::string>> findFirstVisibleEntry(
    const std::string& original_index_key_part, 
    TxnId reader_txn_id) const;

    using off_t = uint16_t;
    static constexpr size_t HEADER_SIZE = sizeof(NodeHeader);
    static constexpr size_t DATA_START_OFFSET = HEADER_SIZE;
    static constexpr size_t KEY_OFFSET_SIZE = sizeof(off_t);
    static constexpr size_t VAL_OFFSET_SIZE = sizeof(off_t);
    static constexpr size_t PAGEID_SIZE = sizeof(PageId);

private:
    std::string index_name_;
    PageId root_page_id_ = INVALID_PAGE_ID;         // Correct non-static member with initializer
    mutable std::shared_mutex root_pointer_mutex_;
    SimpleBufferPoolManager& bpm_;
    //mutable std::shared_mutex tree_structure_mutex_; // Renamed

    bool compression_enabled_;

        // New recursive helper for pessimistic remove
    static bool removeRecursive(
        BTree* tree_instance,
        PageId current_page_id,
        const std::string& key,
        std::vector<PageWriteLatchGuard>& path_latches);

    // Helper to handle underflow after a key has been removed from a W-Latched page.
    // The 'page_under_scrutiny_ptr' is the page that might be underflowing.
    // Its parent is W-latched by the last guard in 'path_latches_to_parent'.
    // Returns true if structure was modified by C/R.
    static bool handleUnderflowAfterRemove(
        BTree* tree_instance,
        Page* page_under_scrutiny_ptr, // W-Latched page that was just modified
        std::vector<PageWriteLatchGuard>& path_latches_to_parent // Latches up to its parent
    );

    std::string encodeCursorValuesToKeyPart(
        const std::vector<OrderByClause>& orderBy,
        const std::vector<ValueType>& cursorValues
    ) const;

    // --- Helper methods for page layout and data access ---
    static Page* insertRecursive(
        BTree* tree_instance, // Pass 'this' from the public method
        PageId current_page_id,
        const std::string& key,
        const std::string& value,
        std::vector<PageWriteLatchGuard>& path_latches);

    // Helper to insert a key (from a child split) into a parent that is already W-latched.
    static void insertKeyIntoLatchedParent(
        BTree* tree_instance,
        Page* parent_page_ptr,
        const std::string& key_from_child_split,
        PageId new_child_sibling_id,
        const std::string& original_insert_key,    // Make sure this is const std::string&
        const std::string& original_insert_value,  // Make sure this is const std::string&
        std::vector<PageWriteLatchGuard>& path_latches);


    // Header Access (Added const overload)
    NodeHeader* getHeader(Page* page) const { return reinterpret_cast<NodeHeader*>(page->getData()); }
    const NodeHeader* getHeader(const Page* page) const { return reinterpret_cast<const NodeHeader*>(page->getData()); } // Const overload
    uint16_t getFreeSpacePointer(const Page* page) const { return getHeader(page)->free_space_pointer; }
    void setFreeSpacePointer(Page* page, uint16_t ptr) { getHeader(page)->free_space_pointer = ptr; }

    // Leaf Specific Layout & Access (Added const overloads)
    off_t* getLeafKeyOffsets(Page* page) const { return reinterpret_cast<off_t*>(page->getData() + DATA_START_OFFSET); }
    const off_t* getLeafKeyOffsets(const Page* page) const { return reinterpret_cast<const off_t*>(page->getData() + DATA_START_OFFSET); } // Const overload
    off_t* getLeafValueOffsets(Page* page) const { return reinterpret_cast<off_t*>(page->getData() + DATA_START_OFFSET + MAX_LEAF_KEYS * KEY_OFFSET_SIZE); }
    const off_t* getLeafValueOffsets(const Page* page) const { return reinterpret_cast<const off_t*>(page->getData() + DATA_START_OFFSET + MAX_LEAF_KEYS * KEY_OFFSET_SIZE); } // Const overload

    std::string getLeafValue(const Page* page, int index) const;
    size_t getLeafFreeSpace(const Page* page) const;

    std::string extractFieldKeyPart(const std::string& composite_key) const;

    void initLeafPage(Page* page, PageId parent_id = INVALID_PAGE_ID);
    off_t addLeafVariableData(Page* page, const std::string& data);

    void removeLeafEntry(Page* page, int index);

    // Internal Specific Layout & Access (Added const overloads)
    PageId* getInternalChildIds(Page* page) const { return reinterpret_cast<PageId*>(page->getData() + DATA_START_OFFSET); }
    const PageId* getInternalChildIds(const Page* page) const { return reinterpret_cast<const PageId*>(page->getData() + DATA_START_OFFSET); } // Const overload
    off_t* getInternalKeyOffsets(Page* page) const { return reinterpret_cast<off_t*>(page->getData() + DATA_START_OFFSET + MAX_INTERNAL_CHILDREN * PAGEID_SIZE); }
    const off_t* getInternalKeyOffsets(const Page* page) const { return reinterpret_cast<const off_t*>(page->getData() + DATA_START_OFFSET + MAX_INTERNAL_CHILDREN * PAGEID_SIZE); } // Const overload
    std::string getInternalKey(const Page* page, int index) const;
 
    void setInternalChildId(Page* page, int index, PageId child_id);
    size_t getInternalFreeSpace(const Page* page) const;
    void initInternalPage(Page* page, PageId parent_id = INVALID_PAGE_ID);
    off_t addInternalVariableData(Page* page, const std::string& data);
    void insertInternalEntry(Page* page, int index, const std::string& key, PageId right_child_id);
    void removeInternalEntry(Page* page, int index);

    // General Page operations
    void setKeyCount(Page* page, int count) { getHeader(page)->key_count = count; }

    
    PageId getParentId(const Page* page) const { return getHeader(page)->parent_page_id; }
    void setParentId(Page* page, PageId parent_id) { getHeader(page)->parent_page_id = parent_id; }
    PageId getNextLeafId(const Page* page) const;
    void setNextLeafId(Page* page, PageId next_id);
    bool isRootPage(const Page* page) const { return getParentId(page) == INVALID_PAGE_ID; }

    bool hasMinimumKeys(const Page* page) const;

    // Page reorganization helpers
    void compactLeafPage(Page* page);
    void compactInternalPage(Page* page);

    // --- Private B+ Tree Operations ---
 
    Page* newPage(PageId& page_id /* out */);
    void deletePage(PageId page_id);

    void startNewTree(const std::string& key, const std::string& value);
    //void insert(const std::string& key, const std::string& value);
    //void insertIntoPage(Page* page, const std::string& key, const std::string& value);
    //void insertIntoParent(Page* old_node_page, const std::string& key, PageId new_node_page_id);

   
    // Made findLeafPage non-const as it modifies pin counts via fetch/unpin
    Page* findLeafPage(const std::string& key);
  


    //void remove(const std::string& key);
    //bool removeFromPage(PageId page_id, const std::string& key);
    void removeEntry(Page* page, int index);
    //bool coalesceOrRedistribute(Page* node_page);
    bool redistribute(Page* node_page /*recipient*/, Page* neighbor_page /*donor*/, bool from_left_sibling);
    bool coalesce(Page* target_node /*receives entries*/, Page* source_node /*gets deleted*/, bool source_is_right_sibling);
    void adjustRoot();
    int getChildIndexInParent(Page* parent_page, PageId child_id);
    std::string findMinKey(PageId subtree_root_id);
    std::string findMaxKey(PageId subtree_root_id);
    void updateChildParentPointers(Page* page);
    void updateRootIdInMeta(PageId new_root_id);
    void printTreeRecursive(PageId page_id, int level, std::vector<bool>& level_markers) const;
    // New or refined helpers for insertion logic:
    PageId splitRoot(Page* old_root_page_wlatched); // Splits the root, assumes tree_mutex_ is held
    //void insertRecursiveAndHandleSplits(PageId current_page_id, const std::string& key, const std::string& value); // Assumes tree_mutex_ is held

public:
   BTree(const std::string& name, SimpleBufferPoolManager& bpm, bool enable_compression = false); 
    ~BTree();

    std::string getName() const { return index_name_; }
    
    PageId getRootPageId() const { return root_page_id_; }
    static std::string encodeTxnIdDescending(TxnId txn_id);
    static TxnId decodeTxnIdDescending(const std::string& encoded);
    static std::string createCompositeKey(const std::string& encoded_index_key_part, const std::string& primary_key, TxnId txn_id);
    static std::optional<std::tuple<std::string, std::string, TxnId>> decodeCompositeKey(const std::string& composite_key);
    static constexpr char const* INDEX_TOMBSTONE = ""; // Define it here
    //static constexpr int MAX_LEAF_KEYS = 32;
    //static constexpr int MAX_INTERNAL_KEYS = 48;

    // --- Public API ---
    //void applyUpdate(const std::string& original_key, TxnId txn_id, const std::string& value);
    void insertEntry(const std::string& composite_key, const std::string& value);
    bool removeKeyPhysical(const std::string& composite_key);
    // Made find const again, will use const_cast internally for page fetching/unpinning
    std::optional<std::string> find(const std::string& original_index_key, TxnId reader_txn_id) const;
    std::vector<std::pair<std::string, std::string>> prefixScan(
        const std::string& original_index_key_prefix,
        TxnId reader_txn_id,
        size_t limit = 0
    ) const;


    
    /**
     * @brief Performs a range scan on the B-Tree index.
     *
     * Retrieves all versions of index entries where the key part (excluding TxnId)
     * falls within the specified range [start_key_part, end_key_part]. It is the
     * caller's responsibility to handle MVCC visibility and select the correct
     * version for each document.
     *
     * @param start_key_part The beginning of the key range.
     * @param end_key_part The end of the key range.
     * @return A vector of pairs, where each pair contains a raw composite B-Tree key and its value (primary key).
     */
    std::vector<std::pair<std::string, std::string>> rangeScanKeys(const std::string &start_key_part, const std::string &end_key_part, bool start_inclusive, bool end_inclusive) const;


    // --- Debugging ---
    void printTree() const;
    void printPage(PageId page_id) const;
    size_t getPageSize() const { return SimpleDiskManager::PAGE_SIZE; }

}; // End class BTree
