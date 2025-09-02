// src/btree_helpers.cpp
#include "../include/btree.h"
#include <algorithm> // For std::sort in compactInternalPage (if needed)

// This namespace contains helpers that are part of the BTree class but implemented here.
// In the header (btree.h), these would be declared as private member functions.

int BTree::findLeafInsertionPoint(Page* page, const std::string& key) const {
    const NodeHeader* header = getHeader(page);
    if (header->key_count == 0) return 0;

    // Binary search for insertion point (first element >= key)
    int low = 0, high = header->key_count - 1;
    int result = header->key_count;

    while (low <= high) {
        int mid = low + (high - low) / 2;
        std::string mid_key = getLeafKey(page, mid);
        if (mid_key >= key) {
            result = mid;
            high = mid - 1;
        } else {
            low = mid + 1;
        }
    }
    return result;
}

bool BTree::leafHasSpaceFor(Page* page, size_t key_len, size_t val_len) const {
    const NodeHeader* header = getHeader(page);
    if (header->key_count >= MAX_LEAF_KEYS) return false;

    // Space needed for new offsets in the fixed-size area
    size_t required_fixed_space = KEY_OFFSET_SIZE + VAL_OFFSET_SIZE;
    // Space needed for new data in the variable-size area
    size_t required_variable_space = key_len + val_len;

    // The available space is the gap between the end of the fixed-size area and
    // the start of the variable-size area.
    size_t fixed_area_end = DATA_START_OFFSET + (header->key_count * (KEY_OFFSET_SIZE + VAL_OFFSET_SIZE));
    
    // Check if the new free_space_pointer would collide with the fixed area after adding the new offsets
    return (header->free_space_pointer >= (fixed_area_end + required_fixed_space + required_variable_space));
}


// --- Compaction and Utility Helpers ---

// Helper to calculate the actual used space by variable data.
// The difference between (PAGE_LOGICAL_SIZE - free_space_pointer) and this value is the fragmented space.
static size_t calculateUsedVariableSpace(const BTree* tree, const Page* page) {
    const BTree::NodeHeader* header = tree->getHeader(page);
    if (header->key_count == 0) return 0;

    size_t total_var_size = 0;
    if (tree->isLeaf(page)) {
        for (int i = 0; i < header->key_count; ++i) {
            // A simplified way to get length without full string construction
            const BTree::off_t* key_offsets = tree->getLeafKeyOffsets(page);
            const BTree::off_t* val_offsets = tree->getLeafValueOffsets(page);
            total_var_size += (val_offsets[i] - key_offsets[i]); // Key length
            
            BTree::off_t val_end = (i > 0) ? key_offsets[i - 1] : BTree::PAGE_LOGICAL_SIZE;
            total_var_size += (val_end - val_offsets[i]); // Value length
        }
    } else { // Internal node
        const BTree::off_t* key_offsets = tree->getInternalKeyOffsets(page);
        for (int i = 0; i < header->key_count; ++i) {
            BTree::off_t key_end = (i + 1 < header->key_count) ? key_offsets[i + 1] : tree->getFreeSpacePointer(page);
            total_var_size += (key_end - key_offsets[i]);
        }
    }
    return total_var_size;
}

bool BTree::shouldCompactAfterDelete(const Page* page) const {
    const NodeHeader* header = getHeader(page);
    if (header->key_count == 0) return true; // Always compact if page becomes empty

    size_t total_var_space_allocated = PAGE_LOGICAL_SIZE - header->free_space_pointer;
    size_t actual_var_space_used = calculateUsedVariableSpace(this, page);
    size_t fragmented_space = total_var_space_allocated - actual_var_space_used;

    // Heuristic: Compact if fragmented space is more than 30% of the total allocated variable space,
    // and is larger than a minimum threshold to avoid compacting for a few bytes.
    return (fragmented_space > 128) && (fragmented_space > total_var_space_allocated * 0.30);
}

// NOTE: Implementations for compactLeafPage and compactInternalPage from btree.cpp should be moved here.
// (Their existing logic is fine, they just need to be moved).