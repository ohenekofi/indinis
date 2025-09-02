// @src/btree.cpp
#include "../include/btree.h" // Includes types, bpm, disk manager implicitly
#include "../include/debug_utils.h"
#include "../include/storage_error/storage_error.h"
#include "../include/storage_error/error_codes.h"
#include "../include/encoding_utils.h"

#include <vector>
#include <iostream>
#include <iomanip> // For formatting print output
#include <cstring>
#include <algorithm>
#include <limits>
#include <functional>
#include <cmath>        // For std::ceil
#include <sstream>      // For printing binary data safely
#include <atomic>

// Platform-specific byte swap includes/macros (ensure these are correct)
#ifdef _WIN32
    #ifndef WIN32_LEAN_AND_MEAN
        #define WIN32_LEAN_AND_MEAN
    #endif
    #include <winsock2.h> // For byte swap functions like ntohll/htonll if available, or manual
    // Manual htobe64/be64toh if needed (Example for Little Endian)
    #if !defined(htobe64) // Avoid redefinition if provided elsewhere
        inline uint64_t htobe64(uint64_t host_64) {
            #if BYTE_ORDER == LITTLE_ENDIAN
                return _byteswap_uint64(host_64); // Windows specific intrinsic
            #else
                 return host_64;
            #endif
        }
        inline uint64_t be64toh(uint64_t big_endian_64) {
            #if BYTE_ORDER == LITTLE_ENDIAN
                 return _byteswap_uint64(big_endian_64);
            #else
                 return big_endian_64;
            #endif
        }
    #endif // !defined(htobe64)
#else // Linux/macOS etc.
    #include <endian.h> // Provides htobe64/be64toh usually
    #ifndef htobe64 // Define manually if endian.h doesn't have them (older systems)
        #if __BYTE_ORDER == __LITTLE_ENDIAN
            #include <byteswap.h>
            #define htobe64(x) bswap_64(x)
            #define be64toh(x) bswap_64(x)
        #else
            #define htobe64(x) (x)
            #define be64toh(x) (x)
        #endif
    #endif // htobe64
#endif // _WIN32

// --- Encoding/Decoding Helpers ---
std::string BTree::encodeTxnIdDescending(TxnId txn_id) {
    uint64_t be_val = htobe64(txn_id);
    // Invert bits to make comparison lexicographical descending
    uint64_t inverted_val = ~be_val;
    return std::string(reinterpret_cast<const char*>(&inverted_val), sizeof(inverted_val));
}

// Helper function to get the end of the current fixed payload for leaf pages
static inline size_t getCurrentLeafFixedPayloadEnd(const BTree::NodeHeader* header) {
    return BTree::DATA_START_OFFSET
           + header->key_count * BTree::KEY_OFFSET_SIZE
           + header->key_count * BTree::VAL_OFFSET_SIZE;
}

std::string BTree::encodeCursorValuesToKeyPart(
    const std::vector<OrderByClause>& orderBy,
    const std::vector<ValueType>& cursorValues) const 
{
    std::string compositeKeyPart;
    for (size_t i = 0; i < orderBy.size(); ++i) {
        std::string encoded_segment = Indinis::encodeValueToStringOrderPreserving(cursorValues[i]);
        if (orderBy[i].direction == IndexSortOrder::DESCENDING) {
            for (char& c : encoded_segment) { c = ~c; }
        }
        compositeKeyPart.append(encoded_segment);
    }
    return compositeKeyPart;
}

std::string BTree::extractFieldKeyPart(const std::string& composite_key) const {
    size_t txn_id_size = sizeof(TxnId);
    if (composite_key.length() < txn_id_size) {
        // This indicates a malformed key, log a warning and return empty.
        LOG_WARN("[BTree::extractFieldKeyPart] Composite key is too short to contain a TxnId. Key hex: {}", hex_dump_string(composite_key));
        return "";
    }
    // The field part is everything *except* the last 8 bytes.
    return composite_key.substr(0, composite_key.length() - txn_id_size);
}

std::vector<std::pair<std::string, std::string>> BTree::paginatedScan(
    const std::vector<OrderByClause>& orderBy,
    const std::optional<std::vector<ValueType>>& startCursor,
    bool startExclusive,
    const std::optional<std::vector<ValueType>>& endCursor,
    bool endExclusive,
    TxnId reader_txn_id)
{
    LOG_INFO("--- [BTree::paginatedScan START] ---");
    LOG_INFO("  - ReaderTxnID: {}, StartExclusive: {}, EndExclusive: {}", reader_txn_id, startExclusive, endExclusive);

    std::vector<std::pair<std::string, std::string>> visible_candidate_keys;
    BTree* non_const_this = const_cast<BTree*>(this);

    // Encode cursors to get the start and end key parts for the B-Tree scan.
    std::string start_key_part = startCursor ? encodeCursorValuesToKeyPart(orderBy, *startCursor) : "";
    std::string end_key_part = endCursor ? encodeCursorValuesToKeyPart(orderBy, *endCursor) : "";

    LOG_INFO("  - Start Key Part (Encoded Hex): {}", start_key_part.empty() ? "N/A" : hex_dump_string(start_key_part));
    LOG_INFO("  - End Key Part (Encoded Hex):   {}", end_key_part.empty() ? "N/A" : hex_dump_string(end_key_part));
    
    auto [current_page_ptr, current_index] = findStartPosition(start_key_part, !startExclusive);

    if (!current_page_ptr) {
        LOG_INFO("--- [BTree::paginatedScan END] --- findStartPosition returned no page. Returning empty.");
        return {};
    }

    std::optional<PageReadLatchGuard> guard(current_page_ptr);

    while (current_page_ptr) {
        LOG_TRACE("  Scanning page {} starting at index {}", current_page_ptr->getPageId(), current_index);
        
        for (; current_index < getKeyCount(current_page_ptr); ++current_index) {
            std::string composite_key = getLeafKey(current_page_ptr, current_index);
            std::string current_field_key_part = extractFieldKeyPart(composite_key);

            // --- End Boundary Check ---
            if (!end_key_part.empty()) {
                int cmp = current_field_key_part.compare(end_key_part);
                if (cmp > 0 || (cmp == 0 && endExclusive)) {
                    LOG_INFO("    End boundary reached at key part (hex) {}. Terminating B-Tree scan.", hex_dump_string(current_field_key_part));
                    goto end_btree_scan;
                }
            }

            // --- Start Boundary Check (for exclusive cursors) ---
            if (startExclusive && startCursor && current_field_key_part == start_key_part) {
                LOG_TRACE("    Skipping key because it matches 'startAfter' cursor's field part.");
                continue;
            }

            // --- MVCC Visibility Check ---
            auto decoded_opt = decodeCompositeKey(composite_key);
            if (!decoded_opt) {
                LOG_WARN("    Could not decode composite key at idx {}. Skipping.", current_index);
                continue;
            }
            
            TxnId current_key_txn_id = std::get<2>(*decoded_opt);

            if (current_key_txn_id <= reader_txn_id) {
                // This version is visible. Add both composite key and primary key to the results.
                visible_candidate_keys.emplace_back(composite_key, getLeafValue(current_page_ptr, current_index));
            }
        }
        
        // Move to the next leaf page
        PageId next_id = getNextLeafId(current_page_ptr);
        non_const_this->unpinPage(current_page_ptr->getPageId(), false, 0);
        
        if (next_id == INVALID_PAGE_ID) break;
        
        current_page_ptr = non_const_this->fetchPage(next_id);
        guard.emplace(current_page_ptr);
        current_index = 0;
    }

end_btree_scan:
    if (current_page_ptr) {
        non_const_this->unpinPage(current_page_ptr->getPageId(), false, 0);
    }

    LOG_INFO("--- [BTree::paginatedScan END] --- Gathered {} visible candidate keys.", visible_candidate_keys.size());
    return visible_candidate_keys;
}

TxnId BTree::decodeTxnIdDescending(const std::string& encoded) {
    if (encoded.size() != sizeof(uint64_t)) {
        throw std::runtime_error("Invalid encoded descending txn_id length: " + std::to_string(encoded.size()));
    }
    uint64_t inverted_val;
    std::memcpy(&inverted_val, encoded.data(), sizeof(inverted_val));
    // Invert bits back
    uint64_t be_val = ~inverted_val;
    return be64toh(be_val);
}



// Helper function to get the end of the current fixed payload for internal pages
static inline size_t getCurrentInternalFixedPayloadEnd(const BTree::NodeHeader* header) {
    return BTree::DATA_START_OFFSET
           + (header->key_count + 1) * BTree::PAGEID_SIZE // N keys => N+1 children
           + header->key_count * BTree::KEY_OFFSET_SIZE;
}
// Static Private Helper: Recursive insertion with path locking / latch coupling.
// - tree_instance: Pointer to the BTree object to call its member functions.
// - current_page_id: The ID of the page currently being processed.
// - key, value: The key-value pair to insert.
// - path_latches: A vector of PageWriteLatchGuards. The last guard in this vector
//                 is for 'current_page_ptr'. This vector holds W-latches on the
//                 current node and its "unsafe" ancestors (nodes that might split).
//                 This function will manage releasing ancestor latches if a child node
//                 is determined to be "safe" for descent.
// Returns: Potentially a Page* to the leaf where insertion happened, but primarily
//          manages state through path_latches and by modifying pages directly.
//          Returning nullptr often signifies the operation completed and path_latches
//          were handled/cleared by a deeper call or this function itself.
Page* BTree::insertRecursive(BTree* tree_instance, PageId current_page_id,
                             const std::string& key, const std::string& value,
                             std::vector<PageWriteLatchGuard>& path_latches) {
    // The current page is already W-latched by the last guard in path_latches.
    Page* current_page_ptr = path_latches.back().get();

    LOG_TRACE("        [insertRecursive] Page: ", current_page_id, ", Key: '", format_key_for_print(key), 
              "', PathLatchCount: ", path_latches.size());

    if (tree_instance->isLeaf(current_page_ptr)) {
        LOG_TRACE("          Page ", current_page_id, " is Leaf.");
        int idx = tree_instance->findKeyIndex(current_page_ptr, key);

        if (idx < tree_instance->getKeyCount(current_page_ptr) && 
            tree_instance->getLeafKey(current_page_ptr, idx) == key) {
            LOG_WARN("          Key '", format_key_for_print(key), 
                     "' already exists in leaf ", current_page_id, ". Idempotent insert.");
            // Key already exists. No modification.
            // Release all held latches and unpin pages.
            for (auto& guard : path_latches) {
                if (guard.get()) { // Ensure guard is valid
                    tree_instance->unpinPage(guard.get()->getPageId(), false, 0); // Not dirty
                }
            }
            path_latches.clear(); // Destroys guards, releasing latches.
            return current_page_ptr; // Return the leaf where key was found.
        }

        if (tree_instance->leafHasSpace(current_page_ptr, key.length(), value.length())) {
            tree_instance->insertLeafEntry(current_page_ptr, idx, key, value);
            LOG_TRACE("          Inserted into leaf ", current_page_id, " (had space).");
            // Mark all pages in path_latches as dirty (conservative, only leaf truly changed here for data).
            for (auto& guard : path_latches) {
                if (guard.get()) {
                     tree_instance->unpinPage(guard.get()->getPageId(), true, 0); // Mark dirty
                }
            }
            path_latches.clear(); // Destroys guards, releasing latches.
            return current_page_ptr; // Return the modified leaf.
        } else { // Leaf is full, needs split.
            LOG_TRACE("          Leaf ", current_page_id, " is full. Splitting.");
            // current_page_ptr (the leaf) is WLatched by path_latches.back().
            Page* new_leaf_page_ptr = tree_instance->splitLeaf(current_page_ptr); // Expects WLatched leaf, returns Pinned & WLatched new sibling
            PageId new_leaf_id = new_leaf_page_ptr->getPageId();
            PageWriteLatchGuard new_leaf_guard(new_leaf_page_ptr); // Take RAII ownership of the new sibling's latch

            std::string middle_key = tree_instance->getLeafKey(new_leaf_page_ptr, 0); // new_leaf_page_ptr is WLatched by its guard

            // Insert original key into correct half
            if (key >= middle_key) {
                tree_instance->insertLeafEntry(new_leaf_page_ptr, tree_instance->findKeyIndex(new_leaf_page_ptr, key), key, value);
            } else {
                tree_instance->insertLeafEntry(current_page_ptr, tree_instance->findKeyIndex(current_page_ptr, key), key, value);
            }

            // Original leaf (current_page_ptr) was modified by split and possibly insert.
            // Its latch is path_latches.back(). Release it and unpin.
            path_latches.back().release();
            tree_instance->unpinPage(current_page_id, true, 0);
            path_latches.pop_back(); // Remove guard for the original leaf.

            // New leaf sibling was modified by insert.
            // Its latch is new_leaf_guard. Release it and unpin.
            new_leaf_guard.release();
            tree_instance->unpinPage(new_leaf_id, true, 0);

            if (path_latches.empty()) {
                // This means the original leaf was the root, which split.
                // This scenario should have been handled by the preemptive root split in insertEntry.
                // If we reach here, it's an unexpected state.
                LOG_FATAL("          [insertRecursive] Path_latches empty after leaf split, but root split should have been preemptive! This indicates a logic error.");
                throw std::logic_error("Unexpected empty path_latches after leaf split during recursive insert.");
            }
            
            Page* parent_page_ptr = path_latches.back().get(); // Parent is the new top of path_latches, WLatched.
            // Call static helper to insert middle_key into the (latched) parent.
            // This helper will manage further splits up the path_latches.
            BTree::insertKeyIntoLatchedParent(tree_instance, parent_page_ptr, middle_key, new_leaf_id,
                                              key, value, /* pass original key/value for context */
                                              path_latches);
            // insertKeyIntoLatchedParent handles unpinning of pages in path_latches
            // and clearing path_latches if the operation completes fully at some level.
            return nullptr; // Signify that path_latches state is managed by the callee or completed.
        }
    } else { // Internal Node
        LOG_TRACE("          Page ", current_page_id, " is Internal.");
        PageId child_page_id_to_descend;
        Page* child_page_ptr = nullptr; 

        // current_page_ptr is WLatched by path_latches.back()
        int child_idx = tree_instance->findKeyIndex(current_page_ptr, key);
        child_page_id_to_descend = tree_instance->getInternalChildId(current_page_ptr, child_idx);

        if (child_page_id_to_descend == INVALID_PAGE_ID) {
            LOG_ERROR("            [insertRecursive] Invalid child page ID (", child_page_id_to_descend, 
                      ") in internal node ", current_page_id, " at index ", child_idx, 
                      " for key '", format_key_for_print(key), "'");
            for (const auto& guard : path_latches) {
                if (guard.get()) tree_instance->unpinPage(guard.get()->getPageId(), false,0 );
            }
            path_latches.clear();
            throw std::runtime_error("BTree inconsistency: Invalid child ID in internal node during recursive insert.");
        }

        child_page_ptr = tree_instance->fetchPage(child_page_id_to_descend); // Pin child
        PageWriteLatchGuard child_guard(child_page_ptr); // W-latch child immediately

        // If child is full, it must be split *before* descending.
        // The current node (parent of the child) and its ancestors must remain latched.
        if (tree_instance->isFull(child_page_ptr)) {
            LOG_TRACE("            Child page ", child_page_id_to_descend, " is full. Splitting child before descent.");
            
            std::string middle_key_from_child;
            Page* new_child_sibling_page_ptr;
            PageId new_child_sibling_id;

            if (tree_instance->isLeaf(child_page_ptr)) { // Child is a full leaf
                new_child_sibling_page_ptr = tree_instance->splitLeaf(child_page_ptr); // child_page_ptr is WLatched by child_guard
                middle_key_from_child = tree_instance->getLeafKey(new_child_sibling_page_ptr, 0);
            } else { // Child is a full internal node
                new_child_sibling_page_ptr = tree_instance->splitInternal(child_page_ptr, middle_key_from_child); // child_page_ptr is WLatched
            }
            new_child_sibling_id = new_child_sibling_page_ptr->getPageId();
            PageWriteLatchGuard new_child_sibling_guard(new_child_sibling_page_ptr); // Manage new sibling's latch

            LOG_TRACE("            Child split complete. New sibling: ", new_child_sibling_id, ". Middle key from child: '", format_key_for_print(middle_key_from_child), "'");

            // Release latches and unpin the split child and its new sibling
            child_guard.release(); 
            tree_instance->unpinPage(child_page_id_to_descend, true, 0); // Original child was modified

            new_child_sibling_guard.release();
            tree_instance->unpinPage(new_child_sibling_id, true, 0); // New sibling has content, is dirty

            // Now, insert middle_key_from_child into current_page_ptr (the parent).
            // current_page_ptr is WLatched by path_latches.back().
            // This call might cause current_page_ptr (parent) itself to split,
            // propagating changes up the chain of latches in path_latches.
            BTree::insertKeyIntoLatchedParent(tree_instance, current_page_ptr, middle_key_from_child, new_child_sibling_id,
                                              key, value, /* original key/value for context */
                                              path_latches);
            return nullptr; // Path latches managed by insertKeyIntoLatchedParent or cleared.

        } else { // Child is NOT full, it's "safe" to descend.
                 // We can release latches on all ancestors of current_page_ptr.
            LOG_TRACE("            Child page ", child_page_id_to_descend, " is safe (not full). Releasing ancestor latches.");
            
            std::vector<PageId> ancestor_pids_to_unpin;
            // Pop all guards from path_latches *except* the last one (for current_page_ptr)
            while (path_latches.size() > 1) {
                PageWriteLatchGuard ancestor_guard = std::move(path_latches.front());
                path_latches.erase(path_latches.begin()); // Not efficient, but clear for now
                if (ancestor_guard.get()) {
                    ancestor_pids_to_unpin.push_back(ancestor_guard.get()->getPageId());
                    ancestor_guard.release(); // Explicit release before unpin (guard destroyed by erase or move)
                }
            }
             for(PageId pid : ancestor_pids_to_unpin) {
                tree_instance->unpinPage(pid, false, 0); // Ancestors weren't modified by this direct path
            }


            // Now, path_latches contains only the guard for current_page_ptr.
            // Release latch on current_page_ptr and unpin it.
            path_latches.back().release();
            tree_instance->unpinPage(current_page_id, false, 0); // current_page_ptr not modified by this path yet
            path_latches.clear(); // Empties vector, destroys the guard for current_page_ptr.

            // The child_page_ptr is WLatched by child_guard. This latch needs to be passed down.
            // So, add child_guard to the (now empty) path_latches for the recursive call.
            path_latches.emplace_back(std::move(child_guard)); 

            return BTree::insertRecursive(tree_instance, child_page_id_to_descend, key, value, path_latches);
        }
    }
    // Fallback/should not be reached if logic is complete.
    // If reached, ensure cleanup.
    LOG_ERROR("        [insertRecursive] Reached unexpected end for page ", current_page_id);
    for (auto& guard : path_latches) { if(guard.get()) tree_instance->unpinPage(guard.get()->getPageId(), false,0 ); }
    path_latches.clear();
    return nullptr;
}



// Static Private Helper: Inserts a key (promoted from a child split) into a parent node.
// - tree_instance: Pointer to the BTree object.
// - parent_page_ptr: Pointer to the parent page, which is ALREADY W-LATCHED by a guard
//                    in path_latches (specifically, path_latches.back()).
// - key_from_child_split: The key to insert into the parent.
// - new_child_sibling_id: The PageId of the new right sibling created by the child's split.
//                         This becomes the right child of key_from_child_split in the parent.
// - original_insert_key, original_insert_value: The original key/value being inserted into the BTree.
//                                                Passed for context if this parent also splits and needs
//                                                to make decisions for further recursion.
// - path_latches: Vector of W-latch guards for parent_page_ptr and its unsafe ancestors.
//                 This function will manage these latches:
//                 - If parent has space, all latches are released and pages unpinned.
//                 - If parent splits, its latch is released/popped, and recursion continues upwards.
void BTree::insertKeyIntoLatchedParent(BTree* tree_instance, Page* parent_page_ptr,
                                       const std::string& key_from_child_split, PageId new_child_sibling_id,
                                       const std::string& original_insert_key, 
                                       const std::string& original_insert_value,
                                       std::vector<PageWriteLatchGuard>& path_latches) {
    PageId parent_page_id = parent_page_ptr->getPageId();
    LOG_TRACE("          [insertKeyIntoLatchedParent] Parent: ", parent_page_id, 
              ", KeyToInsert: '", format_key_for_print(key_from_child_split), 
              "', NewRightChild: ", new_child_sibling_id,
              ", PathLatchCount: ", path_latches.size());

    // parent_page_ptr is WLatched by path_latches.back().

    if (tree_instance->internalHasSpace(parent_page_ptr, key_from_child_split.length())) {
        int ins_idx = tree_instance->findKeyIndex(parent_page_ptr, key_from_child_split);
        tree_instance->insertInternalEntry(parent_page_ptr, ins_idx, key_from_child_split, new_child_sibling_id);
        
        // Update parent pointer of the new_child_sibling_id (it now points to this parent_page_id)
        Page* new_right_sib_ptr = tree_instance->fetchPage(new_child_sibling_id);
        PageWriteLatchGuard sib_guard(new_right_sib_ptr); // W-Latch sibling to update its parent pointer
        tree_instance->setParentId(new_right_sib_ptr, parent_page_id);
        // sib_guard releases latch upon destruction.
        tree_instance->unpinPage(new_child_sibling_id, true, 0); // Sibling's parent pointer updated, mark dirty.

        LOG_TRACE("            Inserted key '", format_key_for_print(key_from_child_split), 
                  "' into parent ", parent_page_id, " (had space). Releasing all path latches.");
        
        // Insertion into this parent is done, and it didn't split.
        // Release all held latches in path_latches and unpin the corresponding pages.
        // Mark them as dirty because an insertion occurred somewhere in their subtree.
        for (auto& guard : path_latches) {
            if (guard.get()) { // Ensure guard is valid
                tree_instance->unpinPage(guard.get()->getPageId(), true, 0); // Mark dirty
            }
        }
        path_latches.clear(); // Destroys all guards, releasing their latches.
        return;

    } else { // Parent is full, must split.
        LOG_TRACE("            Parent ", parent_page_id, " is full. Splitting parent.");
        std::string key_up_to_grandparent; // This will be populated by splitInternal

        // parent_page_ptr is WLatched by path_latches.back(). splitInternal expects this.
        Page* new_parent_sibling_page_ptr = tree_instance->splitInternal(parent_page_ptr, key_up_to_grandparent);
        PageId new_parent_sibling_id = new_parent_sibling_page_ptr->getPageId();
        // new_parent_sibling_page_ptr is returned Pinned and W-Latched by splitInternal.
        PageWriteLatchGuard new_parent_sibling_guard(new_parent_sibling_page_ptr); // Take RAII ownership of its latch.

        LOG_TRACE("            Parent ", parent_page_id, " split. New sibling: ", new_parent_sibling_id, 
                  ". Key to grandparent: '", format_key_for_print(key_up_to_grandparent), "'");

        // Determine which half (original parent or new sibling) gets key_from_child_split.
        // The new_child_sibling_id is the right child of key_from_child_split.
        if (key_from_child_split < key_up_to_grandparent) {
            // key_from_child_split goes into the original parent (parent_page_ptr, which is the left part after split)
            LOG_TRACE("              Key '", format_key_for_print(key_from_child_split), "' goes into original parent (left half) ", parent_page_id);
            int ins_idx = tree_instance->findKeyIndex(parent_page_ptr, key_from_child_split);
            tree_instance->insertInternalEntry(parent_page_ptr, ins_idx, key_from_child_split, new_child_sibling_id);
            // Update parent of new_child_sibling_id if it wasn't already (it should point to parent_page_id)
            Page* nr_sib_ref = tree_instance->fetchPage(new_child_sibling_id); 
            PageWriteLatchGuard g(nr_sib_ref); 
            if (tree_instance->getParentId(nr_sib_ref) != parent_page_id) { // Check to avoid redundant set if already correct
                tree_instance->setParentId(nr_sib_ref, parent_page_id);
            }
            tree_instance->unpinPage(new_child_sibling_id, true, 0);
        } else {
            // key_from_child_split goes into the new parent sibling (new_parent_sibling_page_ptr)
            LOG_TRACE("              Key '", format_key_for_print(key_from_child_split), "' goes into new parent sibling (right half) ", new_parent_sibling_id);
            int ins_idx = tree_instance->findKeyIndex(new_parent_sibling_page_ptr, key_from_child_split);
            tree_instance->insertInternalEntry(new_parent_sibling_page_ptr, ins_idx, key_from_child_split, new_child_sibling_id);
            // Update parent of new_child_sibling_id to point to new_parent_sibling_id
            Page* nr_sib_ref = tree_instance->fetchPage(new_child_sibling_id); 
            PageWriteLatchGuard g(nr_sib_ref); 
            tree_instance->setParentId(nr_sib_ref, new_parent_sibling_id);
            tree_instance->unpinPage(new_child_sibling_id, true, 0);
        }

        // The original parent (parent_page_ptr, now left half of its split) was modified.
        // Its latch is path_latches.back(). Release it and unpin.
        path_latches.back().release();
        tree_instance->unpinPage(parent_page_id, true, 0);
        path_latches.pop_back(); // Remove guard for the (original) parent.

        // The new parent sibling (new_parent_sibling_page_ptr) was modified.
        // Its latch is new_parent_sibling_guard. Release it and unpin.
        new_parent_sibling_guard.release();
        tree_instance->unpinPage(new_parent_sibling_id, true, 0);

        // Now, propagate key_up_to_grandparent upwards.
        if (path_latches.empty()) {
            // This means parent_page_ptr was the root, and it split.
            // The insertEntry's preemptive root split should have created a new root *above* this.
            // The `root_page_id_` in BTree now points to this new actual root.
            LOG_TRACE("            Parent split was the root. A new root was formed. Inserting '", format_key_for_print(key_up_to_grandparent), "' into the new true root.");
            
            PageId current_true_root_id = tree_instance->getRootPageId(); // Fetches current root_page_id_ under its own mutex
            
            Page* actual_root_ptr = tree_instance->fetchPage(current_true_root_id);
            path_latches.emplace_back(actual_root_ptr); // Add W-latch guard for the true new root to path_latches

            // Recursively call this function to insert key_up_to_grandparent into the actual_root_ptr.
            // original_insert_key and original_insert_value are passed for context in case of further splits.
            BTree::insertKeyIntoLatchedParent(tree_instance, actual_root_ptr, key_up_to_grandparent, new_parent_sibling_id,
                                              original_insert_key, original_insert_value, path_latches);
        } else {
            // There's a grandparent. It's now path_latches.back().get().
            Page* grandparent_page_ptr = path_latches.back().get(); // Grandparent is WLatched
            LOG_TRACE("            Propagating key '", format_key_for_print(key_up_to_grandparent), "' to grandparent ", grandparent_page_ptr->getPageId());
            BTree::insertKeyIntoLatchedParent(tree_instance, grandparent_page_ptr, key_up_to_grandparent, new_parent_sibling_id,
                                              original_insert_key, original_insert_value, path_latches);
        }
        return; // Path latches are managed by the recursive call or cleared if insertion completed.
    }
}


std::string BTree::createCompositeKey(const std::string& encoded_index_key_part, const std::string& primary_key, TxnId txn_id) {
    std::string encoded_txn_id_str = encodeTxnIdDescending(txn_id);
    
    // The key ONLY contains the indexed fields and the version.
    std::string composite_key;
    composite_key.reserve(encoded_index_key_part.length() + encoded_txn_id_str.length());
    composite_key.append(encoded_index_key_part);
    composite_key.append(encoded_txn_id_str);

    // Logging for verification
    LOG_TRACE("  [BTree::createCompositeKey CORRECTED] CALLED WITH:");
    LOG_TRACE("    > encoded_index_key_part (hex): {}", hex_dump_string(encoded_index_key_part));
    LOG_TRACE("    > primary_key: '{}'", primary_key);
    LOG_TRACE("    > txn_id: {}", txn_id);
    LOG_TRACE("  [BTree::createCompositeKey CORRECTED] PRODUCED:");
    LOG_TRACE("    > final composite_key (hex): {}", hex_dump_string(composite_key));


    return composite_key;
}



std::optional<std::tuple<std::string, std::string, TxnId>> BTree::decodeCompositeKey(const std::string& composite_key) {
    // This is a full decode of our new, correct key format.
    // It assumes the primary key is NOT in the composite_key. We will return it as an empty string.
    size_t txn_id_size = sizeof(TxnId);
    if (composite_key.length() < txn_id_size) {
        LOG_WARN("  [BTree::decodeCompositeKey] FAILED: Key length {} is less than TxnId size {}.", composite_key.length(), txn_id_size);
        return std::nullopt;
    }

    std::string encoded_index_key_part = composite_key.substr(0, composite_key.length() - txn_id_size);
    std::string encoded_txn_id_str = composite_key.substr(composite_key.length() - txn_id_size);
    
    TxnId txn_id;
    try {
        txn_id = decodeTxnIdDescending(encoded_txn_id_str);
    } catch (const std::exception& e) {
        LOG_WARN("  [BTree::decodeCompositeKey] FAILED: Could not decode TxnId. Error: {}", e.what());
        return std::nullopt;
    }
    
    // The primary key is the B-Tree's value, not part of the key. We return an empty string for it here.
    return std::make_tuple(encoded_index_key_part, "", txn_id);
}

// --- Page Layout Accessor Implementations ---

std::string BTree::getLeafKey(const Page* page, int index) const {
    const NodeHeader* header = getHeader(page);
    if (index < 0 || index >= header->key_count) throw std::out_of_range("getLeafKey index out of range");

    const off_t* key_offsets = getLeafKeyOffsets(page);
    const off_t* val_offsets = getLeafValueOffsets(page);
    const char* page_data_start = page->getData(); // Get raw buffer start

    off_t key_data_start_offset = key_offsets[index];
    off_t key_data_end_offset = val_offsets[index]; // Key data ends where value data begins

    if (key_data_start_offset >= key_data_end_offset || key_data_end_offset > PAGE_LOGICAL_SIZE) {
        LOG_ERROR("!!! Invalid key offsets in getLeafKey. Page: ", page->getPageId(), " Index: ", index, " KeyStart: ", key_data_start_offset, " KeyEnd(ValStart): ", key_data_end_offset, " LogicalSize: ", PAGE_LOGICAL_SIZE);
        throw std::runtime_error("Invalid key offset calculation in getLeafKey");
    }

    size_t key_len = key_data_end_offset - key_data_start_offset;
    const char* key_ptr = page_data_start + key_data_start_offset; // Pointer to start of key data in buffer

    // *** ADDED: Log raw bytes read from buffer ***
    std::stringstream ss_hex;
    ss_hex << std::hex << std::setfill('0');
    for(size_t i = 0; i < key_len; ++i) {
        ss_hex << std::setw(2) << static_cast<int>(static_cast<unsigned char>(key_ptr[i]));
    }
    LOG_TRACE("        [getLeafKey] Page: ", page->getPageId(), ", Index: ", index, ", OffsetStart: ", key_data_start_offset, ", OffsetEnd: ", key_data_end_offset, ", Length: ", key_len, ", RawHex: ", ss_hex.str());
    // ***********************************************

    return std::string(key_ptr, key_len); // Construct string from buffer pointer and length
}

std::string BTree::getLeafValue(const Page* page, int index) const {
    const NodeHeader* header = getHeader(page);

    // Bounds check
    if (index < 0 || index >= header->key_count) {
        throw std::out_of_range("getLeafValue index out of range. Page: " + std::to_string(page->getPageId()) 
                                + ", Index: " + std::to_string(index) 
                                + ", KeyCount: " + std::to_string(header->key_count));
    }

    const off_t* key_offsets = getLeafKeyOffsets(page);
    const off_t* val_offsets = getLeafValueOffsets(page);
    const char* page_data_start = page->getData();

    // The start of our target value's data is given by its offset.
    off_t val_data_start_offset = val_offsets[index];

    // --- START OF CORRECTED LOGIC ---

    off_t val_data_end_offset;

    // The variable data is stored in key-sorted order from the end of the page.
    // So, the end of `value[i]` is the start of `key[i-1]`.
    if (index > 0) {
        // If this is not the first entry on the page, its value's end
        // is the start of the key of the PREVIOUS entry in the sorted list.
        val_data_end_offset = key_offsets[index - 1];
    } else {
        // If this IS the first entry (index 0), its value is the last piece of
        // variable data written, so its end is the end of the logical page itself.
        val_data_end_offset = static_cast<off_t>(PAGE_LOGICAL_SIZE);
    }

    // --- END OF CORRECTED LOGIC ---


    // Defensive check for corrupted offsets.
    // A valid start offset must be less than its end offset, and the end offset
    // must be within the bounds of the page's logical size.
    if (val_data_start_offset > val_data_end_offset || val_data_end_offset > PAGE_LOGICAL_SIZE) {
        LOG_ERROR("!!! Invalid value offsets in getLeafValue. Page: {}, Index: {}, ValStart: {}, ValEnd: {}, FreePtr: {}, LogicalSize: {}",
                  page->getPageId(), index, val_data_start_offset, val_data_end_offset, getFreeSpacePointer(page), PAGE_LOGICAL_SIZE);
        throw std::runtime_error("Invalid value offset calculation in getLeafValue");
    }

    // Calculate length and create the string from the raw page data.
    size_t val_len = val_data_end_offset - val_data_start_offset;
    const char* val_ptr = page_data_start + val_data_start_offset;

    return std::string(val_ptr, val_len);
}

std::string BTree::getInternalKey(const Page* page, int index) const {
    const NodeHeader* header = getHeader(page);
    if (index < 0 || index >= header->key_count) throw std::out_of_range("getInternalKey index out of range");

    const off_t* key_offsets = getInternalKeyOffsets(page);
    const char* data_start = page->getData();
    off_t key_start_offset = key_offsets[index];
    off_t key_end_offset = (index + 1 < header->key_count) ? key_offsets[index + 1] : getFreeSpacePointer(page);

    if (key_start_offset >= key_end_offset || key_end_offset > PAGE_LOGICAL_SIZE) {
        LOG_ERROR("!!! Invalid internal key offsets. Page: ", page->getPageId(), " Index: ", index, " KeyStart: ", key_start_offset, " KeyEnd: ", key_end_offset, " LogicalSize: ", PAGE_LOGICAL_SIZE);
        throw std::runtime_error("Invalid internal key offset calculation");
    }
    return std::string(data_start + key_start_offset, key_end_offset - key_start_offset);
}

PageId BTree::getInternalChildId(const Page* page, int index) const {
    const NodeHeader* header = getHeader(page);
    // Valid child indices are 0 to key_count (inclusive)
    if (index < 0 || index > header->key_count) throw std::out_of_range("getInternalChildId index out of range");
    // Child IDs are stored directly at the start of the data section for internal nodes
    return getInternalChildIds(const_cast<Page*>(page))[index]; // Cast needed as underlying method is non-const
}


PageId BTree::getNextLeafId(const Page* page) const {
     if (!isLeaf(page)) throw std::logic_error("Cannot get next leaf ID from internal node");
     return getHeader(page)->next_page_or_sibling_id;
}

void BTree::setNextLeafId(Page* page, PageId next_id) {
     if (!isLeaf(page)) throw std::logic_error("Cannot set next leaf ID in internal node");
     getHeader(page)->next_page_or_sibling_id = next_id;
}

// --- Space Management Helpers ---

size_t BTree::getLeafFreeSpace(const Page* page) const {
    const NodeHeader* header = getHeader(page);
    // End of fixed-size payload area (key offsets + value offsets)
    size_t fixed_payload_end = DATA_START_OFFSET
                              + header->key_count * KEY_OFFSET_SIZE
                              + header->key_count * VAL_OFFSET_SIZE;
    // Free space is the gap between end of fixed payload and start of variable data
    uint16_t free_ptr = getFreeSpacePointer(page);
    if (free_ptr < fixed_payload_end) { // Should not happen in consistent state
         std::cerr << "Warning: Free space pointer (" << free_ptr
                  << ") is before fixed payload end (" << fixed_payload_end
                  << ") on page " << page->getPageId() << std::endl;
         return 0;
    }
    return free_ptr - fixed_payload_end;
}
size_t BTree::getInternalFreeSpace(const Page* page) const {
     const NodeHeader* header = getHeader(page);
     // End of fixed-size payload area (child IDs + key offsets)
     size_t fixed_payload_end = DATA_START_OFFSET
                               + (header->key_count + 1) * PAGEID_SIZE // N keys => N+1 children
                               + header->key_count * KEY_OFFSET_SIZE;
     uint16_t free_ptr = getFreeSpacePointer(page);
      if (free_ptr < fixed_payload_end) {
         std::cerr << "Warning: Free space pointer (" << free_ptr
                   << ") is before fixed payload end (" << fixed_payload_end
                   << ") on page " << page->getPageId() << std::endl;
         return 0;
      }
     return free_ptr - fixed_payload_end;
}

// Checks if there's enough *total* free space (fixed + variable) for a new leaf entry
bool BTree::leafHasSpace(const Page* page, size_t key_len, size_t val_len) const {
    size_t required_fixed = KEY_OFFSET_SIZE + VAL_OFFSET_SIZE; // Space for new offsets
    size_t required_variable = key_len + val_len;             // Space for new variable data
    return getLeafFreeSpace(page) >= (required_fixed + required_variable);
}

// Checks if there's enough *total* free space for a new internal entry
bool BTree::internalHasSpace(const Page* page, size_t key_len) const {
    size_t required_fixed = PAGEID_SIZE + KEY_OFFSET_SIZE; // Space for new child ID and key offset
    size_t required_variable = key_len;                    // Space for new variable key data
    return getInternalFreeSpace(page) >= (required_fixed + required_variable);
}

// Add data to the end of the page (variable section), return its starting offset
BTree::off_t BTree::addLeafVariableData(Page* page, const std::string& data) {
    LOG_TRACE("              [addLeafVariableData] Page: {}, DataLen: {}, CurrentFreePtr: {}",
              page->getPageId(), data.length(), getFreeSpacePointer(page));
    NodeHeader* header = getHeader(page);
    uint16_t current_free_ptr = getFreeSpacePointer(page);
    size_t data_len = data.length();

    if (data_len == 0) {
        // This is a valid case for empty values.
        return current_free_ptr;
    }

    if (data_len > std::numeric_limits<uint16_t>::max()) {
        LOG_ERROR("              !!! Data length {} exceeds uint16_t max. Page: {}", data_len, page->getPageId());
        throw std::overflow_error("addLeafVariableData: Data length exceeds uint16_t max.");
    }
    if (data_len > current_free_ptr) {
        LOG_ERROR("              !!! Data length {} exceeds current_free_ptr {}. Page: {}", data_len, current_free_ptr, page->getPageId());
        throw std::runtime_error("addLeafVariableData: Data length exceeds available space, risking underflow.");
    }

    uint16_t new_free_ptr = current_free_ptr - static_cast<uint16_t>(data_len);
    LOG_TRACE("              Calculated new_free_ptr: {}", new_free_ptr);
    
    // This is the most important check. The new free pointer must not collide
    // with the end of the fixed-size data area (headers and offsets).
    size_t current_fixed_payload_end = getCurrentLeafFixedPayloadEnd(header);
    LOG_TRACE("              Current fixed payload end offset: {}", current_fixed_payload_end);

    if (new_free_ptr < current_fixed_payload_end) {
        LOG_ERROR("              !!! COLLISION DETECTED! new_free_ptr ({}) < current_fixed_payload_end ({}). Not enough space. Page: {}",
                  new_free_ptr, current_fixed_payload_end, page->getPageId());
        throw std::runtime_error("addLeafVariableData: Collision detected between variable data and current fixed offsets.");
    }

    LOG_TRACE("              Copying {} bytes to offset {}", data_len, new_free_ptr);
    std::memcpy(page->getData() + new_free_ptr, data.data(), data_len);
    setFreeSpacePointer(page, new_free_ptr);
    LOG_TRACE("              [addLeafVariableData] Finished. NewFreePtr set to {}. Returning offset {}", new_free_ptr, new_free_ptr);
    return new_free_ptr;
}




BTree::off_t BTree::addInternalVariableData(Page* page, const std::string& data) {
    // Similar logic to addLeafVariableData, adjusted for internal node structure
    NodeHeader* header = getHeader(page);
    uint16_t current_free_ptr = getFreeSpacePointer(page);
    size_t data_len = data.length();

    if (data_len == 0) {
        return current_free_ptr;
    }

    if (data_len > std::numeric_limits<uint16_t>::max()) {
        throw std::overflow_error("addInternalVariableData: Data length exceeds uint16_t max. Page: " + std::to_string(page->getPageId()));
    }
    if (data_len > current_free_ptr) {
        throw std::runtime_error("addInternalVariableData: Data length exceeds available space, risking underflow. Page: " + std::to_string(page->getPageId()));
    }

    uint16_t new_free_ptr = current_free_ptr - static_cast<uint16_t>(data_len);

    if (new_free_ptr < HEADER_SIZE) {
        throw std::runtime_error("addInternalVariableData: Free space pointer underflow (below header). Page " + std::to_string(page->getPageId()));
    }

    // Critical check: Ensure new_free_ptr does not collide with the *current* fixed-size payload area
    size_t current_fixed_payload_end = getCurrentInternalFixedPayloadEnd(header);
    if (new_free_ptr < current_fixed_payload_end) {
        LOG_ERROR("              !!! COLLISION DETECTED (Internal)! new_free_ptr (", new_free_ptr, ") < current_fixed_payload_end (", current_fixed_payload_end, "). Page: ", page->getPageId());
        throw std::runtime_error("addInternalVariableData: Collision detected between variable data and current fixed offsets. Page: " + std::to_string(page->getPageId()));
    }

    std::memcpy(page->getData() + new_free_ptr, data.data(), data_len);
    setFreeSpacePointer(page, new_free_ptr);
    return new_free_ptr;
}


// Initialize a newly allocated leaf page
void BTree::initLeafPage(Page* page, PageId parent_id) {
    NodeHeader* header = getHeader(page);
    header->is_leaf = true;
    header->key_count = 0;
    header->parent_page_id = parent_id;
    header->next_page_or_sibling_id = INVALID_PAGE_ID;
    // Corrected: Initialize free space pointer to the end of the logical data area
    setFreeSpacePointer(page, static_cast<uint16_t>(PAGE_LOGICAL_SIZE));
}

// Modify BTree::initInternalPage
void BTree::initInternalPage(Page* page, PageId parent_id) {
    NodeHeader* header = getHeader(page);
    header->is_leaf = false;
    header->key_count = 0;
    header->parent_page_id = parent_id;
    header->next_page_or_sibling_id = INVALID_PAGE_ID; // Not used for internal
    // Corrected: Initialize free space pointer to the end of the logical data area
    setFreeSpacePointer(page, static_cast<uint16_t>(PAGE_LOGICAL_SIZE));
}

// --- Page Reorganization ---
// Compacts variable data area and updates offsets. Essential after deletions.
void BTree::compactLeafPage(Page* page) {
    NodeHeader* header = getHeader(page);
    PageId current_next_leaf = header->next_page_or_sibling_id;
    PageId current_parent = header->parent_page_id;

    if (header->key_count == 0) {
        initLeafPage(page, current_parent); // Re-initialize if empty
        setNextLeafId(page, current_next_leaf); // Restore sibling link
        return;
    }

    // Corrected: Temporary buffer should be for logical content
    std::vector<char> temp_buffer(PAGE_LOGICAL_SIZE);
    char* temp_data = temp_buffer.data();
    // Corrected: Free pointer starts at the end of the logical buffer
    uint16_t temp_free_ptr = static_cast<uint16_t>(PAGE_LOGICAL_SIZE);

    NodeHeader* temp_header = reinterpret_cast<NodeHeader*>(temp_data);
    temp_header->is_leaf = true;
    temp_header->key_count = header->key_count;
    temp_header->parent_page_id = current_parent;
    temp_header->next_page_or_sibling_id = current_next_leaf;

    off_t* temp_key_offsets = reinterpret_cast<off_t*>(temp_data + DATA_START_OFFSET);
    off_t* temp_val_offsets = reinterpret_cast<off_t*>(temp_data + DATA_START_OFFSET + MAX_LEAF_KEYS * KEY_OFFSET_SIZE);

    for (int i = 0; i < header->key_count; ++i) {
        std::string key = getLeafKey(page, i); // This read should be fine if offsets were correct before compaction
        std::string val = getLeafValue(page, i);

        temp_free_ptr -= static_cast<uint16_t>(val.length());
        if(temp_free_ptr < DATA_START_OFFSET + (i+1)*(KEY_OFFSET_SIZE+VAL_OFFSET_SIZE) ) { // More robust check against fixed part
             throw std::runtime_error("Compaction overflow (value) - new_free_ptr encroaching fixed area");
        }
        std::memcpy(temp_data + temp_free_ptr, val.data(), val.length());
        temp_val_offsets[i] = temp_free_ptr;

        temp_free_ptr -= static_cast<uint16_t>(key.length());
        if(temp_free_ptr < DATA_START_OFFSET + (i+1)*(KEY_OFFSET_SIZE+VAL_OFFSET_SIZE) - VAL_OFFSET_SIZE ) { // More robust check
            throw std::runtime_error("Compaction overflow (key) - new_free_ptr encroaching fixed area");
        }
        std::memcpy(temp_data + temp_free_ptr, key.data(), key.length());
        temp_key_offsets[i] = temp_free_ptr;
    }
    temp_header->free_space_pointer = temp_free_ptr;

    size_t temp_fixed_payload_end = DATA_START_OFFSET
                                  + temp_header->key_count * KEY_OFFSET_SIZE
                                  + temp_header->key_count * VAL_OFFSET_SIZE;
    if (temp_free_ptr < temp_fixed_payload_end) {
        throw std::runtime_error("Leaf compaction resulted in inconsistent free space pointer.");
    }

    // Corrected: Copy only the logical content size
    std::memcpy(page->getData(), temp_data, PAGE_LOGICAL_SIZE);
}

void BTree::compactInternalPage(Page* page) {
    NodeHeader* header = getHeader(page);
    PageId current_parent = header->parent_page_id;

    if (header->key_count == 0) {
        initInternalPage(page, current_parent);
        return;
    }

    // Corrected: Temporary buffer for logical content
    std::vector<char> temp_buffer(PAGE_LOGICAL_SIZE);
    char* temp_data = temp_buffer.data();
    // Corrected: Free pointer for logical buffer
    uint16_t temp_free_ptr = static_cast<uint16_t>(PAGE_LOGICAL_SIZE);

    NodeHeader* temp_header = reinterpret_cast<NodeHeader*>(temp_data);
    temp_header->is_leaf = false;
    temp_header->key_count = header->key_count;
    temp_header->parent_page_id = current_parent;
    temp_header->next_page_or_sibling_id = INVALID_PAGE_ID;

    PageId* temp_child_ids = reinterpret_cast<PageId*>(temp_data + DATA_START_OFFSET);
    off_t* temp_key_offsets = reinterpret_cast<off_t*>(temp_data + DATA_START_OFFSET + MAX_INTERNAL_CHILDREN * PAGEID_SIZE);

    std::memcpy(temp_child_ids, getInternalChildIds(page), (header->key_count + 1) * PAGEID_SIZE);

    for (int i = 0; i < header->key_count; ++i) {
        std::string key = getInternalKey(page, i);
        temp_free_ptr -= static_cast<uint16_t>(key.length());
        // Add more robust encroachment checks like in compactLeafPage if necessary
        if(temp_free_ptr < DATA_START_OFFSET + MAX_INTERNAL_CHILDREN * PAGEID_SIZE + (i+1)*KEY_OFFSET_SIZE) {
            throw std::runtime_error("Compaction overflow (internal key) - new_free_ptr encroaching fixed area");
        }
        std::memcpy(temp_data + temp_free_ptr, key.data(), key.length());
        temp_key_offsets[i] = temp_free_ptr;
    }
    temp_header->free_space_pointer = temp_free_ptr;

    size_t temp_fixed_payload_end = DATA_START_OFFSET
                                   + (temp_header->key_count + 1) * PAGEID_SIZE
                                   + temp_header->key_count * KEY_OFFSET_SIZE;
    if (temp_free_ptr < temp_fixed_payload_end) {
        throw std::runtime_error("Internal compaction resulted in inconsistent free space pointer.");
    }
    // Corrected: Copy only logical content size
    std::memcpy(page->getData(), temp_data, PAGE_LOGICAL_SIZE);
}

// --- Page Handling Wrappers ---
Page* BTree::fetchPage(PageId page_id) {
    if (page_id == INVALID_PAGE_ID) {
        throw std::invalid_argument("Attempted to fetch INVALID_PAGE_ID");
    }
    Page* page = bpm_.fetchPage(page_id);
    if (page == nullptr) {
        throw std::runtime_error("Buffer pool manager failed to fetch page: " + std::to_string(page_id));
    }
    return page;
}

void BTree::unpinPage(PageId page_id, bool is_dirty, LSN operation_lsn) { // NEW: Added LSN
    if (page_id == INVALID_PAGE_ID) {
       return;
    }
    // Pass the LSN to the BufferPoolManager's unpinPage
    if (!bpm_.unpinPage(page_id, is_dirty, operation_lsn)) { // NEW: Pass LSN
        // This might indicate an issue (e.g., unpinning a page not in buffer pool or already unpinned)
        std::cerr << "Warning: BTree::unpinPage failed to BPM unpin page " << page_id 
                  << " (dirty=" << is_dirty << ", LSN=" << operation_lsn << ")" << std::endl;
    }
}

Page* BTree::newPage(PageId& page_id /* out */) {
    Page* page = bpm_.newPage(page_id);
    if (page == nullptr) {
        throw std::runtime_error("Buffer pool manager failed to allocate new page.");
    }
     std::cout << "DEBUG: BTree::newPage created page " << page_id << std::endl;
    return page; // Returns pinned page, already marked dirty by BPM
}

void BTree::deletePage(PageId page_id) {
    if (page_id == INVALID_PAGE_ID) return;
    //std::cout << "DEBUG: BTree::deletePage requesting delete for page " << page_id << std::endl;
    if (!bpm_.deletePage(page_id)) {
        // Might fail if page is pinned. This should ideally be checked before calling.
        // Or if BPM fails to deallocate on disk.
        std::cerr << "Warning: Buffer pool manager failed to delete page " << page_id << std::endl;
         // Should we throw? If deletion fails, the page might linger.
         // For now, just log a warning.
    }
}

// --- BTree Constructor ---
BTree::BTree(const std::string& name, SimpleBufferPoolManager& bpm, bool enable_compression /* default false */)
    : index_name_(name),
      bpm_(bpm),
      compression_enabled_(enable_compression)
{
    // Protect initial read of root_page_id_
    // No need for root_pointer_mutex_ here as this is constructor context,
    // object isn't shared yet. However, if disk_manager methods have internal locking,
    // it's fine.
    // *** MODIFIED CALL: Use getBTreeRoot ***
    root_page_id_ = bpm_.disk_manager_.getBTreeRoot(index_name_);
    LOG_INFO("BTree '{}' initialized. RootPageID: {}. Logical Page Size: {}. Compression Enabled: {}",
             index_name_, root_page_id_, PAGE_LOGICAL_SIZE, compression_enabled_);
}

// --- BTree Destructor ---
BTree::~BTree() {
    // Buffer pool manager destructor should handle flushing dirty pages.
    // The root ID should be persisted whenever it changes, not just at destruction.
    LOG_INFO("BTree '{}' destructing.", index_name_);
}

// getRootPageId - now uses the new mutex
/*
PageId BTree::getRootPageId() const {
    std::shared_lock<std::shared_mutex> lock(root_pointer_mutex_);
    return root_page_id_;
}
*/
// updateRootIdInMeta - this function only persists. The caller handles root_page_id_ modification under lock.
void BTree::updateRootIdInMeta(PageId new_root_id_value) {
    // This method is called when root_page_id_ member has already been updated
    // and root_pointer_mutex_ is typically held by the caller.
    // *** MODIFIED CALL: Use setBTreeRoot ***
    try {
        bpm_.disk_manager_.setBTreeRoot(index_name_, new_root_id_value);
        LOG_TRACE("BTree '{}': Updated root page ID in metadata to {}.", index_name_, new_root_id_value);
    } catch (const storage::StorageError& e) {
        LOG_ERROR("BTree '{}': Failed to update root ID {} in metadata: {}", index_name_, new_root_id_value, e.toDetailedString());
        // Depending on severity, might rethrow or handle.
        // For now, log and continue, assuming caller handles the in-memory root_page_id_.
    } catch (const std::exception& e) {
        LOG_ERROR("BTree '{}': Std exception updating root ID {} in metadata: {}", index_name_, new_root_id_value, e.what());
    }
}

// --- Start New Tree ---
// Creates the very first page (a leaf) and sets it as the root.
void BTree::startNewTree(const std::string& key, const std::string& value) {
    LOG_TRACE("    [BTree::startNewTree] Index '{}' Key: {}", index_name_, format_key_for_print(key));

    Page* root_page_ptr = nullptr;
    PageId new_root_id = INVALID_PAGE_ID;
    bool page_allocated_and_pinned = false;

    // acquire unique lock on root_pointer_mutex_ for updating root_page_id_
    std::unique_lock<std::shared_mutex> root_ptr_lock(root_pointer_mutex_);
    LOG_TRACE("      Acquired UNIQUE root_pointer_mutex_ for startNewTree.");

    // Re-check root_page_id_ after acquiring the lock.
    // No need to re-read from disk here if root_page_id_ member is the source of truth after init.
    if (root_page_id_ != INVALID_PAGE_ID) {
        LOG_WARN("      startNewTree for BTree '{}': root page ID {} is already set. Aborting startNewTree.", index_name_, root_page_id_);
        return; // root_ptr_lock released on return
    }

    try {
        root_page_ptr = newPage(new_root_id); // newPage can throw
        page_allocated_and_pinned = true;
        LOG_TRACE("      New page {} allocated for root of BTree '{}'.", new_root_id, index_name_);

        PageWriteLatchGuard root_guard(root_page_ptr); // W-latch the new page
        initLeafPage(root_page_ptr, INVALID_PAGE_ID);

        // Update shared root_page_id_ (under root_ptr_lock)
        root_page_id_ = new_root_id;
        // Persist using the new API
        // updateRootIdInMeta already calls setBTreeRoot
        updateRootIdInMeta(root_page_id_); // This persists the new root ID for this B-Tree name
        LOG_TRACE("      Set in-memory and persisted root_page_id_ for BTree '{}' to {}.", index_name_, root_page_id_);

        insertLeafEntry(root_page_ptr, 0, key, value);
        page_allocated_and_pinned = true; // Mark as modified by insertLeafEntry

        // root_guard releases latch
        unpinPage(new_root_id, true, 0 /* LSN placeholder */);
        page_allocated_and_pinned = false;
        LOG_TRACE("      Unpinned new root page {} (dirty).", new_root_id);

    } catch (const storage::StorageError& e) { // Catch specific StorageErrors
        LOG_ERROR("    !!! StorageError in startNewTree for BTree '{}', key '{}': {}", index_name_, format_key_for_print(key), e.toDetailedString());
        // Cleanup logic
        if (root_page_id_ == new_root_id && new_root_id != INVALID_PAGE_ID) {
            try {
                // *** MODIFIED CALL: Use setBTreeRoot to revert if needed ***
                bpm_.disk_manager_.setBTreeRoot(index_name_, INVALID_PAGE_ID);
            } catch (const std::exception& meta_e) {
                LOG_ERROR("      Failed to revert root ID in metadata for BTree '{}' during startNewTree cleanup: {}", index_name_, meta_e.what());
            }
            root_page_id_ = INVALID_PAGE_ID;
        }
        if (page_allocated_and_pinned && new_root_id != INVALID_PAGE_ID) {
            unpinPage(new_root_id, false, 0); // Not dirty if init failed before data change
            try {
                // Only delete if it wasn't successfully set as a root for *another* tree (unlikely here)
                // And if it's not the current root_page_id_ (which we just reset above)
                if (bpm_.disk_manager_.getBTreeRoot(index_name_) != new_root_id) {
                     deletePage(new_root_id);
                }
            } catch (const std::exception& del_e) {
                LOG_ERROR("      Failed to delete allocated page {} during startNewTree cleanup for BTree '{}': {}", new_root_id, index_name_, del_e.what());
            }
        }
        throw; // Re-throw the caught StorageError
    } catch (const std::exception& e) { // Catch other std::exceptions
        LOG_ERROR("    !!! std::exception in startNewTree for BTree '{}', key '{}': {}", index_name_, format_key_for_print(key), e.what());
        // Similar cleanup logic as above, but throw a StorageError
        if (root_page_id_ == new_root_id && new_root_id != INVALID_PAGE_ID) {
             try { bpm_.disk_manager_.setBTreeRoot(index_name_, INVALID_PAGE_ID); } catch (...) {}
            root_page_id_ = INVALID_PAGE_ID;
        }
        if (page_allocated_and_pinned && new_root_id != INVALID_PAGE_ID) {
             unpinPage(new_root_id, false, 0);
             try { if (bpm_.disk_manager_.getBTreeRoot(index_name_) != new_root_id) deletePage(new_root_id); } catch(...) {}
        }
        throw storage::StorageError(storage::ErrorCode::INTERNAL_ERROR, "std::exception in startNewTree: " + std::string(e.what()));
    }
    LOG_TRACE("    [BTree::startNewTree] Finished successfully for BTree '{}'. New root: {}", index_name_, root_page_id_);
    // root_ptr_lock released automatically
}


bool BTree::coalesceOrRedistribute(PageId underflow_node_id, bool node_is_actually_underflowed) {
    LOG_TRACE("      [coalesceOrRedistribute NEW] Attempting to fix node ", underflow_node_id, " (is_underflow_hint: ", node_is_actually_underflowed, ")");

    if (!node_is_actually_underflowed) {
        LOG_TRACE("        Node ", underflow_node_id, " not marked as underflow by caller. No action needed by C/R.");
        // Caller is responsible for unpinning underflow_node_id if it fetched it.
        return false;
    }

    bool structure_modified_overall = false;

    for (int attempt = 0; attempt < MAX_COALESCE_ATTEMPTS; ++attempt) {
        LOG_TRACE("        C/R Attempt #", attempt + 1, " for node ", underflow_node_id);

        std::map<PageId, Page*> fetched_pages_map; // Tracks pages fetched IN THIS ATTEMPT for cleanup
        std::vector<PageWriteLatchGuard> guards_in_order; // Holds guards for RAII latch release

        Page* node_page_ptr = nullptr;
        Page* parent_page_ptr = nullptr;
        Page* left_neighbor_page_ptr = nullptr;
        Page* right_neighbor_page_ptr = nullptr;

        PageId parent_id = INVALID_PAGE_ID;
        PageId left_neighbor_id = INVALID_PAGE_ID;
        PageId right_neighbor_id = INVALID_PAGE_ID;
        int original_index_in_parent = -1; // Store index read before full latching for validation

        try {
            // Step 1: Fetch the underflow node.
            node_page_ptr = fetchPage(underflow_node_id);
            fetched_pages_map[underflow_node_id] = node_page_ptr;
            // No latching yet.

            // Step 2: Preliminary checks (can be done without latches on node_page_ptr yet,
            // but parent_id read needs to be stable or re-validated after latching).
            // For robustness, these reads should ideally be under RLatches if not for the brief parent WLock below.

            // Check if action is still needed (e.g., concurrent insert might have fixed it).
            // This check is done without a latch. It's a heuristic.
            // The definitive check will be done after all relevant pages are W-latched.
            if (hasMinimumKeys(node_page_ptr)) {
                LOG_INFO("          Node ", underflow_node_id, " no longer underflows (Pre-latch check: keys=", getKeyCount(node_page_ptr), "). Aborting C/R attempt.");
                // Mark as not modified by C/R, unpin, and let caller handle its original pin.
                unpinPage(underflow_node_id, false , 0); // Not modified by THIS C/R attempt
                fetched_pages_map.erase(underflow_node_id); // Handled
                return false; // Indicate no structural change by this C/R call.
            }

            parent_id = getParentId(node_page_ptr);
            if (parent_id == INVALID_PAGE_ID) {
                LOG_TRACE("          Node ", underflow_node_id, " is (or became) root. No C/R. adjustRoot will handle.");
                unpinPage(underflow_node_id, false , 0); // Not modified by this C/R
                fetched_pages_map.erase(underflow_node_id);
                return false;
            }

            parent_page_ptr = fetchPage(parent_id);
            fetched_pages_map[parent_id] = parent_page_ptr;

            // Step 3: Briefly W-Latch Parent to Identify Sibling IDs Safely.
            {
                PageWriteLatchGuard temp_parent_guard(parent_page_ptr);
                LOG_TRACE("          Temporarily WLatched parent ", parent_id, " to get sibling IDs.");
                original_index_in_parent = getChildIndexInParent(parent_page_ptr, underflow_node_id);
                if (original_index_in_parent < 0) {
                    LOG_ERROR("          Node ", underflow_node_id, " not found as child of parent ", parent_id, ". Aborting C/R attempt.");
                    // This state is inconsistent.
                    throw std::runtime_error("C/R prep: Node not found in parent.");
                }
                left_neighbor_id = (original_index_in_parent > 0) ? getInternalChildId(parent_page_ptr, original_index_in_parent - 1) : INVALID_PAGE_ID;
                right_neighbor_id = (original_index_in_parent < getKeyCount(parent_page_ptr)) ? getInternalChildId(parent_page_ptr, original_index_in_parent + 1) : INVALID_PAGE_ID;
            } // temp_parent_guard releases latch.
            LOG_TRACE("          Unlatched parent ", parent_id, ". Left Sib: ", left_neighbor_id, ", Right Sib: ", right_neighbor_id);

            // Fetch siblings if they exist
            if (left_neighbor_id != INVALID_PAGE_ID) {
                left_neighbor_page_ptr = fetchPage(left_neighbor_id);
                fetched_pages_map[left_neighbor_id] = left_neighbor_page_ptr;
            }
            if (right_neighbor_id != INVALID_PAGE_ID) {
                right_neighbor_page_ptr = fetchPage(right_neighbor_id);
                fetched_pages_map[right_neighbor_id] = right_neighbor_page_ptr;
            }

            // Step 4: Collect PageIDs and latch in order.
            std::vector<PageId> pids_to_latch_sorted;
            if (node_page_ptr) pids_to_latch_sorted.push_back(underflow_node_id);
            if (parent_page_ptr) pids_to_latch_sorted.push_back(parent_id);
            if (left_neighbor_page_ptr) pids_to_latch_sorted.push_back(left_neighbor_id);
            if (right_neighbor_page_ptr) pids_to_latch_sorted.push_back(right_neighbor_id);

            std::sort(pids_to_latch_sorted.begin(), pids_to_latch_sorted.end());
            pids_to_latch_sorted.erase(std::unique(pids_to_latch_sorted.begin(), pids_to_latch_sorted.end()), pids_to_latch_sorted.end());

            LOG_TRACE("            Page IDs to W-latch (sorted): ");
            // for(PageId pid : pids_to_latch_sorted) { std::cout << pid << " "; } std::cout << std::endl;

            for (PageId current_pid_to_latch : pids_to_latch_sorted) {
                if (fetched_pages_map.count(current_pid_to_latch) && fetched_pages_map[current_pid_to_latch] != nullptr) {
                    LOG_TRACE("            Latching page ", current_pid_to_latch);
                    guards_in_order.emplace_back(fetched_pages_map[current_pid_to_latch]); // WLatch via guard
                } else {
                    // This should not happen if fetch logic is correct.
                    LOG_ERROR("            Logic error: Page ", current_pid_to_latch, " was expected for latching but not fetched.");
                    throw std::runtime_error("C/R internal error: Page to latch not found in fetched map.");
                }
            }
            LOG_TRACE("          All relevant pages W-latched in order via guards.");

            // Re-assign pointers from fetched_pages_map now that they are latched.
            // This is important as the initial ptrs were just for fetching.
            node_page_ptr = fetched_pages_map[underflow_node_id];
            parent_page_ptr = fetched_pages_map[parent_id];
            if (left_neighbor_id != INVALID_PAGE_ID) left_neighbor_page_ptr = fetched_pages_map[left_neighbor_id];
            if (right_neighbor_id != INVALID_PAGE_ID) right_neighbor_page_ptr = fetched_pages_map[right_neighbor_id];


            // Step 5: Validation (all necessary pages are W-Latched by guards)
            // Re-validate parent_id of node_page_ptr and its index_in_parent.
            if (getParentId(node_page_ptr) != parent_id) {
                LOG_WARN("          Validation Fail (Attempt ", attempt + 1, "): Node ", underflow_node_id, "'s parent changed. Original parent: ", parent_id, ", New parent: ", getParentId(node_page_ptr));
                throw std::runtime_error("C/R validation failed: parent of node changed.");
            }
            int current_idx_in_parent_after_latch = getChildIndexInParent(parent_page_ptr, underflow_node_id);
            if (current_idx_in_parent_after_latch < 0) {
                LOG_WARN("          Validation Fail (Attempt ", attempt + 1, "): Node ", underflow_node_id, " no longer found as child of parent ", parent_id);
                throw std::runtime_error("C/R validation failed: node not found in parent after latching.");
            }
            if (current_idx_in_parent_after_latch != original_index_in_parent) {
                LOG_WARN("          Validation Info (Attempt ", attempt + 1, "): Node ", underflow_node_id, "'s index in parent changed from ", original_index_in_parent, " to ", current_idx_in_parent_after_latch, ". This might indicate high contention.");
                 // Depending on severity, might throw or try to adapt. For now, throw for retry.
                throw std::runtime_error("C/R validation failed: node's index in parent changed significantly.");
            }

            if (hasMinimumKeys(node_page_ptr)) { // Node no longer underflows
                LOG_INFO("          Node ", underflow_node_id, " no longer underflows (Post-latch check: keys: ", getKeyCount(node_page_ptr), "). C/R not needed.");
                // Guards will release latches. Unpin pages (not modified by C/R action).
                for (auto const& [pid, page_ptr_iter] : fetched_pages_map) {
                    unpinPage(pid, false,0);
                }
                fetched_pages_map.clear();
                return false; // No structural change by *this C/R call*.
            }

            // Step 6: Choose Sibling and Perform Operation
            Page* chosen_sibling_page_ptr = nullptr;
            bool sibling_is_left = false;
            int min_keys_for_node_type = isLeaf(node_page_ptr) ? MIN_LEAF_KEYS : MIN_INTERNAL_KEYS;

            // Prefer redistribution if possible
            if (left_neighbor_page_ptr && getKeyCount(left_neighbor_page_ptr) > min_keys_for_node_type) {
                chosen_sibling_page_ptr = left_neighbor_page_ptr;
                sibling_is_left = true;
                LOG_TRACE("            Chosen left sibling ", left_neighbor_id, " for REDISTRIBUTION.");
            } else if (right_neighbor_page_ptr && getKeyCount(right_neighbor_page_ptr) > min_keys_for_node_type) {
                chosen_sibling_page_ptr = right_neighbor_page_ptr;
                sibling_is_left = false;
                LOG_TRACE("            Chosen right sibling ", right_neighbor_id, " for REDISTRIBUTION.");
            } else { // Coalesce: prefer left sibling if available
                if (left_neighbor_page_ptr) {
                    chosen_sibling_page_ptr = left_neighbor_page_ptr;
                    sibling_is_left = true;
                    LOG_TRACE("            Chosen left sibling ", left_neighbor_id, " for COALESCE.");
                } else if (right_neighbor_page_ptr) {
                    chosen_sibling_page_ptr = right_neighbor_page_ptr;
                    sibling_is_left = false;
                    LOG_TRACE("            Chosen right sibling ", right_neighbor_id, " for COALESCE.");
                } else {
                    LOG_ERROR("          Node ", underflow_node_id, " has no valid siblings for C/R. Parent key count: ", getKeyCount(parent_page_ptr));
                    throw std::runtime_error("C/R: No valid sibling found for operation.");
                }
            }

            // Perform actual operation. All relevant pages are W-Latched by guards.
            // The redistribute/coalesce helpers expect their input pages to be W-Latched.
            if (getKeyCount(chosen_sibling_page_ptr) > min_keys_for_node_type) { // Redistribute
                structure_modified_overall = redistribute(node_page_ptr /*recipient*/, chosen_sibling_page_ptr /*donor*/, sibling_is_left);
            } else { // Coalesce
                if (sibling_is_left) {
                    // Target (chosen_sibling_page_ptr) receives entries from Source (node_page_ptr)
                    structure_modified_overall = coalesce(chosen_sibling_page_ptr, node_page_ptr, false /*source_is_right_sibling=false*/);
                    // node_page_ptr (underflow_node_id) was deleted. Mark it so it's not unpinned by this function's cleanup.
                    fetched_pages_map.erase(underflow_node_id);
                } else {
                    // Target (node_page_ptr) receives entries from Source (chosen_sibling_page_ptr)
                    PageId coalesced_sibling_id = chosen_sibling_page_ptr->getPageId();
                    structure_modified_overall = coalesce(node_page_ptr, chosen_sibling_page_ptr, true /*source_is_right_sibling=true*/);
                    fetched_pages_map.erase(coalesced_sibling_id);
                }
            }

            // Step 7: Cleanup Latches and Pins for THIS successful attempt.
            // Latches are released by guards_in_order destructor.
            // Unpin all *remaining* fetched pages. Modified pages are dirty.
            for (auto const& [pid, page_ptr_iter] : fetched_pages_map) {
                unpinPage(pid, true, 0); // Mark as dirty because C/R implies modification if page still exists.
            }
            fetched_pages_map.clear(); // Clear map as pages are handled.
            guards_in_order.clear();   // Explicitly destroy guards to release latches before potential next attempt logic.

            LOG_TRACE("        Coalesce/Redistribute attempt #", attempt + 1, " finished successfully. Modified: ", structure_modified_overall);
            return structure_modified_overall; // Exit loop on success

        } catch (const std::exception& e) {
            LOG_ERROR("        C/R attempt #", attempt + 1, " for node ", underflow_node_id, " failed: ", e.what());
            // Guards in guards_in_order will release latches upon destruction if an exception occurs.
            guards_in_order.clear(); // Ensure latches released if not already.

            // Unpin any pages fetched in THIS attempt that weren't handled (e.g., deleted by coalesce)
            for (auto const& [pid, page_ptr_iter] : fetched_pages_map) {
                unpinPage(pid, false,0); // On error path, assume not modified by this specific C/R attempt
            }
            fetched_pages_map.clear();

            if (attempt < MAX_COALESCE_ATTEMPTS - 1) {
                LOG_WARN("          Retrying C/R operation for node ", underflow_node_id);
                std::this_thread::sleep_for(std::chrono::milliseconds(20 * (attempt + 1))); // Increased delay
                continue; // Next attempt
            } else {
                LOG_ERROR("          Max C/R attempts reached for node ", underflow_node_id, ". Error: ", e.what(), ". Rethrowing.");
                throw; // Rethrow original exception if max attempts reached
            }
        }
    } // End of for-loop for attempts

    LOG_ERROR("      [coalesceOrRedistribute NEW] Logic error: exited attempt loop unexpectedly for node ", underflow_node_id);
    return false; // Should be unreachable if loop always throws or returns.
}


// --- splitRoot: Called when current root splits. Needs tree_structure_mutex_ from caller. ---
// Manages root_pointer_mutex_ for root_page_id_ update.
PageId BTree::splitRoot(Page* old_root_page_wlatched) {
    PageId old_root_id = old_root_page_wlatched->getPageId();
    LOG_TRACE("          [BTree::splitRoot] BTree '{}', Current root: {}", index_name_, old_root_id);

    Page* new_root_page_ptr = nullptr;
    PageId new_root_page_id = INVALID_PAGE_ID;
    Page* new_sibling_page_ptr = nullptr;
    PageId new_sibling_id = INVALID_PAGE_ID;
    std::string middle_key_to_promote;
    bool new_root_pinned = false;
    bool new_sibling_pinned = false;

    try {
        new_root_page_ptr = newPage(new_root_page_id); // Can throw
        new_root_pinned = true;
        PageWriteLatchGuard new_root_guard(new_root_page_ptr); // W-Latch new root
        initInternalPage(new_root_page_ptr, INVALID_PAGE_ID);
        LOG_TRACE("            New root page {} created and WLatched for BTree '{}'.", new_root_page_id, index_name_);

        // Update root_page_id_ member and persist it using the new API
        {
            std::unique_lock<std::shared_mutex> root_ptr_lock(root_pointer_mutex_);
            root_page_id_ = new_root_page_id;
            // updateRootIdInMeta calls setBTreeRoot which persists
            updateRootIdInMeta(root_page_id_);
        }
        LOG_TRACE("            Persisted new root_page_id_ as {} for BTree '{}'.", new_root_page_id, index_name_);

        // new_root_page_ptr is latched by new_root_guard
        // old_root_page_wlatched is latched by its caller's guard
        setInternalChildId(new_root_page_ptr, 0, old_root_id);
        setParentId(old_root_page_wlatched, new_root_page_id);
        LOG_TRACE("            Old root {} is now child 0 of new root {}.", old_root_id, new_root_page_id);

        if (isLeaf(old_root_page_wlatched)) {
            new_sibling_page_ptr = splitLeaf(old_root_page_wlatched); // Expects old_root to be WLatched, returns new WLatched & Pinned
        } else {
            new_sibling_page_ptr = splitInternal(old_root_page_wlatched, middle_key_to_promote); // Same expectations
        }
        new_sibling_id = new_sibling_page_ptr->getPageId();
        new_sibling_pinned = true;
        PageWriteLatchGuard new_sibling_guard(new_sibling_page_ptr); // Manage new sibling's latch

        if (isLeaf(old_root_page_wlatched)) { // If old root was leaf, new sibling is also leaf
             middle_key_to_promote = getLeafKey(new_sibling_page_ptr, 0); // new_sibling_page_ptr is WLatched by its guard
        } // Else, middle_key_to_promote was set by splitInternal

        LOG_TRACE("            Old root {} split. New sibling: {}. Middle key: '{}'", old_root_id, new_sibling_id, format_key_for_print(middle_key_to_promote));
        insertInternalEntry(new_root_page_ptr, 0, middle_key_to_promote, new_sibling_id); // new_root_page_ptr is WLatched
        LOG_TRACE("            Inserted middle key and sibling pointer into new root {}.", new_root_page_id);

        // Latches are released by guards. Unpin pages.
        // old_root_page_wlatched was modified, its caller's guard handles its latch, unpin it here.
        unpinPage(old_root_id, true, 0);

        // new_sibling_guard releases its latch.
        unpinPage(new_sibling_id, true, 0);
        new_sibling_pinned = false;

        // new_root_guard releases its latch.
        unpinPage(new_root_page_id, true, 0);
        new_root_pinned = false;

        LOG_TRACE("            Unpinned involved pages (old_root, new_sibling, new_root).");
        return new_root_page_id;

    } catch (const storage::StorageError& e) {
        LOG_ERROR("          !!! StorageError in splitRoot for BTree '{}', old root {}: {}", index_name_, old_root_id, e.toDetailedString());
        if (new_root_pinned && new_root_page_id != INVALID_PAGE_ID) unpinPage(new_root_page_id, true, 0);
        if (new_sibling_pinned && new_sibling_id != INVALID_PAGE_ID) unpinPage(new_sibling_id, true, 0);
        // old_root_page_wlatched unpinning is caller's responsibility if splitRoot fails partially
        // or ensure it's unpinned if its latch guard is local to splitRoot somehow (which it isn't here).
        throw;
    } catch (const std::exception& e) {
         LOG_ERROR("          !!! std::exception in splitRoot for BTree '{}', old root {}: {}", index_name_, old_root_id, e.what());
         if (new_root_pinned && new_root_page_id != INVALID_PAGE_ID) unpinPage(new_root_page_id, true, 0);
         if (new_sibling_pinned && new_sibling_id != INVALID_PAGE_ID) unpinPage(new_sibling_id, true, 0);
         throw storage::StorageError(storage::ErrorCode::INTERNAL_ERROR, "std::exception in splitRoot: " + std::string(e.what()));
    }
}

// Traverses the tree to find the leaf page where the key *should* reside.
// Returns the *pinned* but *UNLATCHED* leaf page.
// Caller is responsible for latching the returned page as needed.
Page* BTree::findLeafPage(const std::string& key) { // Removed exclusive_latch_on_leaf argument
    LOG_TRACE("  [BTree::findLeafPage] Key: ", format_key_for_print(key));

    PageId current_page_id_local;
    {
        std::shared_lock<std::shared_mutex> r_lock(root_pointer_mutex_);
        current_page_id_local = root_page_id_;
    }
    LOG_TRACE("    Read root page ID: ", current_page_id_local, " (under shared root_pointer_mutex_)");

    if (current_page_id_local == INVALID_PAGE_ID) {
        LOG_TRACE("    Tree is empty, returning nullptr.");
        return nullptr;
    }

    Page* current_page_ptr = fetchPage(current_page_id_local); // Pinned
    LOG_TRACE("    Fetched initial page ", current_page_id_local);

    while (true) {
        bool current_is_leaf;
        // Read is_leaf without latch first, this is usually safe if page structure is stable
        // or if subsequent latching re-validates. For robustness, even this could be under R-latch.
        // However, since tree_structure_mutex_ is often held by writers, this is generally okay.
        // Let's assume getHeader()->is_leaf is safe enough for this check before latching.
        current_is_leaf = getHeader(current_page_ptr)->is_leaf;

        if (current_is_leaf) {
            LOG_TRACE("    Reached Leaf page ", current_page_ptr->getPageId(), ". Returning (pinned & unlatched).");
            return current_page_ptr; // Return PINNED but UNLATCHED leaf
        }

        // It's an internal node. Need to R-latch it to find the child.
        PageId next_page_id = INVALID_PAGE_ID;
        { // Latching scope for current_page_ptr (internal node)
            PageReadLatchGuard internal_read_guard(current_page_ptr); // Acquires R-latch
            LOG_TRACE("    Current page ", current_page_ptr->getPageId(), " is Internal. RLatched. Finding child.");
            int child_idx = findKeyIndex(current_page_ptr, key);
            next_page_id = getInternalChildId(current_page_ptr, child_idx);
            LOG_TRACE("    Child index: ", child_idx, ", Next Page ID: ", next_page_id);
            // internal_read_guard releases R-latch upon destruction here
        }


        if (next_page_id == INVALID_PAGE_ID) {
            unpinPage(current_page_ptr->getPageId(), false,0); // Unpin current if error
            LOG_ERROR("Invalid page ID encountered during tree traversal in page ", current_page_ptr->getPageId());
            throw std::runtime_error("Invalid page ID encountered during tree traversal.");
        }

        Page* child_page_ptr = fetchPage(next_page_id); // Pin child
        LOG_TRACE("    Fetched child ", next_page_id);

        unpinPage(current_page_ptr->getPageId(), false , 0); // Unpin parent (current_page_ptr)
        LOG_TRACE("    Unpinned parent ", current_page_ptr->getPageId());

        current_page_ptr = child_page_ptr; // Advance to child
    }
    // Should be unreachable
    return nullptr;
}




// --- Binary Search Helper ---
// Finds the index of the key in the node using binary search.
// If the key is found, returns its index.
// If the key is not found:
//   - In a LEAF node, returns the index where the key *should be inserted*.
//   - In an INTERNAL node, returns the index of the *child pointer* to follow.
int BTree::findKeyIndex(const Page* page, const std::string& key) const {
    const NodeHeader* header = getHeader(page);
    int low = 0;
    int high = header->key_count - 1;
    int result_idx = 0; // Default to first child pointer if key is smaller than all keys

    // Lambda for comparison to avoid repeated getKey calls in loop
    auto compare_func = [&](int index) {
        return isLeaf(page) ? getLeafKey(page, index) : getInternalKey(page, index);
    };

    while (low <= high) {
        int mid = low + (high - low) / 2;
        std::string mid_key = compare_func(mid);

        int cmp = key.compare(mid_key);
        if (cmp == 0) {
            // Exact match found
             // For internal nodes, the key matches index `mid`. The child pointer *after* this key
             // (i.e., index `mid + 1`) contains keys greater than or equal to this.
             // For leaf nodes, this is the correct index `mid`.
             // Let's return `mid` for leaves, and `mid+1` for internal to indicate the child pointer.
             // No, standard binary search (lower_bound equivalent) is better.
             return mid; // Return the index of the matching key

        } else if (cmp < 0) {
            // Key is smaller than mid_key
            high = mid - 1;
        } else { // cmp > 0
            // Key is larger than mid_key
            low = mid + 1;
        }
    }

    // Key not found exactly. `low` is now the index where the key should be inserted (or child to follow).
    // In internal nodes, this `low` index (0 to key_count) corresponds directly to the child pointer.
    // In leaf nodes, `low` index (0 to key_count) is the insertion point.
    return low;
}

void BTree::setInternalChildId(Page* page, int index, PageId child_id) {
    NodeHeader* header = getHeader(page);
    // Valid child indices are 0 to key_count (inclusive)
    // Check bounds before writing
    if (index < 0 || index > header->key_count) {
         throw std::out_of_range("setInternalChildId index out of range. Index: " + std::to_string(index) + ", KeyCount: " + std::to_string(header->key_count) + ", Page: " + std::to_string(page->getPageId()));
    }
    PageId* child_ids = getInternalChildIds(page); // Get pointer to the array of child IDs
    child_ids[index] = child_id; // Write the new child ID at the specified index
}

// --- Insertion Logic ---

// Public API method - applyUpdate (Handles MVCC aspect)
// Public API method - Handles BTree insertion.
// Manages root_pointer_mutex_ for initial root check.
// Manages tree_structure_mutex_ for pessimistic path (structural changes).
// Uses PageLatchGuards for page-level concurrency.
// Public API method - Handles BTree insertion.
void BTree::insertEntry(const std::string& composite_key, const std::string& value) {
    LOG_TRACE("    [BTree::insertEntry] Index '", index_name_, "', Key: '", format_key_for_print(composite_key), "' (Path Locking)");

    PageId root_id_val;
    {
        std::shared_lock<std::shared_mutex> root_ptr_lock(root_pointer_mutex_);
        root_id_val = root_page_id_;
    }

    if (root_id_val == INVALID_PAGE_ID) {
        LOG_TRACE("      Tree empty. Calling startNewTree.");
        startNewTree(composite_key, value); // Handles its own root_pointer_mutex
        return;
    }

    // --- Path Locking starts here ---
    Page* root_page_ptr = fetchPage(root_id_val);
    std::vector<PageWriteLatchGuard> path_latches; // To hold latches along the path
    PageId new_child_id_from_split = INVALID_PAGE_ID; // For potential root split

    // Latch the root exclusively.
    path_latches.emplace_back(root_page_ptr); // RAII: WLatch root
    LOG_TRACE("      WLatch acquired on root ", root_id_val);

    // Check if root is full. If so, it *must* split before proceeding.
    // This simplifies the recursive helper as it won't have to handle root splitting itself.
    if (isFull(root_page_ptr)) {
        LOG_TRACE("      Root page ", root_id_val, " is full. Preemptive root split.");
        // splitRoot expects the old root to be W-Latched (which it is, via path_latches.back()).
        // It also updates root_page_id_ under root_pointer_mutex_.
        PageId new_actual_root_id = splitRoot(root_page_ptr); // Old root latch released by guard in splitRoot, page unpinned.
        path_latches.pop_back(); // Remove guard for old root

        // Now, the new root is `new_actual_root_id`. We need to start traversal from it.
        root_id_val = new_actual_root_id;
        root_page_ptr = fetchPage(root_id_val);
        path_latches.emplace_back(root_page_ptr); // WLatch new root
        LOG_TRACE("      Root split. New root is ", root_id_val, ". WLatch acquired.");
    }

    // Call recursive helper. The root is W-Latched and in path_latches.
    // The helper will manage releasing ancestor latches if children are safe.
    Page* leaf_page_after_insert = insertRecursive(this, root_id_val, composite_key, value, path_latches);

    // All latches in path_latches will be released automatically when they go out of scope.
    // The pages they were guarding need to be unpinned if they are still valid (i.e., not split away).
    // insertRecursive should ideally handle unpinning of pages whose latches it releases early.
    // Any remaining latches in path_latches are for pages that were held until the end.
    // For simplicity, we'll unpin all pages associated with guards still in path_latches here.
    // A more precise unpinning would happen in insertRecursive as locks are released.
    // However, leaf_page_after_insert is the final leaf, which needs unpinning.
    // The path_latches vector might be empty if all ancestor latches were released.

    LOG_TRACE("    [BTree::insertEntry] Path locking insert finished for key '", format_key_for_print(composite_key), "'");
    // Pages are unpinned as guards are destroyed or explicitly in recursive helper.
}

void BTree::insertLeafEntry(Page* page, int index, const std::string& key, const std::string& value) {
    NodeHeader* original_header = getHeader(page);
    if (original_header->key_count >= MAX_LEAF_KEYS) {
        throw std::logic_error("Insert into already full leaf node.");
    }

    std::vector<std::pair<std::string, std::string>> entries;
    entries.reserve(original_header->key_count + 1);
    for (int i = 0; i < original_header->key_count; ++i) {
        entries.emplace_back(getLeafKey(page, i), getLeafValue(page, i));
    }

    entries.emplace_back(key, value);
    
    std::sort(entries.begin(), entries.end(), 
        [](const auto& a, const auto& b) {
            return a.first < b.first;
        });

    PageId parent_id = getParentId(page);
    PageId next_id = getNextLeafId(page);
    initLeafPage(page, parent_id);
    setNextLeafId(page, next_id);

    NodeHeader* header = getHeader(page);
    for (const auto& entry : entries) {
        const int current_idx = header->key_count;
        off_t val_offset = addLeafVariableData(page, entry.second);
        off_t key_offset = addLeafVariableData(page, entry.first);
        
        off_t* key_offsets = getLeafKeyOffsets(page);
        off_t* val_offsets = getLeafValueOffsets(page);
        
        key_offsets[current_idx] = key_offset;
        val_offsets[current_idx] = val_offset;
        
        header->key_count++;
    }
}


void BTree::insertInternalEntry(Page* page, int index, const std::string& key, PageId right_child_id) {
    NodeHeader* original_header = getHeader(page);
    if (original_header->key_count >= MAX_INTERNAL_KEYS) {
        throw std::logic_error("Insert into already full internal node.");
    }
    
    std::vector<std::pair<std::string, PageId>> entries;
    entries.reserve(original_header->key_count + 1);
    for (int i = 0; i < original_header->key_count; ++i) {
        entries.emplace_back(getInternalKey(page, i), getInternalChildId(page, i + 1));
    }

    entries.emplace_back(key, right_child_id);

    std::sort(entries.begin(), entries.end(),
        [](const auto& a, const auto& b) {
            return a.first < b.first;
        });

    PageId parent_id = getParentId(page);
    PageId first_child = getInternalChildId(page, 0);
    initInternalPage(page, parent_id);
    NodeHeader* header = getHeader(page);

    PageId* child_ids_ptr = getInternalChildIds(page);
    child_ids_ptr[0] = first_child;

    for(size_t i = 0; i < entries.size(); ++i) {
        off_t key_offset = addInternalVariableData(page, entries[i].first);
        off_t* key_offsets_ptr = getInternalKeyOffsets(page);
        key_offsets_ptr[i] = key_offset;
        
        child_ids_ptr[i + 1] = entries[i].second;
        
        header->key_count++;
    }
}


// --- Split Implementations ---

// Splits a leaf page. The original page becomes the left sibling,
// a new page is created as the right sibling.
// Returns the *pinned* new right sibling page.
// The first key of the right sibling needs to be inserted into the parent.
Page* BTree::splitLeaf(Page* leaf_page) {
    LOG_TRACE("          [splitLeaf] Splitting page ", leaf_page->getPageId());
    NodeHeader* old_header = getHeader(leaf_page);
    PageId old_page_id = leaf_page->getPageId();
    PageId parent_id = old_header->parent_page_id;

    PageId new_sibling_id;
    Page* new_sibling_page = newPage(new_sibling_id); // Pins new page

    // *** W Latch the new page BEFORE modifying it ***
    LOG_TRACE("            Acquiring W Latch on new sibling ", new_sibling_id);
    new_sibling_page->WLatch();

    try {
        initLeafPage(new_sibling_page, parent_id); // Initialize header

        // ... (determine split point, copy entries using getLeafKey/getLeafValue - safe reads) ...
        int total_keys = old_header->key_count;
        int num_to_move = (total_keys + 1) / 2; // Move ceil(N/2) - ensures minimums
        int num_to_keep = total_keys - num_to_move;
        int start_move_index = num_to_keep;

        std::vector<std::pair<std::string, std::string>> entries_to_move;
        entries_to_move.reserve(num_to_move);
        for(int i = 0; i < num_to_move; ++i) {
            entries_to_move.emplace_back(getLeafKey(leaf_page, start_move_index + i), getLeafValue(leaf_page, start_move_index + i));
        }
        LOG_TRACE("            Moving " , num_to_move , " entries to new sibling.");

        // Truncate old leaf (logically first)
        setKeyCount(leaf_page, num_to_keep); // Safe write (W Latch held)

        // Insert moved entries into new sibling
        for(int i = 0; i < num_to_move; ++i) {
            // insertLeafEntry needs W latch on new_sibling_page, which we hold
            insertLeafEntry(new_sibling_page, i, entries_to_move[i].first, entries_to_move[i].second);
        }

        // Physically compact the old leaf page (W Latch held)
        LOG_TRACE("            Compacting old leaf page ", old_page_id);
        compactLeafPage(leaf_page);

        // Update sibling pointers (Need W latches on both, which we hold)
        setNextLeafId(new_sibling_page, getNextLeafId(leaf_page)); // New takes old's next
        setNextLeafId(leaf_page, new_sibling_id);                 // Old points to new
        LOG_TRACE("            Updated sibling pointers. Old(" , old_page_id , ")->Next=" , new_sibling_id , ", New(" , new_sibling_id , ")->Next=" , getNextLeafId(new_sibling_page));

    } catch (...) {
        // Ensure new sibling latch is released on error
        LOG_ERROR("            !!! EXCEPTION during splitLeaf !!!");
        new_sibling_page->WUnlock();
        // Caller handles unpinning new_sibling_page if needed (e.g., via deletePage?)
        // Caller handles original leaf_page latch/pin.
        throw;
    }

    LOG_TRACE("          [splitLeaf] Completed. Returning new sibling ", new_sibling_id, " (WLatched).");
    // Caller releases leaf_page latch, new_sibling_page latch, and unpins.
    return new_sibling_page;
}


// Splits an internal page. The original page becomes the left sibling,
// a new page is created as the right sibling.
// The middle key is extracted and needs to be inserted into the parent.
// Returns the *pinned* new right sibling page. Outputs the middle key via ref.
Page* BTree::splitInternal(Page* internal_page, std::string& middle_key /* out */) {
    LOG_TRACE("          [splitInternal] Splitting page ", internal_page->getPageId());
   // ... (get headers, parent id) ...
   NodeHeader* old_header = getHeader(internal_page);
   PageId old_page_id = internal_page->getPageId();
   PageId parent_id = old_header->parent_page_id;


   PageId new_sibling_id;
   Page* new_sibling_page = newPage(new_sibling_id);

   // *** W Latch new sibling page ***
   LOG_TRACE("            Acquiring W Latch on new sibling ", new_sibling_id);
   new_sibling_page->WLatch();

   try {
       initInternalPage(new_sibling_page, parent_id);

       // ... (determine split point, copy keys/children - safe reads) ...
       int total_keys = old_header->key_count;
       int mid_idx = total_keys / 2;
       middle_key = getInternalKey(internal_page, mid_idx); // Store key to promote
       LOG_TRACE("            Middle key (to promote): '", format_key_for_print(middle_key), "' at index ", mid_idx);

       int start_move_key_idx = mid_idx + 1;
       int num_keys_to_move = total_keys - start_move_key_idx;
       int start_move_child_idx = mid_idx + 1;
       int num_children_to_move = num_keys_to_move + 1;

       std::vector<std::string> keys_to_move(num_keys_to_move);
       std::vector<PageId> children_to_move(num_children_to_move);
       // Copy data while old page is WLatched
       for(int i=0; i < num_keys_to_move; ++i) keys_to_move[i] = getInternalKey(internal_page, start_move_key_idx + i);
       for(int i=0; i < num_children_to_move; ++i) children_to_move[i] = getInternalChildId(internal_page, start_move_child_idx + i);
        LOG_TRACE("            Moving ", num_keys_to_move, " keys and ", num_children_to_move, " children.");


       // Truncate old internal node (W Latch held)
       setKeyCount(internal_page, mid_idx);

       // Insert moved entries into new sibling (W Latch held on sibling)
       setInternalChildId(new_sibling_page, 0, children_to_move[0]);
       for(int i=0; i < num_keys_to_move; ++i) {
           insertInternalEntry(new_sibling_page, i, keys_to_move[i], children_to_move[i+1]);
       }

       // Compact old page (W Latch held)
        LOG_TRACE("            Compacting old internal page ", old_page_id);
       compactInternalPage(internal_page);

       // Update parent pointers of moved children (Requires W Latching children)
       LOG_TRACE("            Updating parent pointers for children moved to ", new_sibling_id);
       updateChildParentPointers(new_sibling_page); // Assumes W Latch held on new_sibling_page

   } catch (...) {
       LOG_ERROR("            !!! EXCEPTION during splitInternal !!!");
       new_sibling_page->WUnlock();
       throw;
   }

   LOG_TRACE("          [splitInternal] Completed. Returning new sibling ", new_sibling_id, " (WLatched).");
   return new_sibling_page;
}

// Updates the parent pointers of all direct children of the given internal page.
// Assumes 'page' is pinned.
// --- updateChildParentPointers needs PageWriteLatchGuard for children ---
void BTree::updateChildParentPointers(Page* page_raw_ptr) { // page_raw_ptr is WLatched by caller (e.g. splitInternal)
    if (isLeaf(page_raw_ptr)) return;

    PageId parent_id_for_children = page_raw_ptr->getPageId();
    LOG_TRACE("            [updateChildParentPointers] Updating children of page ", parent_id_for_children);
    int count = getKeyCount(page_raw_ptr);
    for (int i = 0; i <= count; ++i) {
        PageId child_id = getInternalChildId(page_raw_ptr, i);
        if (child_id != INVALID_PAGE_ID) {
            Page* child_page_ptr = nullptr;
            bool child_pinned = false;
            try {
                child_page_ptr = fetchPage(child_id);
                child_pinned = true;
                PageWriteLatchGuard child_guard(child_page_ptr); // W-Latch child
                setParentId(child_page_ptr, parent_id_for_children);
                // child_guard releases latch.
                unpinPage(child_id, true, 0); // Mark child dirty
                child_pinned = false;
                LOG_TRACE("              Updated parent pointer for child ", child_id);
            } catch (...) {
                 LOG_ERROR("              !!! EXCEPTION while updating parent pointer for child ", child_id, " !!!");
                  // child_guard (if active) releases latch.
                  if (child_pinned) unpinPage(child_id, true, 0); // Assume dirty on error
                 // Continue trying other children.
            }
        }
    }
    LOG_TRACE("            [updateChildParentPointers] Finished for page ", parent_id_for_children);
}



// Helper to find leaf page, acquiring R-Latches on the path.
// Returns the Pinned leaf page. The path_latches vector will contain R-Latch guards.
Page* BTree::findLeafPageWithReadPathLocking(const std::string& key, std::vector<PageReadLatchGuard>& path_latches) {
    LOG_TRACE("  [BTree::findLeafPageWithReadPathLocking] Key: ", format_key_for_print(key));
    path_latches.clear(); // Ensure path is clear before starting

    PageId current_page_id_local;
    Page* root_page_ptr = nullptr;
    {
        std::shared_lock<std::shared_mutex> r_lock(root_pointer_mutex_);
        current_page_id_local = root_page_id_;
        if (current_page_id_local == INVALID_PAGE_ID) {
            LOG_TRACE("    Tree is empty, returning nullptr.");
            return nullptr;
        }
        // Fetch root while root_pointer_mutex is held to ensure consistency if root changes.
        root_page_ptr = fetchPage(current_page_id_local); // Pin root
    } // root_pointer_mutex_ released

    if (!root_page_ptr) return nullptr; // Should not happen if ID was valid

    path_latches.emplace_back(root_page_ptr); // R-Latch the root
    LOG_TRACE("    Fetched and RLatched root page ", current_page_id_local);

    Page* current_page_ptr = root_page_ptr;

    while (true) {
        // current_page_ptr is R-Latched by the last guard in path_latches
        if (isLeaf(current_page_ptr)) {
            LOG_TRACE("    Reached Leaf page ", current_page_ptr->getPageId(), ". Path latches held. Returning (pinned & RLatched leaf).");
            return current_page_ptr; // Return Pinned & RLatched leaf
        }

        // It's an internal node (already R-Latched). Find the child.
        int child_idx = findKeyIndex(current_page_ptr, key);
        PageId next_page_id = getInternalChildId(current_page_ptr, child_idx);
        LOG_TRACE("    Internal node ", current_page_ptr->getPageId(), ", Child index: ", child_idx, ", Next Page ID: ", next_page_id);

        if (next_page_id == INVALID_PAGE_ID) {
            // Invalid path. path_latches will release on destruction. Pages will be unpinned by caller.
            LOG_ERROR("Invalid page ID encountered during tree traversal in page ", current_page_ptr->getPageId());
            // path_latches destructor will release held R-latches.
            // Caller needs to unpin pages in path_latches.
            throw std::runtime_error("Invalid page ID encountered during read path locking traversal.");
        }

        Page* child_page_ptr = fetchPage(next_page_id); // Pin child
        LOG_TRACE("    Fetched child ", next_page_id);

        // "Crab" over: R-Latch child.
        path_latches.emplace_back(child_page_ptr);
        LOG_TRACE("    RLatched child ", next_page_id);

        // Release ancestor latches if child is "safe" for deletion (not going to immediately cause merge up to parent)
        // For simplicity in this remove path, we might hold all read latches until leaf is W-latched.
        // A more optimized version would release earlier. For now, keep them.

        // The previous page's R-latch (now an ancestor) is path_latches[path_latches.size()-2]
        // Unpinning of ancestors will be handled by the caller of this function if it needs to iterate the path_latches.
        current_page_ptr = child_page_ptr; // Advance to child
    }
    // Should be unreachable
    return nullptr;
}

bool BTree::removeKeyPhysical(const std::string& composite_key) {
    LOG_TRACE("    [BTree::removeKeyPhysical Path Locking] Index '", index_name_, "', Key: '", format_key_for_print(composite_key), "'");

    PageId root_id_val;
    {
        std::shared_lock<std::shared_mutex> root_ptr_lock(root_pointer_mutex_);
        root_id_val = root_page_id_;
    }

    if (root_id_val == INVALID_PAGE_ID) {
        LOG_TRACE("      Tree empty. Key not found.");
        return false;
    }

    std::vector<PageWriteLatchGuard> path_latches;
    Page* root_page_ptr = fetchPage(root_id_val);
    path_latches.emplace_back(root_page_ptr); // WLatch root
    LOG_TRACE("      WLatch acquired on root ", root_id_val);

    // Unlike insert, root underflow is handled *after* deletion attempt by adjustRoot().
    // No preemptive root "un-split" is typically done.

    bool removed = removeRecursive(this, root_id_val, composite_key, path_latches);

    if (removed) {
        adjustRoot(); // Check if root needs to be adjusted (e.g. tree height shrinks)
    }
    
    // path_latches should be empty or cleared by removeRecursive.
    // Any remaining pins/latches are a bug in removeRecursive.
    if (!path_latches.empty()) {
        LOG_ERROR("    [removeKeyPhysical] path_latches not empty after removeRecursive. Leaking latches/pins!");
        for (auto& guard : path_latches) {
            if (guard.get()) unpinPage(guard.get()->getPageId(), false, 0); // Attempt cleanup
        }
        path_latches.clear();
    }

    LOG_TRACE("    [BTree::removeKeyPhysical Path Locking] Finished for key '", format_key_for_print(composite_key), "'. Removed: ", removed);
    return removed;
}

bool BTree::removeRecursive(BTree* tree_instance, PageId current_page_id,
                            const std::string& key,
                            std::vector<PageWriteLatchGuard>& path_latches) {
    Page* current_page_ptr = path_latches.back().get(); // Current page is W-Latched
    LOG_TRACE("        [removeRecursive] Page: ", current_page_id, ", Key: '", format_key_for_print(key),
              "', PathLatchCount: ", path_latches.size());

    bool key_found_and_removed_from_subtree = false;

    if (tree_instance->isLeaf(current_page_ptr)) {
        LOG_TRACE("          Page ", current_page_id, " is Leaf.");
        int idx = tree_instance->findKeyIndex(current_page_ptr, key);

        if (idx < tree_instance->getKeyCount(current_page_ptr) &&
            tree_instance->getLeafKey(current_page_ptr, idx) == key) {
            LOG_TRACE("          Key found. Removing from leaf ", current_page_id);
            tree_instance->removeLeafEntry(current_page_ptr, idx); // Modifies W-Latched leaf
            key_found_and_removed_from_subtree = true;

            // If leaf underflows (and is not root), C/R is needed.
            // The parent is W-latched by path_latches[path_latches.size()-2].
            // current_page_ptr is path_latches.back().
            if (!tree_instance->isRootPage(current_page_ptr) && !tree_instance->hasMinimumKeys(current_page_ptr)) {
                LOG_TRACE("          Leaf ", current_page_id, " underflowed. Handling underflow.");
                // path_latches_to_parent will contain latches up to and including the parent.
                std::vector<PageWriteLatchGuard> path_latches_to_parent;
                for(size_t i=0; i < path_latches.size() - 1; ++i) { // Copy all except leaf's latch
                    path_latches_to_parent.emplace_back(std::move(path_latches[i]));
                }
                path_latches.erase(path_latches.begin(), path_latches.end() -1); // Remove moved guards
                
                // current_page_ptr's latch is still path_latches.back().
                // handleUnderflowAfterRemove will manage current_page_ptr's latch and pin.
                handleUnderflowAfterRemove(tree_instance, current_page_ptr, path_latches_to_parent);
                // After handleUnderflow, path_latches_to_parent might be cleared or modified.
                // And current_page_ptr might be deleted/stale. Its latch from path_latches is also gone.
                path_latches.clear(); // All original path_latches elements handled or moved.
            } else {
                 // No underflow or is root. Release all path latches and unpin pages.
                LOG_TRACE("          Leaf ", current_page_id, " did not underflow or is root. Releasing all path latches.");
                for (auto& guard : path_latches) {
                    if (guard.get()) tree_instance->unpinPage(guard.get()->getPageId(), true, 0); // Modified by remove or descendants
                }
                path_latches.clear();
            }
        } else {
            LOG_TRACE("          Key not found in leaf ", current_page_id, ". Releasing all path latches.");
            for (auto& guard : path_latches) { // Key not found, path not modified by this leaf
                 if (guard.get()) tree_instance->unpinPage(guard.get()->getPageId(), false, 0);
            }
            path_latches.clear();
            key_found_and_removed_from_subtree = false;
        }
        return key_found_and_removed_from_subtree;
    } else { // Internal Node
        LOG_TRACE("          Page ", current_page_id, " is Internal.");
        int child_idx = tree_instance->findKeyIndex(current_page_ptr, key);
        PageId child_page_id_to_descend;
        std::string key_in_internal_node = ""; // If exact match
        bool exact_match_in_internal = (child_idx < tree_instance->getKeyCount(current_page_ptr) &&
                                       (key_in_internal_node = tree_instance->getInternalKey(current_page_ptr, child_idx)) == key);

        if (exact_match_in_internal) {
            LOG_TRACE("          Key '", format_key_for_print(key), "' matches internal key. Descending to right child for replacement.");
            child_page_id_to_descend = tree_instance->getInternalChildId(current_page_ptr, child_idx + 1);
        } else {
            child_page_id_to_descend = tree_instance->getInternalChildId(current_page_ptr, child_idx);
        }
        
        if (child_page_id_to_descend == INVALID_PAGE_ID) { /* error handling */ }

        Page* child_page_ptr = tree_instance->fetchPage(child_page_id_to_descend);
        PageWriteLatchGuard child_guard(child_page_ptr); // W-Latch child

        // "Safe Node" check for releasing ancestor latches.
        // A child is "safe" for deletion if it has strictly MORE than minimum keys.
        // If it only has min_keys, deleting from it *could* cause underflow requiring C/R with parent.
        if (tree_instance->getKeyCount(child_page_ptr) > (tree_instance->isLeaf(child_page_ptr) ? MIN_LEAF_KEYS : MIN_INTERNAL_KEYS) ) {
            LOG_TRACE("            Child page ", child_page_id_to_descend, " is safe. Releasing ancestor latches.");
            // Release all latches in path_latches *except* the last one (for current_page_ptr)
            std::vector<PageId> ancestor_pids_to_unpin;
            while (path_latches.size() > 1) {
                PageWriteLatchGuard ancestor_guard = std::move(path_latches.front());
                path_latches.erase(path_latches.begin());
                if (ancestor_guard.get()) {
                    ancestor_pids_to_unpin.push_back(ancestor_guard.get()->getPageId());
                }
            }
            for(PageId pid : ancestor_pids_to_unpin) tree_instance->unpinPage(pid, false,0); // Ancestors not modified yet
        }

        // Add child_guard to path_latches for the recursive call
        path_latches.emplace_back(std::move(child_guard));
        
        // Recursive call on child
        bool removed_from_child_subtree = removeRecursive(tree_instance, child_page_id_to_descend, key, path_latches);
        key_found_and_removed_from_subtree = removed_from_child_subtree;

        // After recursive call returns, path_latches should be empty (handled by recursive calls)
        // OR it contains latches up to the parent of the node that handled the underflow (or root).
        // If path_latches is NOT empty, it means current_page_ptr (and possibly its ancestors) are still latched.
        // This usually means current_page_ptr is the parent of the node that was just processed (and potentially underflowed).

        if (!path_latches.empty()) { // Current node (and maybe ancestors) still latched
            // If key was found and it was an exact match in *this* internal node:
            if (exact_match_in_internal && removed_from_child_subtree) {
                LOG_TRACE("          Key was in internal node. Finding successor to replace '", format_key_for_print(key_in_internal_node), "' in page ", current_page_id);
                // The key `key_in_internal_node` (at child_idx) needs to be replaced by the new smallest key
                // from its right child's subtree (which is child_page_id_to_descend, or its new first child if it split/merged).
                // This part is tricky because child_page_id_to_descend might have been modified by C/R.
                // The `removeFromPage` logic for internal nodes is more robust here. This pessimistic version is simplified.
                // For now, assume C/R correctly updated parent keys if necessary.
                // A simpler approach for pessimistic:
                // 1. Find actual leaf.
                // 2. Delete from leaf.
                // 3. If leaf underflows, C/R. C/R updates parent keys.
                // This design implies `removeRecursive` doesn't handle internal node key replacement itself,
                // but relies on C/R to fix parent keys if a child that defined that key gets a new min/max.
                // This is a common B-Tree deletion strategy: delete from leaf, then fixup.
                // The current logic implies we just deleted from the child_page_id_to_descend subtree.
                // If C/R happened in that subtree, the parent keys (in current_page_ptr) should have been updated.
            }

            // Check if current_page_ptr itself underflowed due to C/R in its child.
            // (This implies current_page_ptr is W-Latched by path_latches.back())
            if (!tree_instance->isRootPage(current_page_ptr) && !tree_instance->hasMinimumKeys(current_page_ptr)) {
                 LOG_TRACE("          Internal node ", current_page_id, " underflowed. Handling.");
                 std::vector<PageWriteLatchGuard> path_to_currents_parent;
                 for(size_t i=0; i < path_latches.size() - 1; ++i) {
                    path_to_currents_parent.emplace_back(std::move(path_latches[i]));
                 }
                 path_latches.erase(path_latches.begin(), path_latches.end() -1);

                 handleUnderflowAfterRemove(tree_instance, current_page_ptr, path_to_currents_parent);
                 path_latches.clear(); // All handled
            } else {
                // No underflow for current_page_ptr or it's root. Release all remaining latches.
                LOG_TRACE("          Internal node ", current_page_id, " did not underflow or is root. Releasing remaining path latches.");
                for (auto& guard : path_latches) {
                    if (guard.get()) tree_instance->unpinPage(guard.get()->getPageId(), removed_from_child_subtree, 0); // Modified if child was.
                }
                path_latches.clear();
            }
        } // else path_latches empty, all handled deeper.
        return key_found_and_removed_from_subtree;
    }
}

// Helper to manage C/R for a W-Latched page whose parent is also W-Latched (or is root).
// page_under_scrutiny_ptr is W-Latched by path_latches.back() from the caller of removeRecursive.
// path_latches_to_parent contains W-latches for ancestors up to and INCLUDING the parent of page_under_scrutiny_ptr.
// The last guard in path_latches_to_parent is for the direct parent.
bool BTree::handleUnderflowAfterRemove(BTree* tree_instance, Page* page_under_scrutiny_ptr,
                                       std::vector<PageWriteLatchGuard>& path_latches_to_parent) {
    PageId underflow_node_id = page_under_scrutiny_ptr->getPageId();
    LOG_TRACE("            [handleUnderflowAfterRemove] Node: ", underflow_node_id);

    // `page_under_scrutiny_ptr` is W-Latched by its original guard (passed implicitly, not in path_latches_to_parent).
    // Its parent is W-Latched by path_latches_to_parent.back().

    // Release the latch on page_under_scrutiny_ptr. C/R will re-fetch and re-latch it.
    // The page is still pinned. C/R must handle its unpin or deletion.
    // This assumes the caller of removeRecursive, which called handleUnderflow,
    // will handle unpinning current_page_ptr IF C/R doesn't delete it.
    // It's cleaner if C/R always handles the unpin/delete of the node it's called for.
    
    // The new coalesceOrRedistribute takes PageId and a hint.
    // It will fetch and latch pages itself.
    // The parent of underflow_node_id is already W-Latched by path_latches_to_parent.back().
    // This is good, as C/R operations on siblings will require the parent to be W-Latched.

    PageWriteLatchGuard original_underflow_node_guard(std::move(path_latches_to_parent.back())); // This is not quite right.
    // The `page_under_scrutiny_ptr` has its own W-latch from the `removeRecursive` frame.
    // `path_latches_to_parent` contains latches *up to* the parent.
    // `page_under_scrutiny_ptr` is the actual W-latched underflowed node.

    // Release the W-latch on page_under_scrutiny_ptr. Its pin is held.
    // This requires the guard for `page_under_scrutiny_ptr` to be released.
    // The `removeRecursive` function should manage this. Let's assume it has:
    // page_under_scrutiny_ptr->WUnlock(); // Or its guard is released.

    // Now call C/R. It will re-fetch and re-latch as needed.
    // Parent (path_latches_to_parent.back()) remains W-Latched, which helps C/R.
    bool structure_modified = tree_instance->coalesceOrRedistribute(underflow_node_id, true /*node_is_actually_underflowed*/);

    // After C/R, the parent (path_latches_to_parent.back().get()) might have changed.
    // If C/R modified the parent and the parent *itself* now underflows, this needs to propagate.
    // This is the recursive "fixup" part.
    if (!path_latches_to_parent.empty()) {
        Page* parent_ptr = path_latches_to_parent.back().get(); // Parent is still W-Latched
        if (!tree_instance->isRootPage(parent_ptr) && !tree_instance->hasMinimumKeys(parent_ptr)) {
            LOG_TRACE("            Parent ", parent_ptr->getPageId(), " underflowed after child C/R. Propagating fixup.");
            // Prepare latches for the parent's parent
            std::vector<PageWriteLatchGuard> path_to_grandparent;
            for(size_t i=0; i < path_latches_to_parent.size() - 1; ++i) {
                path_to_grandparent.emplace_back(std::move(path_latches_to_parent[i]));
            }
            // Recursively call handleUnderflow for the parent
            handleUnderflowAfterRemove(tree_instance, parent_ptr, path_to_grandparent);
        } else {
             // Parent OK or is root. Release remaining path_latches_to_parent.
            for (auto& guard : path_latches_to_parent) {
                if (guard.get()) tree_instance->unpinPage(guard.get()->getPageId(), structure_modified,0);
            }
        }
    }
    path_latches_to_parent.clear(); // All guards moved or released.
    return structure_modified;
}

// Remove entry at index (handles leaf/internal dispatch)
void BTree::removeEntry(Page* page, int index) {
     if (isLeaf(page)) {
         removeLeafEntry(page, index);
     } else {
         removeInternalEntry(page, index);
     }
}

// Remove entry from leaf page at index, compacting afterwards
void BTree::removeLeafEntry(Page* page, int index) {
    NodeHeader* header = getHeader(page);
    if (index < 0 || index >= header->key_count) {
        throw std::out_of_range("removeLeafEntry index out of range: " + std::to_string(index) + " on page " + std::to_string(page->getPageId()));
    }

    off_t* key_offsets = getLeafKeyOffsets(page);
    off_t* val_offsets = getLeafValueOffsets(page);

    // Shift offsets leftwards to overwrite the removed entry
    // Move elements from index+1 up to count-1
    int num_to_move = header->key_count - index - 1;
    if (num_to_move > 0) {
        std::memmove(key_offsets + index, key_offsets + index + 1, num_to_move * KEY_OFFSET_SIZE);
        std::memmove(val_offsets + index, val_offsets + index + 1, num_to_move * VAL_OFFSET_SIZE);
    }

    header->key_count--;
    compactLeafPage(page); // Reclaim variable space, update free ptr
}

// Remove key at index and child pointer at index+1 from internal page, compacting.
void BTree::removeInternalEntry(Page* page, int index) {
    NodeHeader* header = getHeader(page);
    if (index < 0 || index >= header->key_count) {
        throw std::out_of_range("removeInternalEntry index out of range: " + std::to_string(index) + " on page " + std::to_string(page->getPageId()));
    }

    off_t* key_offsets = getInternalKeyOffsets(page);
    PageId* child_ids = getInternalChildIds(page);

    // Shift key offsets leftwards [index+1 .. count-1] -> [index .. count-2]
    int num_keys_to_move = header->key_count - index - 1;
    if (num_keys_to_move > 0) {
        std::memmove(key_offsets + index, key_offsets + index + 1, num_keys_to_move * KEY_OFFSET_SIZE);
    }

    // Shift child IDs leftwards [index + 2 .. count] -> [index + 1 .. count-1]
    // This removes the child pointer at index + 1
    int num_children_to_move = header->key_count - index; // Children from index+2 to count
    if (num_children_to_move > 0) {
         std::memmove(child_ids + index + 1, child_ids + index + 2, num_children_to_move * PAGEID_SIZE);
    }

    header->key_count--;
    compactInternalPage(page); // Reclaim variable space, update free ptr
}


// Check if node has minimum required keys (handles root case)
bool BTree::hasMinimumKeys(const Page* page) const {
     int key_count = getKeyCount(page);
     if (isRootPage(page)) {
          if (isLeaf(page)) {
              return true; // Leaf root can be empty
          } else {
              // Internal root needs at least 1 key (implying 2 children) to be valid
              // unless it's the *only* node left and about to become empty/leaf.
              // If key_count is 0, it should only have 1 child left.
              return key_count >= 0; // adjustRoot handles the case of 0 keys properly.
          }
     } else {
         // Non-root nodes must have minimum keys
         int min_keys = isLeaf(page) ? MIN_LEAF_KEYS : MIN_INTERNAL_KEYS;
         return key_count >= min_keys;
     }
}

// --- Coalesce/Redistribute ---

// Moves one entry from neighbor_page to node_page through the parent.
// Assumes node_page, neighbor_page, and parent_page are pinned.
bool BTree::redistribute(Page* node_page /*recipient*/, Page* neighbor_page /*donor*/, bool from_left_sibling) {
    PageId parent_id = getParentId(node_page); // Should be same for both
    Page* parent_page = fetchPage(parent_id); // Already pinned by caller? Re-fetch is safe.

    int node_index_in_parent = getChildIndexInParent(parent_page, node_page->getPageId());
    // Key in parent separating the two nodes
    int parent_key_index = from_left_sibling ? node_index_in_parent - 1 : node_index_in_parent;

    bool success = false;
    try {
        if (isLeaf(node_page)) {
            // --- Redistribute Leaf ---
            if (from_left_sibling) {
                // 1. Take LAST entry from left neighbor
                int neighbor_last_idx = getKeyCount(neighbor_page) - 1;
                std::string neighbor_key = getLeafKey(neighbor_page, neighbor_last_idx);
                std::string neighbor_val = getLeafValue(neighbor_page, neighbor_last_idx);
                removeLeafEntry(neighbor_page, neighbor_last_idx); // Remove from neighbor

                // 2. Insert it at the BEGINNING of current node
                insertLeafEntry(node_page, 0, neighbor_key, neighbor_val);

                // 3. Update parent key to be the NEW first key of current node (which is neighbor_key)
                removeInternalEntry(parent_page, parent_key_index); // Remove old parent key
                insertInternalEntry(parent_page, parent_key_index, neighbor_key, node_page->getPageId()); // Insert new key

            } else { // Borrow from right sibling
                // 1. Take FIRST entry from right neighbor
                 std::string neighbor_key = getLeafKey(neighbor_page, 0);
                 std::string neighbor_val = getLeafValue(neighbor_page, 0);
                 removeLeafEntry(neighbor_page, 0); // Remove from neighbor

                 // 2. Insert it at the END of current node
                 insertLeafEntry(node_page, getKeyCount(node_page), neighbor_key, neighbor_val);

                 // 3. Update parent key to be the NEW first key of the RIGHT neighbor
                  std::string new_sep_key = getLeafKey(neighbor_page, 0);
                  removeInternalEntry(parent_page, parent_key_index); // Remove old parent key
                  insertInternalEntry(parent_page, parent_key_index, new_sep_key, getInternalChildId(parent_page, parent_key_index + 1)); // Insert new key, keep right child ptr same

            }
        } else {
            // --- Redistribute Internal ---
            if (from_left_sibling) {
                // 1. Get parent separator key
                std::string parent_sep_key = getInternalKey(parent_page, parent_key_index);
                // 2. Get LAST child from left neighbor
                int neighbor_last_child_idx = getKeyCount(neighbor_page);
                PageId neighbor_child_id = getInternalChildId(neighbor_page, neighbor_last_child_idx);
                // 3. Get LAST key from left neighbor (this will become the new parent separator)
                std::string neighbor_last_key = getInternalKey(neighbor_page, neighbor_last_child_idx - 1);

                // 4. Remove last key/child from neighbor
                 removeInternalEntry(neighbor_page, neighbor_last_child_idx - 1);

                // 5. Insert parent separator key at beginning of node_page's keys
                // 6. Insert neighbor's last child as first child of node_page
                 // Shift existing children right by 1
                 PageId* node_children = getInternalChildIds(node_page);
                 std::memmove(node_children + 1, node_children, (getKeyCount(node_page) + 1) * PAGEID_SIZE);
                 // Set first child
                 setInternalChildId(node_page, 0, neighbor_child_id);
                 // Insert parent key at index 0 (needs existing keys/offsets shifted right first)
                 insertInternalEntry(node_page, 0, parent_sep_key, getInternalChildId(node_page, 1)); // Right child is the old first child

                // 7. Update parent separator key with the key that was removed from neighbor
                removeInternalEntry(parent_page, parent_key_index); // Remove old sep key
                insertInternalEntry(parent_page, parent_key_index, neighbor_last_key, node_page->getPageId()); // Insert new sep key

                // 8. Update parent pointer of moved child
                if(neighbor_child_id != INVALID_PAGE_ID) {
                    Page* moved_child_page = fetchPage(neighbor_child_id);
                    setParentId(moved_child_page, node_page->getPageId());
                    unpinPage(neighbor_child_id, true, 0);
                }

            } else { // Borrow from right sibling
                 // 1. Get parent separator key
                 std::string parent_sep_key = getInternalKey(parent_page, parent_key_index);
                 // 2. Get FIRST child from right neighbor
                 PageId neighbor_child_id = getInternalChildId(neighbor_page, 0);
                 // 3. Get FIRST key from right neighbor (this will become the new parent separator)
                 std::string neighbor_first_key = getInternalKey(neighbor_page, 0);

                 // 4. Remove first key/child from neighbor
                 // Need to handle child[0] specifically.
                 PageId* neighbor_children = getInternalChildIds(neighbor_page);
                 // Shift children [1..count] left to [0..count-1]
                 std::memmove(neighbor_children, neighbor_children + 1, getKeyCount(neighbor_page) * PAGEID_SIZE);
                  removeInternalEntry(neighbor_page, 0); // Remove key[0] and compacts var data


                 // 5. Insert parent separator key at end of node_page's keys
                 // 6. Insert neighbor's first child as last child of node_page
                 // Child goes after last key. Index = current key count + 1
                 setInternalChildId(node_page, getKeyCount(node_page) + 1, neighbor_child_id);
                 // Insert parent key at index = current key count
                  insertInternalEntry(node_page, getKeyCount(node_page), parent_sep_key, neighbor_child_id);


                 // 7. Update parent separator key with neighbor's first key
                 removeInternalEntry(parent_page, parent_key_index); // Remove old sep key
                  insertInternalEntry(parent_page, parent_key_index, neighbor_first_key, getInternalChildId(parent_page, parent_key_index + 1)); // Insert new sep key


                 // 8. Update parent pointer of moved child
                 if (neighbor_child_id != INVALID_PAGE_ID) {
                    Page* moved_child_page = fetchPage(neighbor_child_id);
                    setParentId(moved_child_page, node_page->getPageId());
                    unpinPage(neighbor_child_id, true, 0);
                 }
            }
        }
        success = true;
    } catch(...) {
        unpinPage(parent_id, false , 0); // Unpin parent on error
        // Caller unpins node and neighbor
        throw;
    }
    unpinPage(parent_id, true, 0); // Mark parent dirty
    return success;
}

// Merges source_node into target_node. source_node is then deleted.
// Assumes target_node, source_node, and parent are pinned.
// `source_is_right_sibling`: True if source_node is to the right of target_node.
bool BTree::coalesce(Page* target_node, Page* source_node, bool source_is_right_sibling) {
     PageId target_id = target_node->getPageId();
     PageId source_id = source_node->getPageId();
     PageId parent_id = getParentId(target_node); // Should be same for both
     Page* parent_page = fetchPage(parent_id); // Re-fetch for safety, already pinned by caller

     // Index of the key in the parent that separates target and source
     int parent_key_idx = getChildIndexInParent(parent_page, source_is_right_sibling ? target_id : source_id);

     bool success = false;
     try {
         if (isLeaf(target_node)) {
             // --- Coalesce Leaf ---
             int target_start_idx = getKeyCount(target_node);
             // Move all entries from source to target
             for (int i = 0; i < getKeyCount(source_node); ++i) {
                  insertLeafEntry(target_node, target_start_idx + i, getLeafKey(source_node, i), getLeafValue(source_node, i));
             }
             // Update target's sibling pointer to source's sibling pointer
             setNextLeafId(target_node, getNextLeafId(source_node));

         } else {
             // --- Coalesce Internal ---
             // 1. Get separator key from parent
             std::string parent_sep_key = getInternalKey(parent_page, parent_key_idx);

             // 2. Move separator key down into target node (at the end of existing keys)
             PageId first_child_from_source = getInternalChildId(source_node, 0);
             // Insert key, its right child is the first child from the source node
             insertInternalEntry(target_node, getKeyCount(target_node), parent_sep_key, first_child_from_source);


             // 3. Move remaining keys/children from source to target
             int target_start_idx = getKeyCount(target_node); // Index for next key insertion
             for (int i = 0; i < getKeyCount(source_node); ++i) {
                  PageId child_id_to_move = getInternalChildId(source_node, i + 1);
                  insertInternalEntry(target_node, target_start_idx + i, getInternalKey(source_node, i), child_id_to_move);
             }
             // Update parent pointers of *all* children now in target node
             updateChildParentPointers(target_node); // Simplest way after merge
         }

         // --- Remove entry from parent ---
         // Removes key[parent_key_idx] and child pointer after it (which points to source_node)
         removeInternalEntry(parent_page, parent_key_idx);

         success = true;

     } catch (...) {
          unpinPage(parent_id, false, 0);
          // Caller unpins target and source
          throw;
     }

     unpinPage(parent_id, false, 0); // Mark parent dirty

     // Delete the source page (it's now empty and removed from parent)
     unpinPage(source_id,  true, 0); // Unpin source before deleting (must not be dirty)
     deletePage(source_id);

     // Caller unpins target_node (it was modified)
     return success;
}


void BTree::adjustRoot() {
    std::unique_lock<std::shared_mutex> root_ptr_lock(root_pointer_mutex_); // Lock for root_page_id_
    LOG_TRACE("    [BTree::adjustRoot] Acquired UNIQUE root_pointer_mutex_.");

    // Access root_page_id_ directly as it's protected by root_ptr_lock
    if (root_page_id_ == INVALID_PAGE_ID) {
        LOG_TRACE("      Root already INVALID. No adjustment needed.");
        return;
    }

    Page* root_page_ptr = nullptr; // Renamed
    PageId old_root_id = root_page_id_; // Read under lock
    bool root_adjusted_and_old_root_handled = false;

    try {
        root_page_ptr = fetchPage(old_root_id);
        PageWriteLatchGuard root_guard(root_page_ptr); // W-latch the current root

        NodeHeader* header = getHeader(root_page_ptr);

        if (!header->is_leaf && header->key_count == 0) {
            LOG_TRACE("      Root page ", old_root_id, " is internal and empty. Promoting child.");
            PageId new_root_id = getInternalChildId(root_page_ptr, 0);
            LOG_TRACE("      New root will be page ", new_root_id);

            // root_page_id_ is already exclusively locked by root_ptr_lock
            root_page_id_ = new_root_id;
            updateRootIdInMeta(root_page_id_); // Persist
            LOG_TRACE("      Updated in-memory and persisted root_page_id to ", root_page_id_);

            root_guard.release(); // Release latch on old root before operating on new root or deleting old
            unpinPage(old_root_id, false , 0); // Old root not dirty from this op, just becoming obsolete
            deletePage(old_root_id);      // Delete old root page
            root_page_ptr = nullptr;      // Invalidate pointer to old root
            root_adjusted_and_old_root_handled = true;

            if (new_root_id != INVALID_PAGE_ID) {
                Page* new_root_page_ptr = fetchPage(new_root_id);
                PageWriteLatchGuard new_root_guard(new_root_page_ptr);
                setParentId(new_root_page_ptr, INVALID_PAGE_ID);
                // new_root_guard releases latch
                unpinPage(new_root_id, true, 0); // New root is modified
            } else {
                LOG_TRACE("      Tree became completely empty after root adjustment.");
            }
        }
        // If no adjustment, root_guard releases latch on root_page_ptr when it goes out of scope
    } catch (...) {
        LOG_ERROR("    !!! EXCEPTION in adjustRoot for old root ", old_root_id, " !!!");
        // root_guard (if constructed and active) releases latch.
        if (root_page_ptr && !root_adjusted_and_old_root_handled) { // Only unpin if not already handled
            unpinPage(old_root_id, false , 0); // Assume not dirty if error occurred before full adjustment
        }
        // root_ptr_lock releases automatically.
        throw;
    }

    if (root_page_ptr && !root_adjusted_and_old_root_handled) { // If root was not adjusted and deleted
        // Latch released by root_guard. Unpin original root.
        unpinPage(old_root_id, false , 0); // Not dirty if no change by this function
    }
    LOG_TRACE("    [BTree::adjustRoot] Finished.");
    // root_ptr_lock releases automatically.
}

// Finds the index `i` such that parent's child pointer `children[i]` == child_id.
int BTree::getChildIndexInParent(Page* parent_page, PageId child_id) {
     if (isLeaf(parent_page)) {
         throw std::logic_error("Cannot find child index in a leaf node.");
     }
     int count = getKeyCount(parent_page);
     for(int i=0; i <= count; ++i) { // Check all N+1 child pointers
         if (getInternalChildId(parent_page, i) == child_id) {
             return i;
         }
     }
     // Should not happen in a consistent tree
      throw std::runtime_error("Child page " + std::to_string(child_id) + " not found in parent " + std::to_string(parent_page->getPageId()));
}


// Find the smallest key in the subtree rooted at subtree_root_id (must be in a leaf)
std::string BTree::findMinKey(PageId subtree_root_id) {
     // Assumes tree lock held
     if (subtree_root_id == INVALID_PAGE_ID) throw std::invalid_argument("findMinKey called with invalid page ID");

     PageId current_id = subtree_root_id;
     Page* current_page = fetchPage(current_id); // Pin

     try {
         // Traverse down the leftmost path
         while(!isLeaf(current_page)) {
              PageId next_id = getInternalChildId(current_page, 0); // Go to child[0]
              unpinPage(current_id, false , 0); // Unpin internal node
              current_id = next_id;
              if (current_id == INVALID_PAGE_ID) throw std::logic_error("Invalid page ID encountered in findMinKey traversal.");
              current_page = fetchPage(current_id); // Pin next
         }
         // Now at the leftmost leaf
         if (getKeyCount(current_page) == 0) {
              unpinPage(current_id, false, 0);
             throw std::logic_error("Empty leaf encountered while finding min key in subtree starting at " + std::to_string(subtree_root_id));
         }
         std::string min_key = getLeafKey(current_page, 0); // First key in the leftmost leaf
         unpinPage(current_id, false , 0); // Unpin leaf
         return min_key;
     } catch (...) {
         unpinPage(current_id, false, 0); // Unpin on error
         throw;
     }
}

// Find the largest key in the subtree rooted at subtree_root_id (must be in a leaf)
std::string BTree::findMaxKey(PageId subtree_root_id) {
     // Assumes tree lock held
      if (subtree_root_id == INVALID_PAGE_ID) throw std::invalid_argument("findMaxKey called with invalid page ID");

     PageId current_id = subtree_root_id;
     Page* current_page = fetchPage(current_id); // Pin
     try {
         // Traverse down the rightmost path
         while(!isLeaf(current_page)) {
              PageId next_id = getInternalChildId(current_page, getKeyCount(current_page)); // Go to last child
              unpinPage(current_id, false , 0);
              current_id = next_id;
               if (current_id == INVALID_PAGE_ID) throw std::logic_error("Invalid page ID encountered in findMaxKey traversal.");
              current_page = fetchPage(current_id); // Pin next
         }
         // Now at the rightmost leaf of the subtree
         int count = getKeyCount(current_page);
         if (count == 0) {
              unpinPage(current_id, false , 0);
             throw std::logic_error("Empty leaf encountered while finding max key in subtree starting at " + std::to_string(subtree_root_id));
         }
         std::string max_key = getLeafKey(current_page, count - 1); // Last key
         unpinPage(current_id, false, 0); // Unpin leaf
         return max_key;
     } catch (...) {
         unpinPage(current_id, false, 0); // Unpin on error
         throw;
     }
}

// --- Public API Methods ---

// --- BTree::find (const method) ---
// Uses PageReadLatchGuard on the leaf.
// Find the value for a key part, visible to the reader transaction.
// Handles composite keys (original_key_part + descending_txn_id).
std::optional<std::string> BTree::find(const std::string& original_index_key_part, TxnId reader_txn_id) const {
    BTree* non_const_this = const_cast<BTree*>(this);
    Page* leaf_page_ptr = non_const_this->findLeafPage(original_index_key_part);

    if (leaf_page_ptr == nullptr) {
        return std::nullopt;
    }

    PageReadLatchGuard leaf_guard(leaf_page_ptr);
    PageId leaf_page_id = leaf_page_ptr->getPageId();

    try {
        int index = findKeyIndex(leaf_page_ptr, original_index_key_part);

        // Iterate forward from the starting index.
        for (int i = index; i < getKeyCount(leaf_page_ptr); ++i) {
            std::string current_composite_key = getLeafKey(leaf_page_ptr, i);
            auto decoded_opt = decodeCompositeKey(current_composite_key);

            if (!decoded_opt) continue;

            const auto& [current_original_part, primary_key, current_key_txn_id] = *decoded_opt;

            // Stop if we've scanned past the relevant key part.
            if (current_original_part != original_index_key_part) {
                break;
            }

            // --- START OF FIX ---
            // Check visibility.
            if (current_key_txn_id <= reader_txn_id) {
                // This is the FIRST visible version we have encountered. Because TxnIDs are
                // sorted descending, this IS the latest visible version.
                
                std::string btree_value = getLeafValue(leaf_page_ptr, i);
                
                // Unpin the page before returning the result.
                non_const_this->unpinPage(leaf_page_id, false, 0);

                if (btree_value != BTree::INDEX_TOMBSTONE) {
                    // It's a visible data entry. Return it.
                    return btree_value;
                } else {
                    // It's a visible tombstone. The key is considered deleted.
                    return std::nullopt;
                }
            }
            // If not visible, continue loop to check the next older version of the same key part.
            // --- END OF FIX ---
        }

        non_const_this->unpinPage(leaf_page_id, false, 0);
        return std::nullopt; // No visible version found for this key part.

    } catch (...) {
        if (leaf_page_ptr) {
            non_const_this->unpinPage(leaf_page_ptr->getPageId(), false, 0);
        }
        throw;
    }
}

std::optional<std::pair<std::string, std::string>> BTree::findFirstVisibleEntry(
    const std::string& original_index_key_part, 
    TxnId reader_txn_id) const 
{
    BTree* non_const_this = const_cast<BTree*>(this);
    Page* leaf_page_ptr = non_const_this->findLeafPage(original_index_key_part);

    if (!leaf_page_ptr) {
        return std::nullopt;
    }

    PageReadLatchGuard leaf_guard(leaf_page_ptr);
    PageId leaf_page_id = leaf_page_ptr->getPageId();

    try {
        int index = findKeyIndex(leaf_page_ptr, original_index_key_part);

        for (int i = index; i < getKeyCount(leaf_page_ptr); ++i) {
            std::string current_composite_key = getLeafKey(leaf_page_ptr, i);
            auto decoded_opt = decodeCompositeKey(current_composite_key);

            if (!decoded_opt) continue;

            const auto& [current_original_part, pk, current_key_txn_id] = *decoded_opt;

            if (current_original_part != original_index_key_part) {
                break; // Scanned past all versions of our key.
            }

            // Check visibility.
            if (current_key_txn_id <= reader_txn_id) {
                // This is the first visible version, which is the latest.
                std::string btree_value = getLeafValue(leaf_page_ptr, i);
                non_const_this->unpinPage(leaf_page_id, false, 0);

                if (btree_value != BTree::INDEX_TOMBSTONE) {
                    // It's a live entry. Return the composite key and its value (primary key).
                    return std::make_pair(current_composite_key, btree_value);
                } else {
                    // It's a tombstone. The key is not considered to exist.
                    return std::nullopt;
                }
            }
        }
        
        non_const_this->unpinPage(leaf_page_id, false, 0);
        return std::nullopt;

    } catch (...) {
        if (leaf_page_ptr) {
            non_const_this->unpinPage(leaf_page_ptr->getPageId(), false, 0);
        }
        throw;
    }
}

// Scan keys where the original_index_key_part starts with original_index_key_prefix,
// visible to the reader_txn_id.
// Returns pairs of <actual_original_key_part, btree_value_for_that_version>.
std::vector<std::pair<std::string, std::string>> BTree::prefixScan(
    const std::string& original_index_key_prefix,
    TxnId reader_txn_id,
    size_t limit) const {
    LOG_TRACE("  [BTree::prefixScan] Prefix: '", format_key_for_print(original_index_key_prefix), "', ReaderTxn: ", reader_txn_id, ", Limit: ", limit);
    std::vector<std::pair<std::string, std::string>> results;

    // Use const_cast for non-const BPM operations (fetch, unpin)
    BTree* non_const_this = const_cast<BTree*>(this);

    Page* current_page_ptr = nullptr;
    PageId current_page_id_val = INVALID_PAGE_ID; // Use a distinct name
    // Optional guard for RAII; initialized when a page is successfully fetched and needs latching.
    std::optional<PageReadLatchGuard> current_page_guard_opt;

    try {
        // findLeafPage returns a PINNED but UNLATCHED page.
        // Start scan from the beginning of the range matching the prefix.
        current_page_ptr = non_const_this->findLeafPage(original_index_key_prefix);

        if (current_page_ptr == nullptr) {
            LOG_TRACE("    BTree::prefixScan: findLeafPage returned nullptr for prefix '", format_key_for_print(original_index_key_prefix), "'. No matching leaf found or tree empty.");
            return results;
        }

        current_page_guard_opt.emplace(current_page_ptr); // Construct guard, R-latches current_page_ptr
        current_page_id_val = current_page_ptr->getPageId();
        LOG_TRACE("    BTree::prefixScan: Initial leaf page ", current_page_id_val, " (RLatched by guard).");

        // findKeyIndex will find the first key >= original_index_key_prefix
        int current_index = findKeyIndex(current_page_ptr, original_index_key_prefix);

        // Keep track of the last original_index_key_part processed to return only one version per original key part
        // (specifically, the latest visible one according to reader_txn_id).
        std::string last_processed_original_key_part = "";

        while (current_page_ptr != nullptr && current_page_guard_opt && current_page_guard_opt->isLatched()) {
            const NodeHeader* header = getHeader(current_page_ptr); // Safe: R-latched by guard
            LOG_TRACE("      BTree::prefixScan: Scanning page ", current_page_id_val, " from index ", current_index, " (Keys: ", header->key_count, ")");

            for (; current_index < header->key_count; ++current_index) {
                if (limit > 0 && results.size() >= limit) {
                    LOG_TRACE("      BTree::prefixScan: Limit of ", limit, " reached. Stopping scan.");
                    // Guard will release latch. Unpin before returning.
                    current_page_guard_opt->release(); // Explicit release as we are returning early
                    non_const_this->unpinPage(current_page_id_val, false , 0);
                    current_page_ptr = nullptr; // Mark as handled
                    return results;
                }

                std::string current_composite_key = getLeafKey(current_page_ptr, current_index); // Safe: R-latched
                 auto decoded_opt = decodeCompositeKey(current_composite_key);

                if (!decoded_opt) {
                    LOG_WARN("      BTree::prefixScan: Failed to decode composite key '", format_key_for_print(current_composite_key), "' at index ", current_index, " on page ", current_page_id_val);
                    continue; // Skip malformed key
                }

                const std::string& current_original_part = std::get<0>(*decoded_opt);
                TxnId current_key_txn_id = std::get<2>(*decoded_opt);

                LOG_TRACE("        BTree::prefixScan: Checking CompKey='", format_key_for_print(current_composite_key), "', OrigPart='", format_key_for_print(current_original_part), "', TxnID=", current_key_txn_id);

                // Check if we've scanned past the desired prefix
                if (current_original_part.rfind(original_index_key_prefix, 0) != 0) {
                    LOG_TRACE("        BTree::prefixScan: Key part '", format_key_for_print(current_original_part), "' no longer matches prefix '", format_key_for_print(original_index_key_prefix), "'. End of scan for this prefix.");
                    // Guard will release latch. Unpin before returning.
                    current_page_guard_opt->release();
                    non_const_this->unpinPage(current_page_id_val, false, 0);
                    current_page_ptr = nullptr;
                    return results;
                }

                // Visibility check and ensure we only return the latest visible version for each original_index_key_part
                if (current_key_txn_id <= reader_txn_id) {
                    if (last_processed_original_key_part != current_original_part) {
                        // This is the first (and thus latest due to descending TxnId sort) visible version for this new original_index_key_part
                        std::string btree_value = getLeafValue(current_page_ptr, current_index);
                        if (btree_value != BTree::INDEX_TOMBSTONE) {
                            results.emplace_back(current_original_part, btree_value);
                            LOG_TRACE("          BTree::prefixScan: Added '", format_key_for_print(current_original_part), "' -> '", format_key_for_print(btree_value), "' (Txn: ", current_key_txn_id, ")");
                        } else {
                            LOG_TRACE("          BTree::prefixScan: Found visible tombstone for '", format_key_for_print(current_original_part), "' (Txn: ", current_key_txn_id, "). Skipping.");
                        }
                        last_processed_original_key_part = current_original_part; // Mark this original key as processed for its latest version
                    } else {
                         LOG_TRACE("          BTree::prefixScan: Already processed latest visible version for '", format_key_for_print(current_original_part), "'. Skipping older visible version (Txn: ", current_key_txn_id, ").");
                    }
                } else {
                    LOG_TRACE("        BTree::prefixScan: Key '", format_key_for_print(current_original_part), "' (Txn: ", current_key_txn_id, ") not visible to reader Txn ", reader_txn_id);
                }
            } // End for loop over keys in current page

            // Finished current page. Get next leaf page.
            PageId next_page_id = getNextLeafId(current_page_ptr); // Safe: R-latched by guard

            // Release resources for the current page before processing the next
            if (current_page_guard_opt.has_value()) current_page_guard_opt->release(); // Release latch
            non_const_this->unpinPage(current_page_id_val, false , 0);
            current_page_ptr = nullptr; // Invalidate pointer as its guard is released and page unpinned
            LOG_TRACE("      BTree::prefixScan: Finished page ", current_page_id_val, ". Next page ID: ", next_page_id);

            current_page_id_val = next_page_id;
            current_index = 0; // Reset index for the new page

            if (current_page_id_val != INVALID_PAGE_ID) {
                current_page_ptr = non_const_this->fetchPage(current_page_id_val); // Pin new page
                current_page_guard_opt.emplace(current_page_ptr); // Create and acquire R-latch for new page
                LOG_TRACE("      BTree::prefixScan: Moved to next page ", current_page_id_val, " (RLatched by guard).");
            } else {
                current_page_guard_opt.reset(); // No more pages, clear the optional guard
                LOG_TRACE("      BTree::prefixScan: No more leaf pages in chain.");
                // Loop will terminate as current_page_ptr is null.
            }
        } // End while loop over pages
    } catch (const std::exception& e) {
        LOG_ERROR("    !!! EXCEPTION in BTree::prefixScan for prefix '", format_key_for_print(original_index_key_prefix), "': ", e.what());
        // Ensure cleanup if an exception occurred
        if (current_page_guard_opt.has_value() && current_page_guard_opt->isLatched()) {
            current_page_guard_opt->release();
        }
        if (current_page_ptr && current_page_id_val != INVALID_PAGE_ID) {
            non_const_this->unpinPage(current_page_id_val, false, 0);
        }
        // Depending on desired behavior, you might rethrow or return partial results.
        // Current behavior: return whatever results were collected before the error.
    }
    LOG_TRACE("  [BTree::prefixScan] Scan complete. Found ", results.size(), " results for prefix '", format_key_for_print(original_index_key_prefix), "'.");
    return results;
}

Page* BTree::findLeftmostLeaf() const {
    PageId current_id = getRootPageId();
    if (current_id == INVALID_PAGE_ID) return nullptr;
    Page* current_page = const_cast<BTree*>(this)->fetchPage(current_id);
    while (!isLeaf(current_page)) {
        PageId next_id = getInternalChildId(current_page, 0);
        const_cast<BTree*>(this)->unpinPage(current_page->getPageId(), false, 0);
        current_page = const_cast<BTree*>(this)->fetchPage(next_id);
    }
    return current_page; // Returned pinned
}

std::pair<Page*, int> BTree::findStartPosition(const std::string& start_key_part, bool start_inclusive) const {
    // 1. Handle empty start key: return the very first key in the tree.
    if (start_key_part.empty()) {
        Page* leftmost_leaf = findLeftmostLeaf();
        return {leftmost_leaf, leftmost_leaf ? 0 : -1}; // Return index 0 or -1 if tree is empty
    }

    // 2. Find the leaf page where the start key would be located.
    Page* leaf_page = const_cast<BTree*>(this)->findLeafPage(start_key_part);
    if (!leaf_page) {
        return {nullptr, -1};
    }

    // 3. Find the index of the first key >= start_key_part.
    int idx = findKeyIndex(leaf_page, start_key_part);

    // 4. For exclusive searches (>), advance the index past all keys that match the start prefix.
    if (!start_inclusive) {
        while (idx < getKeyCount(leaf_page) && getLeafKey(leaf_page, idx).rfind(start_key_part, 0) == 0) {
            idx++;
        }
    }

    // This implementation is now correct, assuming the logic to handle
    // advancing to the next page is handled within the main rangeScanKeys loop.
    return {leaf_page, idx};
}
/*
Parameters:
start_key_part: Lower bound of the range (empty for no lower bound).
end_key_part: Upper bound of the range (empty for no upper bound).
start_inclusive: True if the start bound is inclusive (>=), false if exclusive (>).
end_inclusive: True if the end bound is inclusive (<=), false if exclusive (<).
*/

std::vector<std::pair<std::string, std::string>> BTree::rangeScanKeys(
    const std::string& start_key_part, 
    const std::string& end_key_part,bool start_inclusive, bool end_inclusive) const {
    
    std::vector<std::pair<std::string, std::string>> results;
    auto [current_page_ptr, current_index] = findStartPosition(start_key_part, start_inclusive);

    if (!current_page_ptr) return results;

    std::optional<PageReadLatchGuard> guard(current_page_ptr);

    while (current_page_ptr) {
        for (; current_index < getKeyCount(current_page_ptr); ++current_index) {
            std::string current_composite_key = getLeafKey(current_page_ptr, current_index);
            
            // Use string::compare for prefix checking. This is robust.
            // Check end bound first.
            if (!end_key_part.empty()) {
                int cmp = current_composite_key.compare(0, end_key_part.length(), end_key_part);
                if (end_inclusive) {
                    if (cmp > 0) goto end_scan_label; // current > end
                } else {
                    if (cmp >= 0) goto end_scan_label; // current >= end
                }
            }

            // Check start bound. We already started at the right place, but this is a good sanity check.
            if (!start_key_part.empty()) {
                int cmp = current_composite_key.compare(0, start_key_part.length(), start_key_part);
                    if (start_inclusive) {
                    if (cmp < 0) continue; // Should not happen with findStartPosition
                } else {
                    if (cmp <= 0) continue; // Should not happen with corrected findStartPosition
                }
            }
            
            results.emplace_back(current_composite_key, getLeafValue(current_page_ptr, current_index));
        }

        PageId next_id = getNextLeafId(current_page_ptr);
        const_cast<BTree*>(this)->unpinPage(current_page_ptr->getPageId(), false, 0);

        if (next_id == INVALID_PAGE_ID) break;
        
        current_page_ptr = const_cast<BTree*>(this)->fetchPage(next_id);
        guard.emplace(current_page_ptr);
        current_index = 0;
    }

end_scan_label:
    if (current_page_ptr) {
        const_cast<BTree*>(this)->unpinPage(current_page_ptr->getPageId(), false, 0);
    }
    return results;
}


    // Helper to safely print potentially non-printable string data


void BTree::printTree() const {
    // Uses root_pointer_mutex via getRootPageId(), then R-latches pages.
    // No tree_structure_mutex needed for a read-only print.
    PageId current_root = getRootPageId(); // This now correctly uses root_pointer_mutex_

    std::cout << "======== B+ Tree (" << index_name_ << ") ========" << std::endl;
    if (current_root == INVALID_PAGE_ID) {
        std::cout << "Tree is empty." << std::endl;
    } else {
        std::vector<bool> level_markers;
        printTreeRecursive(current_root, 0, level_markers);
    }
    std::cout << "=================================" << std::endl;
}

// Recursive helper for printing tree structure
void BTree::printTreeRecursive(PageId page_id, int level, std::vector<bool>& level_markers) const {
    Page* page = nullptr;
    try {
        page = const_cast<BTree*>(this)->fetchPage(page_id);
        const NodeHeader* header = getHeader(page);

        // Indentation and vertical lines
        for (int i = 0; i < level; ++i) {
            std::cout << (level_markers[i] ? "|  " : "   ");
        }
        std::cout << "+--";

        // Print Page Info
        std::cout << "Page " << page_id
                  << " (" << (header->is_leaf ? "Leaf" : "Internal")
                  << ", Keys: " << header->key_count
                  << ", Parent: " << header->parent_page_id
                  << (header->is_leaf ? ", Next: " + std::to_string(header->next_page_or_sibling_id) : "")
                  << ")" << std::endl;

        // Print Keys (and Values for Leaf)
        for (int i = 0; i < level + 1; ++i) {
             std::cout << (level_markers[i] ? "|  " : "   ");
        }
        std::cout << "Keys: [";
        for (int i = 0; i < header->key_count; ++i) {
            std::string key = header->is_leaf ? getLeafKey(page, i) : getInternalKey(page, i);
            std::cout << "'" << format_key_for_print(key) << "'";
            if (header->is_leaf) {
                std::string val = getLeafValue(page, i);
                 // Keep value print concise
                std::cout << ":'" << (val.length() > 10 ? format_key_for_print(val.substr(0, 10)) + "..." : format_key_for_print(val)) << "'";
            }
            if (i < header->key_count - 1) std::cout << ", ";
        }
        std::cout << "]" << std::endl;


        // Recurse for children if Internal Node
        if (!header->is_leaf) {
            if (level + 1 > level_markers.size()) level_markers.resize(level + 1);
            for (int i = 0; i <= header->key_count; ++i) {
                PageId child_id = getInternalChildId(page, i);
                 // Mark if this is the last child at this level for line drawing
                 level_markers[level] = (i < header->key_count);
                 printTreeRecursive(child_id, level + 1, level_markers);
            }
        }

        const_cast<BTree*>(this)->unpinPage(page_id, false , 0); // Unpin after processing

    } catch (const std::exception& e) {
        if (page) const_cast<BTree*>(this)->unpinPage(page_id, false , 0);
        for (int i = 0; i < level; ++i) std::cout << "   ";
        std::cout << "+-- Error accessing page " << page_id << ": " << e.what() << std::endl;
    }
}


void BTree::printPage(PageId page_id) const {
     //std::shared_lock<std::shared_mutex> lock(tree_structure_mutex_);
     std::cout << "------- Page " << page_id << " Content -------" << std::endl;
     if (page_id == INVALID_PAGE_ID) {
         std::cout << "Invalid Page ID." << std::endl;
         std::cout << "----------------------------" << std::endl;
         return;
     }

     Page* page = nullptr;
     try {
         page = const_cast<BTree*>(this)->fetchPage(page_id);
         const NodeHeader* header = getHeader(page);

         std::cout << "Type:        " << (header->is_leaf ? "Leaf" : "Internal") << std::endl;
         std::cout << "Key Count:   " << header->key_count << std::endl;
         std::cout << "Parent ID:   " << header->parent_page_id << std::endl;
         std::cout << "Free Ptr:    " << header->free_space_pointer << std::endl;

         if (header->is_leaf) {
             std::cout << "Next Leaf:   " << header->next_page_or_sibling_id << std::endl;
             std::cout << "--- Leaf Entries ---" << std::endl;
             for (int i = 0; i < header->key_count; ++i) {
                 std::string key = getLeafKey(page, i);
                 std::string val = getLeafValue(page, i);
                 std::cout << "[" << i << "] Key: '" << format_key_for_print(key) << "'"
                           << " | Value: '" << format_key_for_print(val) << "'"
                            << " (KeyOff: " << getLeafKeyOffsets(page)[i]
                            << ", ValOff: " << getLeafValueOffsets(page)[i] << ")" << std::endl;
             }
         } else {
             std::cout << "--- Internal Entries ---" << std::endl;
              std::cout << "Child[0]: " << getInternalChildId(page, 0) << std::endl;
              for (int i = 0; i < header->key_count; ++i) {
                  std::string key = getInternalKey(page, i);
                   std::cout << "[" << i << "] Key: '" << format_key_for_print(key) << "'"
                             << " (KeyOff: " << getInternalKeyOffsets(page)[i] << ")" << std::endl;
                   std::cout << "Child[" << i + 1 << "]: " << getInternalChildId(page, i + 1) << std::endl;
              }
         }

         const_cast<BTree*>(this)->unpinPage(page_id, false , 0);

     } catch (const std::exception& e) {
          if (page) const_cast<BTree*>(this)->unpinPage(page_id, false , 0);
          std::cerr << "Error printing page " << page_id << ": " << e.what() << std::endl;
     }
      std::cout << "----------------------------" << std::endl;
}

// Helper function to check if a node is full
bool BTree::isFull(const Page* page) const {
    return getKeyCount(page) >= (isLeaf(page) ? MAX_LEAF_KEYS : MAX_INTERNAL_KEYS);
}
