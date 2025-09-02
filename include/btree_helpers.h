// include/lsm/btree_helpers.h
#pragma once

#include "btree.h" // Include the main BTree header to get class definitions
#include <string>
#include <vector>

namespace engine {
namespace lsm {
namespace BTreeHelpers { // Use a nested namespace to group the helpers

// Forward declare if BTree isn't fully defined, but including btree.h is better
// class BTree;
// class Page;

// These are now free functions, taking a BTree instance as the first argument
// to access its configuration and page manipulation methods.

int findLeafInsertionPoint(const BTree* tree, const Page* page, const std::string& key);

bool leafHasSpaceFor(const BTree* tree, const Page* page, size_t key_len, size_t val_len);

bool shouldCompactAfterDelete(const BTree* tree, const Page* page);

// A struct for Level 2 optimizations
struct KeyValuePair {
    std::string key;
    std::string value;
};

void rebuildLeafPage(BTree* tree, Page* page, const std::vector<KeyValuePair>& entries);

} // namespace BTreeHelpers
} // namespace lsm
} // namespace engine