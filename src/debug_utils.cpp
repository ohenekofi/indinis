#include "debug_utils.h"  // For the declarations
#include "lsm_tree.h"     // For the full definition of LSMTree and LSMTree::MemTable
#include <cstdint>        // For uintptr_t (good to be explicit)

// Definition for the raw pointer version
std::string MemTableToHexString(const LSMTree::MemTable* mt_ptr) {
    if (!mt_ptr) {
        return "nullptr";
    }
    std::ostringstream oss;
    oss << "0x" << std::hex << std::setw(sizeof(void*) * 2) << std::setfill('0')
        << reinterpret_cast<uintptr_t>(mt_ptr);
    return oss.str();
}

// Definition for the shared_ptr version
std::string MemTableToHexString(const std::shared_ptr<LSMTree::MemTable>& mt_sptr) {
    return MemTableToHexString(mt_sptr.get()); // Calls the raw pointer version
}