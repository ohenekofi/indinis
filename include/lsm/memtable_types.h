// include/lsm/memtable_types.h
#pragma once

namespace engine {
namespace lsm {

/**
 * @brief Defines the available concrete implementations for a MemTableRep.
 * This is used by factory functions to create the correct type of memtable.
 */
enum class MemTableType { 
    SKIP_LIST, 
    HASH_TABLE, 
    VECTOR,  
    COMPRESSED_SKIP_LIST,
    PREFIX_HASH_TABLE,
    RCU
};

} // namespace lsm
} // namespace engine