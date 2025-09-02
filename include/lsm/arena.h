// include/lsm/arena.h
#pragma once

#include <vector>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <atomic>
#include <mutex>

namespace engine {
namespace lsm {

/**
 * @class Arena
 * @brief A fast, thread-safe memory allocator for the SkipList MemTable.
 *
 * It works by allocating large blocks of memory and then serving smaller
 * allocation requests from these blocks using simple pointer arithmetic (a "bump allocator").
 * This is much faster than calling `new` or `malloc` for every node and avoids
 * memory fragmentation. It is not a general-purpose allocator as memory cannot be freed individually.
 * The entire arena is freed at once when the MemTable is destroyed.
 */
class Arena {
public:
    Arena();
    ~Arena() = default;

    // Arena is non-copyable and non-movable.
    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;

    /**
     * @brief Allocates a block of memory of the given size.
     * @param bytes The number of bytes to allocate.
     * @return A pointer to the allocated memory.
     */
    char* Allocate(size_t bytes);

    /**
     * @brief Allocates a block of memory with alignment.
     * @param bytes The number of bytes to allocate.
     * @return A pointer to the allocated memory, aligned to the system's pointer size.
     */
    char* AllocateAligned(size_t bytes);

    /**
     * @brief Returns an estimate of the total memory allocated by the arena.
     */
    size_t MemoryUsage() const;

private:
    char* AllocateFallback(size_t bytes);
    char* AllocateNewBlock(size_t block_bytes);

    // Allocation state
    std::atomic<char*> alloc_ptr_;
    std::atomic<size_t> alloc_bytes_remaining_;
    
    // Vector of allocated memory blocks for RAII cleanup.
    // This part is not thread-safe for concurrent additions of new blocks,
    // so it must be protected by a mutex.
    std::vector<std::unique_ptr<char[]>> blocks_;
    mutable std::mutex blocks_mutex_;
};

} // namespace lsm
} // namespace engine