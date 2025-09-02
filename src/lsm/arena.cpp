// src/lsm/arena.cpp
#include "../../include/lsm/arena.h"
#include <algorithm>

namespace engine {
namespace lsm {

Arena::Arena() : alloc_ptr_(nullptr), alloc_bytes_remaining_(0) {}

char* Arena::Allocate(size_t bytes) {
    if (bytes <= alloc_bytes_remaining_.load(std::memory_order_relaxed)) {
        char* result = alloc_ptr_.fetch_add(bytes, std::memory_order_relaxed);
        alloc_bytes_remaining_.fetch_sub(bytes, std::memory_order_relaxed);
        return result;
    }
    return AllocateFallback(bytes);
}

char* Arena::AllocateAligned(size_t bytes) {
    const int align = sizeof(void*);
    size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_.load()) & (align - 1);
    size_t slop = (current_mod == 0) ? 0 : align - current_mod;
    size_t needed = bytes + slop;
    char* result;
    if (needed <= alloc_bytes_remaining_.load()) {
        result = alloc_ptr_.load() + slop;
        alloc_ptr_.store(result + bytes);
        alloc_bytes_remaining_.fetch_sub(needed);
    } else {
        result = AllocateFallback(bytes);
    }
    return result;
}

size_t Arena::MemoryUsage() const {
    std::lock_guard<std::mutex> lock(blocks_mutex_);
    size_t total = 0;
    // Heuristic: A block is typically 4096 bytes. Summing the vector capacity might not be accurate.
    // A better way is to track it during allocation.
    // For now, this is a rough estimate.
    total = blocks_.size() * 4096;
    return total;
}

char* Arena::AllocateFallback(size_t bytes) {
    if (bytes > 4096 / 4) {
        // Object is > 1/4 of block size. Allocate it separately to avoid wasting space.
        return AllocateNewBlock(bytes);
    }
    // Allocate new block and then serve the request from it.
    char* new_block_start = AllocateNewBlock(4096);
    char* result = new_block_start;
    alloc_ptr_.store(new_block_start + bytes);
    alloc_bytes_remaining_.store(4096 - bytes);
    return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
    std::lock_guard<std::mutex> lock(blocks_mutex_);
    auto new_block = std::make_unique<char[]>(block_bytes);
    char* result = new_block.get();
    blocks_.push_back(std::move(new_block));
    return result;
}

} // namespace lsm
} // namespace engine