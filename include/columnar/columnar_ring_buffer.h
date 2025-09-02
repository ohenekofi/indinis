// include/columnar/columnar_ring_buffer.h
#pragma once

#include <atomic>
#include <vector>
#include <thread>
#include <cstddef> // For alignas

namespace engine {
namespace columnar {

/**
 * @class ColumnarRingBuffer
 * @brief A high-performance, multiple-producer, single-consumer (MPSC) lock-free ring buffer
 *        specialized for the columnar store's ingestion path.
 */
template<typename T>
class ColumnarRingBuffer {
private:
    struct alignas(64) Slot {
        std::atomic<uint64_t> sequence{0};
        T item;
    };

    static constexpr size_t BUFFER_SIZE = 65536; 
    static_assert((BUFFER_SIZE > 0) && ((BUFFER_SIZE & (BUFFER_SIZE - 1)) == 0), "BUFFER_SIZE must be a power of 2");

    std::vector<Slot> buffer_;
    alignas(64) std::atomic<uint64_t> write_cursor_{0};
    alignas(64) std::atomic<uint64_t> read_cursor_{0};

public:
    ColumnarRingBuffer() : buffer_(BUFFER_SIZE) {
        for (size_t i = 0; i < BUFFER_SIZE; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~ColumnarRingBuffer() = default;
    ColumnarRingBuffer(const ColumnarRingBuffer&) = delete;
    ColumnarRingBuffer& operator=(const ColumnarRingBuffer&) = delete;

    void write(const T& item_to_write) {
        uint64_t pos = write_cursor_.fetch_add(1, std::memory_order_relaxed);
        Slot& slot = buffer_[pos & (BUFFER_SIZE - 1)];
        while (slot.sequence.load(std::memory_order_acquire) != pos) {
            std::this_thread::yield();
        }
        slot.item = item_to_write;
        slot.sequence.store(pos + 1, std::memory_order_release);
    }

    size_t drainTo(std::vector<T>& destination) {
        destination.clear();
        const size_t max_drain_count = 1024;
        const uint64_t current_read_pos = read_cursor_.load(std::memory_order_relaxed);
        const uint64_t latest_write_pos = write_cursor_.load(std::memory_order_acquire);
        if (current_read_pos >= latest_write_pos) return 0;
        const size_t count_to_drain = std::min(static_cast<size_t>(latest_write_pos - current_read_pos), max_drain_count);
        destination.reserve(count_to_drain);
        for (size_t i = 0; i < count_to_drain; ++i) {
            const uint64_t pos = current_read_pos + i;
            Slot& slot = buffer_[pos & (BUFFER_SIZE - 1)];
            while (slot.sequence.load(std::memory_order_acquire) != pos + 1) {
                std::this_thread::yield();
            }
            destination.push_back(std::move(slot.item));
            slot.sequence.store(pos + BUFFER_SIZE, std::memory_order_release);
        }
        read_cursor_.store(current_read_pos + count_to_drain, std::memory_order_release);
        return count_to_drain;
    }
    
    size_t getApproxSize() const {
        const uint64_t write_pos = write_cursor_.load(std::memory_order_relaxed);
        const uint64_t read_pos = read_cursor_.load(std::memory_order_relaxed);
        return (write_pos > read_pos) ? (write_pos - read_pos) : 0;
    }
};

} // namespace columnar
} // namespace engine