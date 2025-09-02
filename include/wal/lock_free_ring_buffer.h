#pragma once

// --- REMOVED --- No longer needed as the type is generic via template
// #include "../types.h" // For LogRecord

#include <atomic>
#include <vector>
#include <thread>
#include <cstddef> // For alignas

/**
 * @class LockFreeRingBuffer
 * @brief A high-performance, multiple-producer, single-consumer (MPSC) lock-free ring buffer.
 *
 * --- MODIFIED --- This class is now a template to be reusable for any data type `T`.
 */
template<typename T> // --- NEW ---
class LockFreeRingBuffer {
private:
    // A Slot in the buffer. Aligned to a 64-byte cache line to prevent "false sharing".
    struct alignas(64) Slot {
        std::atomic<uint64_t> sequence{0};
        T item; // --- MODIFIED --- Was: LogRecord record;
    };

    // Buffer size must be a power of 2 for efficient bitwise-AND modulo arithmetic.
    static constexpr size_t BUFFER_SIZE = 65536; 
    static_assert((BUFFER_SIZE > 0) && ((BUFFER_SIZE & (BUFFER_SIZE - 1)) == 0), "BUFFER_SIZE must be a power of 2");

    // The buffer itself.
    std::vector<Slot> buffer_;

    // Padded to prevent false sharing between cursors and adjacent data.
    alignas(64) std::atomic<uint64_t> write_cursor_{0};
    alignas(64) std::atomic<uint64_t> read_cursor_{0};

public:
    LockFreeRingBuffer() : buffer_(BUFFER_SIZE) {
        // Initialize sequence numbers.
        for (size_t i = 0; i < BUFFER_SIZE; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~LockFreeRingBuffer() = default;
    LockFreeRingBuffer(const LockFreeRingBuffer&) = delete;
    LockFreeRingBuffer& operator=(const LockFreeRingBuffer&) = delete;

    /**
     * @brief Writes an item to the buffer. (Producer-side)
     * This is a blocking write. If the buffer is full, the thread will spin-wait.
     * @param item_to_write The item of type T to write.
     */
    // --- MODIFIED --- Method signature changed to accept generic type T
    void write(const T& item_to_write) {
        uint64_t pos = write_cursor_.fetch_add(1, std::memory_order_relaxed);
        Slot& slot = buffer_[pos & (BUFFER_SIZE - 1)];

        // Spin-wait until the slot is available.
        while (slot.sequence.load(std::memory_order_acquire) != pos) {
            std::this_thread::yield();
        }

        // Write the data and publish it.
        // --- MODIFIED --- Assignment to generic item member
        slot.item = item_to_write;

        // Publish the write by updating the sequence number.
        slot.sequence.store(pos + 1, std::memory_order_release);
    }

    /**
     * @brief Drains all available items from the buffer into a destination vector. (Consumer-side)
     * This is a non-blocking operation. It reads as many items as are currently published.
     * @param destination The vector of type T to which items will be moved. It is cleared first.
     * @return The number of items drained from the buffer.
     */
    // --- MODIFIED --- Method signature changed to accept vector of generic type T
    size_t drainTo(std::vector<T>& destination) {
        destination.clear();
        const size_t max_drain_count = 1024;

        const uint64_t current_read_pos = read_cursor_.load(std::memory_order_relaxed);
        const uint64_t latest_write_pos = write_cursor_.load(std::memory_order_acquire);

        if (current_read_pos >= latest_write_pos) {
            return 0; // Buffer is empty.
        }

        const size_t available_count = latest_write_pos - current_read_pos;
        const size_t count_to_drain = std::min(available_count, max_drain_count);
        destination.reserve(count_to_drain);

        for (size_t i = 0; i < count_to_drain; ++i) {
            const uint64_t pos = current_read_pos + i;
            Slot& slot = buffer_[pos & (BUFFER_SIZE - 1)];

            // Spin-wait until the data in this slot is fully published.
            while (slot.sequence.load(std::memory_order_acquire) != pos + 1) {
                std::this_thread::yield();
            }

            // Move the item out.
            // --- MODIFIED --- Move generic item member
            destination.push_back(std::move(slot.item));

            // Free the slot for the *next* writer that will claim this physical slot.
            slot.sequence.store(pos + BUFFER_SIZE, std::memory_order_release);
        }

        // Update the read cursor in a single atomic operation after draining the batch.
        read_cursor_.store(current_read_pos + count_to_drain, std::memory_order_release);
        return count_to_drain;
    }
    
    size_t getApproxSize() const {
        const uint64_t write_pos = write_cursor_.load(std::memory_order_relaxed);
        const uint64_t read_pos = read_cursor_.load(std::memory_order_relaxed);
        return (write_pos > read_pos) ? (write_pos - read_pos) : 0;
    }
};