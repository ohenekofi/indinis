// src/threading/rate_limiter.cpp
#include "../../include/threading/rate_limiter.h"
#include <algorithm> // For std::min

namespace engine {
namespace threading {

TokenBucketRateLimiter::TokenBucketRateLimiter(size_t rate_bytes_per_sec, size_t capacity_bytes)
    : rate_bytes_per_sec_(rate_bytes_per_sec),
      capacity_bytes_(capacity_bytes > 0 ? capacity_bytes : rate_bytes_per_sec), // Capacity must be at least the rate
      current_tokens_(capacity_bytes), // Start with a full bucket
      last_fill_time_(std::chrono::steady_clock::now())
{}

void TokenBucketRateLimiter::refill() {
    // This private method assumes the mutex is already held by the caller.
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(now - last_fill_time_);
    
    double tokens_to_add = elapsed.count() * rate_bytes_per_sec_;
    
    if (tokens_to_add > 0) {
        current_tokens_ = std::min(current_tokens_ + tokens_to_add, static_cast<double>(capacity_bytes_));
        last_fill_time_ = now;
    }
}

void TokenBucketRateLimiter::consume(size_t bytes) {
    if (rate_bytes_per_sec_ == 0) { // If rate is 0, block forever (effectively disabled)
        // This is a safety check; a real system might handle this differently.
        return;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    
    // If the requested amount is larger than the entire bucket capacity,
    // we handle it by directly sleeping for the required time. This prevents
    // starving smaller requests while waiting for a huge one.
    if (bytes > capacity_bytes_) {
        double required_time_sec = static_cast<double>(bytes) / rate_bytes_per_sec_;
        lock.unlock(); // Don't hold lock while sleeping
        std::this_thread::sleep_for(std::chrono::duration<double>(required_time_sec));
        return;
    }

    // Wait until enough tokens are available in the bucket.
    cv_.wait(lock, [this, bytes] {
        refill(); // Refill tokens based on time elapsed since last check.
        return current_tokens_ >= bytes;
    });

    // Consume the tokens.
    current_tokens_ -= bytes;
}

} // namespace threading
} // namespace engine