// include/threading/rate_limiter.h
#pragma once

#include <chrono>
#include <mutex>
#include <condition_variable>
#include <cstdint>

namespace engine {
namespace threading {

/**
 * @class TokenBucketRateLimiter
 * @brief A thread-safe rate limiter based on the token bucket algorithm.
 */
class TokenBucketRateLimiter {
public:
    /**
     * @param rate_bytes_per_sec The average rate limit in bytes per second.
     * @param capacity_bytes The maximum burst capacity in bytes.
     */
    TokenBucketRateLimiter(size_t rate_bytes_per_sec, size_t capacity_bytes);

    TokenBucketRateLimiter(const TokenBucketRateLimiter&) = delete;
    TokenBucketRateLimiter& operator=(const TokenBucketRateLimiter&) = delete;

    /**
     * @brief Consumes the specified number of bytes (tokens). Blocks if not enough tokens are available.
     * @param bytes The number of bytes to consume.
     */
    void consume(size_t bytes);

private:
    void refill();

    const size_t rate_bytes_per_sec_;
    const size_t capacity_bytes_;
    
    std::mutex mutex_;
    std::condition_variable cv_;
    double current_tokens_;
    std::chrono::steady_clock::time_point last_fill_time_;
};

} // namespace threading
} // namespace engine