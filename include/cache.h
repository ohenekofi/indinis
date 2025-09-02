#pragma once

#include <unordered_map>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <chrono>
#include <functional>
#include <optional>
#include <string>
#include <vector>
#include <algorithm>

namespace cache {

// Forward declarations
class CacheValue;
class CacheStats;

// Cache eviction policies
enum class EvictionPolicy {
    LRU,    // Least Recently Used
    LFU,    // Least Frequently Used
    FIFO,   // First In, First Out
    TTL     // Time To Live based
};

// Cache configuration
struct CacheConfig {
    size_t max_size = 1000;
    EvictionPolicy policy = EvictionPolicy::LRU;
    std::chrono::milliseconds default_ttl = std::chrono::milliseconds(0); // 0 = no TTL
    bool enable_stats = true;
    size_t shard_count = 16; // For reduced lock contention
};

// Thread-safe cache value wrapper
class CacheValue {
private:
    std::string data_;
    std::chrono::steady_clock::time_point created_at_;
    mutable std::chrono::steady_clock::time_point accessed_at_;
    std::chrono::milliseconds ttl_;
    mutable std::atomic<uint64_t> access_count_{0};

public:
    CacheValue(const std::string& data, std::chrono::milliseconds ttl = std::chrono::milliseconds(0))
        : data_(data), ttl_(ttl) {
        auto now = std::chrono::steady_clock::now();
        created_at_ = now;
        accessed_at_ = now;
    }

    const std::string& get_data() const {
        access_count_.fetch_add(1, std::memory_order_relaxed);
        accessed_at_ = std::chrono::steady_clock::now();
        return data_;
    }

    void set_data(const std::string& data) {
        data_ = data;
        accessed_at_ = std::chrono::steady_clock::now();
    }

    bool is_expired() const {
        if (ttl_.count() == 0) return false;
        auto now = std::chrono::steady_clock::now();
        return (now - created_at_) > ttl_;
    }

    uint64_t get_access_count() const {
        return access_count_.load(std::memory_order_relaxed);
    }

    std::chrono::steady_clock::time_point get_accessed_at() const {
        return accessed_at_;
    }

    std::chrono::steady_clock::time_point get_created_at() const {
        return created_at_;
    }

    void update_access_time() const {
        accessed_at_ = std::chrono::steady_clock::now();
    }
};

// Cache statistics
class CacheStats {
private:
    mutable std::mutex stats_mutex_; // Mutex for reset operation
    std::atomic<uint64_t> hits_{0};
    std::atomic<uint64_t> misses_{0};
    std::atomic<uint64_t> evictions_{0};
    std::atomic<uint64_t> expired_removals_{0};

public:
    // Default constructor
    CacheStats() = default;

    // --- NEW: Explicit Copy Constructor ---
    CacheStats(const CacheStats& other) {
        // Atomically load values from 'other' and store them in 'this'.
        // The new stats_mutex_ in 'this' object will be default-constructed (unlocked).
        hits_.store(other.hits_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        misses_.store(other.misses_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        evictions_.store(other.evictions_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        expired_removals_.store(other.expired_removals_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    }

    // --- NEW: Explicit Copy Assignment Operator (Good practice if copy constructor is defined) ---
    CacheStats& operator=(const CacheStats& other) {
        if (this == &other) {
            return *this;
        }
        // Lock own mutex if we were modifying something that wasn't atomic, but here we are just
        // copying atomic values. The 'other' object's atomics are read atomically.
        // No lock needed on 'other.stats_mutex_' because getters are const and use atomics.
        // No lock needed on 'this->stats_mutex_' because we are only assigning to atomics.
        hits_.store(other.hits_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        misses_.store(other.misses_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        evictions_.store(other.evictions_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        expired_removals_.store(other.expired_removals_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        return *this;
    }

    // --- Move constructor and assignment can be defaulted if they do the right thing
    //     or also be explicitly defined if necessary. Defaulting should be fine
    //     as std::atomic and std::mutex have their respective move behaviors.
    //     std::mutex is not move-assignable by default.
    //     However, since we are typically returning a *copy* for stats, copy is more critical.
    // ---
    // CacheStats(CacheStats&& other) noexcept = default; // Might be fine if std::mutex default move is ok for its use
    // CacheStats& operator=(CacheStats&& other) noexcept = default;

    // --- Explicit Move Constructor (safer if std::mutex needs specific handling, though not strictly required if copied mutex is just default constructed)
    CacheStats(CacheStats&& other) noexcept
    : hits_(other.hits_.load(std::memory_order_relaxed)), // Move from atomic is a load
      misses_(other.misses_.load(std::memory_order_relaxed)),
      evictions_(other.evictions_.load(std::memory_order_relaxed)),
      expired_removals_(other.expired_removals_.load(std::memory_order_relaxed))
    {
        // For 'other' after move, reset its values if desired, or leave as is (atomics are moved from)
        other.hits_.store(0, std::memory_order_relaxed);
        other.misses_.store(0, std::memory_order_relaxed);
        other.evictions_.store(0, std::memory_order_relaxed);
        other.expired_removals_.store(0, std::memory_order_relaxed);
        // The mutex member `stats_mutex_` in `this` will be default constructed.
        // The `other.stats_mutex_` is left in a valid but unspecified state (as per std::mutex move).
    }

    // --- Explicit Move Assignment Operator
    CacheStats& operator=(CacheStats&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        hits_.store(other.hits_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        misses_.store(other.misses_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        evictions_.store(other.evictions_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        expired_removals_.store(other.expired_removals_.load(std::memory_order_relaxed), std::memory_order_relaxed);

        other.hits_.store(0, std::memory_order_relaxed);
        other.misses_.store(0, std::memory_order_relaxed);
        other.evictions_.store(0, std::memory_order_relaxed);
        other.expired_removals_.store(0, std::memory_order_relaxed);
        // Mutexes are not move-assignable. `this->stats_mutex_` remains as is.
        return *this;
    }


    void record_hit() { hits_.fetch_add(1, std::memory_order_relaxed); }
    void record_miss() { misses_.fetch_add(1, std::memory_order_relaxed); }
    void record_eviction() { evictions_.fetch_add(1, std::memory_order_relaxed); }
    void record_expired_removal() { expired_removals_.fetch_add(1, std::memory_order_relaxed); }

    uint64_t get_hits() const { return hits_.load(std::memory_order_relaxed); }
    uint64_t get_misses() const { return misses_.load(std::memory_order_relaxed); }
    uint64_t get_evictions() const { return evictions_.load(std::memory_order_relaxed); }
    uint64_t get_expired_removals() const { return expired_removals_.load(std::memory_order_relaxed); }

    double get_hit_rate() const {
        // Use relaxed memory order for loads as these are just stats reads
        uint64_t current_hits = hits_.load(std::memory_order_relaxed);
        uint64_t current_misses = misses_.load(std::memory_order_relaxed);
        uint64_t total_requests = current_hits + current_misses;
        return total_requests > 0 ? static_cast<double>(current_hits) / total_requests : 0.0;
    }

    void reset() {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        hits_.store(0, std::memory_order_relaxed);
        misses_.store(0, std::memory_order_relaxed);
        evictions_.store(0, std::memory_order_relaxed);
        expired_removals_.store(0, std::memory_order_relaxed);
    }
};

// LRU Cache implementation
class LRUCache {
private:
    using KeyType = std::string;
    using ValueType = std::shared_ptr<CacheValue>;
    using ListIterator = std::list<std::pair<KeyType, ValueType>>::iterator;

    std::list<std::pair<KeyType, ValueType>> items_list_;
    std::unordered_map<KeyType, ListIterator> items_map_;
    mutable std::shared_mutex cache_mutex_;
    size_t max_size_;
    CacheStats& stats_;

    void move_to_front(ListIterator it) {
        items_list_.splice(items_list_.begin(), items_list_, it);
    }

    void evict_lru() {
        if (!items_list_.empty()) {
            auto last = items_list_.end();
            --last;
            items_map_.erase(last->first);
            items_list_.erase(last);
            stats_.record_eviction();
        }
    }

public:
    LRUCache(size_t max_size, CacheStats& stats) : max_size_(max_size), stats_(stats) {}

    bool put(const KeyType& key, const ValueType& value) {
        std::unique_lock<std::shared_mutex> lock(cache_mutex_);
        
        auto it = items_map_.find(key);
        if (it != items_map_.end()) {
            // Update existing item
            it->second->second = value;
            move_to_front(it->second);
            return true;
        }

        // Add new item
        if (items_list_.size() >= max_size_) {
            evict_lru();
        }

        items_list_.emplace_front(key, value);
        items_map_[key] = items_list_.begin();
        return true;
    }

    std::optional<ValueType> get(const KeyType& key) {
        std::shared_lock<std::shared_mutex> shared_lock(cache_mutex_);
        
        auto it = items_map_.find(key);
        if (it == items_map_.end()) {
            stats_.record_miss();
            return std::nullopt;
        }

        // Check if expired
        if (it->second->second->is_expired()) {
            shared_lock.unlock();
            std::unique_lock<std::shared_mutex> unique_lock(cache_mutex_);
            items_map_.erase(it);
            items_list_.erase(it->second);
            stats_.record_expired_removal();
            stats_.record_miss();
            return std::nullopt;
        }

        // Move to front (need to upgrade to unique lock)
        shared_lock.unlock();
        std::unique_lock<std::shared_mutex> unique_lock(cache_mutex_);
        
        // Re-check after lock upgrade
        it = items_map_.find(key);
        if (it != items_map_.end() && !it->second->second->is_expired()) {
            move_to_front(it->second);
            stats_.record_hit();
            return it->second->second;
        }
        
        stats_.record_miss();
        return std::nullopt;
    }

    bool remove(const KeyType& key) {
        std::unique_lock<std::shared_mutex> lock(cache_mutex_);
        
        auto it = items_map_.find(key);
        if (it == items_map_.end()) {
            return false;
        }

        items_list_.erase(it->second);
        items_map_.erase(it);
        return true;
    }

    void clear() {
        std::unique_lock<std::shared_mutex> lock(cache_mutex_);
        items_list_.clear();
        items_map_.clear();
    }

    size_t size() const {
        std::shared_lock<std::shared_mutex> lock(cache_mutex_);
        return items_list_.size();
    }

    void cleanup_expired() {
        std::unique_lock<std::shared_mutex> lock(cache_mutex_);
        
        auto it = items_list_.begin();
        while (it != items_list_.end()) {
            if (it->second->is_expired()) {
                items_map_.erase(it->first);
                it = items_list_.erase(it);
                stats_.record_expired_removal();
            } else {
                ++it;
            }
        }
    }
};

// LFU Cache implementation
class LFUCache {
private:
    using KeyType = std::string;
    using ValueType = std::shared_ptr<CacheValue>;
    
    struct FrequencyNode {
        uint64_t frequency;
        std::list<std::pair<KeyType, ValueType>> items;
        std::shared_ptr<FrequencyNode> prev, next;
        
        FrequencyNode(uint64_t freq) : frequency(freq) {}
    };

    std::unordered_map<KeyType, std::pair<ValueType, std::list<std::pair<KeyType, ValueType>>::iterator>> items_map_;
    std::unordered_map<uint64_t, std::shared_ptr<FrequencyNode>> freq_map_;
    std::shared_ptr<FrequencyNode> head_, tail_;
    mutable std::shared_mutex cache_mutex_;
    size_t max_size_;
    CacheStats& stats_;

    void init_frequency_list() {
        head_ = std::make_shared<FrequencyNode>(0);
        tail_ = std::make_shared<FrequencyNode>(UINT64_MAX);
        head_->next = tail_;
        tail_->prev = head_;
    }

    void remove_frequency_node(std::shared_ptr<FrequencyNode> node) {
        node->prev->next = node->next;
        node->next->prev = node->prev;
        freq_map_.erase(node->frequency);
    }

    void insert_frequency_node_after(std::shared_ptr<FrequencyNode> prev_node, std::shared_ptr<FrequencyNode> new_node) {
        new_node->next = prev_node->next;
        new_node->prev = prev_node;
        prev_node->next->prev = new_node;
        prev_node->next = new_node;
    }

    void update_frequency(const KeyType& key, const ValueType& value) {
        uint64_t old_freq = value->get_access_count() - 1;
        uint64_t new_freq = value->get_access_count();

        auto old_freq_node = freq_map_[old_freq];
        auto item_it = items_map_[key].second;
        
        old_freq_node->items.erase(item_it);
        
        if (old_freq_node->items.empty() && old_freq > 0) {
            remove_frequency_node(old_freq_node);
        }

        auto new_freq_it = freq_map_.find(new_freq);
        std::shared_ptr<FrequencyNode> new_freq_node;
        
        if (new_freq_it == freq_map_.end()) {
            new_freq_node = std::make_shared<FrequencyNode>(new_freq);
            freq_map_[new_freq] = new_freq_node;
            
            auto insert_after = old_freq_node;
            if (old_freq_node->items.empty() && old_freq > 0) {
                insert_after = old_freq_node->prev;
            }
            insert_frequency_node_after(insert_after, new_freq_node);
        } else {
            new_freq_node = new_freq_it->second;
        }

        new_freq_node->items.emplace_back(key, value);
        items_map_[key].second = std::prev(new_freq_node->items.end());
    }

    void evict_lfu() {
        auto min_freq_node = head_->next;
        if (min_freq_node != tail_ && !min_freq_node->items.empty()) {
            auto& lfu_item = min_freq_node->items.front();
            items_map_.erase(lfu_item.first);
            min_freq_node->items.pop_front();
            
            if (min_freq_node->items.empty()) {
                remove_frequency_node(min_freq_node);
            }
            stats_.record_eviction();
        }
    }

public:
    LFUCache(size_t max_size, CacheStats& stats) : max_size_(max_size), stats_(stats) {
        init_frequency_list();
    }

    bool put(const KeyType& key, const ValueType& value) {
        std::unique_lock<std::shared_mutex> lock(cache_mutex_);
        
        auto it = items_map_.find(key);
        if (it != items_map_.end()) {
            // Update existing item
            it->second.first = value;
            update_frequency(key, value);
            return true;
        }

        // Add new item
        if (items_map_.size() >= max_size_) {
            evict_lfu();
        }

        uint64_t initial_freq = 1;
        auto freq_it = freq_map_.find(initial_freq);
        std::shared_ptr<FrequencyNode> freq_node;
        
        if (freq_it == freq_map_.end()) {
            freq_node = std::make_shared<FrequencyNode>(initial_freq);
            freq_map_[initial_freq] = freq_node;
            insert_frequency_node_after(head_, freq_node);
        } else {
            freq_node = freq_it->second;
        }

        freq_node->items.emplace_back(key, value);
        items_map_[key] = {value, std::prev(freq_node->items.end())};
        return true;
    }

    std::optional<ValueType> get(const KeyType& key) {
        std::unique_lock<std::shared_mutex> lock(cache_mutex_);
        
        auto it = items_map_.find(key);
        if (it == items_map_.end()) {
            stats_.record_miss();
            return std::nullopt;
        }

        // Check if expired
        if (it->second.first->is_expired()) {
            // Remove expired item
            auto freq_node = freq_map_[it->second.first->get_access_count()];
            freq_node->items.erase(it->second.second);
            if (freq_node->items.empty()) {
                remove_frequency_node(freq_node);
            }
            items_map_.erase(it);
            stats_.record_expired_removal();
            stats_.record_miss();
            return std::nullopt;
        }

        // Update frequency
        auto value = it->second.first;
        value->get_data(); // This increments access count
        update_frequency(key, value);
        
        stats_.record_hit();
        return value;
    }

    bool remove(const KeyType& key) {
        std::unique_lock<std::shared_mutex> lock(cache_mutex_);
        
        auto it = items_map_.find(key);
        if (it == items_map_.end()) {
            return false;
        }

        auto freq_node = freq_map_[it->second.first->get_access_count()];
        freq_node->items.erase(it->second.second);
        if (freq_node->items.empty()) {
            remove_frequency_node(freq_node);
        }
        items_map_.erase(it);
        return true;
    }

    void clear() {
        std::unique_lock<std::shared_mutex> lock(cache_mutex_);
        items_map_.clear();
        freq_map_.clear();
        init_frequency_list();
    }

    size_t size() const {
        std::shared_lock<std::shared_mutex> lock(cache_mutex_);
        return items_map_.size();
    }

    void cleanup_expired() {
        std::unique_lock<std::shared_mutex> lock(cache_mutex_);
        
        auto it = items_map_.begin();
        while (it != items_map_.end()) {
            if (it->second.first->is_expired()) {
                auto freq_node = freq_map_[it->second.first->get_access_count()];
                freq_node->items.erase(it->second.second);
                if (freq_node->items.empty()) {
                    remove_frequency_node(freq_node);
                }
                it = items_map_.erase(it);
                stats_.record_expired_removal();
            } else {
                ++it;
            }
        }
    }
};

// Main Cache class - facade pattern
class Cache {
private:
    CacheConfig config_;
    std::unique_ptr<CacheStats> stats_;
    std::unique_ptr<LRUCache> lru_cache_;
    std::unique_ptr<LFUCache> lfu_cache_;
    
    // Background cleanup
    std::atomic<bool> cleanup_running_{true};
    std::thread cleanup_thread_;

    void background_cleanup() {
        while (cleanup_running_.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(60)); // Cleanup every minute
            
            if (!cleanup_running_.load()) break;
            
            try {
                cleanup_expired();
            } catch (const std::exception& e) {
                std::cerr << "Background cache cleanup error: " << e.what() << std::endl;
            }
        }
    }

public:
    explicit Cache(const CacheConfig& config = CacheConfig{}) : config_(config) {
        if (config_.enable_stats) {
            stats_ = std::make_unique<CacheStats>();
        }

        switch (config_.policy) {
            case EvictionPolicy::LRU:
                lru_cache_ = std::make_unique<LRUCache>(config_.max_size, *stats_);
                break;
            case EvictionPolicy::LFU:
                lfu_cache_ = std::make_unique<LFUCache>(config_.max_size, *stats_);
                break;
            default:
                lru_cache_ = std::make_unique<LRUCache>(config_.max_size, *stats_);
                break;
        }

        // Start background cleanup thread
        cleanup_thread_ = std::thread(&Cache::background_cleanup, this);
    }

    ~Cache() {
        cleanup_running_.store(false);
        if (cleanup_thread_.joinable()) {
            cleanup_thread_.join();
        }
    }

    // Disable copy constructor and assignment
    Cache(const Cache&) = delete;
    Cache& operator=(const Cache&) = delete;

    // Enable move constructor and assignment
    Cache(Cache&&) = default;
    Cache& operator=(Cache&&) = default;

    bool put(const std::string& key, const std::string& value, 
             std::chrono::milliseconds ttl = std::chrono::milliseconds(0)) {
        if (key.empty()) return false;
        
        auto cache_ttl = (ttl.count() > 0) ? ttl : config_.default_ttl;
        auto cache_value = std::make_shared<CacheValue>(value, cache_ttl);

        switch (config_.policy) {
            case EvictionPolicy::LRU:
                return lru_cache_->put(key, cache_value);
            case EvictionPolicy::LFU:
                return lfu_cache_->put(key, cache_value);
            default:
                return false;
        }
    }

    std::optional<std::string> get(const std::string& key) {
        if (key.empty()) return std::nullopt;

        std::optional<std::shared_ptr<CacheValue>> result;
        
        switch (config_.policy) {
            case EvictionPolicy::LRU:
                result = lru_cache_->get(key);
                break;
            case EvictionPolicy::LFU:
                result = lfu_cache_->get(key);
                break;
            default:
                return std::nullopt;
        }

        if (result && result.value()) {
            return result.value()->get_data();
        }
        return std::nullopt;
    }

    bool remove(const std::string& key) {
        if (key.empty()) return false;

        switch (config_.policy) {
            case EvictionPolicy::LRU:
                return lru_cache_->remove(key);
            case EvictionPolicy::LFU:
                return lfu_cache_->remove(key);
            default:
                return false;
        }
    }

    void clear() {
        switch (config_.policy) {
            case EvictionPolicy::LRU:
                lru_cache_->clear();
                break;
            case EvictionPolicy::LFU:
                lfu_cache_->clear();
                break;
        }
        
        if (stats_) {
            stats_->reset();
        }
    }

    size_t size() const {
        switch (config_.policy) {
            case EvictionPolicy::LRU:
                return lru_cache_->size();
            case EvictionPolicy::LFU:
                return lfu_cache_->size();
            default:
                return 0;
        }
    }

    bool contains(const std::string& key) {
        return get(key).has_value();
    }

    void cleanup_expired() {
        switch (config_.policy) {
            case EvictionPolicy::LRU:
                lru_cache_->cleanup_expired();
                break;
            case EvictionPolicy::LFU:
                lfu_cache_->cleanup_expired();
                break;
        }
    }

    // Statistics
    const CacheStats* get_stats() const {
        return stats_.get();
    }

    // Configuration access
    const CacheConfig& get_config() const {
        return config_;
    }
};

} // namespace cache

// Example usage and test functions
#ifdef CACHE_LIBRARY_EXAMPLE

#include <iostream>
#include <cassert>
#include <thread>
#include <chrono>

void example_basic_usage() {
    std::cout << "\n=== Basic Usage Example ===\n";
    
    cache::CacheConfig config;
    config.max_size = 3;
    config.policy = cache::EvictionPolicy::LRU;
    
    cache::Cache cache(config);
    
    // Basic operations
    cache.put("key1", "value1");
    cache.put("key2", "value2");
    cache.put("key3", "value3");
    
    std::cout << "Cache size: " << cache.size() << std::endl;
    
    auto value = cache.get("key1");
    if (value) {
        std::cout << "Retrieved: " << *value << std::endl;
    }
    
    // This should evict key2 (LRU)
    cache.put("key4", "value4");
    
    if (!cache.get("key2")) {
        std::cout << "key2 was evicted (LRU policy)" << std::endl;
    }
    
    // Statistics
    auto stats = cache.get_stats();
    if (stats) {
        std::cout << "Hit rate: " << stats->get_hit_rate() * 100 << "%" << std::endl;
        std::cout << "Hits: " << stats->get_hits() << ", Misses: " << stats->get_misses() << std::endl;
    }
}

void example_ttl_usage() {
    std::cout << "\n=== TTL Example ===\n";
    
    cache::Cache cache;
    
    // Add item with 100ms TTL
    cache.put("temp_key", "temp_value", std::chrono::milliseconds(100));
    
    auto value = cache.get("temp_key");
    if (value) {
        std::cout << "Retrieved before expiry: " << *value << std::endl;
    }
    
    // Wait for expiry
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    
    value = cache.get("temp_key");
    if (!value) {
        std::cout << "Key expired and was removed" << std::endl;
    }
}

void example_lfu_usage() {
    std::cout << "\n=== LFU Example ===\n";
    
    cache::CacheConfig config;
    config.max_size = 3;
    config.policy = cache::EvictionPolicy::LFU;
    
    cache::Cache cache(config);
    
    cache.put("key1", "value1");
    cache.put("key2", "value2");
    cache.put("key3", "value3");
    
    // Access key1 multiple times
    cache.get("key1");
    cache.get("key1");
    cache.get("key1");
    
    // Access key2 once
    cache.get("key2");
    
    // key3 is never accessed, so it should be evicted first
    cache.put("key4", "value4");
    
    if (!cache.get("key3")) {
        std::cout << "key3 was evicted (LFU policy - never accessed)" << std::endl;
    }
    
    if (cache.get("key1")) {
        std::cout << "key1 still exists (most frequently used)" << std::endl;
    }
}

int main() {
    example_basic_usage();
    example_ttl_usage();
    example_lfu_usage();
    
    std::cout << "\nAll examples completed successfully!" << std::endl;
    return 0;
}

#endif // CACHE_LIBRARY_EXAMPLE