// src/lsm/fifo_compaction_strategy.cpp
#include "../../include/lsm/fifo_compaction_strategy.h"
#include "../../include/lsm_tree.h"
#include "../../include/debug_utils.h"
#include "../../include/storage_error/storage_error.h"
#include "../../include/storage_error/error_codes.h"

namespace engine {
namespace lsm {

FIFOCompactionStrategy::FIFOCompactionStrategy(const FIFOCompactionConfig& config) 
    : config_(config),
      creation_time_(std::chrono::steady_clock::now())
{
    if (!config_.is_valid()) {
        throw storage::StorageError(storage::ErrorCode::INVALID_CONFIGURATION,
            "FIFOCompactionStrategy: Invalid configuration - " + config_.to_string());
    }
    LOG_INFO("[FIFOStrategy] Created with config: {}", config_.to_string());
}

FIFOCompactionStrategy::~FIFOCompactionStrategy() {
    auto lifetime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - creation_time_
    );
    LOG_INFO("[FIFOStrategy] Destroyed after {}s. Final metrics: Files_deleted={}, Bytes_deleted={}MB, Avg_select_time={:.2f}us",
             lifetime.count(),
             metrics_.total_files_deleted.load(),
             metrics_.total_bytes_deleted.load() / (1024*1024),
             metrics_.average_selection_time_us());
}

std::optional<CompactionJobForIndinis> FIFOCompactionStrategy::SelectCompaction(
    const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& levels_snapshot,
    int max_level
) const {
    auto start_time = std::chrono::steady_clock::now();
    
    // If TTL is disabled, this strategy does nothing.
    if (config_.ttl_seconds.count() <= 0) {
        metrics_.record_selection_time(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time));
        return std::nullopt;
    }
    
    auto now = std::chrono::system_clock::now();
    auto expiry_time = now - config_.ttl_seconds;
    std::vector<std::shared_ptr<SSTableMetadata>> files_to_delete;

    // FIFO compaction typically operates only on L0, but scanning all levels is more robust.
    // We scan from oldest to newest to delete the oldest files first.
    for (int level_idx = max_level - 1; level_idx >= 0; --level_idx) {
        if (static_cast<size_t>(level_idx) >= levels_snapshot.size()) continue;

        // Sort files by age (oldest first) to ensure we delete the oldest first
        auto level_files = levels_snapshot[level_idx];
        std::sort(level_files.begin(), level_files.end(), 
            [](const auto& a, const auto& b) {
                return a->sstable_id < b->sstable_id;
            });

        for (const auto& sstable : level_files) {
            if (files_to_delete.size() >= config_.max_files_to_delete_in_one_go) {
                goto create_job; // Exit loops if we've hit our batch limit
            }

            // If the SSTable's creation time is before the expiry time, it's a candidate for deletion.
            if (sstable->creation_timestamp < expiry_time) {
                files_to_delete.push_back(sstable);
            } else {
                // Since files are sorted by age, we can stop searching this level
                // as soon as we find a file that is not expired.
                break;
            }
        }
    }

create_job:
    metrics_.record_selection_time(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time));

    if (files_to_delete.empty()) {
        return std::nullopt;
    }

    metrics_.expired_files_found.fetch_add(files_to_delete.size());

    // Group files by their level to create a valid deletion job.
    // For simplicity, we'll just return the first one found. A more advanced
    // implementation could return a job per level if multiple levels have expired files.
    const auto& first_expired_file = files_to_delete.front();
    
    LOG_INFO("[FIFOStrategy] Found {} expired SSTable(s). Scheduling deletion of SSTable ID {} from L{}.",
             files_to_delete.size(), first_expired_file->sstable_id, first_expired_file->level);

    uint64_t bytes_in_job = 0;
    for(const auto& file : files_to_delete) {
        bytes_in_job += file->size_bytes;
    }
    metrics_.total_files_deleted.fetch_add(files_to_delete.size());
    metrics_.total_bytes_deleted.fetch_add(bytes_in_job);

    // A FIFO "compaction" job is a deletion job. The source list contains the file(s) to delete,
    // and the target list is empty. We must also specify the correct level.
    return CompactionJobForIndinis(
        first_expired_file->level, // The level of the files being deleted
        std::move(files_to_delete),
        {} // No overlapping files, as this is a deletion
    );
}

FIFOCompactionMetricsSnapshot FIFOCompactionStrategy::get_metrics() const {
    FIFOCompactionMetricsSnapshot snapshot;
    snapshot.expired_files_found = metrics_.expired_files_found.load(std::memory_order_relaxed);
    snapshot.total_files_deleted = metrics_.total_files_deleted.load(std::memory_order_relaxed);
    snapshot.total_bytes_deleted = metrics_.total_bytes_deleted.load(std::memory_order_relaxed);
    snapshot.average_selection_time_us = metrics_.average_selection_time_us();
    return snapshot;
}


} // namespace lsm
} // namespace engine