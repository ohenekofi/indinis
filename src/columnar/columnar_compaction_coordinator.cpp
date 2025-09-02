// src/columnar_compaction_coordinator.cpp
#include "../../include/columnar/columnar_compaction_coordinator.h"
#include "../../include/columnar/columnar_store.h" // Full definition is required to call store methods
#include "../../include/debug_utils.h"

#include <filesystem>
#include <algorithm> // For std::make_move_iterator

namespace fs = std::filesystem;

namespace engine {
namespace columnar {

/**
 * @brief Constructs the coordinator for a parallel compaction job.
 * @param store A pointer to the parent ColumnarStore.
 * @param level The source level of the compaction.
 * @param target_level The destination level for the new files.
 * @param total_tasks The total number of parallel sub-tasks this job was split into.
 * @param L_files A vector of metadata for the source files from level L.
 * @param L_plus_1_files A vector of metadata for the overlapping files from level L+1.
 */
ColumnarCompactionJobCoordinator::ColumnarCompactionJobCoordinator(
    ColumnarStore* store,
    int level,
    int target_level,
    size_t total_tasks,
    std::vector<std::shared_ptr<ColumnarFileMetadata>> L_files,
    std::vector<std::shared_ptr<ColumnarFileMetadata>> L_plus_1_files
) : store_ptr_(store),
    level_L_(level),
    target_output_level_(target_level),
    total_tasks_(total_tasks),
    completed_tasks_(0),
    failed_(false),
    input_files_L_(std::move(L_files)),
    input_files_L_plus_1_(std::move(L_plus_1_files))
{
    if (!store_ptr_) {
        throw std::invalid_argument("ColumnarCompactionJobCoordinator cannot be created with a null store pointer.");
    }
}
/**
 * @brief Thread-safely adds the metadata of newly created files from a completed sub-task.
 * @param new_files A vector of metadata for the files created by the worker thread.
 */
void ColumnarCompactionJobCoordinator::addSubTaskResult(std::vector<std::shared_ptr<ColumnarFileMetadata>> new_files) {
    std::lock_guard<std::mutex> lock(mutex_);
    newly_created_files_.insert(
        newly_created_files_.end(),
        std::make_move_iterator(new_files.begin()),
        std::make_move_iterator(new_files.end())
    );
}

/**
 * @brief Atomically flags the entire compaction job as failed.
 */
void ColumnarCompactionJobCoordinator::reportTaskFailure() {
    failed_.store(true, std::memory_order_release);
}

/**
 * @brief Atomically checks if the job has been flagged as failed.
 */
bool ColumnarCompactionJobCoordinator::hasFailed() const {
    return failed_.load(std::memory_order_acquire);
}

/**
 * @brief Atomically increments the counter for completed tasks.
 * @return The new number of completed tasks after the increment.
 */
size_t ColumnarCompactionJobCoordinator::taskCompleted() {
    // fetch_add returns the value *before* the addition, so we add 1 for the new value.
    return completed_tasks_.fetch_add(1, std::memory_order_acq_rel) + 1;
}

/**
 * @brief Cleans up a failed compaction job.
 * This method is called by the last worker thread if `hasFailed()` is true.
 * It deletes any temporary output files that were created by successful sub-tasks
 * before the failure occurred.
 */
void ColumnarCompactionJobCoordinator::abortJob() {
    LOG_ERROR("[ColumnarCoordinator L{}] Aborting job due to a sub-task failure. Cleaning up temporary files.", level_L_);
    std::lock_guard<std::mutex> lock(mutex_);
    
    for (const auto& meta : newly_created_files_) {
        try {
            if (fs::exists(meta->filename)) {
                fs::remove(meta->filename);
                LOG_INFO("  - Cleaned up temporary file from aborted job: {}", meta->filename);
            }
        } catch (const fs::filesystem_error& e) {
            // Log the error but continue cleaning up other files.
            LOG_ERROR("[ColumnarCoordinator] Filesystem error during cleanup of '{}': {}", meta->filename, e.what());
        }
    }
    
    // Notify the scheduler that a compaction attempt has finished (even in failure),
    // so it can potentially schedule another job.
    store_ptr_->notifyScheduler();
}

/**
 * @brief Finalizes a successful compaction job.
 * This method is called by the last worker thread to complete if the job has not failed.
 * It orchestrates the atomic update of the ColumnarStore's metadata and the physical
 * deletion of the old input files.
 */
void ColumnarCompactionJobCoordinator::finalizeJob() {
    LOG_INFO("[ColumnarCoordinator L{}] Finalizing successful job. New files: {}, Old files to remove: {}.",
             level_L_, newly_created_files_.size(),
             input_files_L_.size() + input_files_L_plus_1_.size());

    // 1. Atomically update the store's level metadata in a single operation.
    // This is the critical step that makes the compaction "live".
    store_ptr_->atomicallyUpdateLevels(input_files_L_, input_files_L_plus_1_, newly_created_files_);
    LOG_INFO("[ColumnarCoordinator L{}] Store metadata has been atomically updated.", level_L_);

    // 2. Physically delete the old, now-unreferenced files from disk.
    // This is safe because no new read operations will ever be directed to these files.
    auto delete_files = [](const auto& files_metadata) {
        for (const auto& meta : files_metadata) {
            try {
                if (fs::exists(meta->filename)) {
                    fs::remove(meta->filename);
                }
            } catch (const fs::filesystem_error& e) {
                // A failed deletion is not critical. The file is orphaned and can be
                // cleaned up by a startup scan. Log the error but do not throw.
                LOG_ERROR("[ColumnarCoordinator] Filesystem error while deleting old file '{}': {}", meta->filename, e.what());
            }
        }
    };
    
    delete_files(input_files_L_);
    delete_files(input_files_L_plus_1_);
    LOG_INFO("[ColumnarCoordinator L{}] Old input files have been deleted from disk.", level_L_);
    
    // 3. Notify the scheduler that a compaction has finished.
    store_ptr_->notifyScheduler();
}

} // namespace columnar
} // namespace engine