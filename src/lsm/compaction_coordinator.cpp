// src/lsm/compaction_coordinator.cpp
#include "../../include/lsm/compaction_coordinator.h"
#include "../../include/lsm_tree.h"
#include "../../include/lsm/version.h"
#include "../../include/debug_utils.h"
#include "../../include/lsm/liveness_oracle.h"
#include "../../include/storage_engine.h"
#include <filesystem>

namespace fs = std::filesystem;

namespace engine {
namespace lsm {

CompactionJobCoordinator::CompactionJobCoordinator(
    ::LSMTree* tree, // --- MODIFIED --- Use global scope qualifier
    int level,
    int target_level,
    size_t total_tasks,
    std::vector<std::shared_ptr<SSTableMetadata>> L_files,
    std::vector<std::shared_ptr<SSTableMetadata>> L_plus_1_files
) : lsm_tree_ptr_(tree), // --- MODIFIED --- lsm_tree_ptr_ is ::LSMTree*
    level_L_(level),
    target_output_level_(target_level),
    total_tasks_(total_tasks),
    completed_tasks_(0),
    failed_(false),
    input_files_L_(std::move(L_files)),
    input_files_L_plus_1_(std::move(L_plus_1_files))
{}

// [FIX] Implementation for the missing method.
void CompactionJobCoordinator::addSubTaskResult(std::vector<std::shared_ptr<SSTableMetadata>> new_files) {
    std::lock_guard<std::mutex> lock(mutex_);
    newly_created_files_.insert(
        newly_created_files_.end(),
        std::make_move_iterator(new_files.begin()),
        std::make_move_iterator(new_files.end())
    );
}

// [NEW] Implementation for failure reporting.
void CompactionJobCoordinator::reportTaskFailure() {
    failed_.store(true, std::memory_order_release);
}

// [NEW] Implementation for checking the failure status.
bool CompactionJobCoordinator::hasFailed() const {
    return failed_.load(std::memory_order_acquire);
}

size_t CompactionJobCoordinator::taskCompleted() {
    return completed_tasks_.fetch_add(1, std::memory_order_acq_rel) + 1;
}

// [NEW] Implementation for the failure cleanup path.
void CompactionJobCoordinator::abortJob() {
    LOG_ERROR("[CompactionCoordinator L{}] Aborting job due to sub-task failure. Cleaning up temporary files.", level_L_);
    std::lock_guard<std::mutex> lock(mutex_);
    
    for (const auto& meta : newly_created_files_) {
        try {
            if (fs::exists(meta->filename)) {
                fs::remove(meta->filename);
                LOG_INFO("  - Cleaned up temporary file from aborted job: {}", meta->filename);
            }
        } catch (const fs::filesystem_error& e) {
            LOG_ERROR("[CompactionCoordinator] Filesystem error during cleanup of '{}': {}", meta->filename, e.what());
        }
    }
    
    // --- FIX: Use the new public method ---
    lsm_tree_ptr_->notifyScheduler();
}

void CompactionJobCoordinator::finalizeJob() {
    LOG_INFO("[LSMCoordinator L{}] Finalizing successful job. New files: {}, Old files to remove: {}.",
             level_L_, newly_created_files_.size(),
             input_files_L_.size() + input_files_L_plus_1_.size());

    // --- Phase 1: Atomically update the LSM-Tree's VersionSet ---
    VersionEdit edit;
    
    for (const auto& new_meta : newly_created_files_) {
        edit.AddFile(target_output_level_, new_meta);
    }
    
    for (const auto& old_meta : input_files_L_) {
        edit.DeleteFile(old_meta->level, old_meta->sstable_id);
    }
    for (const auto& old_meta : input_files_L_plus_1_) {
        edit.DeleteFile(old_meta->level, old_meta->sstable_id);
    }

    lsm_tree_ptr_->applyAndPersistVersionEdit(edit);
    LOG_INFO("[LSMCoordinator L{}] VersionSet metadata atomically updated.", level_L_);


    // --- Phase 2: Update the Liveness Oracle ---
    // --- THIS BLOCK IS NOW CORRECT ---
    if (lsm_tree_ptr_->getStorageEngine() && lsm_tree_ptr_->getStorageEngine()->getLivenessOracle()) {
        // Use the fully qualified type name for the pointer
        engine::lsm::LivenessOracle* oracle = lsm_tree_ptr_->getStorageEngine()->getLivenessOracle();

        // The logic to add new filters was correctly moved to execute_sub_compaction.
        // Here, we just remove the filters for the old, now-obsolete SSTables.
        for (const auto& old_meta : input_files_L_) {
            oracle->removeLivenessFilter(old_meta->sstable_id);
        }
        for (const auto& old_meta : input_files_L_plus_1_) {
            oracle->removeLivenessFilter(old_meta->sstable_id);
        }
        LOG_INFO("[LSMCoordinator L{}] LivenessOracle updated: removed {} old filters.",
                 level_L_, input_files_L_.size() + input_files_L_plus_1_.size());
    }

    // --- Phase 3: Physically delete the old files from disk ---
    auto delete_files = [](const auto& files_metadata) {
        for (const auto& meta : files_metadata) {
            try {
                if (fs::exists(meta->filename)) fs::remove(meta->filename);
                // Also remove the corresponding bloom filter sidecar file.
                std::string bf_path = meta->filename;
                size_t dat_pos = bf_path.rfind(".dat");
                if (dat_pos != std::string::npos) {
                    bf_path.replace(dat_pos, 4, ".bf");
                    if (fs::exists(bf_path)) {
                        fs::remove(bf_path);
                    }
                }
            } catch (const fs::filesystem_error& e) {
                LOG_ERROR("[LSMCoordinator] Filesystem error deleting old file '{}': {}", meta->filename, e.what());
            }
        }
    };
    
    delete_files(input_files_L_);
    delete_files(input_files_L_plus_1_);
    LOG_INFO("[LSMCoordinator L{}] Old input files and their sidecars have been deleted from disk.", level_L_);
    
    lsm_tree_ptr_->notifyScheduler();
}

} // namespace lsm
} // namespace engine