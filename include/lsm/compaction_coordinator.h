// include/lsm/compaction_coordinator.h
#pragma once

#include "sstable_meta.h" // This file already correctly uses engine::lsm
#include "version.h"      // This file also correctly uses engine::lsm
#include <atomic>
#include <mutex>
#include <vector>
#include <memory>

// --- MODIFIED --- Forward-declare LSMTree in the global scope, as it's defined there.
class LSMTree;

namespace engine {
namespace lsm {

// This struct remains in the engine::lsm namespace
struct SubCompactionTask {
    std::string start_key;
    std::string end_key;
    std::vector<std::shared_ptr<SSTableMetadata>> input_files;
};

// This class also remains in the engine::lsm namespace
class CompactionJobCoordinator {
public:
    CompactionJobCoordinator(
        ::LSMTree* tree, // --- MODIFIED --- Use the global scope qualifier ::
        int level,
        int target_level,
        size_t total_tasks,
        std::vector<std::shared_ptr<SSTableMetadata>> L_files,
        std::vector<std::shared_ptr<SSTableMetadata>> L_plus_1_files
    );

    void addSubTaskResult(std::vector<std::shared_ptr<SSTableMetadata>> new_files);
    void reportTaskFailure();
    bool hasFailed() const;
    size_t taskCompleted();
    void finalizeJob();
    void abortJob();

    const size_t total_tasks_;
    const int target_output_level_;

private:
    ::LSMTree* lsm_tree_ptr_; // --- MODIFIED --- Use the global scope qualifier ::
    int level_L_;
    
    std::atomic<size_t> completed_tasks_;
    std::atomic<bool> failed_{false};
    
    std::mutex mutex_;
    std::vector<std::shared_ptr<SSTableMetadata>> newly_created_files_;
    std::vector<std::shared_ptr<SSTableMetadata>> input_files_L_;
    std::vector<std::shared_ptr<SSTableMetadata>> input_files_L_plus_1_;
};

} // namespace lsm
} // namespace engine