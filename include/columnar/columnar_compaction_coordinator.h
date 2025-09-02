// include/columnar/columnar_compaction_coordinator.h
#pragma once

#include "columnar_file.h"
#include "column_types.h"
#include <atomic>
#include <mutex>
#include <vector>
#include <memory>
#include <string>

// Forward-declare ColumnarStore to avoid circular dependency
namespace engine {
namespace columnar {
    class ColumnarStore;
}
}

namespace engine {
namespace columnar {

// Represents a single, parallelizable unit of a larger compaction job.
struct SubCompactionTask {
    std::string start_key; // Inclusive
    std::string end_key;   // Exclusive
    // --- MODIFIED --- This is now a valid, fully-defined type
    std::vector<std::shared_ptr<ColumnarFileMetadata>> input_files;
};

// Manages the state and finalization of a parallel compaction job.
class ColumnarCompactionJobCoordinator {
public:
    ColumnarCompactionJobCoordinator(
        ColumnarStore* store,
        int level,
        int target_level,
        size_t total_tasks,
        // --- MODIFIED --- These types are now valid
        std::vector<std::shared_ptr<ColumnarFileMetadata>> L_files,
        std::vector<std::shared_ptr<ColumnarFileMetadata>> L_plus_1_files
    );

    // Called by a worker thread after it finishes its sub-compaction.
    void addSubTaskResult(std::vector<std::shared_ptr<ColumnarFileMetadata>> new_files);

    // Called by a worker thread if its sub-compaction fails.
    void reportTaskFailure();

    // Checks if any task in this job has reported a failure.
    bool hasFailed() const;

    // Atomically increments the completed task count and returns the new value.
    size_t taskCompleted();

    // Finalization methods, called by the *last* worker to finish.
    void finalizeJob(); // On success
    void abortJob();    // On failure

    const size_t total_tasks_;
    const int target_output_level_;

private:
    ColumnarStore* store_ptr_;
    int level_L_;
    
    std::atomic<size_t> completed_tasks_;
    std::atomic<bool> failed_{false};
    
    std::mutex mutex_;
    std::vector<std::shared_ptr<ColumnarFileMetadata>> newly_created_files_;
    std::vector<std::shared_ptr<ColumnarFileMetadata>> input_files_L_;
    std::vector<std::shared_ptr<ColumnarFileMetadata>> input_files_L_plus_1_;
};

} // namespace columnar
} // namespace engine