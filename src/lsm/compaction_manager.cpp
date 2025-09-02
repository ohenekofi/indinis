//src/lsm/compaction_manager.cpp
#include "../../include/lsm/compaction_manager.h"
#include "../../include/lsm_tree.h"
#include "../../include/debug_utils.h" // For LOG_... macros
#include <stdexcept>

CompactionManagerForIndinisLSM::CompactionManagerForIndinisLSM(
    LSMTree* tree_ptr,
    std::shared_ptr<engine::threading::ThreadPool> pool
)
    : lsm_tree_ptr_(tree_ptr), thread_pool_(pool)
{
    // A production system must validate its dependencies upon construction.
    if (!lsm_tree_ptr_) {
        throw std::invalid_argument("LSMTree pointer cannot be null for CompactionManager.");
    }
    if (!thread_pool_) {
        throw std::invalid_argument("ThreadPool pointer cannot be null for CompactionManager.");
    }
    LOG_INFO("[CompactionManager] Initialized and linked to ThreadPool '{}'.", pool->getName());
}

CompactionManagerForIndinisLSM::~CompactionManagerForIndinisLSM() {
    LOG_INFO("[CompactionManager] Shutting down. (ThreadPool lifecycle managed externally).");
    // The manager does not own the thread pool, so it does not stop it.
    // The StorageEngine that owns the ThreadPool is responsible for its shutdown.
}

void CompactionManagerForIndinisLSM::schedule_job(CompactionJobForIndinis job) {
    if (!thread_pool_) {
        LOG_ERROR("[CompactionManager] Cannot schedule job for L{}, ThreadPool is not available.", job.level_L);
        return;
    }

    // 1. Determine the priority of the compaction job.
    // L0 compactions are critical to unblock the write path and are given high priority.
    auto priority = (job.level_L == 0)
        ? engine::threading::TaskPriority::HIGH
        : engine::threading::TaskPriority::NORMAL;

    // 2. Create a self-contained task (lambda function) for the thread pool to execute.
    // We capture the necessary resources by value. `std::move(job)` is used for efficiency.
    // The raw pointer `lsm_tree_ptr_` is safe to copy.
    auto task = [lsm_ptr = lsm_tree_ptr_, job = std::move(job)]() {
        // This is the code that will be executed by a worker thread from the pool.
        // It is critical that tasks submitted to a general-purpose pool do not
        // let exceptions escape, as that would terminate the worker thread.
        try {
            // Delegate the actual, complex work to the LSMTree's execution method.
            lsm_ptr->execute_compaction_job(
                job.level_L,
                job.sstables_L,
                job.sstables_L_plus_1
            );
        } catch (const std::exception& e) {
            LOG_ERROR("[CompactionTask] Exception during execute_compaction_job for L{}: {}",
                      job.level_L, e.what());
        } catch (...) {
            LOG_ERROR("[CompactionTask] Unknown exception during execute_compaction_job for L{}.",
                      job.level_L);
        }
    };

    // 3. Schedule the task on the shared thread pool with its determined priority.
    thread_pool_->schedule(std::move(task), priority);

    LOG_INFO("[CompactionManager] Scheduled L{}->L{} compaction job with {} priority. Input files L: {}, L+1: {}.",
             job.level_L, job.level_L + 1,
             (priority == engine::threading::TaskPriority::HIGH ? "HIGH" : "NORMAL"),
             job.sstables_L.size(), job.sstables_L_plus_1.size());
}

void CompactionManagerForIndinisLSM::schedule_job_on_pool(std::function<void()> task) {
    if (thread_pool_) {
        // Sub-compaction tasks can be normal priority.
        thread_pool_->schedule(std::move(task), engine::threading::TaskPriority::NORMAL);
    } else {
        LOG_ERROR("[CompactionManager] Cannot schedule sub-task, ThreadPool is not available.");
    }
}