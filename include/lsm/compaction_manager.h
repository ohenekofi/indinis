//include/lsm/compaction_manager.h
#pragma once

#include <memory>
#include "../threading/thread_pool.h" // The new priority-aware, adaptive pool

// Forward-declare LSMTree to avoid including the full header here,
// as we only need the CompactionJobForIndinis struct which is defined below it.
class LSMTree;

// This struct is defined in lsm_tree.h, but we need its definition here.
// For a clean separation, this struct *could* be moved to its own header,
// but for now we will assume it's available after including lsm_tree.h
#include "../lsm_tree.h" // Includes the definition for CompactionJobForIndinis

/**
 * @class CompactionManagerForIndinisLSM
 * @brief Dispatches compaction jobs to a shared, priority-aware thread pool.
 *
 * This class acts as an intermediary between the LSM-Tree's scheduler and the
 * engine's shared thread pool. Its primary role is to receive a CompactionJob,
 * determine its priority (e.g., L0 compactions are high priority), and
 * schedule it for execution on the thread pool. It does not own or manage
 * threads directly.
 */
class CompactionManagerForIndinisLSM {
public:
    /**
     * @brief Constructs the CompactionManager.
     * @param tree_ptr A raw pointer to the parent LSMTree instance. This is used
     *        to call the `execute_compaction_job` method. The manager does not
     *        own this pointer.
     * @param pool A shared pointer to the engine's global thread pool where
     *        compaction tasks will be executed.
     */
    CompactionManagerForIndinisLSM(
        LSMTree* tree_ptr,
        std::shared_ptr<engine::threading::ThreadPool> pool
    );

    /**
     * @brief Destructor. Signals the associated ThreadPool if it needs to shut down
     *        any specific tasks, though general pool shutdown is managed elsewhere.
     */
    ~CompactionManagerForIndinisLSM();

    // The CompactionManager is tied to its LSMTree and ThreadPool, so it should not be copied or moved.
    CompactionManagerForIndinisLSM(const CompactionManagerForIndinisLSM&) = delete;
    CompactionManagerForIndinisLSM& operator=(const CompactionManagerForIndinisLSM&) = delete;
    CompactionManagerForIndinisLSM(CompactionManagerForIndinisLSM&&) = delete;
    CompactionManagerForIndinisLSM& operator=(CompactionManagerForIndinisLSM&&) = delete;

    /**
     * @brief Schedules a compaction job for execution on the thread pool.
     * The priority of the job is determined internally based on its level.
     * @param job The compaction job to schedule.
     */
    void schedule_job(CompactionJobForIndinis job);
    void schedule_job_on_pool(std::function<void()> task); // This is the new method needed.
    std::shared_ptr<engine::threading::ThreadPool> getThreadPool() const;

private:
    LSMTree* lsm_tree_ptr_;
    std::shared_ptr<engine::threading::ThreadPool> thread_pool_;
};