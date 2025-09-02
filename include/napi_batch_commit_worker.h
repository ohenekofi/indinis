// src/napi_batch_commit_worker.h
#pragma once

#include <napi.h>
#include <memory>
#include <string>
#include <vector>
#include "../include/storage_engine.h" // Needs full definition for shared_ptr member
#include "../include/types.h"          // For BatchOperation

/**
 * @class BatchCommitWorker
 * @brief An N-API AsyncWorker for handling a batch commit on a background thread.
 */
class BatchCommitWorker : public Napi::AsyncWorker {
public:
    BatchCommitWorker(
        Napi::Env env,
        std::shared_ptr<StorageEngine> engine,
        std::vector<BatchOperation> operations
    );

    Napi::Promise Promise();

protected:
    void Execute() override;
    void OnOK() override;
    void OnError(const Napi::Error& e) override;

private:
    std::shared_ptr<StorageEngine> engine_;
    std::vector<BatchOperation> operations_;
    Napi::Promise::Deferred deferred_;
};