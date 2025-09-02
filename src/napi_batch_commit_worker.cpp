// src/napi_batch_commit_worker.cpp
#include "../include/napi_batch_commit_worker.h"
#include "../include/storage_error/storage_error.h"

BatchCommitWorker::BatchCommitWorker(
    Napi::Env env,
    std::shared_ptr<StorageEngine> engine,
    std::vector<BatchOperation> operations
) : Napi::AsyncWorker(env),
    engine_(engine),
    operations_(std::move(operations)),
    deferred_(Napi::Promise::Deferred::New(env)) {}

Napi::Promise BatchCommitWorker::Promise() {
    return deferred_.Promise();
}

void BatchCommitWorker::Execute() {
    try {
        if (!engine_) {
            SetError("Engine instance is no longer valid.");
            return;
        }
        // This is the call to the C++ core logic. It's synchronous on this worker thread.
        engine_->commitBatch(operations_);
    } catch (const storage::StorageError& e) {
        SetError(e.toString());
    } catch (const std::exception& e) {
        SetError("C++ error during batch commit: " + std::string(e.what()));
    }
}

void BatchCommitWorker::OnOK() {
    deferred_.Resolve(Env().Undefined());
}

void BatchCommitWorker::OnError(const Napi::Error& e) {
    deferred_.Reject(e.Value());
}