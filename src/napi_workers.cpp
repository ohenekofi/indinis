// src/napi_workers.cpp
#include "../include/napi_workers.h"
#include "../include/storage_error/storage_error.h" // For catching storage::StorageError

IngestFileWorker::IngestFileWorker(
    Napi::Env env,
    std::shared_ptr<StorageEngine> engine,
    const std::string& store_path,
    const std::string& file_path,
    bool move_file
) : Napi::AsyncWorker(env),
    engine_(engine),
    store_path_(store_path),
    file_path_(file_path),
    move_file_(move_file),
    deferred_(Napi::Promise::Deferred::New(env)) {}

Napi::Promise IngestFileWorker::Promise() {
    return deferred_.Promise();
}

void IngestFileWorker::Execute() {
    try {
        if (!engine_) {
            SetError("Engine instance is no longer valid.");
            return;
        }
        engine_->ingestExternalFile(store_path_, file_path_, move_file_);
        // On success, we do nothing here. The promise will be resolved in OnOK.
    } catch (const storage::StorageError& e) {
        // SetError is the thread-safe way to pass an error message back to the main thread.
        SetError(e.toString());
    } catch (const std::exception& e) {
        SetError("C++ error during file ingestion: " + std::string(e.what()));
    }
}

void IngestFileWorker::OnOK() {
    // This runs on the main event loop thread.
    // It's safe to interact with N-API and resolve the promise.
    deferred_.Resolve(Napi::Boolean::New(Env(), true));
}

void IngestFileWorker::OnError(const Napi::Error& e) {
    // This runs on the main event loop thread.
    // Reject the promise with the error that was set in Execute().
    deferred_.Reject(e.Value());
}

ColumnarIngestFileWorker::ColumnarIngestFileWorker(
    Napi::Env env,
    std::shared_ptr<StorageEngine> engine,
    const std::string& store_path,
    const std::string& file_path,
    bool move_file
) : Napi::AsyncWorker(env),
    engine_(engine),
    store_path_(store_path),
    file_path_(file_path),
    move_file_(move_file),
    deferred_(Napi::Promise::Deferred::New(env)) {}

Napi::Promise ColumnarIngestFileWorker::Promise() {
    return deferred_.Promise();
}

void ColumnarIngestFileWorker::Execute() {
    try {
        if (!engine_) {
            SetError("Engine instance is no longer valid or has been closed.");
            return;
        }
        // This is the call to our new C++ feature. It runs on the background thread.
        engine_->ingestColumnarFile(store_path_, file_path_, move_file_);
    } catch (const storage::StorageError& e) {
        // SetError is the thread-safe way to pass an error message back to the main thread.
        SetError(e.toString());
    } catch (const std::exception& e) {
        SetError("C++ error during columnar file ingestion: " + std::string(e.what()));
    }
}

void ColumnarIngestFileWorker::OnOK() {
    // This runs on the main event loop thread when Execute() completes without error.
    deferred_.Resolve(Env().Undefined()); // Resolve the promise with 'undefined' on success
}

void ColumnarIngestFileWorker::OnError(const Napi::Error& e) {
    // This runs on the main event loop thread if SetError() was called.
    deferred_.Reject(e.Value()); // Reject the promise with the error message.
}