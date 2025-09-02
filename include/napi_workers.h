// include/napi_workers.h
#pragma once

#include <napi.h>
#include <memory>
#include <string>
#include "storage_engine.h" // Needs full definition for shared_ptr member

/**
 * @class IngestFileWorker
 * @brief An N-API AsyncWorker for handling the external file ingestion process
 *        on a background thread and safely returning the result to Node.js.
 */
class IngestFileWorker : public Napi::AsyncWorker {
public:
    IngestFileWorker(
        Napi::Env env,
        std::shared_ptr<StorageEngine> engine,
        const std::string& store_path,
        const std::string& file_path,
        bool move_file
    );

    // Exposes the promise that will be resolved/rejected upon completion.
    Napi::Promise Promise();

protected:
    // This method is executed on a background libuv worker thread.
    void Execute() override;

    // This method is executed on the main Node.js event loop thread
    // if the background task completes successfully.
    void OnOK() override;

    // This method is executed on the main Node.js event loop thread
    // if the background task fails (throws or calls SetError).
    void OnError(const Napi::Error& e) override;

private:
    std::shared_ptr<StorageEngine> engine_;
    std::string store_path_;
    std::string file_path_;
    bool move_file_;
    Napi::Promise::Deferred deferred_;
};

/**
 * @class ColumnarIngestFileWorker
 * @brief An N-API AsyncWorker for handling the columnar file ingestion process
 *        on a background thread and safely returning the result to Node.js.
 */
class ColumnarIngestFileWorker : public Napi::AsyncWorker {
public:
    ColumnarIngestFileWorker(
        Napi::Env env,
        std::shared_ptr<StorageEngine> engine,
        const std::string& store_path,
        const std::string& file_path,
        bool move_file
    );

    // Exposes the promise that will be resolved/rejected upon completion.
    Napi::Promise Promise();

protected:
    void Execute() override;
    void OnOK() override;
    void OnError(const Napi::Error& e) override;

private:
    std::shared_ptr<StorageEngine> engine_;
    std::string store_path_;
    std::string file_path_;
    bool move_file_;
    Napi::Promise::Deferred deferred_;
};