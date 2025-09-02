// src/napi_sstable_builder_wrapper.cpp
#include "napi_sstable_builder_wrapper.h"
#include "napi_type_conversions.h" // For ConvertNapiToValueType
#include "../include/types.h"
#include <iostream>

Napi::FunctionReference SSTableBuilderWrapper::constructor;

Napi::Object SSTableBuilderWrapper::Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "SSTableBuilderWrapper", {
        InstanceMethod("add", &SSTableBuilderWrapper::Add),
        InstanceMethod("finish", &SSTableBuilderWrapper::Finish),
        InstanceMethod("close", &SSTableBuilderWrapper::Close),
    });
    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();
    exports.Set("SSTableBuilderWrapper", func);
    return exports;
}

SSTableBuilderWrapper::SSTableBuilderWrapper(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<SSTableBuilderWrapper>(info) {
    Napi::Env env = info.Env();

    if (info.Length() < 2 || !info[0].IsString() || !info[1].IsObject()) {
        Napi::TypeError::New(env, "SSTableBuilder constructor requires filepath (string) and options (object).").ThrowAsJavaScriptException();
        return;
    }

    std::string filepath = info[0].As<Napi::String>().Utf8Value();
    Napi::Object js_options = info[1].As<Napi::Object>();

    engine::lsm::SSTableBuilder::Options cpp_options;
    
    // Parse options from JS object (add more as needed for tests)
    if (js_options.Has("compressionType")) {
        std::string type_str = js_options.Get("compressionType").As<Napi::String>().Utf8Value();
        if (type_str == "ZSTD") cpp_options.compression_type = CompressionType::ZSTD;
        else if (type_str == "LZ4") cpp_options.compression_type = CompressionType::LZ4;
        else cpp_options.compression_type = CompressionType::NONE;
    }

    try {
        builder_ = std::make_unique<engine::lsm::SSTableBuilder>(filepath, cpp_options);
    } catch (const std::exception& e) {
        Napi::Error::New(env, "Failed to create C++ SSTableBuilder: " + std::string(e.what())).ThrowAsJavaScriptException();
    }
}

Napi::Value SSTableBuilderWrapper::Add(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);

    if (info.Length() < 1 || !info[0].IsObject()) {
        deferred.Reject(Napi::TypeError::New(env, "add() requires a record object.").Value());
        return deferred.Promise();
    }
    
    Napi::Object js_record = info[0].As<Napi::Object>();
    
    try {
        Record cpp_record; // Use Record from types.h

        if (!js_record.Has("key") || !js_record.Get("key").IsString()) {
            throw std::runtime_error("Record must have a 'key' string.");
        }
        cpp_record.key = js_record.Get("key").As<Napi::String>().Utf8Value();
        
        if (!js_record.Has("value") || !js_record.Get("value").IsBuffer()) {
            throw std::runtime_error("Record must have a 'value' Buffer containing the serialized ValueType.");
        }
        // The value from JS is already a binary-serialized ValueType
        Napi::Buffer<char> value_buffer = js_record.Get("value").As<Napi::Buffer<char>>();
        std::string serialized_value(value_buffer.Data(), value_buffer.Length());
        
        auto value_opt = LogRecord::deserializeValueTypeBinary(serialized_value);
        if (!value_opt) {
            throw std::runtime_error("Failed to deserialize provided record value buffer.");
        }
        cpp_record.value = *value_opt;
        
        if (!js_record.Has("commit_txn_id") || !js_record.Get("commit_txn_id").IsBigInt()) {
             throw std::runtime_error("Record must have a 'commit_txn_id' BigInt.");
        }
        bool lossless;
        cpp_record.commit_txn_id = js_record.Get("commit_txn_id").As<Napi::BigInt>().Uint64Value(&lossless);
        cpp_record.txn_id = cpp_record.commit_txn_id;

        if (js_record.Has("deleted") && js_record.Get("deleted").IsBoolean()) {
            cpp_record.deleted = js_record.Get("deleted").As<Napi::Boolean>().Value();
        }

        // The actual C++ call that might throw
        builder_->add(cpp_record);
        
        deferred.Resolve(env.Undefined());

    } catch (const std::exception& e) {
        deferred.Reject(Napi::Error::New(env, "Error in SSTableBuilder::Add: " + std::string(e.what())).Value());
    }

    return deferred.Promise();
}

void SSTableBuilderWrapper::Finish(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    try {
        builder_->finish();
    } catch (const std::exception& e) {
        Napi::Error::New(env, "Error in SSTableBuilder::Finish: " + std::string(e.what())).ThrowAsJavaScriptException();
    }
}

void SSTableBuilderWrapper::Close(const Napi::CallbackInfo& info) {
    // This method forces the C++ destructor of the SSTableBuilder to run,
    // which will close and delete the temp file if `finish()` was not called.
    if (builder_) {
        builder_.reset(); // This is the key line. It destructs the managed object.
    }
}