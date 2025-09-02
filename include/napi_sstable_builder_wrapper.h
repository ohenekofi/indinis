// src/napi_sstable_builder_wrapper.h
#pragma once

#include <napi.h>
#include "../include/lsm/sstable_builder.h"
#include <memory>

class SSTableBuilderWrapper : public Napi::ObjectWrap<SSTableBuilderWrapper> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    SSTableBuilderWrapper(const Napi::CallbackInfo& info);

    Napi::Value Add(const Napi::CallbackInfo& info); 
    void Finish(const Napi::CallbackInfo& info);
    void Close(const Napi::CallbackInfo& info); 
    static Napi::FunctionReference constructor;

private:
    
    std::unique_ptr<engine::lsm::SSTableBuilder> builder_;
};