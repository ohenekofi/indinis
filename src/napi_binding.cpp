// @src/napi_binding.cpp
#include <napi.h>
#include "napi_wrappers.h"

// --- Module Initialization ---
Napi::Object InitModule(Napi::Env env, Napi::Object exports) {
    IndinisWrapper::Init(env, exports);   // Initialize Indinis class wrapper
    TransactionWrapper::Init(env, exports); // Initialize Transaction class wrapper (sets up constructor reference)
    return exports;
}

NODE_API_MODULE(indinis, InitModule) // Match module name in binding.gyp