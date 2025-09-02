// @src/napi_globals.cpp
#include "napi_globals.h"

// --- Global StorageEngine Instance Cache Definition ---
// The actual definitions of the global variables declared in napi_globals.h
std::map<std::string, std::shared_ptr<StorageEngine>> g_active_engine_instances;
std::mutex g_engine_instances_mutex;