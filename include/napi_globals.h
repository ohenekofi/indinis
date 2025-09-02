// @src/napi_globals.h
#ifndef NAPI_GLOBALS_H
#define NAPI_GLOBALS_H

#include <map>
#include <string>
#include <memory>
#include <mutex>

// Forward declaration to avoid including the full StorageEngine header here
class StorageEngine;

// --- Global StorageEngine Instance Cache ---
// Declared here as 'extern' to indicate they are defined in a .cpp file
extern std::map<std::string, std::shared_ptr<StorageEngine>> g_active_engine_instances;
extern std::mutex g_engine_instances_mutex; // Protects g_active_engine_instances

#endif // NAPI_GLOBALS_H