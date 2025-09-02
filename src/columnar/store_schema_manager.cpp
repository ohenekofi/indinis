// src/columnar/store_schema_manager.cpp

#include "../../include/columnar/store_schema_manager.h"
#include "../../include/debug_utils.h"

// --- DEPENDENCIES ARE NOW LOCALIZED TO THE .CPP FILE ---
#include <nlohmann/json.hpp>
#include <filesystem>
#include <fstream>
#include <regex>

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace engine {
namespace columnar {

// --- JSON Serialization/Deserialization Implementations ---
// These functions live in the .cpp file but in the same namespace as the types
// they operate on. This is how the nlohmann library finds them via
// Argument-Dependent Lookup (ADL).

NLOHMANN_JSON_SERIALIZE_ENUM(ColumnType, {
    {ColumnType::STRING, "STRING"},
    {ColumnType::INT32, "INT32"},
    {ColumnType::INT64, "INT64"},
    {ColumnType::FLOAT, "FLOAT"},
    {ColumnType::DOUBLE, "DOUBLE"},
    {ColumnType::BOOLEAN, "BOOLEAN"},
    {ColumnType::TIMESTAMP, "TIMESTAMP"},
    {ColumnType::BINARY, "BINARY"},
    {ColumnType::JSON, "JSON"},
    {ColumnType::ARRAY_STRING, "ARRAY_STRING"},
    {ColumnType::ARRAY_INT64, "ARRAY_INT64"}
})

void to_json(json& j, const ColumnDefinition& p) {
    j = json{{"name", p.name}, {"column_id", p.column_id}, {"type", p.type}, {"nullable", p.nullable}};
}

void from_json(const json& j, ColumnDefinition& p) {
    j.at("name").get_to(p.name);
    j.at("column_id").get_to(p.column_id);
    j.at("type").get_to(p.type);
    j.at("nullable").get_to(p.nullable);
    if (j.contains("maxLength")) {
        p.maxLength = j.at("maxLength").get<uint32_t>();
    }
}

void to_json(json& j, const ColumnSchema& p) {
    j = json{
        {"store_path", p.store_path},
        {"schema_version", p.schema_version},
        {"columns", p.columns}
    };
}

void from_json(const json& j, ColumnSchema& p) {
    j.at("store_path").get_to(p.store_path);
    j.at("schema_version").get_to(p.schema_version);
    j.at("columns").get_to(p.columns);
    p.build_map();
}

// --- StoreSchemaManager Implementation (remains identical) ---
StoreSchemaManager::StoreSchemaManager(const std::string& schema_storage_dir)
    : schema_storage_dir_(schema_storage_dir) {
    try {
        fs::create_directories(schema_storage_dir_);
        loadAllSchemas();
    } catch (const std::exception& e) {
        LOG_FATAL("Failed to initialize StoreSchemaManager at {}: {}", schema_storage_dir_, e.what());
        throw;
    }
}

// ... (rest of the .cpp file is identical to the previous version) ...

std::string StoreSchemaManager::sanitizeStorePathForFilename(const std::string& store_path) {
    std::string sanitized = store_path;
    std::replace(sanitized.begin(), sanitized.end(), '/', '_');
    std::replace(sanitized.begin(), sanitized.end(), '\\', '_');
    sanitized = std::regex_replace(sanitized, std::regex("[^a-zA-Z0-9_-]"), "");
    return sanitized;
}

std::string StoreSchemaManager::getSchemaFilePath(const std::string& store_path, uint32_t version) const {
    std::string sanitized_path = sanitizeStorePathForFilename(store_path);
    std::string filename = "store_" + sanitized_path + "_v" + std::to_string(version) + ".json";
    return (fs::path(schema_storage_dir_) / filename).string();
}

void StoreSchemaManager::loadAllSchemas() {
    std::unique_lock lock(mutex_);
    LOG_INFO("Loading all schemas from directory: {}", schema_storage_dir_);
    for (const auto& entry : fs::directory_iterator(schema_storage_dir_)) {
        if (entry.is_regular_file() && entry.path().extension() == ".json") {
            try {
                std::ifstream file(entry.path());
                if (!file.is_open()) {
                    LOG_ERROR("Could not open schema file for reading: {}", entry.path().string());
                    continue;
                }
                json j;
                file >> j;
                ColumnSchema schema = j.get<ColumnSchema>();
                schemas_[schema.store_path][schema.schema_version] = schema;
                LOG_INFO("  Loaded schema for store '{}', version {}", schema.store_path, schema.schema_version);
            } catch (const std::exception& e) {
                LOG_ERROR("Failed to load or parse schema file {}: {}", entry.path().string(), e.what());
            }
        }
    }
}

void StoreSchemaManager::persistSchema(const ColumnSchema& schema) {
    std::string file_path = getSchemaFilePath(schema.store_path, schema.schema_version);
    try {
        json j = schema;
        std::ofstream file(file_path);
        if (!file.is_open()) {
            throw std::runtime_error("Could not open file for writing.");
        }
        file << j.dump(4);
        LOG_INFO("Persisted schema for store '{}', version {} to {}", schema.store_path, schema.schema_version, file_path);
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to persist schema to {}: {}", file_path, e.what());
        throw;
    }
}

bool StoreSchemaManager::registerSchema(const ColumnSchema& schema) {
    std::unique_lock lock(mutex_);
    
    if (schema.store_path.empty() || schema.columns.empty()) {
        LOG_WARN("Attempted to register invalid schema (empty store_path or columns).");
        return false;
    }
    
    auto& versions_for_store = schemas_[schema.store_path];
    auto it = versions_for_store.find(schema.schema_version);

    if (it != versions_for_store.end()) {
        LOG_TRACE("Schema for store '{}' version {} already exists. Registration is idempotent.", schema.store_path, schema.schema_version);
        return true;
    }

    try {
        persistSchema(schema);
        versions_for_store[schema.schema_version] = schema;
        return true;
    } catch (...) {
        return false;
    }
}

std::optional<ColumnSchema> StoreSchemaManager::getLatestSchema(const std::string& store_path) const {
    std::shared_lock lock(mutex_);
    auto it = schemas_.find(store_path);
    if (it == schemas_.end() || it->second.empty()) {
        return std::nullopt;
    }
    auto latest_it = std::max_element(it->second.begin(), it->second.end(),
        [](const auto& a, const auto& b) {
            return a.first < b.first;
        });
    return latest_it->second;
}

std::optional<ColumnSchema> StoreSchemaManager::getSchema(const std::string& store_path, uint32_t version) const {
    std::shared_lock lock(mutex_);
    auto store_it = schemas_.find(store_path);
    if (store_it == schemas_.end()) {
        return std::nullopt;
    }
    auto version_it = store_it->second.find(version);
    if (version_it == store_it->second.end()) {
        return std::nullopt;
    }
    return version_it->second;
}

} // namespace columnar
} // namespace engine