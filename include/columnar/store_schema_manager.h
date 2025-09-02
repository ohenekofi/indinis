// include/columnar/store_schema_manager.h
#pragma once

#include "column_types.h"
#include <string>
#include <optional>
#include <shared_mutex>
#include <unordered_map>

namespace engine {
namespace columnar {

class StoreSchemaManager {
public:
    explicit StoreSchemaManager(const std::string& schema_storage_dir);

    // Registers a new schema. If a schema for the store_path with the same version exists, it's a no-op.
    // If a different schema for that version exists, it fails.
    bool registerSchema(const ColumnSchema& schema);

    // Retrieves the schema with the highest version number for a given store path.
    std::optional<ColumnSchema> getLatestSchema(const std::string& store_path) const;
    
    // Retrieves a specific schema version for a store path.
    std::optional<ColumnSchema> getSchema(const std::string& store_path, uint32_t version) const;

    static std::string sanitizeStorePathForFilename(const std::string& store_path);

private:
    void loadAllSchemas();
    void persistSchema(const ColumnSchema& schema);
    std::string getSchemaFilePath(const std::string& store_path, uint32_t version) const;


    std::string schema_storage_dir_;
    // Map: store_path -> (Map: version -> Schema)
    std::unordered_map<std::string, std::unordered_map<uint32_t, ColumnSchema>> schemas_;
    mutable std::shared_mutex mutex_;
};

} // namespace columnar
} // namespace engine