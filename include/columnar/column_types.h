// include/columnar/column_types.h
#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <stdexcept>
#include "../../include/types.h" // For ValueType, TxnId
#include "../../include/bloom_filter.h"

// NO nlohmann::json forward declaration here.

namespace engine {
namespace columnar {

// Defines the data type of a single column.
enum class ColumnType : uint8_t {
    STRING,
    INT32,
    INT64,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    TIMESTAMP, // Stored as INT64 (microseconds since epoch)
    BINARY,
    JSON,
    ARRAY_STRING,
    ARRAY_INT64
};

// Definition for one column within a schema.
struct ColumnDefinition {
    std::string name;
    uint32_t column_id;
    ColumnType type;
    bool nullable = true;
    std::optional<uint32_t> maxLength; 
};

struct ColumnarFileMetadata {
    std::string filename;
    uint64_t file_id;
    int level;
    size_t size_bytes;
    uint64_t row_count;
    uint32_t schema_version;
    std::string min_primary_key;
    std::string max_primary_key;
    TxnId min_txn_id;
    TxnId max_txn_id;
    std::unique_ptr<BloomFilter> pk_bloom_filter;
    std::unordered_map<uint32_t, std::pair<ValueType, ValueType>> column_stats; // column_id -> {min, max}
};
// Represents the schema for a specific version of a store's data.
struct ColumnSchema {
    std::string store_path;
    uint32_t schema_version = 1;
    std::vector<ColumnDefinition> columns;
    std::unordered_map<std::string, uint32_t> name_to_id_map;

    std::optional<ColumnDefinition> getColumn(const std::string& name) const {
        auto it = name_to_id_map.find(name);
        if (it == name_to_id_map.end()) {
            return std::nullopt;
        }
        uint32_t id_to_find = it->second;
        for (const auto& col : columns) {
            if (col.column_id == id_to_find) {
                return col;
            }
        }
        return std::nullopt;
    }

    void build_map() {
        name_to_id_map.clear();
        for(const auto& col : columns) {
            name_to_id_map[col.name] = col.column_id;
        }
    }
};

// NO to_json/from_json function declarations here.

// --- Structs for Aggregation Plan ---
enum class AggregationType : uint8_t { SUM, COUNT, AVG, MIN, MAX };

struct AggregationSpec {
    AggregationType op;
    std::string field;
    std::string result_field_name;
};

struct AggregationPlan {
    std::vector<std::string> group_by_fields;
    std::vector<AggregationSpec> aggregations;
};

} // namespace columnar
} // namespace engine