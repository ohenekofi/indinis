#pragma once

#include "column_types.h"
#include <vector>
#include <unordered_map>
#include <string>
#include <variant>

// Using the same ValueType from the main types.h for compatibility during ingestion
#include "../../include/types.h"

namespace engine {
namespace columnar {

using ColumnVector = std::vector<ValueType>;

class ColumnarBuffer {
public:
    explicit ColumnarBuffer(ColumnSchema schema);

    void add(const std::string& primary_key, const std::unordered_map<uint32_t, ValueType>& flattened_record);

    bool isFull() const;
    size_t getRowCount() const;
    const ColumnSchema& getSchema() const { return schema_; }
    
    const std::unordered_map<uint32_t, ColumnVector>& getColumnsData() const;
    const std::vector<std::string>& getPrimaryKeys() const;
    size_t getEstimatedSizeBytes() const;

private:
    ColumnSchema schema_;
    std::vector<std::string> primary_keys_; // Store PKs in insertion order
    std::unordered_map<uint32_t, ColumnVector> columns_data_; // column_id -> vector of values
    
    size_t row_count_ = 0; // Added to fix the error
    size_t estimated_size_bytes_ = 0;
    
    static constexpr size_t MAX_ROWS_IN_BUFFER = 65536;
    static constexpr size_t MAX_BUFFER_SIZE_BYTES = 16 * 1024 * 1024; // 16 MB
};

} // namespace columnar
} // namespace engine