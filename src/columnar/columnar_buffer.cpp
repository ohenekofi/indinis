// src/columnar/columnar_buffer.cpp

#include "../../include/columnar/columnar_buffer.h"
#include <variant> // For std::holds_alternative, std::get

namespace engine {
namespace columnar {

// Constructor: Initializes the buffer with a schema and pre-allocates space for column vectors.
ColumnarBuffer::ColumnarBuffer(ColumnSchema schema) 
    : schema_(std::move(schema)), 
      row_count_(0),
      estimated_size_bytes_(0) {
    // Pre-create the column vectors for each column defined in the schema.
    for (const auto& col_def : schema_.columns) {
        columns_data_[col_def.column_id] = ColumnVector();
    }
}

// Adds a single flattened record to the buffer.
void ColumnarBuffer::add(const std::string& primary_key, const std::unordered_map<uint32_t, ValueType>& flattened_record) {
    primary_keys_.push_back(primary_key);
    estimated_size_bytes_ += primary_key.length();
    
    for (const auto& col_def : schema_.columns) {
        auto it = flattened_record.find(col_def.column_id);
        ColumnVector& column_vec = columns_data_[col_def.column_id];

        if (it != flattened_record.end()) {
            const ValueType& value = it->second;
            column_vec.push_back(value);
            
            // Estimate size
            if (std::holds_alternative<std::string>(value)) {
                estimated_size_bytes_ += std::get<std::string>(value).length();
            } else if (std::holds_alternative<std::vector<uint8_t>>(value)) {
                estimated_size_bytes_ += std::get<std::vector<uint8_t>>(value).size();
            } else {
                estimated_size_bytes_ += 16; // Generic overhead for fixed-size types
            }
        } else {
            column_vec.push_back(std::monostate{}); // Push null representation
            estimated_size_bytes_ += 1;
        }
    }
    
    row_count_++; // Now this is valid as it refers to the member of ColumnarBuffer
}

// Checks if the buffer has reached its capacity.
bool ColumnarBuffer::isFull() const {
    return (row_count_ >= MAX_ROWS_IN_BUFFER) || (estimated_size_bytes_ >= MAX_BUFFER_SIZE_BYTES);
}

// --- Getter Implementations ---

size_t ColumnarBuffer::getRowCount() const {
    return row_count_;
}

const std::unordered_map<uint32_t, ColumnVector>& ColumnarBuffer::getColumnsData() const {
    return columns_data_;
}

const std::vector<std::string>& ColumnarBuffer::getPrimaryKeys() const {
    return primary_keys_;
}

size_t ColumnarBuffer::getEstimatedSizeBytes() const {
    return estimated_size_bytes_;
}

} // namespace columnar
} // namespace engine