//src/columnar/columnar_file_iterator.cpp
#include "../../include/columnar/columnar_file_iterator.h"
#include "../../include/columnar/columnar_file.h" // Include the full definition HERE
#include "../../include/debug_utils.h"

#include <stdexcept>

namespace engine {
namespace columnar {

// Define the implementation struct inside the .cpp file.
// This is completely hidden from other parts of the code.
struct ColumnarFileIterator::PImpl {
    std::shared_ptr<ColumnarFile> file_;
    std::vector<std::string> primary_keys_;
    std::vector<uint32_t> columns_to_read_;
    std::unordered_map<uint32_t, ColumnVector> column_data_cache_;
    size_t current_row_index_ = 0;

    // Helper to load a column on demand.
    void ColumnarFileIterator::PImpl::loadColumn(uint32_t column_id) {
        if (column_data_cache_.count(column_id)) {
            return; // Already loaded
        }
        
        // 
        
        // 1. Get all row groups from the file.
        const auto& row_groups_in_file = file_->getRowGroups();
        if (row_groups_in_file.empty()) {
            // If there are no row groups, create an empty vector and cache it.
            column_data_cache_[column_id] = ColumnVector();
            return;
        }

        // 2. Build a vector of all row group indices (e.g., [0, 1, 2, ...]).
        std::vector<int> all_row_group_indices;
        all_row_group_indices.reserve(row_groups_in_file.size());
        for (size_t i = 0; i < row_groups_in_file.size(); ++i) {
            all_row_group_indices.push_back(static_cast<int>(i));
        }

        // 3. Call readColumns with the correct arguments.
        // The iterator itself doesn't know about encryption state, so we pass false/empty for now.
        // A better design would pass the DEK to the iterator's constructor.
        // For now, let's assume no encryption for this specific call path.
        auto single_column_map = file_->readColumns(
            {column_id},                    // The single column we want
            all_row_group_indices,          // All row groups in the file
            false,                          // encryption_active (placeholder)
            {}                              // dek (placeholder)
        );

        //
        
        if (single_column_map.count(column_id)) {
            column_data_cache_[column_id] = std::move(single_column_map.at(column_id));
        } else {
            // If the column doesn't exist, create an empty vector filled with nulls.
            column_data_cache_[column_id] = ColumnVector(primary_keys_.size(), std::monostate{});
        }
    }
};

// --- Implement the public methods of ColumnarFileIterator ---

ColumnarFileIterator::ColumnarFileIterator(std::shared_ptr<ColumnarFile> file, const std::vector<uint32_t>& columns_to_read)
    : pimpl_(std::make_unique<PImpl>()) // Create the PImpl object
{
    pimpl_->file_ = std::move(file);
    pimpl_->columns_to_read_ = columns_to_read;
    if (pimpl_->file_) {
        pimpl_->primary_keys_ = pimpl_->file_->getPrimaryKeys();
    }
}

// The destructor must be defined in the .cpp file where PImpl is a complete type.
ColumnarFileIterator::~ColumnarFileIterator() = default;

bool ColumnarFileIterator::hasNext() const {
    return pimpl_->current_row_index_ < pimpl_->primary_keys_.size();
}

std::pair<std::string, std::unordered_map<uint32_t, ValueType>> ColumnarFileIterator::next() {
    if (!hasNext()) {
        throw std::out_of_range("ColumnarFileIterator out of bounds.");
    }

    for (uint32_t col_id : pimpl_->columns_to_read_) {
        pimpl_->loadColumn(col_id);
    }
    
    std::unordered_map<uint32_t, ValueType> row_data;
    for (uint32_t col_id : pimpl_->columns_to_read_) {
        const auto& col_vector = pimpl_->column_data_cache_.at(col_id);
        if (pimpl_->current_row_index_ < col_vector.size()) {
            row_data[col_id] = col_vector[pimpl_->current_row_index_];
        } else {
            row_data[col_id] = std::monostate{};
        }
    }

    const std::string& primary_key = pimpl_->primary_keys_[pimpl_->current_row_index_];
    
    pimpl_->current_row_index_++;

    return {primary_key, row_data};
}

} // namespace columnar
} // namespace engine