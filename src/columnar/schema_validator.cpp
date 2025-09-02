#include "../../include/columnar/schema_validator.h"
#include <nlohmann/json.hpp>
#include <magic_enum/magic_enum.hpp> 
#include <variant>

namespace engine {
namespace columnar {

bool SchemaValidator::isTypeCompatible(const nlohmann::json& json_value, ColumnType expected_type) {
    if (json_value.is_null()) {
        return true; // Null is always compatible (handled by ColumnDefinition::nullable)
    }
    
    switch (expected_type) {
        case ColumnType::STRING:
        case ColumnType::JSON: // JSON is stored as a string
            return json_value.is_string();
        
        case ColumnType::INT32:
        case ColumnType::INT64:
        case ColumnType::TIMESTAMP: // Stored as INT64
            return json_value.is_number_integer() || json_value.is_number_unsigned();
            
        case ColumnType::FLOAT:
        case ColumnType::DOUBLE:
            return json_value.is_number(); // Accepts both integer and float
            
        case ColumnType::BOOLEAN:
            return json_value.is_boolean();
            
        case ColumnType::BINARY:
            return json_value.is_binary();
        
        // Basic array validation
        case ColumnType::ARRAY_STRING:
            if (!json_value.is_array()) return false;
            for (const auto& item : json_value) {
                if (!item.is_string()) return false;
            }
            return true;
            
        case ColumnType::ARRAY_INT64:
            if (!json_value.is_array()) return false;
            for (const auto& item : json_value) {
                if (!item.is_number_integer() && !item.is_number_unsigned()) return false;
            }
            return true;
            
        default:
            return false;
    }
}

std::optional<std::string> SchemaValidator::validate(const Record& record, const ColumnSchema& schema) {
    // If the record is marked for deletion, it doesn't need data validation.
    if (record.deleted) {
        return std::nullopt;
    }
    
    // The record's value must be a string that can be parsed as JSON.
    if (!std::holds_alternative<std::string>(record.value)) {
        return "Record value is not a string, cannot perform JSON schema validation.";
    }
    
    const std::string& json_str = std::get<std::string>(record.value);
    nlohmann::json doc_json;
    try {
        doc_json = nlohmann::json::parse(json_str);
    } catch (const nlohmann::json::parse_error& e) {
        return "Record value is not valid JSON: " + std::string(e.what());
    }
    
    // Check each column defined in the schema.
    for (const auto& col_def : schema.columns) {
        if (doc_json.contains(col_def.name)) {
            const auto& field_val = doc_json.at(col_def.name);
            
            // Check nullability
            if (field_val.is_null() && !col_def.nullable) {
                return "Field '" + col_def.name + "' is not nullable but received a null value.";
            }
            
            // Check type compatibility
            if (!isTypeCompatible(field_val, col_def.type)) {
                return "Type mismatch for field '" + col_def.name + "'. Expected type compatible with " +
                       std::string(magic_enum::enum_name(col_def.type)) + ", but got value '" + field_val.dump() + "'.";
            }
            
        } else {
            // Field is missing from the document. Check if it was required.
            if (!col_def.nullable) {
                return "Required field '" + col_def.name + "' is missing from the document.";
            }
        }
    }
    
    // All checks passed.
    return std::nullopt;
}

} // namespace columnar
} // namespace engine