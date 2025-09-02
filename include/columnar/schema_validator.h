#pragma once

#include "column_types.h"
#include "../../include/types.h" // For Record
#include <nlohmann/json.hpp>
#include <string>
#include <optional>
#include <variant> // Needed for implementation
#include <magic_enum/magic_enum.hpp> // Needed for implementation

namespace engine {
namespace columnar {

/**
 * @class SchemaValidator
 * @brief Validates records against a given columnar schema.
 */
class SchemaValidator {
public:
    /**
     * @brief Validates a single record against a schema.
     * This function is defined inline within the header to resolve linking issues.
     */
    static std::optional<std::string> validate(const Record& record, const ColumnSchema& schema) {
        if (record.deleted) {
            return std::nullopt;
        }

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

        for (const auto& col_def : schema.columns) {
            if (doc_json.contains(col_def.name)) {
                const auto& field_val = doc_json.at(col_def.name);

                if (field_val.is_null() && !col_def.nullable) {
                    return "Field '" + col_def.name + "' is not nullable but received a null value.";
                }

                if (!isTypeCompatible(field_val, col_def.type)) {
                    return "Type mismatch for field '" + col_def.name + "'. Expected type compatible with " +
                           std::string(magic_enum::enum_name(col_def.type)) + ", but got value '" + field_val.dump() + "'.";
                }

                if (col_def.maxLength.has_value()) {
                    if (col_def.type == ColumnType::STRING && field_val.is_string()) {
                        if (field_val.get<std::string>().length() > col_def.maxLength.value()) {
                            return "Field '" + col_def.name + "' with length " + std::to_string(field_val.get<std::string>().length()) +
                                   " exceeds maxLength of " + std::to_string(col_def.maxLength.value()) + ".";
                        }
                    } else if (col_def.type == ColumnType::BINARY && field_val.is_binary()) {
                        if (field_val.get_binary().size() > col_def.maxLength.value()) {
                            return "Field '" + col_def.name + "' with size " + std::to_string(field_val.get_binary().size()) +
                                   " bytes exceeds maxLength of " + std::to_string(col_def.maxLength.value()) + ".";
                        }
                    }
                }

            } else {
                if (!col_def.nullable) {
                    return "Required field '" + col_def.name + "' is missing from the document.";
                }
            }
        }
        return std::nullopt;
    }

private:
    /**
     * @brief Checks if a single JSON value is compatible with a given ColumnType.
     * This function is defined inline within the header.
     */
    static bool isTypeCompatible(const nlohmann::json& json_value, ColumnType expected_type) {
        if (json_value.is_null()) {
            return true;
        }

        switch (expected_type) {
            case ColumnType::STRING:
            case ColumnType::JSON:
                return json_value.is_string();
            
            case ColumnType::INT32:
            case ColumnType::INT64:
            case ColumnType::TIMESTAMP:
                return json_value.is_number_integer() || json_value.is_number_unsigned();

            case ColumnType::FLOAT:
            case ColumnType::DOUBLE:
                return json_value.is_number();

            case ColumnType::BOOLEAN:
                return json_value.is_boolean();

            case ColumnType::BINARY:
                return json_value.is_binary();
            
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
};

} // namespace columnar
} // namespace engine