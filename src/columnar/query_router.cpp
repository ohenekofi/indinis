// src/columnar/query_router.cpp

#include "../../include/columnar/query_router.h"
#include "../../include/storage_engine.h"
#include "../../include/debug_utils.h"
#include "../../include/storage_error/storage_error.h"

namespace engine {
namespace columnar {

QueryRouter::QueryRouter(::StorageEngine* engine) : engine_ptr_(engine) {}

QueryRouter::ExecutionPath QueryRouter::routeQuery(
    const std::string& store_path, 
    const std::vector<FilterCondition>& filters,
    const std::optional<AggregationPlan>& aggPlan
) {
    if (!engine_ptr_) {
        LOG_WARN("QueryRouter: engine_ptr_ is null. Defaulting to LSM_ONLY.");
        return ExecutionPath::LSM_ONLY;
    }

    // --- New Routing Rule 1: Aggregations ALWAYS go to columnar ---
    if (aggPlan.has_value() && !aggPlan->aggregations.empty()) {
        LOG_INFO("Query Router: Aggregation query detected. Routing to COLUMNAR_ONLY path.");
        // Check if a columnar store can even exist for this path.
        if (!engine_ptr_->getSchemaManager()->getLatestSchema(store_path)) {
            throw storage::StorageError(storage::ErrorCode::SCHEMA_VIOLATION, 
                "Aggregation query submitted for store '" + store_path + "', but no columnar schema is registered for it.");
        }
        return ExecutionPath::COLUMNAR_ONLY;
    }

    // --- Existing Routing Logic for Filter Queries ---
    if (filters.empty()) {
        LOG_INFO("Query Router: No filters. Routing to LSM_ONLY path.");
        return ExecutionPath::LSM_ONLY;
    }
    
    if (!engine_ptr_->getSchemaManager()->getLatestSchema(store_path)) {
        LOG_INFO("Query Router: No columnar schema for store '{}'. Routing to LSM_ONLY path.", store_path);
        return ExecutionPath::LSM_ONLY;
    }

    if (filters.size() > 1 || filters[0].op != FilterOperator::EQUAL) {
        LOG_INFO("Query Router: Analytical filter shape detected. Routing to COLUMNAR_ONLY path.");
        return ExecutionPath::COLUMNAR_ONLY;
    }

    LOG_INFO("Query Router: Single equality filter detected. Routing to LSM_ONLY path.");
    return ExecutionPath::LSM_ONLY;
}


} // namespace columnar
} // namespace engine