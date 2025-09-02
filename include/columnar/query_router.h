// include/columnar/query_router.h
#pragma once

#include "../../include/types.h"
#include "column_types.h"
#include <vector>
#include <string>

// --- THIS IS THE FIX ---
// Forward-declare the StorageEngine class in the global namespace,
// BEFORE we enter the 'engine' namespace.
class StorageEngine;
// ----------------------

namespace engine {
namespace columnar {

class QueryRouter {
public:
    // Now that `StorageEngine` is known globally, `::StorageEngine*` is valid.
    explicit QueryRouter(::StorageEngine* engine);

    enum class ExecutionPath {
        LSM_ONLY,
        COLUMNAR_ONLY,
        HYBRID
    };

    ExecutionPath routeQuery(
        const std::string& store_path,
        const std::vector<FilterCondition>& filters,
        const std::optional<AggregationPlan>& aggPlan
    );

private:
    // This pointer type is also now correctly resolved.
    ::StorageEngine* engine_ptr_;
};

} // namespace columnar
} // namespace engine