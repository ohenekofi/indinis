// include/lsm/compaction_filter.h
#pragma once

#include "../types.h" // For ValueType
#include <string>
#include <memory>

namespace engine {
namespace lsm {

/**
 * @brief The decision returned by a CompactionFilter.
 */
enum class FilterDecision {
    KEEP,   // Keep the record as is.
    MODIFY, // Modify the record's value.
    DROP    // Drop the record entirely.
};

/**
 * @class CompactionFilter
 * @brief An interface for user-defined logic to be applied during compaction.
 *
 * Users can implement this interface to modify or drop key-value pairs as they
 * are being processed by a compaction job. This is useful for implementing
 * record-level TTLs, data transformations, or other cleanup tasks.
 */
class CompactionFilter {
public:
    virtual ~CompactionFilter() = default;

    /**
     * @brief The core filtering method, called for each live key-value pair during compaction.
     *
     * @param level The level of the SSTable file where the key-value pair originated.
     * @param key The key of the record.
     * @param value The existing value of the record.
     * @param new_value [out] If the function returns MODIFY, this must be populated
     *                  with the new value for the key. It is ignored otherwise.
     * @return A FilterDecision indicating whether to keep, modify, or drop the record.
     */
    virtual FilterDecision Filter(
        int level,
        const std::string& key,
        const ValueType& value,
        ValueType* new_value
    ) const = 0;

    /**
     * @brief A name for the filter, used for logging and debugging.
     */
    virtual const char* Name() const = 0;
};

} // namespace lsm
} // namespace engine