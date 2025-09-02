// include/lsm/interfaces.h
#pragma once

namespace engine {
namespace lsm {

/**
 * @class IFlushable
 * @brief An abstract interface for components that can be triggered to flush
 * their in-memory data.
 *
 * This is used by the WriteBufferManager to trigger a flush on an LSMTree
 * without creating a circular dependency between the two components.
 */
class IFlushable {
public:
    virtual ~IFlushable() = default;

    /**
     * @brief Signals the component to initiate an asynchronous flush of its
     * oldest/fullest in-memory buffer.
     */
    virtual void TriggerFlush() = 0;
};

} // namespace lsm
} // namespace engine