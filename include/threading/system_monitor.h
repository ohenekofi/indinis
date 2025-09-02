// include/threading/system_monitor.h
#pragma once

#include <memory>

namespace engine {
namespace threading {

/**
 * @class SystemMonitor
 * @brief Provides a cross-platform way to get system-wide CPU utilization.
 *
 * This class uses the PImpl (Pointer to Implementation) idiom to hide the
 * platform-specific details of metric collection from the header file,
 * providing a clean and dependency-free interface.
 */
class SystemMonitor {
public:
    SystemMonitor();
    ~SystemMonitor();

    SystemMonitor(const SystemMonitor&) = delete;
    SystemMonitor& operator=(const SystemMonitor&) = delete;

    /**
     * @brief Gets the current system-wide CPU utilization.
     * @return A value between 0.0 (fully idle) and 1.0 (fully utilized).
     *         The first call after construction may return 0.0 as it establishes
     *         a baseline. Subsequent calls measure the delta since the last call.
     */
    double getCpuUtilization();

private:
    // PImpl idiom to hide platform-specific members
    struct PImpl;
    std::unique_ptr<PImpl> pimpl_;
};

} // namespace threading
} // namespace engine
