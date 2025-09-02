// src/threading/system_monitor.cpp
#include "../../include/threading/system_monitor.h"
#include <stdexcept>

// Platform-specific headers and implementation
#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#elif __linux__
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#elif __APPLE__
#include <mach/mach.h>
#endif

namespace engine {
namespace threading {

// --- PImpl Struct Definition ---
// This struct contains the platform-specific state needed for calculations.
struct SystemMonitor::PImpl {
#ifdef _WIN32
    ULARGE_INTEGER last_idle_time;
    ULARGE_INTEGER last_kernel_time;
    ULARGE_INTEGER last_user_time;
#elif __linux__
    unsigned long long last_user_time = 0;
    unsigned long long last_nice_time = 0;
    unsigned long long last_system_time = 0;
    unsigned long long last_idle_time = 0;
    unsigned long long last_iowait_time = 0;
    unsigned long long last_irq_time = 0;
    unsigned long long last_softirq_time = 0;
#elif __APPLE__
    processor_info_array_t prev_cpu_info = nullptr;
    mach_msg_type_number_t prev_num_cpu_info = 0;
#endif
};

// --- Constructor ---
SystemMonitor::SystemMonitor() : pimpl_(std::make_unique<PImpl>()) {
    // Initial call to establish a baseline for the first delta calculation.
    getCpuUtilization();
}

// --- Destructor ---
SystemMonitor::~SystemMonitor() {
#ifdef __APPLE__
    if (pimpl_->prev_cpu_info) {
        vm_deallocate(mach_task_self(), (vm_address_t)pimpl_->prev_cpu_info, pimpl_->prev_num_cpu_info * sizeof(integer_t));
    }
#endif
}

// --- getCpuUtilization Implementation ---
double SystemMonitor::getCpuUtilization() {
#ifdef _WIN32
    ULARGE_INTEGER idle_time, kernel_time, user_time;
    if (!GetSystemTimes((FILETIME*)&idle_time, (FILETIME*)&kernel_time, (FILETIME*)&user_time)) {
        return 0.0;
    }

    ULONGLONG idle_delta = idle_time.QuadPart - pimpl_->last_idle_time.QuadPart;
    ULONGLONG kernel_delta = kernel_time.QuadPart - pimpl_->last_kernel_time.QuadPart;
    ULONGLONG user_delta = user_time.QuadPart - pimpl_->last_user_time.QuadPart;
    ULONGLONG total_system_time = kernel_delta + user_delta;
    ULONGLONG total_time = total_system_time - idle_delta;

    pimpl_->last_idle_time = idle_time;
    pimpl_->last_kernel_time = kernel_time;
    pimpl_->last_user_time = user_time;

    if (total_system_time == 0) return 0.0;
    return static_cast<double>(total_time) / static_cast<double>(total_system_time);

#elif __linux__
    std::ifstream stat_file("/proc/stat");
    if (!stat_file) return 0.0;
    
    std::string line;
    std::getline(stat_file, line);
    std::istringstream iss(line);
    
    std::string cpu_label;
    unsigned long long user, nice, system, idle, iowait, irq, softirq;
    iss >> cpu_label >> user >> nice >> system >> idle >> iowait >> irq >> softirq;

    if (pimpl_->last_idle_time == 0) { // First call
        pimpl_->last_user_time = user;
        pimpl_->last_nice_time = nice;
        pimpl_->last_system_time = system;
        pimpl_->last_idle_time = idle;
        pimpl_->last_iowait_time = iowait;
        pimpl_->last_irq_time = irq;
        pimpl_->last_softirq_time = softirq;
        return 0.0;
    }

    unsigned long long prev_idle = pimpl_->last_idle_time + pimpl_->last_iowait_time;
    unsigned long long current_idle = idle + iowait;
    
    unsigned long long prev_non_idle = pimpl_->last_user_time + pimpl_->last_nice_time + pimpl_->last_system_time + pimpl_->last_irq_time + pimpl_->last_softirq_time;
    unsigned long long current_non_idle = user + nice + system + irq + softirq;

    unsigned long long prev_total = prev_idle + prev_non_idle;
    unsigned long long current_total = current_idle + current_non_idle;

    unsigned long long total_delta = current_total - prev_total;
    unsigned long long idle_delta = current_idle - prev_idle;
    
    pimpl_->last_user_time = user;
    pimpl_->last_nice_time = nice;
    pimpl_->last_system_time = system;
    pimpl_->last_idle_time = idle;
    pimpl_->last_iowait_time = iowait;
    pimpl_->last_irq_time = irq;
    pimpl_->last_softirq_time = softirq;

    if (total_delta == 0) return 0.0;
    return static_cast<double>(total_delta - idle_delta) / static_cast<double>(total_delta);

#elif __APPLE__
    natural_t num_cpus;
    processor_info_array_t cpu_info;
    mach_msg_type_number_t num_cpu_info;

    if (host_processor_info(mach_host_self(), PROCESSOR_CPU_LOAD_INFO, &num_cpus, &cpu_info, &num_cpu_info) != KERN_SUCCESS) {
        return 0.0;
    }
    
    if (!pimpl_->prev_cpu_info) { // First call
        pimpl_->prev_cpu_info = cpu_info;
        pimpl_->prev_num_cpu_info = num_cpu_info;
        return 0.0;
    }

    double total_ticks = 0;
    double idle_ticks = 0;
    
    for (natural_t i = 0; i < num_cpus; ++i) {
        unsigned long long user_ticks = cpu_info[i * CPU_STATE_MAX + CPU_STATE_USER] - pimpl_->prev_cpu_info[i * CPU_STATE_MAX + CPU_STATE_USER];
        unsigned long long system_ticks = cpu_info[i * CPU_STATE_MAX + CPU_STATE_SYSTEM] - pimpl_->prev_cpu_info[i * CPU_STATE_MAX + CPU_STATE_SYSTEM];
        unsigned long long nice_ticks = cpu_info[i * CPU_STATE_MAX + CPU_STATE_NICE] - pimpl_->prev_cpu_info[i * CPU_STATE_MAX + CPU_STATE_NICE];
        unsigned long long current_idle_ticks = cpu_info[i * CPU_STATE_MAX + CPU_STATE_IDLE] - pimpl_->prev_cpu_info[i * CPU_STATE_MAX + CPU_STATE_IDLE];
        
        total_ticks += user_ticks + system_ticks + nice_ticks + current_idle_ticks;
        idle_ticks += current_idle_ticks;
    }

    vm_deallocate(mach_task_self(), (vm_address_t)pimpl_->prev_cpu_info, pimpl_->prev_num_cpu_info * sizeof(integer_t));
    pimpl_->prev_cpu_info = cpu_info;
    pimpl_->prev_num_cpu_info = num_cpu_info;

    if (total_ticks == 0) return 0.0;
    return (total_ticks - idle_ticks) / total_ticks;

#else
    // Fallback for unsupported platforms
    return 0.0;
#endif
}

} // namespace threading
} // namespace engine