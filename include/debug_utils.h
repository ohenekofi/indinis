// include/debug_utils.h (Revised LOG_TRACE using C++17 fold expression)
#pragma once

#include <string>
#include <sstream>
#include <iomanip>
#include <cctype> // For std::isprint
#include <iostream> // Added for logging
#include <mutex>

// Helper to safely print potentially non-printable string data
inline std::string format_key_for_print(const std::string& key) {
    std::ostringstream oss;
    bool has_binary = false; // Not used, but okay
    for (unsigned char c : key) {
        if (std::isprint(c)) {
            oss << c;
        } else {
            oss << "\\x" << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(c);
            // has_binary = true; // Not used
        }
    }
    return oss.str();
}

inline std::string format_key_for_print_simple(const std::string& key) {
    std::ostringstream oss;
    for (unsigned char c : key) {
        if (std::isprint(c)) {
            oss << c;
        } else {
            // Keep it simple for this specific debug: just print \xHH
            oss << "\\x" << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(c);
        }
    }
    return oss.str();
}

// For direct hex dump of a string's content
inline std::string hex_dump_string(const std::string& str) {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (unsigned char c : str) {
        oss << std::setw(2) << static_cast<int>(c);
    }
    return oss.str();
}


// --- Logging Macros ---

// Helper function for the variadic template macro
template<typename... Args>
void print_log_line(std::ostream& os, Args&&... args) {
    // C++17 Fold Expression: applies operator<< between args
    (os << ... << std::forward<Args>(args));
    os << std::endl;
}

// Define logging levels if desired
// #define INDINIS_DEBUG_LOG
// const int CURRENT_LOG_LEVEL = 2; // Example

#ifdef INDINIS_DEBUG_LOG
    #define LOG_DEBUG(level, ...) \
        do { \
            /* if (CURRENT_LOG_LEVEL >= level) { */ \
                std::cout << "[" << #level << "] "; \
                print_log_line(std::cout, __VA_ARGS__); \
            /* } */ \
        } while(0)
#else
    #define LOG_DEBUG(level, ...) // No-op when not debugging
#endif

#define LOG_INFO(...) do { std::cout << "[INFO] "; print_log_line(std::cout, __VA_ARGS__); } while(0)
#define LOG_WARN(...) do { std::cerr << "[WARN] "; print_log_line(std::cerr, __VA_ARGS__); } while(0)
#define LOG_ERROR(...) do { std::cerr << "[ERROR] "; print_log_line(std::cerr, __VA_ARGS__); } while(0)
#define LOG_FATAL(...) do { std::cerr << "[FATAL] "; print_log_line(std::cerr, __VA_ARGS__); /* exit? */ } while(0)

// Simple Trace Log (Always On for this Debugging)
#define LOG_TRACE(...) do { std::cout << "[TRACE] "; print_log_line(std::cout, __VA_ARGS__); } while(0)