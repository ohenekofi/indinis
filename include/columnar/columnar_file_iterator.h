//src/columnar/columnar_file_iterator.h
#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include <memory>
#include "../../include/types.h" // For ValueType

namespace engine {
namespace columnar {

// Forward-declare ColumnarFile because we only use a pointer/shared_ptr to it in the header.
class ColumnarFile;

class ColumnarFileIterator {
public:
    // The constructor will be implemented in the .cpp file where the full ColumnarFile definition is available.
    ColumnarFileIterator(std::shared_ptr<ColumnarFile> file, const std::vector<uint32_t>& columns_to_read);
    ~ColumnarFileIterator(); // Declare destructor to be defined in .cpp file

    bool hasNext() const;
    std::pair<std::string, std::unordered_map<uint32_t, ValueType>> next();
    
private:
    // Use a unique_ptr to an incomplete PImpl (Pointer to Implementation) struct
    // This completely hides the implementation details and dependencies from the header.
    struct PImpl; 
    std::unique_ptr<PImpl> pimpl_;
};

} // namespace columnar
} // namespace engine