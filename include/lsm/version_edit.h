//include/lsm/version_edit.h
#pragma once

#include "sstable_meta.h" 

#include <vector>
#include <memory>
#include <utility>

namespace engine {
namespace lsm {

struct VersionEdit {
    std::vector<std::pair<int, std::shared_ptr<SSTableMetadata>>> new_files;
    std::vector<std::pair<int, uint64_t>> deleted_files;

    void AddFile(int level, std::shared_ptr<SSTableMetadata> file_meta) {
        new_files.emplace_back(level, std::move(file_meta));
    }

    void DeleteFile(int level, uint64_t file_id) {
        deleted_files.emplace_back(level, file_id);
    }
};

} // namespace lsm
} // namespace engine