//include/lsm/version.h
#pragma once

#include "sstable_meta.h" // <<< FIX: Include the new header
#include "version_edit.h"
#include <vector>
#include <memory>
#include <atomic>
#include <mutex>

namespace engine {
namespace lsm {

class VersionSet;

class Version {
    friend class VersionSet;
public:
    void Ref();
    void Unref();

    const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& GetLevels() const {
        return levels_;
    }

private:
    explicit Version(const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& initial_levels);
    ~Version();

    const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>> levels_;
    std::atomic<int> ref_count_{0};
    Version* next_ = nullptr;
    Version* prev_ = nullptr;
};

class VersionSet {
public:
    VersionSet();
    ~VersionSet();

    Version* GetCurrent();
    void ApplyChanges(const VersionEdit& edit);
    void Initialize(std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>&& initial_levels);
    uint64_t GetCurrentVersionIdForDebug() const;
    int GetLiveVersionsCountForDebug() const;

private:
    std::atomic<Version*> current_version_{nullptr};
    std::mutex writer_mutex_;
    Version dummy_head_;
};

class VersionUnreffer { // <<< This was the correct name.
public:
    explicit VersionUnreffer(Version* v) : version_(v) {}
    ~VersionUnreffer() {
        if (version_) {
            version_->Unref();
        }
    }
    VersionUnreffer(const VersionUnreffer&) = delete;
    VersionUnreffer& operator=(const VersionUnreffer&) = delete;
private:
    Version* version_;
};

} // namespace lsm
} // namespace engine