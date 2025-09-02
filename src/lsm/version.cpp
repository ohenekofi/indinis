// src/lsm/version.cpp
#include "../../include/lsm/version.h"
#include <algorithm> // For std::remove_if, std::sort
#include <set>       // For std::set

namespace engine {
namespace lsm {

// --- Version Implementation ---

// FIX: Corrected the type in the constructor parameter from LSMTree::SSTable to SSTableMetadata.
Version::Version(const std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>& initial_levels)
    : levels_(initial_levels) {
    Ref(); // A new version starts with one reference for itself (from the list).
}

Version::~Version() {
    // Destructor is intentionally simple. Cleanup is handled by smart pointers in levels_.
}

void Version::Ref() {
    ref_count_.fetch_add(1, std::memory_order_relaxed);
}

void Version::Unref() {
    // Use memory_order_acq_rel to synchronize with other threads that might be reading/writing ref_count_.
    if (ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        delete this;
    }
}


// --- VersionSet Implementation ---

// FIX: Correctly initialize the dummy_head_ member. The Version constructor requires a vector of
// levels, so we provide an empty one for the dummy node.
VersionSet::VersionSet() : dummy_head_(std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>{}) {
    dummy_head_.next_ = &dummy_head_;
    dummy_head_.prev_ = &dummy_head_;
}

VersionSet::~VersionSet() {
    // Unref the current version to signal it's no longer the head.
    Version* current = current_version_.load(std::memory_order_relaxed);
    if (current) {
        current->Unref();
    }
    // Clean up the entire linked list to break cycles and release any stranded versions.
    Version* p = dummy_head_.next_;
    while (p != &dummy_head_) {
        Version* next = p->next_;
        p->Unref(); // Unref each version in the list
        p = next;
    }
}

// FIX: Corrected the type in the parameter and used std::move for efficiency.
void VersionSet::Initialize(std::vector<std::vector<std::shared_ptr<SSTableMetadata>>>&& initial_levels) {
    Version* initial_version = new Version(std::move(initial_levels));
    
    // Append to the circular doubly-linked list.
    initial_version->next_ = &dummy_head_;
    initial_version->prev_ = dummy_head_.prev_;
    dummy_head_.prev_->next_ = initial_version;
    dummy_head_.prev_ = initial_version;
    
    current_version_.store(initial_version, std::memory_order_release);
}

Version* VersionSet::GetCurrent() {
    Version* current = current_version_.load(std::memory_order_acquire);
    if (current) {
        current->Ref();
    }
    return current;
}

void VersionSet::ApplyChanges(const VersionEdit& edit) {
    std::lock_guard<std::mutex> lock(writer_mutex_);

    Version* base_version = GetCurrent(); // Gets current and Refs it.
    
    // FIX: Use the correct type for the new levels metadata.
    std::vector<std::vector<std::shared_ptr<SSTableMetadata>>> new_levels = base_version->GetLevels();

    // Apply deletions.
    for (const auto& del : edit.deleted_files) {
        int level = del.first;
        uint64_t file_id = del.second;
        if (level >= 0 && static_cast<size_t>(level) < new_levels.size()) {
            auto& level_vec = new_levels[level];
            level_vec.erase(
                std::remove_if(level_vec.begin(), level_vec.end(),
                    // FIX: Corrected the lambda parameter type.
                    [file_id](const std::shared_ptr<SSTableMetadata>& sst) {
                        return sst->sstable_id == file_id;
                    }),
                level_vec.end());
        }
    }
    
    // Apply additions and track which levels were modified.
    std::set<int> modified_levels;
    for (const auto& add : edit.new_files) {
        int level = add.first;
        const auto& file_meta = add.second;
        if (level >= 0 && static_cast<size_t>(level) < new_levels.size()) {
            new_levels[level].push_back(file_meta);
            modified_levels.insert(level);
        }
    }
    
    // Re-sort only the levels that were modified.
    for (int level : modified_levels) {
        if (level == 0) {
            // L0 is sorted by sstable_id descending (newest first).
            std::sort(new_levels[level].begin(), new_levels[level].end(),
                      [](const auto& a, const auto& b) { return a->sstable_id > b->sstable_id; });
        } else {
            // L1+ are sorted by min_key ascending.
            std::sort(new_levels[level].begin(), new_levels[level].end(),
                      [](const auto& a, const auto& b) { return a->min_key < b->min_key; });
        }
    }

    // Create and install the new version.
    Version* new_version = new Version(new_levels);
    
    // Add to linked list.
    new_version->next_ = &dummy_head_;
    new_version->prev_ = dummy_head_.prev_;
    dummy_head_.prev_->next_ = new_version;
    dummy_head_.prev_ = new_version;
    
    // Atomically swap the current version pointer.
    current_version_.store(new_version, std::memory_order_release);
    
    // Unreference the base version. It will be deleted once its ref_count reaches zero.
    base_version->Unref();

    // Prune old, unreferenced versions from the list. Any version with ref_count == 1
    // is only referenced by the list itself and can be safely removed.
    Version* p = dummy_head_.next_;
    while (p != new_version) {
        // A version is prunable if only the list holds a reference (ref_count == 1).
        if (p->ref_count_.load(std::memory_order_acquire) == 1) {
            Version* to_delete = p;
            p = p->next_;
            to_delete->prev_->next_ = to_delete->next_;
            to_delete->next_->prev_ = to_delete->prev_;
            to_delete->Unref(); // This will trigger the delete.
        } else {
            p = p->next_;
        }
    }
}

uint64_t VersionSet::GetCurrentVersionIdForDebug() const {
    Version* current = current_version_.load(std::memory_order_relaxed);
    return reinterpret_cast<uintptr_t>(current);
}

int VersionSet::GetLiveVersionsCountForDebug() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(writer_mutex_));
    int count = 0;
    for (Version* v = dummy_head_.next_; v != &dummy_head_; v = v->next_) {
        count++;
    }
    return count;
}

} // namespace lsm
} // namespace engine