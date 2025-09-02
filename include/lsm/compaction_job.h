// include/lsm/compaction_job.h
#pragma once

#include "sstable_meta.h" // Needs SSTableMetadata
#include <vector>
#include <memory>

// Forward-declare to avoid including the full LSMTree header here if not needed
// class LSMTree; // Not needed as the struct is self-contained

struct CompactionJobForIndinis {
    int level_L;
    std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> sstables_L;
    std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> sstables_L_plus_1;

    CompactionJobForIndinis(
        int l,
        std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> s_l,
        std::vector<std::shared_ptr<engine::lsm::SSTableMetadata>> s_l_plus_1
    )
        : level_L(l),
          sstables_L(std::move(s_l)),
          sstables_L_plus_1(std::move(s_l_plus_1)) {}
};