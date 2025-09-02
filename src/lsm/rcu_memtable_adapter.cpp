//src/lsm/rcu_memtable_adapter.cpp
#include "../../include/lsm/rcu_memtable_adapter.h"

namespace engine {
namespace lsm {

RCUMemTableRepAdapter::RCUMemTableRepAdapter(Arena& arena) {
    // Arena parameter is unused by design, as RCU manages its own memory.
    
    // FIX 1: RCUMemTable constructor requires RCUConfig parameter.
    ::lsm::memtable::RCUConfig config;
    // These defaults are sensible.
    config.grace_period = std::chrono::milliseconds(100);
    
    rcu_memtable_ = std::make_unique<::lsm::memtable::RCUMemTable>(config);
}

void RCUMemTableRepAdapter::Add(const std::string& key, const std::string& value, uint64_t seq) {
    // FIX 2: Add null pointer check for safety.
    if (!rcu_memtable_) {
        throw std::runtime_error("RCUMemTableRepAdapter::Add called on uninitialized adapter.");
    }
    rcu_memtable_->Add(key, value, seq);
}

RCUMemTableRepAdapter::~RCUMemTableRepAdapter() {
    // Ensure proper shutdown of the RCU memtable and its background thread.
    if (rcu_memtable_) {
        rcu_memtable_->shutdown();
    }
}


void RCUMemTableRepAdapter::Delete(const std::string& key, uint64_t seq) {
    if (!rcu_memtable_) {
        throw std::runtime_error("RCUMemTableRepAdapter::Delete called on uninitialized adapter.");
    }
    rcu_memtable_->Delete(key, seq);
}

std::unique_ptr<MemTableIterator> RCUMemTableRepAdapter::NewIterator() const {
    if (!rcu_memtable_) {
        return nullptr;
    }
    return rcu_memtable_->NewIterator();
}

size_t RCUMemTableRepAdapter::ApproximateMemoryUsage() const {
    if (!rcu_memtable_) {
        return 0;
    }
    return rcu_memtable_->ApproximateMemoryUsage();
}

size_t RCUMemTableRepAdapter::Count() const {
    if (!rcu_memtable_) {
        return 0;
    }
    return rcu_memtable_->Count();
}

bool RCUMemTableRepAdapter::Empty() const {
    if (!rcu_memtable_) {
        return true;
    }
    return rcu_memtable_->Empty();
}

std::string RCUMemTableRepAdapter::GetName() const {
    // FIX 3: RCUMemTable has a GetName() method, so we can delegate to it.
    if (!rcu_memtable_) {
        return "RCUMemTable (uninitialized)";
    }
    return rcu_memtable_->GetName();
}

bool RCUMemTableRepAdapter::Get(const std::string& key, std::string& value, uint64_t& seq) const {
    if (!rcu_memtable_) {
        return false;
    }
    return rcu_memtable_->Get(key, value, seq);
}

::lsm::memtable::RCUStatisticsSnapshot RCUMemTableRepAdapter::getRcuStatistics() const {
    if (!rcu_memtable_) {
        return {}; // Return default-constructed statistics
    }
    return rcu_memtable_->get_statistics();
}

void RCUMemTableRepAdapter::BeginTxnRead(::Transaction* tx_ptr) {
    if (!rcu_memtable_) {
        throw std::runtime_error("RCUMemTableRepAdapter::BeginTxnRead called on uninitialized adapter.");
    }
    // FIX 4: Add null pointer check before casting.
    if (!tx_ptr) {
        throw std::invalid_argument("BeginTxnRead received a null transaction pointer.");
    }
    rcu_memtable_->BeginTxnRead(reinterpret_cast<uintptr_t>(tx_ptr));
}

void RCUMemTableRepAdapter::EndTxnRead(::Transaction* tx_ptr) {
    if (!rcu_memtable_) {
        // Don't throw from here if this is called during shutdown/cleanup.
        return;
    }
    if (!tx_ptr) {
        // Can't throw from a potential cleanup path, just log.
        // LOG_WARN("EndTxnRead received a null transaction pointer.");
        return;
    }
    rcu_memtable_->EndTxnRead(reinterpret_cast<uintptr_t>(tx_ptr));
}

MemTableType RCUMemTableRepAdapter::GetType() const {
    return MemTableType::RCU;
}

} // namespace lsm
} // namespace engine