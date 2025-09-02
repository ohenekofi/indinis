// include/lsm/rcu_memtable_adapter.h
#pragma once

#include "memtable_rep.h"
#include "memtable.h" // The new RCU header
#include <memory>

class Transaction;

namespace engine {
namespace lsm {

// Forward-declare Arena as it's only used as a reference in the constructor
class Arena;

/**
 * @class RCUMemTableRepAdapter
 * @brief An adapter that makes the new lsm::memtable::RCUMemTable conform to the
 * existing MemTableRep interface.
 */
class RCUMemTableRepAdapter : public MemTableRep {
public:
    explicit RCUMemTableRepAdapter(Arena& arena);
    //~RCUMemTableRepAdapter() override = default;
    ~RCUMemTableRepAdapter() override;

    RCUMemTableRepAdapter(const RCUMemTableRepAdapter&) = delete;
    RCUMemTableRepAdapter& operator=(const RCUMemTableRepAdapter&) = delete;
    
    // --- MemTableRep Interface Implementation ---
    void Add(const std::string& key, const std::string& value, uint64_t seq) override;
    void Delete(const std::string& key, uint64_t seq) override;
    bool Get(const std::string& key, std::string& value, uint64_t& seq) const override;
    std::unique_ptr<MemTableIterator> NewIterator() const override;
    
    size_t ApproximateMemoryUsage() const override;
    size_t Count() const override;
    bool Empty() const override;
    std::string GetName() const override;
    void BeginTxnRead(::Transaction* tx_ptr);
    void EndTxnRead(::Transaction* tx_ptr);
    
    // ---  Method to expose internal stats for N-API ---
    // ===  Use fully qualified namespace and remove duplicate declaration ===
    ::lsm::memtable::RCUStatisticsSnapshot getRcuStatistics() const;
    MemTableType GetType() const override;

    ::lsm::memtable::RCUMemTable* getInternalRcuMemtable() const {
        return rcu_memtable_.get();
    }

private:
    // Use the fully qualified name here as well for clarity and consistency
    std::unique_ptr<::lsm::memtable::RCUMemTable> rcu_memtable_;
};

} // namespace lsm
} // namespace engine