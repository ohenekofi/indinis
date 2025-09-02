//src/columnar/columnar_store.cpp
#include "../../include/columnar/columnar_store.h"
#include "../../include/columnar/columnar_file_iterator.h"
#include "../../include/storage_engine.h" // Make sure this comes AFTER the columnar_store.h include
#include "../../include/debug_utils.h"
#include "../../include/disk_manager.h"
#include "../../include/columnar/columnar_store.h"


#include <filesystem>
#include <algorithm>
#include <stdexcept>
#include <cmath>
#include <xsimd/xsimd.hpp>
#include <nlohmann/json.hpp>

namespace fs = std::filesystem;
using json = nlohmann::json;




namespace engine {
namespace columnar {

namespace {

    using batch_double = xsimd::batch<double, xsimd::default_arch>;
    using batch_int64 = xsimd::batch<int64_t, xsimd::default_arch>;

    template <typename T, class B, class F>
    void apply_numeric_batch_filter(
        const std::vector<T>& data_vec,
        std::vector<bool>& selection_vector,
        size_t offset,
        size_t batch_size,
        F&& predicate_op)
    {
        for (size_t i = offset; i < offset + batch_size; i += B::size) {
            // Load a batch of data from the column vector.
            auto data_batch = B::load_unaligned(&data_vec[i]);
            
            // Apply the predicate (e.g., data_batch > filter_batch). Result is a boolean batch.
            auto mask_batch = predicate_op(data_batch);

            // For each element in the batch, if its mask is false, update the selection vector.
            for (size_t j = 0; j < B::size; ++j) {
                if (i + j < selection_vector.size() && !mask_batch.get(j)) {
                    selection_vector[i + j] = false;
                }
            }
        }
    }

    size_t countPathSegments(const std::string& path) {
        if (path.empty()) return 0;
        size_t count = 0;
        bool in_segment = false;
        for (char c : path) {
            if (c == '/') {
                if (in_segment) {
                    count++;
                    in_segment = false;
                }
            } else {
                in_segment = true;
            }
        }
        if (in_segment) count++;
        return count;
    }

    bool does_pk_range_overlap(
        const std::string& min_pk,
        const std::string& max_pk,
        const FilterCondition& filter)
    {
        if (min_pk.empty() || max_pk.empty()) {
            return true; // No stats for this file, must scan.
        }

        // We only handle filters where the value is a single string.
        if (!std::holds_alternative<ValueType>(filter.value) ||
            !std::holds_alternative<std::string>(std::get<ValueType>(filter.value))) {
            return true; // Filter is not on a single string value, cannot prune by PK.
        }
        
        const std::string& filter_val = std::get<std::string>(std::get<ValueType>(filter.value));

        switch (filter.op) {
            case FilterOperator::EQUAL:
                // If filter value is between min and max (inclusive), it might be in the file.
                return (filter_val >= min_pk && filter_val <= max_pk);
            
            case FilterOperator::GREATER_THAN:
                // If the file's max key is greater than the filter value, there could be overlap.
                return max_pk > filter_val;

            case FilterOperator::GREATER_THAN_OR_EQUAL:
                // If the file's max key is greater than or equal to the filter value.
                return max_pk >= filter_val;

            case FilterOperator::LESS_THAN:
                // If the file's min key is less than the filter value.
                return min_pk < filter_val;

            case FilterOperator::LESS_THAN_OR_EQUAL:
                // If the file's min key is less than or equal to the filter value.
                return min_pk <= filter_val;

            default:
                // For other operators (like array contains), we can't prune based on PK range.
                return true;
        }
    }   

    json value_to_json(const ValueType& v) {
        return std::visit([](auto&& arg) -> json {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, std::monostate>) {
                return nullptr;
            } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                // nlohmann::json can't directly store binary, so we might return a placeholder or a base64 string.
                return "binary_placeholder"; 
            } else {
                // This handles int64_t, double, bool, and string automatically.
                return arg;
            }
        }, v);
    }

    bool evaluateFilter(const ValueType& doc_value, const FilterCondition& filter) {
        const ValueType& filter_value_variant = std::get<ValueType>(filter.value);

        return std::visit([&](auto&& d_val) {
            return std::visit([&](auto&& f_val) {
                if constexpr (std::is_same_v<std::decay_t<decltype(d_val)>, std::monostate> ||
                              std::is_same_v<std::decay_t<decltype(f_val)>, std::monostate>) {
                    return false;
                }
                if constexpr (std::is_arithmetic_v<std::decay_t<decltype(d_val)>> &&
                              std::is_arithmetic_v<std::decay_t<decltype(f_val)>>) {
                    double doc_d = static_cast<double>(d_val);
                    double filter_d = static_cast<double>(f_val);
                    switch (filter.op) {
                        case FilterOperator::EQUAL: return doc_d == filter_d;
                        case FilterOperator::GREATER_THAN: return doc_d > filter_d;
                        case FilterOperator::GREATER_THAN_OR_EQUAL: return doc_d >= filter_d;
                        case FilterOperator::LESS_THAN: return doc_d < filter_d;
                        case FilterOperator::LESS_THAN_OR_EQUAL: return doc_d <= filter_d;
                        default: return false;
                    }
                }
                if constexpr (std::is_same_v<std::decay_t<decltype(d_val)>, std::string> &&
                              std::is_same_v<std::decay_t<decltype(f_val)>, std::string>) {
                    int cmp = d_val.compare(f_val);
                    switch (filter.op) {
                        case FilterOperator::EQUAL: return cmp == 0;
                        case FilterOperator::GREATER_THAN: return cmp > 0;
                        case FilterOperator::GREATER_THAN_OR_EQUAL: return cmp >= 0;
                        case FilterOperator::LESS_THAN: return cmp < 0;
                        case FilterOperator::LESS_THAN_OR_EQUAL: return cmp <= 0;
                        default: return false;
                    }
                }
                return false;
            }, filter_value_variant);
        }, doc_value);
    }

    // Now this function can safely call evaluateFilter.
    bool does_range_overlap(const ValueType& min_val, const ValueType& max_val, const FilterCondition& filter) {
        if (std::holds_alternative<std::monostate>(min_val) || std::holds_alternative<std::monostate>(max_val)) {
            return true;
        }

        const ValueType& filter_val = std::get<ValueType>(filter.value);
        try {
            if (filter.op == FilterOperator::EQUAL) return (filter_val >= min_val && filter_val <= max_val);
            if (filter.op == FilterOperator::GREATER_THAN) return (max_val > filter_val);
            if (filter.op == FilterOperator::GREATER_THAN_OR_EQUAL) return (max_val >= filter_val);
            if (filter.op == FilterOperator::LESS_THAN) return (min_val < filter_val);
            if (filter.op == FilterOperator::LESS_THAN_OR_EQUAL) return (min_val <= filter_val);
        } catch (const std::bad_variant_access&) {
            return true; // Mismatched types, cannot prune, must scan.
        }
        return true; // For other operators, we must scan.
    }

    void apply_vectorized_filter(
        const ColumnVector& data_vector,
        const FilterCondition& filter,
        std::vector<bool>& selection_vector)
    {
        const size_t row_count = data_vector.size();
        if (row_count == 0) return;

        // --- Case 1: Handle Numeric Filters (int64_t) with SIMD ---
        if (std::holds_alternative<int64_t>(std::get<ValueType>(filter.value))) {
            const int64_t filter_val = std::get<int64_t>(std::get<ValueType>(filter.value));
            
            // Create a temporary vector of the raw numeric type for efficient SIMD processing.
            std::vector<int64_t> numeric_data;
            numeric_data.reserve(row_count);
            for(const auto& v : data_vector) {
                // Use a default value (e.g., 0) for non-matching types or nulls.
                // This ensures the predicate still runs but won't match.
                numeric_data.push_back(std::holds_alternative<int64_t>(v) ? std::get<int64_t>(v) : 0);
            }

            // Create a SIMD batch that contains the filter value broadcasted across all lanes.
            auto filter_batch = batch_int64(filter_val);

            // Dispatch to the correct templated batch filter based on the operator.
            switch (filter.op) {
                case FilterOperator::EQUAL:
                    apply_numeric_batch_filter<int64_t, batch_int64>(numeric_data, selection_vector, 0, row_count,
                        [&](const auto& batch) { return batch == filter_batch; });
                    break;
                case FilterOperator::GREATER_THAN:
                    apply_numeric_batch_filter<int64_t, batch_int64>(numeric_data, selection_vector, 0, row_count,
                        [&](const auto& batch) { return batch > filter_batch; });
                    break;
                // ... implement cases for >=, <, <= similarly ...
                case FilterOperator::GREATER_THAN_OR_EQUAL:
                    apply_numeric_batch_filter<int64_t, batch_int64>(numeric_data, selection_vector, 0, row_count,
                        [&](const auto& batch) { return batch >= filter_batch; });
                    break;
                case FilterOperator::LESS_THAN:
                    apply_numeric_batch_filter<int64_t, batch_int64>(numeric_data, selection_vector, 0, row_count,
                        [&](const auto& batch) { return batch < filter_batch; });
                    break;
                case FilterOperator::LESS_THAN_OR_EQUAL:
                    apply_numeric_batch_filter<int64_t, batch_int64>(numeric_data, selection_vector, 0, row_count,
                        [&](const auto& batch) { return batch <= filter_batch; });
                    break;
                default:
                    // Fall back to row-by-row for unsupported operators on this type.
                    goto fallback_filter;
            }
            return; // SIMD processing is complete for this filter.
        }

        // --- Case 2: Handle Numeric Filters (double) with SIMD ---
        if (std::holds_alternative<double>(std::get<ValueType>(filter.value))) {
            const double filter_val = std::get<double>(std::get<ValueType>(filter.value));
            std::vector<double> numeric_data;
            numeric_data.reserve(row_count);
            for(const auto& v : data_vector) {
                numeric_data.push_back(std::holds_alternative<double>(v) ? std::get<double>(v) : 0.0);
            }
            auto filter_batch = batch_double(filter_val);
            // ... similar switch statement for double operations ...
            return;
        }

    fallback_filter:
        // --- Fallback Case: Handle Strings and other types row-by-row ---
        // This uses the old logic, which is still necessary for non-numeric types.
        for (size_t i = 0; i < row_count; ++i) {
            if (selection_vector[i]) { // Only process rows that are still candidates
                if (!evaluateFilter(data_vector[i], filter)) {
                    selection_vector[i] = false;
                }
            }
        }
    }

    // Helper class to iterate over records within a columnar file during compaction.
    class ColumnarReader {
    public:
        ColumnarReader(
            std::shared_ptr<engine::columnar::ColumnarFile> file,
            bool encryption_active,
            const std::vector<unsigned char>& dek
        )
            : file_(file),
              encryption_is_active_(encryption_active),
              dek_(dek),
              current_row_index_(0) 
        {
            if (file_) {
                all_primary_keys_ = file_->getPrimaryKeys();
            }
        }

        bool hasMore() const {
            return current_row_index_ < all_primary_keys_.size();
        }

        const std::string& peekKey() const {
            if (!hasMore()) {
                static const std::string empty_string;
                return empty_string;
            }
            return all_primary_keys_[current_row_index_];
        }
        
        // This is a placeholder for fetching the full row.
        // For our GC-focused compaction, we only need the key, so this is sufficient.
        // A full query engine would need a `readRow` method here.
        
        void advance() {
            if (hasMore()) {
                current_row_index_++;
            }
        }
        
        uint64_t getFileId() const { return file_ ? file_->getFileId() : 0; }
        const std::string& getFilename() const { return file_->getPath(); }

        std::unordered_map<uint32_t, ValueType> readCurrentRowData() {
            if (!hasMore()) {
                return {};
            }
            
            // In a highly optimized engine, this would read only the single row's data from
            // each column chunk on disk. For our implementation, we'll read full columns
            // into memory once and then access them by index.
            if (all_columns_data_.empty()) {
                const auto& row_groups_in_file = file_->getRowGroups();
                if (row_groups_in_file.empty()) {
                    return {}; // No data to read
                }
                
                // ---  ---
                // 1. Build a list of all column IDs in the file.
                std::vector<uint32_t> all_col_ids;
                for (const auto& chunk_meta : row_groups_in_file[0].column_chunks) {
                    all_col_ids.push_back(chunk_meta.column_id);
                }

                // 2. Build a list of all row group indices (0, 1, 2, ...).
                std::vector<int> all_row_group_indices;
                all_row_group_indices.reserve(row_groups_in_file.size());
                for (int i = 0; i < row_groups_in_file.size(); ++i) {
                    all_row_group_indices.push_back(i);
                }

                // 3. Call readColumns with the complete list of row groups.
                all_columns_data_ = file_->readColumns(all_col_ids, all_row_group_indices, encryption_is_active_, dek_);
                // --- ---
            }

            std::unordered_map<uint32_t, ValueType> row_data;
            for (auto const& [col_id, col_vector] : all_columns_data_) {
                if (current_row_index_ < col_vector.size()) {
                    row_data[col_id] = col_vector[current_row_index_];
                }
            }
            return row_data;
        }

    private:
        std::shared_ptr<engine::columnar::ColumnarFile> file_;
        bool encryption_is_active_;
        const std::vector<unsigned char>& dek_;
        size_t current_row_index_;
        std::vector<std::string> all_primary_keys_;
        std::unordered_map<uint32_t, ColumnVector> all_columns_data_;
    };

    // Helper struct for the compaction's priority queue
    struct CompactionHeapEntry {
        std::string primary_key;
        size_t reader_index;

        // Make this a min-heap by comparing greater-than
        bool operator>(const CompactionHeapEntry& other) const {
            return primary_key > other.primary_key;
        }
    };

} // end anonymous namespace

struct AggregateState {
    double sum = 0.0;
    int64_t count = 0;
    ValueType min = std::monostate{};
    ValueType max = std::monostate{};
};


// --- CompactionManager Implementation (Cloned from LSMTree) ---
ColumnarCompactionManager::ColumnarCompactionManager(ColumnarStore* store_ptr, size_t num_threads)
    : store_ptr_(store_ptr), shutdown_(false) {
    for (size_t i = 0; i < num_threads; ++i) {
        workers_.emplace_back(&ColumnarCompactionManager::workerThreadLoop, this);
    }
    LOG_INFO("ColumnarCompactionManager for store '{}' initialized with {} workers.", store_ptr_->store_path_, num_threads);
}

ColumnarCompactionManager::~ColumnarCompactionManager() {
    shutdown_.store(true);
    queue_cv_.notify_all();
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void ColumnarCompactionManager::scheduleJob(ColumnarCompactionJob job) {
    std::lock_guard lock(queue_mutex_);
    job_queue_.push(std::move(job));
    queue_cv_.notify_one();
}

void ColumnarCompactionManager::workerThreadLoop() {
    while (!shutdown_.load()) {
        ColumnarCompactionJob job{0, {}, {}};
        {
            std::unique_lock lock(queue_mutex_);
            queue_cv_.wait(lock, [this] { return !job_queue_.empty() || shutdown_.load(); });
            if (shutdown_.load()) break;
            job = std::move(job_queue_.front());
            job_queue_.pop();
        }
        
        
        try {
            store_ptr_->executeCompactionJob(job.level_L, job.files_L, job.files_L_plus_1);
        } catch (const std::exception& e) {
            LOG_ERROR("Exception in ColumnarCompactionManager worker thread during executeCompactionJob for L{}: {}",
                      job.level_L, e.what());
            // Decide on error handling: retry job, log and drop, etc.
        }
        // ----------------------------------------------------
    }
}


// --- ColumnarStore Implementation ---

ColumnarStore::ColumnarStore(
    const std::string& store_path,
    const std::string& data_dir,
    StoreSchemaManager* schema_manager,
    size_t write_buffer_max_size,
    CompressionType compression_type,
    int compression_level,
    bool encryption_is_active,
    EncryptionScheme encryption_scheme,
    const std::vector<unsigned char>& dek
)
    : storage_engine_ptr_(nullptr),
      schema_manager_(schema_manager),
      store_path_(store_path),
      data_dir_(data_dir),
      write_buffer_max_size_(write_buffer_max_size),
      encryption_is_active_(encryption_is_active),
      default_encryption_scheme_(encryption_scheme),
      database_dek_(dek),
      compression_type_(compression_type),
      compression_level_(compression_level)
{
    if (!schema_manager_) {
        throw std::invalid_argument("StoreSchemaManager cannot be null for ColumnarStore.");
    }
    fs::create_directories(data_dir_);
    levels_.resize(7);

    active_write_buffers_.resize(NUM_WRITE_BUFFER_SHARDS);
    active_buffer_mutexes_.resize(NUM_WRITE_BUFFER_SHARDS); 
    for(size_t i = 0; i < NUM_WRITE_BUFFER_SHARDS; ++i) {
        active_buffer_mutexes_[i] = std::make_unique<std::mutex>();
    }
    // Use a unique_ptr for the ring buffer which requires a template argument
    ingestion_queue_ = std::make_unique<LockFreeRingBuffer<ColumnarWriteRequest>>();
    
    // Call the newly implemented metadata loading function.
    loadFileMetadata();
}

void ColumnarStore::setStorageEngine(StorageEngine* engine) {
    storage_engine_ptr_ = engine;
    
    auto schema_opt = getLatestStoreSchema();
    if (!schema_opt) {
        LOG_WARN("ColumnarStore for '{}' linked to engine, but no schema is registered. Store will remain dormant.", store_path_);
        return;
    }
    
    // --- MODIFIED --- Initialize all sharded buffers
    for (size_t i = 0; i < NUM_WRITE_BUFFER_SHARDS; ++i) {
        active_write_buffers_[i] = std::make_shared<ColumnarBuffer>(*schema_opt);
    }
    
    compaction_manager_ = std::make_unique<ColumnarCompactionManager>(this, 2);
    startBackgroundThreads(); // Call renamed method
    LOG_INFO("ColumnarStore for path '{}' is now fully initialized and active.", store_path_);
}

void ColumnarStore::stopBackgroundThreads() {
    ingestion_worker_shutdown_.store(true);
    // You might need a CV to wake the ingestion thread if it's based on a conditional wait
    if (ingestion_thread_.joinable()) ingestion_thread_.join();

    flush_workers_shutdown_.store(true);
    immutable_buffer_ready_cv_.notify_all();
    for (auto& worker : flush_worker_threads_) {
        if (worker.joinable()) worker.join();
    }

    scheduler_running_.store(false);
    scheduler_cv_.notify_one();
    if (scheduler_thread_.joinable()) scheduler_thread_.join();
    
    if (compaction_manager_) {
        compaction_manager_.reset();
    }
}


ColumnarStore::~ColumnarStore() {
    LOG_INFO("Shutting down ColumnarStore for path '{}'...", store_path_);
    stopBackgroundThreads(); // Call renamed method
    
    // --- NEW --- Process any remaining items in the ingestion queue
    if (ingestion_queue_) {
        // In a real shutdown, you would drain the queue and process it here
        // For simplicity, we assume it's drained before stop is called.
    }

    std::vector<std::shared_ptr<ColumnarBuffer>> all_buffers;
    {
        std::unique_lock lock(buffer_mutex_);
        // --- MODIFIED --- Gather from sharded active buffers and immutable queue
        for(const auto& buffer : active_write_buffers_) {
            if (buffer && buffer->getRowCount() > 0) {
                all_buffers.push_back(buffer);
            }
        }
        all_buffers.insert(all_buffers.end(), immutable_buffers_.begin(), immutable_buffers_.end());
    }

    if (!all_buffers.empty()) {
        LOG_INFO("Performing final synchronous flush of {} buffer(s)...", all_buffers.size());
        flushBuffersToSingleFile(all_buffers);
    }
    LOG_INFO("ColumnarStore for path '{}' shut down.", store_path_);
}

void ColumnarStore::startBackgroundThreads() {
    ingestion_worker_shutdown_.store(false);
    ingestion_thread_ = std::thread(&ColumnarStore::ingestionWorkerLoop, this);
    
    flush_workers_shutdown_.store(false);
    for (size_t i = 0; i < 2; ++i) { 
        flush_worker_threads_.emplace_back(&ColumnarStore::backgroundFlushWorkerLoop, this);
    }
    
    scheduler_running_.store(true);
    scheduler_thread_ = std::thread(&ColumnarStore::schedulerThreadLoop, this);
}
// --- Lifecycle Methods (Cloned Logic) ---
void ColumnarStore::startCompactionAndFlushThreads() {
    flush_workers_shutdown_.store(false);
    for (size_t i = 0; i < 2; ++i) { 
        flush_worker_threads_.emplace_back(&ColumnarStore::backgroundFlushWorkerLoop, this);
    }
    
    scheduler_running_.store(true);
    scheduler_thread_ = std::thread(&ColumnarStore::schedulerThreadLoop, this);
}

void ColumnarStore::stopCompactionAndFlushThreads() {
    flush_workers_shutdown_.store(true);
    immutable_buffer_ready_cv_.notify_all();
    for (auto& worker : flush_worker_threads_) {
        if (worker.joinable()) worker.join();
    }

    scheduler_running_.store(false);
    scheduler_cv_.notify_one();
    if (scheduler_thread_.joinable()) scheduler_thread_.join();
    
    if (compaction_manager_) {
        compaction_manager_.reset();
    }
}

std::optional<std::shared_ptr<ColumnarFileMetadata>> ColumnarStore::flushBuffersToSingleFile(
    std::vector<std::shared_ptr<ColumnarBuffer>> buffers_to_flush
) {
    // 1. Guard Clause: Correctly handles the case of no work to be done.
    if (buffers_to_flush.empty()) {
        return std::nullopt;
    }

    // 2. File Identification: Atomically gets a new, unique ID and determines its path.
    uint64_t file_id = next_columnar_file_id_.fetch_add(1);
    int level = 0; // Flushes from memory always go to Level 0.
    std::string path = generateFilePath(level, file_id);

    size_t total_rows = 0;
    for(const auto& buffer : buffers_to_flush) total_rows += buffer->getRowCount();
    
    LOG_INFO("Flushing {} columnar buffers ({} total rows) for store '{}' to L0 file: {}",
             buffers_to_flush.size(), total_rows, store_path_, path);

    try {
        // 3. Delegation to File Writer: The complex task of writing the multi-row-group file
        //    format is correctly delegated to the specialized ColumnarFile::create method.
        auto file_ptr = ColumnarFile::create(
            path,
            file_id,
            buffers_to_flush[0]->getSchema().schema_version, // Assumes all buffers in a batch share a schema, which is a safe assumption.
            buffers_to_flush, // Passes the entire vector of buffers.
            compression_type_,
            compression_level_,
            encryption_is_active_,
            default_encryption_scheme_,
            database_dek_
        );
        
        // 4. Metadata Creation: After the physical file is successfully written,
        //    create the in-memory metadata object that the store will use for tracking.
        auto metadata = std::make_shared<ColumnarFileMetadata>();
        metadata->filename = path;
        metadata->file_id = file_id;
        metadata->level = level;
        metadata->schema_version = file_ptr->getSchemaVersion();
        metadata->row_count = file_ptr->getTotalRowCount();
        metadata->size_bytes = fs::file_size(path);
        
        // 5. Aggregate Metadata Calculation: Correctly calculates the min/max primary keys
        //    across all the buffers that were just flushed into this single file.
        std::string min_pk, max_pk;
        if (!buffers_to_flush.empty() && !buffers_to_flush.front()->getPrimaryKeys().empty()) {
            min_pk = buffers_to_flush.front()->getPrimaryKeys().front();
            max_pk = buffers_to_flush.back()->getPrimaryKeys().back();
        }
        metadata->min_primary_key = min_pk;
        metadata->max_primary_key = max_pk;

        // Deep-copies the Bloom Filter from the temporary file object into the metadata.
        metadata->pk_bloom_filter = std::make_unique<BloomFilter>(*file_ptr->getPrimaryKeyFilter());

        // Here, column stats would also be aggregated from file_ptr->getRowGroups() if needed.
        
        // 6. Thread-Safe State Update: Correctly uses a mutex to protect the shared `levels_`
        //    data structure while adding the metadata for the new file.
        {
            std::lock_guard lock(levels_metadata_mutex_);
            levels_[level].push_back(metadata);
            // Sorts L0 by file ID (newest first) to ensure correct read order.
            std::sort(levels_[level].begin(), levels_[level].end(),
                [](const auto& a, const auto& b) { return a->file_id > b->file_id; });
        }
        
        LOG_INFO("Successfully flushed columnar buffers to {}", path);

        // 7. Successful Return: Returns the newly created metadata object.
        return metadata;

    } catch (const std::exception& e) {
        // 8. Robust Error Handling: If any part of the file creation fails, it logs the error,
        //    crucially removes the partial/corrupt file from disk, and returns an empty
        //    optional to signal failure to the caller.
        LOG_ERROR("Failed to flush columnar buffers to file {}: {}", path, e.what());
        fs::remove(path); // Cleanup partial file
        return std::nullopt;
    }
}

void ColumnarStore::backgroundFlushWorkerLoop() {
    while (!flush_workers_shutdown_.load()) {
        std::vector<std::shared_ptr<ColumnarBuffer>> buffers_to_flush;
        {
            std::unique_lock lock(immutable_cv_mutex_);
            immutable_buffer_ready_cv_.wait_for(lock, std::chrono::seconds(5), [this] {
                // Wake up if there's at least one buffer OR if we're shutting down
                std::shared_lock data_lock(buffer_mutex_);
                return !immutable_buffers_.empty() || flush_workers_shutdown_.load();
            });

            if (flush_workers_shutdown_.load()) break;
            
            std::unique_lock data_lock(buffer_mutex_);
            if (!immutable_buffers_.empty()) {
                // --- NEW: Grab all available immutable buffers ---
                buffers_to_flush = std::move(immutable_buffers_);
                immutable_buffers_.clear();
                // ---------------------------------------------
            }
        }
        immutable_slot_available_cv_.notify_all();
        
        if (!buffers_to_flush.empty()) {
            flushBuffersToSingleFile(std::move(buffers_to_flush));
            scheduler_cv_.notify_one();
        }
    }
}

// --- Core Data Path Methods ---
void ColumnarStore::shadowInsert(const Record& record) {
    auto schema_opt = getLatestStoreSchema();
    if (!schema_opt) return;

    auto flattened_data = flattenRecord(record, *schema_opt);
    if (flattened_data.empty()) return;

    // --- MODIFIED --- The entire method is now just a non-blocking write to a queue.
    ingestion_queue_->write({record.key, std::move(flattened_data)});
}


void ColumnarStore::shadowDelete(const std::string& primary_key) {
    ingestion_queue_->write({primary_key, {}});
}

void ColumnarStore::ingestionWorkerLoop() {
    LOG_INFO("[Columnar Ingestion Worker] Started for store '{}'.", store_path_);
    std::vector<ColumnarWriteRequest> batch;
    
    while (!ingestion_worker_shutdown_.load()) {
        size_t drained_count = ingestion_queue_->drainTo(batch);
        
        if (drained_count == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        auto schema_opt = getLatestStoreSchema();
        if (!schema_opt) continue;

        for (const auto& request : batch) {
            size_t shard_idx = std::hash<std::string>{}(request.primary_key) % NUM_WRITE_BUFFER_SHARDS;
            
            // --- THIS IS THE FIX ---
            // Lock by dereferencing the unique_ptr to get to the actual mutex.
            std::lock_guard<std::mutex> lock(*active_buffer_mutexes_[shard_idx]);
            
            auto& active_buffer = active_write_buffers_[shard_idx];
            if (active_buffer->isFull()) {
                {
                    // --- MODIFIED --- Use the correct mutex name
                    std::lock_guard<std::shared_mutex> immutable_lock(buffer_mutex_);
                    immutable_buffers_.push_back(active_buffer);
                }
                immutable_buffer_ready_cv_.notify_one();
                
                active_write_buffers_[shard_idx] = std::make_shared<ColumnarBuffer>(*schema_opt);
            }
            
            active_write_buffers_[shard_idx]->add(request.primary_key, request.flattened_record);
        }
    }
    LOG_INFO("[Columnar Ingestion Worker] Stopped for store '{}'.", store_path_);
}


std::optional<std::shared_ptr<ColumnarFileMetadata>> ColumnarStore::flushWriteBufferToFile(
    std::shared_ptr<ColumnarBuffer> buffer_to_flush
) {
    if (!buffer_to_flush || buffer_to_flush->getRowCount() == 0) {
        return std::nullopt; // Nothing to flush
    }

    uint64_t file_id = next_columnar_file_id_.fetch_add(1);
    int level = 0; // Flushing from buffer always goes to L0
    std::string path = generateFilePath(level, file_id);

    LOG_INFO("Flushing columnar buffer with {} rows for store '{}' to L0 file: {}",
             buffer_to_flush->getRowCount(), store_path_, path);

    try {
        // Create the physical file on disk
        // Note: The `create` method expects a vector of buffers.
        auto file_ptr = ColumnarFile::create(
            path, 
            file_id, 
            buffer_to_flush->getSchema().schema_version, 
            {buffer_to_flush}, // Pass the buffer inside a vector
            compression_type_, 
            compression_level_, 
            encryption_is_active_,
            default_encryption_scheme_, 
            database_dek_
        );
        
        // --- MODIFIED --- The local variable type is also corrected
        auto metadata = std::make_shared<ColumnarFileMetadata>();
        metadata->filename = path;
        metadata->file_id = file_id;
        metadata->level = level;
        metadata->schema_version = file_ptr->getSchemaVersion();
        metadata->row_count = file_ptr->getTotalRowCount();
        metadata->size_bytes = fs::file_size(path);
        
        metadata->pk_bloom_filter = std::make_unique<BloomFilter>(*file_ptr->getPrimaryKeyFilter());
        const auto& pks = buffer_to_flush->getPrimaryKeys();
        if (!pks.empty()) {
            metadata->min_primary_key = pks.front();
            metadata->max_primary_key = pks.back();
        }
        
        // Add the metadata to the level tracker
        {
            std::lock_guard lock(levels_metadata_mutex_);
            levels_[level].push_back(metadata);
            std::sort(levels_[level].begin(), levels_[level].end(),
                [](const auto& a, const auto& b) { return a->file_id > b->file_id; });
        }
        
        LOG_INFO("Successfully flushed columnar buffer to {}", path);

        return metadata;

    } catch (const std::exception& e) {
        LOG_ERROR("Failed to flush columnar buffer to file {}: {}", path, e.what());
        fs::remove(path); // Clean up partial file
        return std::nullopt;
    }
}

// --- Helper Methods ---
std::optional<ColumnSchema> ColumnarStore::getStoreSchema(uint32_t version) {
    // Check cache with a shared lock first for performance
    {
        std::shared_lock lock(schema_cache_mutex_);
        auto it = schema_cache_.find(version);
        if (it != schema_cache_.end()) {
            return it->second;
        }
    }

    // Not in cache, fetch from the main manager
    if (!schema_manager_) {
        return std::nullopt;
    }
    
    auto schema_opt = schema_manager_->getSchema(store_path_, version);
    if (schema_opt) {
        // Populate the cache for next time
        std::unique_lock lock(schema_cache_mutex_);
        schema_cache_[version] = *schema_opt;
    }
    
    return schema_opt;
}

// Gets the latest schema (this can remain const as it doesn't modify the cache).
std::optional<ColumnSchema> ColumnarStore::getLatestStoreSchema() const {
    if (!schema_manager_) {
        return std::nullopt;
    }
    return schema_manager_->getLatestSchema(store_path_);
}

std::unordered_map<uint32_t, ValueType> ColumnarStore::flattenRecord(const Record& record, const ColumnSchema& schema) {
    std::unordered_map<uint32_t, ValueType> flattened;
    if (record.deleted || !std::holds_alternative<std::string>(record.value)) {
        return flattened;
    }
    
    try {
        json j = json::parse(std::get<std::string>(record.value));
        for (const auto& col_def : schema.columns) {
            // This is a simple flattener. A production version would handle deep dot notation.
            if (j.contains(col_def.name)) {
                const auto& field_val = j.at(col_def.name);
                ValueType val;
                if (field_val.is_null()) val = std::monostate{};
                else if (field_val.is_string()) val = field_val.get<std::string>();
                else if (field_val.is_number_integer()) val = field_val.get<int64_t>();
                else if (field_val.is_number_float()) val = field_val.get<double>();
                else if (field_val.is_boolean()) val = field_val.get<bool>();
                else if (field_val.is_binary()) val = field_val.get_binary();
                else val = field_val.dump(); // Fallback to storing as JSON string
                
                flattened[col_def.column_id] = val;
            }
        }
    } catch (const json::parse_error& e) {
        LOG_WARN("Failed to flatten record for key {}: {}", record.key, e.what());
    }
    return flattened;
}

std::string ColumnarStore::generateFilePath(int level, uint64_t id) const {
    std::ostringstream oss;
    oss << "col_L" << level << "_" << std::setfill('0') << std::setw(10) << id << ".cstore";
    return (fs::path(data_dir_) / oss.str()).string();
}

void ColumnarStore::loadFileMetadata() {
    std::lock_guard lock(levels_metadata_mutex_);
    LOG_INFO("Loading columnar file metadata from: {}", data_dir_);
    
    uint64_t max_file_id_found = 0;
    
    try {
        if (!fs::exists(data_dir_)) {
            LOG_INFO("Columnar data directory does not exist for store '{}'. Nothing to load.", store_path_);
            return;
        }

        for (const auto& entry : fs::directory_iterator(data_dir_)) {
            const auto& path = entry.path();
            if (!entry.is_regular_file() || path.extension() != ".cstore") {
                continue;
            }
            
            std::string filename = path.filename().string();
            // Expected format: col_L<level>_<id>.cstore
            int level = -1;
            uint64_t file_id = 0;
            if (sscanf(filename.c_str(), "col_L%d_%llu.cstore", &level, &file_id) != 2) {
                LOG_WARN("Could not parse level and ID from columnar file: {}", filename);
                continue;
            }

            if (level < 0 || level >= levels_.size()) {
                LOG_ERROR("Invalid level {} parsed from file {}. Skipping.", level, filename);
                continue;
            }

            max_file_id_found = std::max(max_file_id_found, file_id);

            try {
                auto file_ptr = ColumnarFile::open(path.string(), file_id);
                if (!file_ptr) {
                    LOG_ERROR("Failed to open and read metadata from columnar file: {}", path.string());
                    continue;
                }

                auto metadata = std::make_shared<ColumnarFileMetadata>();
                metadata->filename = path.string();
                metadata->file_id = file_id;
                metadata->level = level;
                metadata->schema_version = file_ptr->getSchemaVersion();
                metadata->row_count = file_ptr->getTotalRowCount();
                metadata->size_bytes = fs::file_size(path);
                metadata->pk_bloom_filter = std::make_unique<BloomFilter>(*file_ptr->getPrimaryKeyFilter());

                // Min/max keys and txn ranges would be read from the file footer if they were stored there.
                // For now, we leave them default. This is a detail for Phase 3.

                levels_[level].push_back(std::move(metadata));
                LOG_TRACE("Loaded metadata for L{} columnar file: {}", level, filename);

            } catch (const std::exception& e) {
                LOG_ERROR("Exception while opening/processing columnar file {}: {}", path.string(), e.what());
            }
        }

        // Sort the levels correctly, just like in LSMTree
        for (int i = 0; i < levels_.size(); ++i) {
            if (i == 0) { // Level 0 is sorted by file ID (newest first)
                std::sort(levels_[i].begin(), levels_[i].end(),
                    [](const auto& a, const auto& b) { return a->file_id > b->file_id; });
            } else { // Other levels are sorted by key range (min_primary_key)
                std::sort(levels_[i].begin(), levels_[i].end(),
                    [](const auto& a, const auto& b) { return a->min_primary_key < b->min_primary_key; });
            }
        }

    } catch (const fs::filesystem_error& e) {
        LOG_ERROR("Filesystem error while loading columnar file metadata from {}: {}", data_dir_, e.what());
    }

    next_columnar_file_id_.store(max_file_id_found + 1);
    LOG_INFO("Finished loading columnar metadata for store '{}'. {} levels populated. Next file ID will be {}.", 
             store_path_, levels_.size(), next_columnar_file_id_.load());
}

// --- Placeholder methods for logic cloned from LSMTree but not yet fully adapted ---
void ColumnarStore::schedulerThreadLoop() {
    // Logic is identical to LSMTree, calls checkAndScheduleCompactions()
}

void ColumnarStore::checkAndScheduleCompactions() {
    if (!compaction_manager_) {
        return;
    }
    
    // The selection logic is now implemented, so this call will work.
    auto job_opt = selectCompactionCandidate();
    
    if (job_opt) {
        LOG_INFO("[Columnar Scheduler] Scheduling compaction job for L{}.", job_opt->level_L);
        compaction_manager_->scheduleJob(std::move(*job_opt));
    }
}

// Calculates the combined key range for a set of files.
/* static */ std::pair<std::string, std::string> ColumnarStore::compute_key_range(
    const std::vector<std::shared_ptr<ColumnarFileMetadata>>& files) 
{
    if (files.empty()) {
        return {"", ""};
    }

    std::string global_min_key = "";
    std::string global_max_key = "";

    // Find the first non-empty min/max to initialize the globals.
    // This is more robust than assuming the first file has valid keys.
    for (const auto& file : files) {
        if (!file->min_primary_key.empty()) {
            global_min_key = file->min_primary_key;
            break;
        }
    }
    for (const auto& file : files) {
        if (!file->max_primary_key.empty()) {
            global_max_key = file->max_primary_key;
            break;
        }
    }

    // Iterate through all files to find the absolute min and max keys.
    for (const auto& file : files) {
        if (!file->min_primary_key.empty() && (global_min_key.empty() || file->min_primary_key < global_min_key)) {
            global_min_key = file->min_primary_key;
        }
        if (!file->max_primary_key.empty() && (global_max_key.empty() || file->max_primary_key > global_max_key)) {
            global_max_key = file->max_primary_key;
        }
    }
    return {global_min_key, global_max_key};
}

// Finds all files in a target level that overlap with a given key range.
std::vector<std::shared_ptr<ColumnarFileMetadata>> ColumnarStore::find_overlapping_files(
    int target_level,
    const std::string& min_key,
    const std::string& max_key
) {
    std::vector<std::shared_ptr<ColumnarFileMetadata>> overlapping_files;
    if (target_level < 0 || target_level >= levels_.size() || min_key.empty() || max_key.empty()) {
        return overlapping_files;
    }

    const auto& level_files = levels_[target_level];
    for (const auto& file : level_files) {
        // Check for overlap: !(file_max < range_min || file_min > range_max)
        bool no_overlap = (!file->max_primary_key.empty() && file->max_primary_key < min_key) ||
                          (!file->min_primary_key.empty() && file->min_primary_key > max_key);
        if (!no_overlap) {
            overlapping_files.push_back(file);
        }
    }
    return overlapping_files;
}


std::optional<ColumnarCompactionJob> ColumnarStore::selectCompactionCandidate() {
    std::lock_guard<std::mutex> lock(levels_metadata_mutex_);
    
    // 1. L0 -> L1 Compaction (Highest Priority)
    if (levels_[0].size() >= L0_COMPACTION_TRIGGER) {
        // --- MODIFIED --- Use non-nested name for local variables
        std::vector<std::shared_ptr<ColumnarFileMetadata>> l0_files = levels_[0];
        auto key_range = compute_key_range(l0_files);
        
        std::vector<std::shared_ptr<ColumnarFileMetadata>> l1_overlapping_files;
        if (levels_.size() > 1) {
            l1_overlapping_files = find_overlapping_files(1, key_range.first, key_range.second);
        }
        
        return ColumnarCompactionJob{0, std::move(l0_files), std::move(l1_overlapping_files)};
    }

    // 2. Size-Tiered Compaction for L1+
    for (int level_idx = 1; level_idx < levels_.size() - 1; ++level_idx) {
        uint64_t current_size = 0;
        for (const auto& file : levels_[level_idx]) {
            current_size += file->size_bytes;
        }

        // Target size for L1 is SSTABLE_TARGET_SIZE_BASE. For L2+, it's multiplied.
        uint64_t target_size = SSTABLE_TARGET_SIZE_BASE * std::pow(LEVEL_SIZE_MULTIPLIER, level_idx - 1);

        if (current_size > target_size) {
            LOG_INFO("[Columnar Compaction] L{} size trigger reached ({} > {}). Selecting a candidate file.",
                     level_idx, current_size, target_size);

            // Simple strategy: pick the oldest file (lowest ID) to compact.
            // A scoring system (like LSM-Tree) would be more advanced.
            auto oldest_file_it = std::min_element(levels_[level_idx].begin(), levels_[level_idx].end(),
                [](const auto& a, const auto& b) {
                    return a->file_id < b->file_id;
                });
            
            if (oldest_file_it != levels_[level_idx].end()) {
                std::vector<std::shared_ptr<ColumnarFileMetadata>> files_to_compact = {*oldest_file_it};
                auto key_range = compute_key_range(files_to_compact);
                
                auto overlapping_next_level = find_overlapping_files(
                    level_idx + 1, key_range.first, key_range.second);

                return ColumnarCompactionJob{level_idx, std::move(files_to_compact), std::move(overlapping_next_level)};
            }
        }
    }

    return std::nullopt;
}


bool ColumnarStore::executeCompactionJob(
    int level_L,
    std::vector<std::shared_ptr<ColumnarFileMetadata>> files_L,
    std::vector<std::shared_ptr<ColumnarFileMetadata>> files_L_plus_1)
{
    LOG_INFO("[ColumnarCompaction L{}] Starting job. Input files: {} from L{}, {} from L+1.",
             level_L, files_L.size(), files_L_plus_1.size());

    // --- 1. Validation and Setup ---
    if (files_L.empty()) {
        LOG_INFO("[ColumnarCompaction L{}] Job has no source files from L{}. Nothing to do.", level_L, level_L);
        return true;
    }
    if (!storage_engine_ptr_ || !storage_engine_ptr_->getSharedCompactionPool()) {
        throw storage::StorageError(storage::ErrorCode::STORAGE_NOT_INITIALIZED,
            "Cannot run parallel columnar compaction: shared thread pool is not available.");
    }

    // --- 2. Split the Compaction Job into Parallelizable Sub-Tasks ---
    std::vector<SubCompactionTask> sub_tasks;
    auto all_input_files = files_L;
    all_input_files.insert(all_input_files.end(), files_L_plus_1.begin(), files_L_plus_1.end());

    if (level_L > 0 && !files_L_plus_1.empty()) {
        // For L1+ compactions, we can split the work based on the non-overlapping key ranges
        // of the target level files (files_L_plus_1).
        std::vector<std::string> boundary_keys;
        for (const auto& file : files_L_plus_1) {
            boundary_keys.push_back(file->min_primary_key);
        }
        // No need to sort, as L1+ files are already sorted by min_key.

        std::string last_boundary = "";
        for (const auto& boundary : boundary_keys) {
            sub_tasks.push_back({last_boundary, boundary, {}});
            last_boundary = boundary;
        }
        sub_tasks.push_back({last_boundary, "", {}}); // Add the final range to +infinity

        // Assign input files to the sub-tasks they overlap with.
        for (auto& task : sub_tasks) {
            for (const auto& input_file : all_input_files) {
                bool no_overlap = !input_file->max_primary_key.empty() && input_file->max_primary_key < task.start_key ||
                                  !task.end_key.empty() && !input_file->min_primary_key.empty() && input_file->min_primary_key >= task.end_key;
                if (!no_overlap) {
                    task.input_files.push_back(input_file);
                }
            }
        }
    } else {
        // For L0->L1 compactions (or any case with no target files), we cannot easily split by key range.
        // We will process the entire compaction as a single sub-task.
        // An advanced implementation could sample keys to find split points even for L0.
        sub_tasks.push_back({"", "", all_input_files});
    }

    LOG_INFO("[ColumnarCompaction L{}] Job split into {} parallel sub-task(s).", level_L, sub_tasks.size());
    if (sub_tasks.empty()) {
        return true; // No work to be done.
    }

    // --- 3. Create a Coordinator and Dispatch Tasks ---
    auto coordinator = std::make_shared<ColumnarCompactionJobCoordinator>(
        this, level_L, level_L + 1, sub_tasks.size(), files_L, files_L_plus_1
    );
    
    // Schedule each sub-task on the shared thread pool.
    for (const auto& task_details : sub_tasks) {
        if(task_details.input_files.empty()) {
             // This can happen if a split range contains no overlapping input files.
             // We still need to account for it in the coordinator.
             if (coordinator->taskCompleted() == coordinator->total_tasks_) {
                if (coordinator->hasFailed()) coordinator->abortJob();
                else coordinator->finalizeJob();
             }
             continue;
        }

        auto sub_task_lambda = [this, coordinator, task_details]() {
            // Early exit if another sub-task in the same job has already failed.
            if (coordinator->hasFailed()) {
                LOG_WARN("[ColumnarSubCompaction] Skipping execution as job has already failed.");
                return;
            }

            try {
                // Execute the actual merge logic for this sub-task.
                auto new_files = executeSubCompactionTask(task_details);
                // Report the newly created files to the coordinator.
                coordinator->addSubTaskResult(std::move(new_files));
            } catch (const std::exception& e) {
                LOG_ERROR("[ColumnarSubCompaction] CRITICAL EXCEPTION in task for key range ['{}', '{}'): {}",
                          task_details.start_key, task_details.end_key, e.what());
                coordinator->reportTaskFailure();
            }

            // The last thread to finish is responsible for finalizing or aborting the job.
            if (coordinator->taskCompleted() == coordinator->total_tasks_) {
                if (coordinator->hasFailed()) {
                    coordinator->abortJob();
                } else {
                    coordinator->finalizeJob();
                }
            }
        };
        
        storage_engine_ptr_->getSharedCompactionPool()->schedule(
            std::move(sub_task_lambda), 
            threading::TaskPriority::NORMAL // Columnar compactions are generally normal priority
        );
    }
    
    return true; // The job has been successfully dispatched and will run asynchronously.
}

// --- NEW --- executeSubCompactionTask ---
std::vector<std::shared_ptr<ColumnarFileMetadata>> ColumnarStore::executeSubCompactionTask(
    const SubCompactionTask& task
) {
    LOG_INFO("[ColumnarSubCompaction] Starting task for key range ['{}', '{}'). Input files: {}.",
             task.start_key.empty() ? "-inf" : format_key_for_print(task.start_key),
             task.end_key.empty() ? "+inf" : format_key_for_print(task.end_key),
             task.input_files.size());
    
    // --- 1. Setup Readers and Min-Heap for K-Way Merge ---
    std::vector<std::unique_ptr<ColumnarReader>> readers;
    std::priority_queue<CompactionHeapEntry, std::vector<CompactionHeapEntry>, std::greater<CompactionHeapEntry>> min_heap;

    for (const auto& meta : task.input_files) {
        try {
            // Open a reader for each input file. ColumnarReader is a helper class defined
            // in the anonymous namespace of this file for iterating through a columnar file.
            auto file_ptr = ColumnarFile::open(meta->filename, meta->file_id);
            if (file_ptr && file_ptr->getTotalRowCount() > 0) {
                auto reader = std::make_unique<ColumnarReader>(std::move(file_ptr), encryption_is_active_, database_dek_);
                // Prime the heap with the first key from the reader if it has data.
                if (reader->hasMore()) {
                    min_heap.push({reader->peekKey(), readers.size()});
                    readers.push_back(std::move(reader));
                }
            }
        } catch (const std::exception& e) {
            LOG_ERROR("[ColumnarSubCompaction] Failed to open input file {} for compaction: {}. Skipping file.", 
                      meta->filename, e.what());
            // Continue with the other files; this one will be ignored.
        }
    }

    if (min_heap.empty()) {
        LOG_INFO("[ColumnarSubCompaction] No valid records found in any input files for this task. Task complete.");
        return {}; // No new files created.
    }

    // --- 2. Prepare for Writing Output Files ---
    auto schema_opt = getLatestStoreSchema();
    if (!schema_opt) {
        throw storage::StorageError(storage::ErrorCode::SCHEMA_VIOLATION, 
            "Cannot run sub-compaction for store '" + store_path_ + "', no schema available.");
    }
    
    std::vector<std::shared_ptr<ColumnarFileMetadata>> new_output_files_metadata;
    auto output_buffer = std::make_shared<ColumnarBuffer>(*schema_opt);
    std::string last_processed_key = "";

    // --- 3. Main Merge Loop ---
    while (!min_heap.empty()) {
        CompactionHeapEntry top = min_heap.top();
        min_heap.pop();

        // Stop processing if we have gone past the exclusive end key for this task.
        if (!task.end_key.empty() && top.primary_key >= task.end_key) {
            continue;
        }

        // Group all versions of the same key together for processing.
        if (top.primary_key == last_processed_key) {
            // We have already processed the latest version of this key. This entry is an
            // older, superseded version from a different input file. We can discard it.
        } else {
            // This is the first time we've seen this primary key. It is guaranteed to be the
            // latest version because the heap gives us the globally smallest key, and our
            // file-opening order doesn't matter for correctness here.
            last_processed_key = top.primary_key;

            // **Garbage Collection Logic:** Check the primary LSM store to see if this
            // record is still "live" (i.e., not deleted).
            if (!storage_engine_ptr_) {
                throw storage::StorageError(storage::ErrorCode::STORAGE_NOT_INITIALIZED, 
                    "Cannot run compaction GC: storage_engine_ptr_ is null.");
            }
            
            // Use the highest possible TxnId to read the absolute latest committed state.
            auto record_opt = storage_engine_ptr_->get(top.primary_key, std::numeric_limits<TxnId>::max());
            
            if (record_opt && !record_opt->deleted) {
                // The record is LIVE. We must keep its columnar data.
                // The reader at `top.reader_index` points to the row we need to copy.
                auto row_data_to_keep = readers[top.reader_index]->readCurrentRowData();
                
                // An empty row_data map indicates a tombstone in the columnar file. We only
                // write a new row if the source row was not a tombstone itself.
                if (!row_data_to_keep.empty()) {
                    output_buffer->add(top.primary_key, row_data_to_keep);

                    // Check if the output buffer is full and needs to be flushed to a new file.
                    if (output_buffer->isFull()) {
                        auto new_meta_opt = flushBuffersToSingleFile({output_buffer});
                        if (new_meta_opt) {
                            new_output_files_metadata.push_back(*new_meta_opt);
                        } else {
                            // A failed flush is a critical error for the task.
                            throw std::runtime_error("Failed to flush full buffer during sub-compaction.");
                        }
                        // Start a fresh buffer for the next records.
                        output_buffer = std::make_shared<ColumnarBuffer>(*schema_opt);
                    }
                }
            } else {
                // The record is DELETED or does NOT EXIST in the primary store.
                // We garbage collect it by simply not adding it to the output buffer.
                LOG_TRACE("[ColumnarSubCompaction GC] Pruning key '{}' as it is deleted or absent from primary store.", top.primary_key);
            }
        }

        // Advance the reader that provided the key we just processed.
        auto& reader = readers[top.reader_index];
        reader->advance();
        if (reader->hasMore()) {
            min_heap.push({reader->peekKey(), top.reader_index});
        }
    }

    // --- 4. Final Flush ---
    // After the loop, flush any remaining data in the final buffer.
    if (output_buffer->getRowCount() > 0) {
        auto final_meta_opt = flushBuffersToSingleFile({output_buffer});
        if (final_meta_opt) {
            new_output_files_metadata.push_back(*final_meta_opt);
        } else {
            throw std::runtime_error("Failed to flush final buffer during sub-compaction.");
        }
    }

    LOG_INFO("[ColumnarSubCompaction] Task for key range ['{}', '{}') finished successfully. Created {} new file(s).",
             task.start_key.empty() ? "-inf" : format_key_for_print(task.start_key),
             task.end_key.empty() ? "+inf" : format_key_for_print(task.end_key),
             new_output_files_metadata.size());

    return new_output_files_metadata;
}

// --- NEW --- atomicallyUpdateLevels ---
void ColumnarStore::atomicallyUpdateLevels(
    const std::vector<std::shared_ptr<ColumnarFileMetadata>>& files_to_delete_L,
    const std::vector<std::shared_ptr<ColumnarFileMetadata>>& files_to_delete_L_plus_1,
    const std::vector<std::shared_ptr<ColumnarFileMetadata>>& files_to_add)
{
    std::lock_guard lock(levels_metadata_mutex_);
    
    // Create sets of file IDs for efficient lookup
    std::unordered_set<uint64_t> ids_to_delete_L, ids_to_delete_L_plus_1;
    for(const auto& meta : files_to_delete_L) ids_to_delete_L.insert(meta->file_id);
    for(const auto& meta : files_to_delete_L_plus_1) ids_to_delete_L_plus_1.insert(meta->file_id);

    // Remove old files from metadata lists
    int level_L = files_to_delete_L.empty() ? -1 : files_to_delete_L[0]->level;
    if(level_L != -1) {
        auto& level_vec = levels_[level_L];
        level_vec.erase(std::remove_if(level_vec.begin(), level_vec.end(),
            [&](const auto& meta){ return ids_to_delete_L.count(meta->file_id); }), level_vec.end());
    }

    int level_L_plus_1 = files_to_delete_L_plus_1.empty() ? -1 : files_to_delete_L_plus_1[0]->level;
    if(level_L_plus_1 != -1) {
        auto& level_vec = levels_[level_L_plus_1];
        level_vec.erase(std::remove_if(level_vec.begin(), level_vec.end(),
            [&](const auto& meta){ return ids_to_delete_L_plus_1.count(meta->file_id); }), level_vec.end());
    }

    // Add new files to the target level
    if (!files_to_add.empty()) {
        int target_level = files_to_add[0]->level;
        levels_[target_level].insert(levels_[target_level].end(), files_to_add.begin(), files_to_add.end());
        std::sort(levels_[target_level].begin(), levels_[target_level].end(),
            [](const auto& a, const auto& b) { return a->min_primary_key < b->min_primary_key; });
    }
}

// --- NEW --- notifyScheduler ---
void ColumnarStore::notifyScheduler() {
    scheduler_cv_.notify_one();
}

// --- NEW --- ingestFile ---
void ColumnarStore::ingestFile(const std::string& file_path, bool move_file) {
    LOG_INFO("Ingesting columnar file '{}' into store '{}'. Move: {}", file_path, store_path_, move_file);

    // 1. Validate the file by trying to open it and read its metadata.
    auto temp_file_ptr = ColumnarFile::open(file_path, 0); // Use dummy ID for validation
    if (!temp_file_ptr) {
        throw std::runtime_error("Validation failed: External file is not a valid or readable .cstore file.");
    }

    // 2. Assign a new ID and determine its final path.
    uint64_t new_file_id = next_columnar_file_id_.fetch_add(1);
    int target_level = 0; // All external ingests go to L0.
    std::string final_path = generateFilePath(target_level, new_file_id);

    // 3. Create the final metadata object.
    auto metadata = std::make_shared<ColumnarFileMetadata>();
    metadata->filename = final_path;
    metadata->file_id = new_file_id;
    metadata->level = target_level;
    metadata->schema_version = temp_file_ptr->getSchemaVersion();
    metadata->row_count = temp_file_ptr->getTotalRowCount();
    metadata->pk_bloom_filter = std::make_unique<BloomFilter>(*temp_file_ptr->getPrimaryKeyFilter());
    
    const auto& pks = temp_file_ptr->getPrimaryKeys();
    if (!pks.empty()) {
        metadata->min_primary_key = pks.front();
        metadata->max_primary_key = pks.back();
    }
    
    // 4. Move or copy the file into place.
    try {
        if (move_file) {
            fs::rename(file_path, final_path);
        } else {
            fs::copy_file(file_path, final_path);
        }
        metadata->size_bytes = fs::file_size(final_path);
    } catch (const fs::filesystem_error& e) {
        throw std::runtime_error("Filesystem error during file ingestion: " + std::string(e.what()));
    }

    // 5. Atomically add the new file's metadata to the store's levels.
    atomicallyUpdateLevels({}, {}, {metadata});

    // 6. Notify the scheduler that a new file has appeared.
    notifyScheduler();
    LOG_INFO("Successfully ingested file '{}' as L0 file '{}'.", file_path, final_path);
}

std::vector<Record> ColumnarStore::executeAggregationQuery(
    const AggregationPlan& plan,
    const std::vector<FilterCondition>& filters)
{
    auto schema_opt = getLatestStoreSchema();
    if (!schema_opt) return {};
    const auto& schema = *schema_opt;

    // 1. Identify all columns needed for grouping, filtering, and aggregating.
    std::vector<uint32_t> columns_to_read;
    std::unordered_map<std::string, uint32_t> field_to_id;
    for (const auto& field_name : plan.group_by_fields) {
        auto col_opt = schema.getColumn(field_name);
        if (!col_opt) throw std::runtime_error("Group by field not in schema: " + field_name);
        columns_to_read.push_back(col_opt->column_id);
        field_to_id[field_name] = col_opt->column_id;
    }
    for (const auto& agg : plan.aggregations) {
        auto col_opt = schema.getColumn(agg.field);
        if (!col_opt) throw std::runtime_error("Aggregate field not in schema: " + agg.field);
        columns_to_read.push_back(col_opt->column_id);
        field_to_id[agg.field] = col_opt->column_id;
    }
    for (const auto& filter : filters) {
        auto col_opt = schema.getColumn(filter.field);
        if (!col_opt) continue; 
        columns_to_read.push_back(col_opt->column_id);
        
        if (field_to_id.find(filter.field) == field_to_id.end()) {
            field_to_id[filter.field] = col_opt->column_id;
        }
        // ---------------------------------------------
    }

        // Remove duplicates to avoid reading the same column multiple times
    std::sort(columns_to_read.begin(), columns_to_read.end());
    columns_to_read.erase(
        std::unique(columns_to_read.begin(), columns_to_read.end()),
        columns_to_read.end()
    );

    LOG_TRACE("Aggregation will read {} unique columns.", columns_to_read.size());

    // --- The hash map to hold the aggregate results ---
    // Key is a concatenated string of group-by values.
    std::unordered_map<std::string, std::vector<AggregateState>> result_map;
    
    // --- 2. Scan files with pruning ---
    // ... (File pruning logic based on filters, same as in executeQuery) ...
    std::vector<std::shared_ptr<ColumnarFileMetadata>> all_files_metadata;
    {
        std::lock_guard lock(levels_metadata_mutex_);
        for (const auto& level : levels_) {
            all_files_metadata.insert(all_files_metadata.end(), level.begin(), level.end());
        }
    }

    std::vector<std::shared_ptr<ColumnarFileMetadata>> files_to_scan;
    for (const auto& file_meta : all_files_metadata) {
        bool file_is_relevant = true;
        for (const auto& filter : filters) {
            auto col_opt = schema.getColumn(filter.field);
            if (!col_opt) {
                // Should not happen if query is validated against schema first
                file_is_relevant = false;
                break;
            }

            auto stats_it = file_meta->column_stats.find(col_opt->column_id);
            if (stats_it == file_meta->column_stats.end()) {
                continue; // No stats for this column, must scan the file to be safe.
            }
            
            if (!does_range_overlap(stats_it->second.first, stats_it->second.second, filter)) {
                file_is_relevant = false;
                LOG_TRACE("Pruning file {} for aggregation based on stats for column '{}'.", file_meta->filename, filter.field);
                break;
            }
        }
        if (file_is_relevant) {
            files_to_scan.push_back(file_meta);
        }
    }
    LOG_INFO("Aggregation query plan: Pruned to {} files from a total of {}.", files_to_scan.size(), all_files_metadata.size());

    // std::vector<std::shared_ptr<ColumnarFileMetadata>> files_to_scan; // populated by pruning
    
    for (const auto& file_meta : files_to_scan) {
        auto file = ColumnarFile::open(file_meta->filename, file_meta->file_id);
        if (!file) continue;

        // Compute relevant row group indices for this file
        std::vector<int> relevant_row_group_indices;
        const auto& row_groups_in_file = file->getRowGroups();
        for (int i = 0; i < row_groups_in_file.size(); ++i) {
            const auto& rg_meta = row_groups_in_file[i];
            bool group_is_relevant = true;
            for (const auto& filter : filters) {
                auto col_opt = schema.getColumn(filter.field);
                if (!col_opt) { group_is_relevant = false; break; }
                auto chunk_it = std::find_if(rg_meta.column_chunks.begin(), rg_meta.column_chunks.end(),
                    [&](const ColumnChunkMetadata& c) { return c.column_id == col_opt->column_id; });
                if (chunk_it != rg_meta.column_chunks.end()) {
                    if (!does_range_overlap(chunk_it->min_value, chunk_it->max_value, filter)) {
                        group_is_relevant = false;
                        break;
                    }
                }
            }
            if (group_is_relevant) {
                relevant_row_group_indices.push_back(i);
            }
        }

        auto column_data = file->readColumns(columns_to_read, relevant_row_group_indices, encryption_is_active_, database_dek_);
        
        // a. Apply filters to get a selection vector
        std::vector<bool> selection_vector(file->getTotalRowCount(), true);
        for (const auto& filter : filters) {
            apply_vectorized_filter(column_data.at(field_to_id[filter.field]), filter, selection_vector);
        }

        // b. Perform vectorized aggregation
        for (size_t i = 0; i < file->getTotalRowCount(); ++i) {
            if (!selection_vector[i]) continue;

            std::string group_key;
            if (plan.group_by_fields.empty()) {
                group_key = "global_aggregation_key"; // A single, constant key for aggregations without a GROUP BY
            } else {
                std::ostringstream key_builder;
                for (size_t g_idx = 0; g_idx < plan.group_by_fields.size(); ++g_idx) {
                    const auto& field_name = plan.group_by_fields[g_idx];
                    const auto& col_vec = column_data.at(field_to_id.at(field_name));
                    const auto& val = col_vec[i];
                    
                    // Append a type-safe string representation of the value
                    std::visit([&key_builder](auto&& arg) {
                        using T = std::decay_t<decltype(arg)>;
                        if constexpr (std::is_same_v<T, std::monostate>) {
                            key_builder << "NULL";
                        } else if constexpr (std::is_same_v<T, bool>) {
                            key_builder << (arg ? "true" : "false");
                        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                            // Convert binary to hex string or use a placeholder
                            key_builder << "BINARY";
                        } else {
                            key_builder << arg; // Works for int64, double, string
                        }
                    }, val);

                    if (g_idx < plan.group_by_fields.size() - 1) {
                        key_builder << "|"; // Use a separator for compound keys
                    }
                }
                group_key = key_builder.str();
            }
                    
            
            if (result_map.find(group_key) == result_map.end()) {
                result_map[group_key].resize(plan.aggregations.size());
            }
            
           
            for (size_t agg_idx = 0; agg_idx < plan.aggregations.size(); ++agg_idx) {
                const auto& agg_spec = plan.aggregations[agg_idx];
                const auto& agg_col_vec = column_data.at(field_to_id.at(agg_spec.field));
                const auto& val = agg_col_vec[i];
                
                AggregateState& state = result_map[group_key][agg_idx];
                
                if (agg_spec.op == AggregationType::COUNT) {
                    // Only increment the count if the value for the specified field is not null.
                    if (!std::holds_alternative<std::monostate>(val)) {
                        state.count++;
                    }
                } else if (!std::holds_alternative<std::monostate>(val)) {
                    // This 'else if' block handles SUM, AVG, MIN, MAX, which all ignore nulls.
                    
                    // For AVG, we still need to count the non-null items.
                    if (agg_spec.op == AggregationType::AVG) {
                        state.count++;
                    }

                    // Apply SUM, which is also used by AVG
                    if (agg_spec.op == AggregationType::SUM || agg_spec.op == AggregationType::AVG) {
                        if (std::holds_alternative<int64_t>(val)) {
                            state.sum += static_cast<double>(std::get<int64_t>(val));
                        } else if (std::holds_alternative<double>(val)) {
                            state.sum += std::get<double>(val);
                        }
                    }

                    // Apply MIN
                    if (agg_spec.op == AggregationType::MIN) {
                        if (std::holds_alternative<std::monostate>(state.min) || val < state.min) {
                            state.min = val;
                        }
                    }
                    
                    // Apply MAX
                    if (agg_spec.op == AggregationType::MAX) {
                        if (std::holds_alternative<std::monostate>(state.max) || val > state.max) {
                            state.max = val;
                        }
                    }
                }
            }
        }
    }

    // --- 3. Reconstruct final result set ---
    std::vector<Record> final_results;
    for (const auto& [group_key, states] : result_map) {
        json result_json;
        
        // If this was a GROUP BY query, parse the group_key to reconstruct the grouping fields.
        if (!plan.group_by_fields.empty()) {
            std::stringstream ss(group_key);
            std::string segment;
            size_t field_idx = 0;
            
            while (std::getline(ss, segment, '|') && field_idx < plan.group_by_fields.size()) {
                const auto& field_name = plan.group_by_fields[field_idx];
                const auto& col_def_opt = schema.getColumn(field_name);

                if (segment == "NULL" || !col_def_opt) {
                    result_json[field_name] = nullptr;
                } else {
                    // Correctly convert the string segment back to its original type for the JSON output.
                    try {
                        switch (col_def_opt->type) {
                            case ColumnType::INT64:
                                result_json[field_name] = std::stoll(segment);
                                break;
                            case ColumnType::DOUBLE:
                                result_json[field_name] = std::stod(segment);
                                break;
                            case ColumnType::BOOLEAN:
                                result_json[field_name] = (segment == "true" || segment == "1");
                                break;
                            default: // STRING, BINARY, etc.
                                result_json[field_name] = segment;
                                break;
                        }
                    } catch (const std::exception&) {
                        // Fallback to string if conversion fails (should be rare)
                        result_json[field_name] = segment;
                    }
                }
                field_idx++;
            }
        }

        for (size_t i = 0; i < plan.aggregations.size(); ++i) {
            const auto& agg_spec = plan.aggregations[i];
            const auto& state = states[i];

            switch (agg_spec.op) {
                case AggregationType::SUM:
                    result_json[agg_spec.result_field_name] = state.sum;
                    break;
                case AggregationType::COUNT:
                    result_json[agg_spec.result_field_name] = state.count;
                    break;
                case AggregationType::AVG:
                    // Avoid division by zero
                    if (state.count > 0) {
                        result_json[agg_spec.result_field_name] = state.sum / state.count;
                    } else {
                        result_json[agg_spec.result_field_name] = nullptr; // Or 0.0, depending on desired SQL behavior
                    }
                    break;
                case AggregationType::MIN:
                    // The value_to_json helper will handle converting the ValueType variant.
                    result_json[agg_spec.result_field_name] = value_to_json(state.min);
                    break;
                case AggregationType::MAX:
                    result_json[agg_spec.result_field_name] = value_to_json(state.max);
                    break;
            }
        }
        
        Record rec;
        rec.key = "aggregate/" + group_key; // A synthetic primary key for the result row
        rec.value = result_json.dump();
        final_results.push_back(rec);
    }
    return final_results;
}

std::vector<Record> ColumnarStore::executeQuery(
    const std::vector<FilterCondition>& filters,
    const std::optional<AggregationPlan>& aggregation_plan,
    size_t limit)
{
    if (aggregation_plan.has_value() && !aggregation_plan->aggregations.empty()) {
        // Aggregation logic needs a similar refactoring but is separate.
        LOG_INFO("ColumnarStore: Routing to AGGREGATION query for store '{}'.", store_path_);
        return executeAggregationQuery(*aggregation_plan, filters);
    }

    LOG_INFO("ColumnarStore: Executing FILTER query for store '{}' with {} filters and limit {}.", store_path_, filters.size(), limit);

    // --- Phase 1 & 2: Prune files using all available statistics ---
    std::vector<std::shared_ptr<ColumnarFileMetadata>> files_to_scan;
    {
        std::lock_guard lock(levels_metadata_mutex_);
        for (const auto& level : levels_) {
            for (const auto& file_meta : level) {
                // Get the specific schema for this file to interpret its metadata and data.
                auto file_schema_opt = getStoreSchema(file_meta->schema_version);
                if (!file_schema_opt) {
                    LOG_WARN("Could not find schema version {} for file {}. Skipping file.",
                             file_meta->schema_version, file_meta->filename);
                    continue;
                }
                const auto& file_schema = *file_schema_opt;
                
                bool file_is_relevant = true;
                for (const auto& filter : filters) {
                    if (filter.field == "_id") {
                        // PRUNING FEATURE 1: Primary Key Pruning (Bloom + Range)
                        if (file_meta->pk_bloom_filter && filter.op == FilterOperator::EQUAL &&
                            std::holds_alternative<ValueType>(filter.value) &&
                            std::holds_alternative<std::string>(std::get<ValueType>(filter.value))) {
                            
                            const std::string& pk_to_find = std::get<std::string>(std::get<ValueType>(filter.value));
                            if (!file_meta->pk_bloom_filter->mightContain(pk_to_find)) {
                                file_is_relevant = false;
                                LOG_TRACE("Pruning file {} by PK Bloom Filter for key '{}'.", file_meta->filename, pk_to_find);
                                break; 
                            }
                        }
                        if (!does_pk_range_overlap(file_meta->min_primary_key, file_meta->max_primary_key, filter)) {
                            file_is_relevant = false;
                            LOG_TRACE("Pruning file {} by PK range.", file_meta->filename);
                            break;
                        }
                    } else { 
                        // PRUNING FEATURE 2: Column Statistics Pruning
                        auto col_opt = file_schema.getColumn(filter.field);
                        if (!col_opt) {
                            file_is_relevant = false; break; 
                        }
                        auto stats_it = file_meta->column_stats.find(col_opt->column_id);
                        if (stats_it != file_meta->column_stats.end()) {
                            if (!does_range_overlap(stats_it->second.first, stats_it->second.second, filter)) {
                                file_is_relevant = false;
                                LOG_TRACE("Pruning file {} by column stats for '{}'.", file_meta->filename, filter.field);
                                break;
                            }
                        }
                    }
                } // end for(filters)
                
                if (file_is_relevant) {
                    files_to_scan.push_back(file_meta);
                }
            } // end for(file_meta)
        } // end for(levels_)
    }
    LOG_INFO("Query plan: Pruned to {} files to scan.", files_to_scan.size());

    // --- Phase 3: Iterate over pruned files and row groups, apply filters, and collect results ---
    std::set<std::string> passing_primary_keys;
    const size_t expected_pk_segments = countPathSegments(this->store_path_) + 1;
    
    for (const auto& file_meta : files_to_scan) {
        if (limit > 0 && passing_primary_keys.size() >= limit) {
            break; // Stop processing more files if we've already hit the limit.
        }

        try {
            // Get the specific schema for THIS file to interpret its data.
            auto file_schema_opt = getStoreSchema(file_meta->schema_version);
            if (!file_schema_opt) {
                LOG_WARN("Could not find schema version {} for file {}. Skipping file.",
                         file_meta->schema_version, file_meta->filename);
                continue;
            }
            const auto& file_schema = *file_schema_opt;

            // Identify which columns the iterator needs to read for this file's schema.
            std::vector<uint32_t> columns_to_read_for_this_file;
            std::unordered_set<uint32_t> columns_to_read_set;
            for (const auto& filter : filters) {
                if (filter.field == "_id") continue; // PK is not a column
                auto col_opt = file_schema.getColumn(filter.field);
                if (col_opt) {
                    columns_to_read_set.insert(col_opt->column_id);
                }
            }
            columns_to_read_for_this_file.assign(columns_to_read_set.begin(), columns_to_read_set.end());

            // Open the physical file.
            auto file = ColumnarFile::open(file_meta->filename, file_meta->file_id);
            if (!file || file->getTotalRowCount() == 0) continue;

            // Create an iterator that will read the necessary columns from the file on demand.
            auto iter = std::make_unique<ColumnarFileIterator>(std::move(file), columns_to_read_for_this_file);

            while (iter->hasNext()) {
                if (limit > 0 && passing_primary_keys.size() >= limit) {
                    goto end_iteration; // Use goto to break out of nested loops efficiently.
                }
                
                auto [primary_key, row_data] = iter->next();
                
                // **Filter 1: Hierarchical Path Check**
                // Ensure this key is a direct child of the store, not from a nested sub-store.
                if (countPathSegments(primary_key) != expected_pk_segments) {
                    continue; 
                }
                
                // **Filter 2: Apply Column Value Filters**
                bool row_passes_all_filters = true;
                for (const auto& filter : filters) {
                    if (filter.field == "_id") continue; // PK filter was already used for file pruning.

                    auto col_opt = file_schema.getColumn(filter.field);
                    if (!col_opt) { // Should not happen if file pruning is correct
                        row_passes_all_filters = false; 
                        break;
                    }
                    
                    uint32_t col_id = col_opt->column_id;
                    if (row_data.count(col_id)) {
                        if (!evaluateFilter(row_data.at(col_id), filter)) {
                            row_passes_all_filters = false;
                            break;
                        }
                    } else {
                        // The row doesn't have the required field for the filter, so it cannot match.
                        row_passes_all_filters = false;
                        break;
                    }
                }
                
                if (row_passes_all_filters) {
                    passing_primary_keys.insert(primary_key);
                }
            } // end while(iter->hasNext())

        } catch (const std::exception& e) {
            LOG_ERROR("Error processing columnar file {}: {}", file_meta->filename, e.what());
        }
    } 
end_iteration:;

    LOG_INFO("Columnar scan found {} candidate primary keys after filtering.", passing_primary_keys.size());

    // --- Phase 4: Reconstruct Final Records by Fetching from Primary LSM Store ---
    std::vector<Record> final_results;
    if (!storage_engine_ptr_) {
        LOG_ERROR("Storage engine pointer is null, cannot reconstruct records.");
        return final_results;
    }

    TxnId latest_txn_id = std::numeric_limits<TxnId>::max();
    for (const auto& pk : passing_primary_keys) {
        // The limit check is implicitly handled because we stop filling passing_primary_keys once the limit is hit.
        auto record_opt = storage_engine_ptr_->get(pk, latest_txn_id);
        if (record_opt && !record_opt->deleted) {
            final_results.push_back(*record_opt);
        }
    }
    
    LOG_INFO("Columnar query for store '{}' finished, returning {} reconstructed records.", store_path_, final_results.size());
    return final_results;
}

ColumnarStoreStats ColumnarStore::getStats() const {
    ColumnarStoreStats stats;

    // 1. Get stats from the lock-free ingestion queue
    if (ingestion_queue_) {
        stats.ingestion_queue_depth = ingestion_queue_->getApproxSize();
    }

    // 2. Get stats from the sharded active write buffers
    stats.active_buffer_count = NUM_WRITE_BUFFER_SHARDS;
    for (size_t i = 0; i < NUM_WRITE_BUFFER_SHARDS; ++i) {
        // We must lock each shard's mutex to safely read its buffer's state.
        std::lock_guard<std::mutex> lock(*active_buffer_mutexes_[i]);
        if (active_write_buffers_[i]) {
            stats.total_rows_in_active_buffers += active_write_buffers_[i]->getRowCount();
            // This is an estimation; a more accurate size could be tracked in ColumnarBuffer.
            stats.total_bytes_in_active_buffers += active_write_buffers_[i]->getEstimatedSizeBytes();
        }
    }

    // 3. Get stats from the immutable buffer queue
    {
        std::shared_lock lock(buffer_mutex_);
        stats.immutable_buffer_count = immutable_buffers_.size();
        for (const auto& buffer : immutable_buffers_) {
            stats.total_rows_in_immutable_buffers += buffer->getRowCount();
            stats.total_bytes_in_immutable_buffers += buffer->getEstimatedSizeBytes();
        }
    }

    // 4. Get file counts per level from the on-disk metadata tracker
    {
        std::lock_guard lock(levels_metadata_mutex_);
        stats.files_per_level.reserve(levels_.size());
        for (const auto& level_vec : levels_) {
            stats.files_per_level.push_back(level_vec.size());
        }
    }
    
    return stats;
}


} // namespace columnar
} // namespace engine