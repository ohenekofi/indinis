//src/test/concurrency_stress.test.cpp
#include "gtest/gtest.h"
#include "../include/storage_engine.h"
#include "../include/types.h"
#include "../include/cache.h"
#include "../include/wal.h"
#include "../include/indinis.h" // <<< FIX: Add this include for the full Transaction definition

#include <filesystem>
#include <thread>
#include <vector>
#include <string>
#include <cstdlib>
#include <ctime>

namespace fs = std::filesystem;

class ConcurrencyStressTest : public ::testing::Test {
protected:
    std::string test_dir;
    std::unique_ptr<StorageEngine> db;

    void SetUp() override {
        test_dir = "./test_data_concurrency_stress_" + std::to_string(time(nullptr)) + "_" + std::to_string(rand());
        fs::create_directories(test_dir);

        ExtWALManagerConfig wal_config;
        // === FIX: Call .string() on the path object ===
        wal_config.wal_directory = (fs::path(test_dir) / "wal").string();
        // ===============================================

        cache::CacheConfig cache_config;

        db = std::make_unique<StorageEngine>(
            test_dir,
            std::chrono::seconds(5),
            wal_config,
            16 * 1024,
            CompressionType::NONE,
            0,
            std::nullopt,
            EncryptionScheme::NONE,
            0,
            false,
            cache_config,
            engine::lsm::MemTableType::SKIP_LIST
        );
    }

    void TearDown() override {
        db.reset(); 
        if (fs::exists(test_dir)) {
            std::error_code ec;
            fs::remove_all(test_dir, ec);
            if (ec) {
                std::cerr << "Warning: Could not clean up test directory " << test_dir << ": " << ec.message() << std::endl;
            }
        }
    }
};

TEST_F(ConcurrencyStressTest, HighContentionReadWriteDelete) {
    ASSERT_NE(db, nullptr);

    const int num_threads = 8;
    const int ops_per_thread = 500;
    const std::string key_prefix = "stress/key_";
    const int key_space_size = 100;

    auto worker_lambda = [&](int thread_id) {
        for (int i = 0; i < ops_per_thread; ++i) {
            std::string key = key_prefix + std::to_string((thread_id * ops_per_thread + i) % key_space_size);
            
            try {
                // This line now compiles because indinis.h provides the full Transaction definition
                auto txn = db->beginTransaction();
                double op_type = static_cast<double>(rand()) / RAND_MAX;
                
                if (op_type < 0.6) {
                    txn->put(key, "value_from_thread_" + std::to_string(thread_id) + "_op_" + std::to_string(i));
                } else if (op_type < 0.9) {
                    txn->get(key);
                } else {
                    txn->remove(key);
                }
                
                db->commitTransaction(txn->getId());

            } catch (const std::exception& e) {
                std::cerr << "Thread " << thread_id << " caught exception: " << e.what() << std::endl;
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker_lambda, i);
    }

    for (auto& t : threads) {
        t.join();
    }

    SUCCEED(); 
}