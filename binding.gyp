{
  "targets": [
    {
      "target_name": "indinis",
      "cflags!": [ "-fno-exceptions" ],
      "cflags_cc!": [ "-fno-exceptions" ],
      "sources": [
        "src/napi_binding.cpp",
        "src/napi_globals.cpp",
        "src/napi_type_conversions.cpp",
        "src/napi_indinis_wrapper.cpp",
        "src/napi_transaction_wrapper.cpp",
        "src/indinis_impl.cpp", 
        "src/storage_engine.cpp",
        "src/lsm_tree.cpp",
        "src/lsm/arena.cpp",
        "src/lsm/write_buffer_manager.cpp",
        "src/lsm/memtable_tuner.cpp",
        "src/lsm/skiplist.cpp",
        "src/index_manager.cpp",
        "src/btree.cpp",
        "src/buffer_pool_manager.cpp",
        "src/disk_manager.cpp",
        "src/types.cpp",
        #"src/wal_manager.cpp", =>>deprecated for sharding   
        "src/wal/wal_segment.cpp",  
        "src/wal/segment_manager.cpp",
        "src/wal_record.cpp",
        "src/wal/wal_shard.cpp",
        "src/wal/sharded_wal_manager.cpp",
        "src/wal/group_commit.cpp",
        "src/wal/wal_metrics.cpp",
        "src/compression_utils.cpp",
        "src/encryption_library.cpp"  ,
        "src/storage_error/storage_error.cpp",
        "src/storage_error/error_context.cpp",
        "src/storage_error/error_utils.cpp",
        "src/columnar/store_schema_manager.cpp",
        "src/columnar/columnar_store.cpp",
        "src/columnar/columnar_buffer.cpp",
        "src/columnar/columnar_file.cpp",
        "src/columnar/query_router.cpp",
        "src/columnar/columnar_file_iterator.cpp",
        "src/lsm/arena.cpp",
        "src/lsm/skiplist.cpp",
        "src/lsm/memtable_tuner.cpp",
        "src/lsm/write_buffer_manager.cpp",
        "src/lsm/partitioned_memtable.cpp",
        "src/lsm/vector_memtable.cpp",
        "src/lsm/hash_memtable.cpp",
        "src/lsm/compressed_memtable.cpp",
        "src/lsm/prefix_hash_memtable.cpp",
        "src/lsm/version.cpp",
        "src/lsm/rcu_memtable.cpp",
        "src/lsm/rcu_reclamation_manager.cpp",
        "src/lsm/rcu_memtable_adapter.cpp",
        "src/lsm/compaction_manager.cpp",
        "src/threading/system_monitor.cpp",
        "src/threading/thread_pool.cpp",
        "src/threading/adaptive_thread_pool_manager.cpp",
        "src/lsm/leveled_compaction_strategy.cpp",
        "src/lsm/universal_compaction_strategy.cpp",
        "src/serialization_utils.cpp",
        "src/lsm/fifo_compaction_strategy.cpp",
        "src/lsm/hybrid_compaction_strategy.cpp",
        "src/lsm/compaction_coordinator.cpp",
        "src/threading/rate_limiter.cpp", 
        "src/lsm/sstable_reader.cpp",
        "src/lsm/sstable_builder.cpp",
        "src/napi_sstable_builder_wrapper.cpp",
        "src/napi_workers.cpp",
        "src/columnar/columnar_compaction_coordinator.cpp",
        "src/lsm/liveness_oracle.cpp",
        "src/napi_batch_commit_worker.cpp",

         # --- ZLIB SOURCE FILES (crc32/compression)---
        "src/vendor/zlib/adler32.c",
        "src/vendor/zlib/compress.c",
        "src/vendor/zlib/crc32.c",
        "src/vendor/zlib/deflate.c",
        "src/vendor/zlib/infback.c",
        "src/vendor/zlib/inffast.c",
        "src/vendor/zlib/inflate.c",
        "src/vendor/zlib/inftrees.c",
        "src/vendor/zlib/trees.c",
        "src/vendor/zlib/uncompr.c",
        "src/vendor/zlib/zutil.c",

         # --- ZSTD SOURCE FILES  (common, compress and decompress)---
        "src/vendor/zstd/lib/common/debug.c",
        "src/vendor/zstd/lib/common/entropy_common.c",
        "src/vendor/zstd/lib/common/error_private.c",
        "src/vendor/zstd/lib/common/fse_decompress.c",
        "src/vendor/zstd/lib/common/pool.c",
        "src/vendor/zstd/lib/common/threading.c",
        "src/vendor/zstd/lib/common/xxhash.c",
        "src/vendor/zstd/lib/common/zstd_common.c",

        "src/vendor/zstd/lib/compress/fse_compress.c",
        "src/vendor/zstd/lib/compress/hist.c",
        "src/vendor/zstd/lib/compress/huf_compress.c",
        "src/vendor/zstd/lib/compress/zstd_compress.c",
        "src/vendor/zstd/lib/compress/zstd_compress_literals.c",
        "src/vendor/zstd/lib/compress/zstd_compress_sequences.c",
        "src/vendor/zstd/lib/compress/zstd_compress_superblock.c",
        "src/vendor/zstd/lib/compress/zstd_double_fast.c",
        "src/vendor/zstd/lib/compress/zstd_fast.c",
        "src/vendor/zstd/lib/compress/zstd_lazy.c",
        "src/vendor/zstd/lib/compress/zstd_ldm.c",
        "src/vendor/zstd/lib/compress/zstd_opt.c",
        "src/vendor/zstd/lib/compress/zstdmt_compress.c",
        "src/vendor/zstd/lib/compress/zstd_preSplit.c",

        "src/vendor/zstd/lib/decompress/huf_decompress.c",
        "src/vendor/zstd/lib/decompress/zstd_decompress.c",
        "src/vendor/zstd/lib/decompress/zstd_decompress_block.c",
        "src/vendor/zstd/lib/decompress/zstd_ddict.c",
        
        "src/vendor/zstd/lib/dictBuilder/cover.c",
        "src/vendor/zstd/lib/dictBuilder/divsufsort.c",
        "src/vendor/zstd/lib/dictBuilder/fastcover.c",
        "src/vendor/zstd/lib/dictBuilder/zdict.c",

        # --- LZ4 SOURCE FILES ---
        "src/vendor/lz4/lz4.c",
        "src/vendor/lz4/lz4hc.c",

      ],
      "include_dirs": [
        "<!@(node -p \"require('node-addon-api').include\")",
        "include",
        "src/vendor",
        "src/vendor/zlib",
        "src/vendor/zstd/lib",
        "src/vendor/zstd/lib/common",
        "src/vendor/zstd/lib/compress",
        "src/vendor/zstd/lib/decompress",
        "src/third_party",
        "C:/vcpkg/installed/x64-windows/include",
        "src/vendor/concurrentqueue",
        # Include directories for GoogleTest
        "src/vendor/googletest/googletest/include",
        "src/vendor/googletest/googlemock/include"
      ],
      "defines": [ "NAPI_DISABLE_CPP_EXCEPTIONS", "ZSTD_MULTITHREAD=1", "ZSTD_LEGACY_SUPPORT=0" ],
      "conditions": [
        ["OS=='win'", {
          "msvs_settings": {
            "VCCLCompilerTool": {
              "ExceptionHandling": 1,
              "AdditionalOptions": ["/std:c++17"],
              "PreprocessorDefinitions": [
                "WIN32_LEAN_AND_MEAN",
                "_CRT_SECURE_NO_WARNINGS",
                "NOMINMAX"
              ]
            }
          },
          "libraries": [
            "bcrypt.lib"
          ],
          "link_settings": {
            "library_dirs": [
              "C:/vcpkg/installed/x64-windows/lib"
            ]
          }
        }],
        ["OS=='mac'", {
          "xcode_settings": {
            "GCC_ENABLE_CPP_EXCEPTIONS": "YES",
            "CLANG_CXX_STANDARD": "c++17",
            "CLANG_CXX_LIBRARY": "libc++",
            "MACOSX_DEPLOYMENT_TARGET": "10.15",
            "GCC_PREPROCESSOR_DEFINITIONS": [
              "BYTE_ORDER=LITTLE_ENDIAN",
              "DEBUG_LOG_LEVEL=2"
            ]
          },
           "link_settings": {
            "libraries": [
              
              "-framework", "Security",
              "-framework", "CoreFoundation"
            ]
          }
        }],
        ["OS=='linux'", {
          "cflags_cc": [
            "-fexceptions",
            "-std=c++17"
          ],
          "defines": [
            "BYTE_ORDER=__BYTE_ORDER",
            "LITTLE_ENDIAN=__LITTLE_ENDIAN",
            "BIG_ENDIAN=__BIG_ENDIAN",
            "DEBUG_LOG_LEVEL=2"
          ],
          "libraries": [
            "-lssl", "-lcrypto"
          ]
        }]
      ]
    }
   
  ]
}
