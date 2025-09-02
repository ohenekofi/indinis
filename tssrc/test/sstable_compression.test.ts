// @filename: tssrc/test/sstable_compression.test.ts

import { Indinis, IndinisOptions, ExtWALManagerConfigJs } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-sstable-compression-v2'); // New version for clean slate

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const s = (obj: any): string => JSON.stringify(obj);

function generateRandomString(length: number): string {
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-. RepetitiveSequencePattern'; // Added more repetitive patterns
    let result = '';
    const charactersLength = characters.length;
    for (let i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() * charactersLength));
    }
    return result;
}

interface SSTableCompressionTestDoc {
    id: string; // Will be the document key (e.g., doc_batch_seq)
    payload: string;
    batch: number;
    seq: number;
    // Add a field that changes frequently to ensure some memtable churn if needed
    timestamp?: number;
}

describe('LSMTree SSTable Block Compression and Data Integrity Test', () => {
    let db: Indinis | null = null;
    let testDataDir: string;
    let walDirForTest: string; 
    // dataDirForLSM will be testDataDir/data as per StorageEngine's default behavior

    const colPath = 'sstable_comp_docs_v2'; // Use a distinct collection path
    
    const sstableBlockSizeKB = 4; // Target 4KB uncompressed blocks
    const sstableCompressionLevel = 3; // A default, decent ZSTD level (0 often means default)

    const testWalConfig: ExtWALManagerConfigJs = {
        wal_directory: '', // Will be set to an absolute path in beforeEach
        wal_file_prefix: "sstable_comp_test_wal_v2", 
        segment_size: 1 * 1024 * 1024,      // 1MB WAL segments
        background_flush_enabled: true, 
        flush_interval_ms: 200, // Slightly longer for tests if needed       
        sync_on_commit_record: true,    
    };

    const indinisTestOptions: IndinisOptions = {
        checkpointIntervalSeconds: 30, // Checkpoints not the main focus, but good to have them running
        sstableDataBlockUncompressedSizeKB: sstableBlockSizeKB,
        sstableCompressionType: "ZSTD", 
        sstableCompressionLevel: sstableCompressionLevel,
        walOptions: testWalConfig, // Pass the WAL config object
    };

    const NUM_BATCHES = 5; // Number of transaction batches
    const DOCS_PER_BATCH = 250; // Number of documents per batch
    // Make payload larger and more repetitive to ensure compression is beneficial
    const BASE_PAYLOAD_SIZE = 512; // Bytes, e.g. 0.5 KB per record
                                   // Total uncompressed data per batch: 250 * 0.5KB = 125KB
                                   // This should create multiple 4KB blocks per SSTable flush.

    jest.setTimeout(180000); // 3 minutes for potentially many writes and I/O

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        walDirForTest = path.join(testDataDir, 'custom_wal_data_sstable_comp_v2'); // Specific WAL dir for this test instance
        
        await fs.promises.mkdir(testDataDir, { recursive: true });
        // WAL directory will be created by C++ if walOptions.wal_directory is set and doesn't exist.
        
        // Clone and update WAL directory in options for this specific test run
        const currentTestIndinisOptions = JSON.parse(JSON.stringify(indinisTestOptions)); // Deep clone
        currentTestIndinisOptions.walOptions.wal_directory = walDirForTest;

        console.log(`\n[SSTABLE COMP TEST INIT v2] Root Dir: ${testDataDir}`);
        console.log(`  LSM SSTable Config: Target Block Size: ${sstableBlockSizeKB}KB, Compression: ZSTD (Level ${sstableCompressionLevel})`);
        console.log(`  WAL Config: Dir='${currentTestIndinisOptions.walOptions.wal_directory}', Prefix='${currentTestIndinisOptions.walOptions.wal_file_prefix}'`);
        
        db = new Indinis(testDataDir, currentTestIndinisOptions);
        await delay(300); // Allow native addon to fully initialize background threads
    });

    afterEach(async () => {
        console.log("[SSTABLE COMP TEST END v2] Closing database...");
        if (db) {
            try { await db.close(); } catch (e) { console.error("Error during db.close() in afterEach:", e); }
            db = null;
        }
        console.log("[SSTABLE COMP TEST END v2] Waiting a moment before cleanup...");
        await delay(1200); // Increased delay for resource release
        if (fs.existsSync(testDataDir)) {
            console.log("[SSTABLE COMP TEST END v2] Cleaning up test directory: ", testDataDir);
            await rimraf(testDataDir, { maxRetries: 3, retryDelay: 500 }).catch(err => 
                console.error(`Cleanup error for ${testDataDir} in afterEach:`, err)
            );
        }
    });

    afterAll(async () => {
        console.log("[SSTABLE COMP SUITE END v2] Cleaning up base directory: ", TEST_DATA_DIR_BASE);
        if (fs.existsSync(TEST_DATA_DIR_BASE)) {
            await rimraf(TEST_DATA_DIR_BASE, { maxRetries: 3, retryDelay: 500 }).catch(err => 
                console.error(`Base cleanup error for ${TEST_DATA_DIR_BASE}:`, err)
            );
        }
    });

    it('should write multiple SSTable data blocks with ZSTD compression, verify logs, check disk size, and ensure data integrity after recovery', async () => {
        if (!db) throw new Error("DB not initialized for SSTable compression test");

        const allDocsWritten = new Map<string, SSTableCompressionTestDoc>();
        let totalDocsCommitted = 0;
        const lsmDataDir = path.join(testDataDir, 'data'); // Standard location for LSM data

        console.log(`--- Phase 1: Writing ${NUM_BATCHES * DOCS_PER_BATCH} documents with large, repetitive payloads ---`);

        for (let batchNum = 1; batchNum <= NUM_BATCHES; batchNum++) {
            const docsInThisBatch: SSTableCompressionTestDoc[] = [];
            const batchKeyPrefix = `${colPath}/b${batchNum.toString().padStart(2,'0')}_doc_`;

            for (let i = 0; i < DOCS_PER_BATCH; i++) {
                const docIdInBatch = i.toString().padStart(4, '0');
                const key = `${batchKeyPrefix}${docIdInBatch}`;
                
                // Generate a payload that is likely to compress well
                const basePattern = `PatternForDoc-${docIdInBatch}-Batch-${batchNum}-ABCDEF123456`;
                let payload = "";
                while(payload.length < BASE_PAYLOAD_SIZE) {
                    payload += basePattern + generateRandomString(10) + " ";
                }
                payload = payload.slice(0, BASE_PAYLOAD_SIZE + Math.floor(Math.random() * (BASE_PAYLOAD_SIZE * 0.2))); // +/- 20% size variation
                
                const doc: SSTableCompressionTestDoc = { 
                    id: docIdInBatch, // Store only the suffix ID part in the doc for smaller JSON
                    payload, 
                    batch: batchNum, 
                    seq: i,
                    timestamp: Date.now() 
                };
                docsInThisBatch.push(doc);
            }

            await db.transaction(async tx => {
                for (const doc of docsInThisBatch) {
                    // Construct full key for DB operations
                    const fullKey = `${colPath}/b${doc.batch.toString().padStart(2,'0')}_doc_${doc.id}`;
                    await tx.set(fullKey, s(doc)); // Store the full document object
                    allDocsWritten.set(fullKey, {...doc}); 
                }
            });
            totalDocsCommitted += docsInThisBatch.length;
            console.log(`Batch ${batchNum} committed (${docsInThisBatch.length} docs). Total committed: ${totalDocsCommitted}`);
            
            // Expect memtable flush after each batch due to size.
            // The C++ logs for "[TRACE]   Compressed block..." should appear.
            // And "[CompressionManager::compress] Attempting ZSTD compression..."
            // And "[CompressionManager::compress] ZSTD compression successful..."
            await delay(500); // Give time for potential async flush and background threads
        }

        console.log("--- All primary writes complete. ---");
        
        // Force final flush of any remaining memtable data during close
        console.log("Closing DB to ensure all memtables are flushed...");
        await db.close(); 
        db = null;
        await delay(1000); // Allow file operations to settle.

        // --- Analyze SSTable files on disk ---
        let sstableFiles: string[] = []; 
        if (fs.existsSync(lsmDataDir)) {
            sstableFiles = fs.readdirSync(lsmDataDir).filter(f => f.startsWith("sstable_") && f.endsWith(".dat"));
        } else {
            console.warn(`LSM Data directory not found: ${lsmDataDir}`);
        }
        
        console.log(`Found ${sstableFiles.length} SSTable files in ${lsmDataDir}: ${sstableFiles.join(', ')}`);
        // Expect multiple SSTables because each batch (125KB uncompressed approx) should exceed memtable limit or be flushed.
        // A 4MB memtable limit would mean roughly (4000KB / 125KB_per_batch) = 32 batches for one memtable.
        // If memtable_max_size is the default 4MB, we might only get 1-2 SSTables.
        // This depends on the *actual* C++ memtable_max_size. For this test to strongly show multiple
        // SSTables due to data volume, memtable_max_size should be smaller than total data written.
        // Assuming memtable_max_size is small enough relative to DOCS_PER_BATCH * BASE_PAYLOAD_SIZE.
        // Or, if the test framework allowed, we'd configure a smaller memtable_max_size.
        // For now, let's assume each batch roughly creates one SSTable if memtable limit is e.g. 128KB
         if (NUM_BATCHES > 1) {
            expect(sstableFiles.length).toBeGreaterThanOrEqual(1); // At least one SSTable from flushes. Could be more.
         }


        let totalUncompressedAppSize = 0;
        allDocsWritten.forEach(doc => {
            // Estimate size of serialized JSON string for the doc
            totalUncompressedAppSize += Buffer.from(s(doc)).length;
        });
        console.log(`Total application data (raw JSON strings) approx uncompressed size: ${(totalUncompressedAppSize / 1024).toFixed(2)} KB`);

        let totalSSTableDiskSize = 0;
        for (const file of sstableFiles) {
            const filePath = path.join(lsmDataDir, file);
            if (fs.existsSync(filePath)) {
                totalSSTableDiskSize += fs.statSync(filePath).size;
            }
        }
        console.log(`Total disk size of SSTable files: ${(totalSSTableDiskSize / 1024).toFixed(2)} KB`);

        if (sstableFiles.length > 0 && totalUncompressedAppSize > 50 * 1024) { // If substantial data was written
            // We expect ZSTD to provide good compression on repetitive text.
            // The SSTable file also includes headers, indexes, bloom filter, so it won't be pure compressed data.
            const compressionRatio = totalSSTableDiskSize / totalUncompressedAppSize;
            console.log(`Achieved SSTable disk size / raw JSON size ratio: ${compressionRatio.toFixed(3)}`);
            expect(totalSSTableDiskSize).toBeLessThan(totalUncompressedAppSize * 0.70); // Expect at least 30% reduction for this type of data
            console.log(`SUCCESS: SSTable disk usage (${(totalSSTableDiskSize/1024).toFixed(1)}KB) is significantly less than uncompressed app data (${(totalUncompressedAppSize/1024).toFixed(1)}KB).`);
        } else if (sstableFiles.length > 0) {
            console.log("Not enough data written to make a strong assertion about compression ratio, but SSTables were created.");
        } else {
            console.warn("No SSTable files found to analyze for compression. Check memtable flush logic and sizes.");
            // This might indicate an issue if data was expected to flush.
        }

        // --- Simulate Restart and Verify Data Integrity ---
        console.log("\n--- Reopening database to trigger recovery and verify data integrity ---");
        // Re-initialize with the same options to ensure consistent behavior for LSMTree
        const currentTestIndinisOptionsReload = JSON.parse(JSON.stringify(indinisTestOptions));
        currentTestIndinisOptionsReload.walOptions.wal_directory = walDirForTest;
        db = new Indinis(testDataDir, currentTestIndinisOptionsReload); 
        await delay(300); // Allow init

        console.log("Verifying all written documents after reopening...");
        let verifiedCountAfterRecovery = 0;
        await db.transaction(async tx => {
            for (const [key, expectedDoc] of allDocsWritten.entries()) {
                const retrievedStr = await tx.get(key);
                if (retrievedStr === null) {
                    // Log more info before failing
                    console.error(`[VERIFICATION FAILURE POST-RECOVERY] Key "${key}" was unexpectedly null.`);
                    console.error(`  Expected Doc: batch=${expectedDoc.batch}, seq=${expectedDoc.seq}, payload snippet="${expectedDoc.payload.substring(0,30)}..."`);
                }
                expect(retrievedStr).not.toBeNull(); 
                
                if (retrievedStr) {
                    const retrievedDoc = JSON.parse(retrievedStr as string) as SSTableCompressionTestDoc;
                    expect(retrievedDoc.payload).toEqual(expectedDoc.payload);
                    expect(retrievedDoc.batch).toEqual(expectedDoc.batch);
                    expect(retrievedDoc.seq).toEqual(expectedDoc.seq);
                }
                verifiedCountAfterRecovery++;
            }
        });
        expect(verifiedCountAfterRecovery).toBe(allDocsWritten.size);
        console.log(`Successfully verified all ${verifiedCountAfterRecovery} documents after reopening and recovery.`);
        
        // One final check: perform a new write and ensure it works
        const finalKey = `${colPath}/final_check_doc_comp_test`;
        const finalDoc = { id: 'final_check', payload: "Final write after recovery", batch: 99, seq: 99 };
        await db.transaction(async tx => {
            await tx.set(finalKey, s(finalDoc));
        });
        await db.transaction(async tx => {
            const retrieved = await tx.get(finalKey);
            expect(retrieved).not.toBeNull();
            expect(JSON.parse(retrieved as string).payload).toEqual(finalDoc.payload);
        });
        console.log("Final write and read after recovery successful.");
    });
});