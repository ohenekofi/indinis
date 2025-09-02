// @tssrc/test/concurrent_engine_operations.test.ts

import { Indinis, IndinisOptions, StorageValue, IndexOptions, IndexInfo, ExtWALManagerConfigJs } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';
import { Worker, isMainThread, workerData, parentPort } from 'worker_threads';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-concurrent-engine-ops-v2'); // New version for clarity
const JEST_HOOK_TIMEOUT = 120000; 
const TEST_CASE_TIMEOUT = 240000; 

const s = (obj: any): string => JSON.stringify(obj);
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

interface WorkerInput {
    testDataDir: string; // Main data directory (shared)
    workerId: number;
    numOperations: number;
    collectionPath: string;
    indexPrefix: string;
    sharedDataMapKeys: string[];
    // IndinisOptions are now constructed by the worker using testDataDir and shared WAL dir
}

interface WorkerResult {
    workerId: number;
    operationsAttempted: number;
    successfulCommits: number;
    failedCommits: number;
    readOperations: number;
    // setData: Map<string, any>; // Removing this as reconciling last writer is too complex for this test
    errors: string[];
}

// --- Helper to construct IndinisOptions consistently ---
// This will now be used by both the main thread and worker threads.
const getSharedTestIndinisOptions = (baseDataDir: string): IndinisOptions => {
    const sharedWalDir = path.join(baseDataDir, 'shared_wal_ops'); // All instances use this
    // The C++ layer will create this directory if it doesn't exist.
    return {
        checkpointIntervalSeconds: 3,
        walOptions: {
            wal_directory: sharedWalDir,
            segment_size: 256 * 1024, // 256KB segments
            wal_file_prefix: `concurrent_shared_wal_`,
            flush_interval_ms: 50,
            sync_on_commit_record: true,
            background_flush_enabled: true,
        },
        sstableDataBlockUncompressedSizeKB: 16,
        sstableCompressionType: "ZSTD",
        sstableCompressionLevel: 1,
        // enableCache: true, // Optional
        // cacheOptions: { maxSize: 500, policy: "LRU", enableStats: true }
    };
};


// --- Code to be executed by worker threads ---
if (!isMainThread) {
    (async () => {
        if (!parentPort) throw new Error("Worker started without parentPort");
        const input = workerData as WorkerInput;
        let db: Indinis | null = null;
        const workerResult: WorkerResult = {
            workerId: input.workerId,
            operationsAttempted: 0,
            successfulCommits: 0,
            failedCommits: 0,
            readOperations: 0,
            errors: [],
        };

        try {
            // Worker uses the shared testDataDir and constructs options using the shared helper
            const workerDbOptions = getSharedTestIndinisOptions(input.testDataDir);
            console.log(`Worker ${input.workerId}: Initializing DB for dir ${input.testDataDir} with WAL dir ${workerDbOptions.walOptions?.wal_directory}`);
            db = new Indinis(input.testDataDir, workerDbOptions);
            await delay(100 + Math.random() * 200); // Stagger worker starts slightly

            for (let i = 0; i < input.numOperations; i++) {
                workerResult.operationsAttempted++;
                const opType = Math.random();
                const localKeySuffix = `item_w${input.workerId}_op${i}`;
                const sharedKeyIndex = Math.floor(Math.random() * input.sharedDataMapKeys.length);
                const targetKey = (Math.random() < 0.3 && input.sharedDataMapKeys.length > 0) ?
                                   input.sharedDataMapKeys[sharedKeyIndex] :
                                   `${input.collectionPath}/${localKeySuffix}`;

                try {
                    await db.transaction(async tx => {
                        if (opType < 0.55) { // 55% writes (increased slightly)
                            const value = {
                                worker: input.workerId, op: i, ts: Date.now(),
                                data: `W${input.workerId}|Op${i}|K${targetKey.split('/').pop()}|V${Math.random().toString(36).slice(2, 8)}`
                            };
                            await tx.set(targetKey, s(value));
                        } else if (opType < 0.90) { // 35% reads
                            await tx.get(targetKey);
                            workerResult.readOperations++;
                        } else { // 10% deletes
                            await tx.remove(targetKey);
                        }
                    });
                    workerResult.successfulCommits++;
                } catch (e: any) {
                    workerResult.failedCommits++;
                    workerResult.errors.push(`Op ${i} on key ${targetKey}: ${e.message.substring(0, 100)}`); // Keep error msg short
                }
                if (i % 50 === 0) await delay(Math.random() * 5);
            }
        } catch (e: any) {
            workerResult.errors.push(`Worker ${input.workerId} CRITICAL error: ${e.message.substring(0, 200)}`);
            console.error(`Worker ${input.workerId} CRITICAL ERROR: ${e.message}`);
        } finally {
            if (db) {
                try { 
                    // console.log(`Worker ${input.workerId} closing DB.`);
                    await db.close(); 
                } catch (closeErr: any) { 
                    workerResult.errors.push(`Worker ${input.workerId} DB close error: ${closeErr.message.substring(0,100)}`);
                }
            }
            parentPort.postMessage(workerResult);
        }
    })();
}
// --- End Worker Thread Code ---


describe('Indinis Concurrent Engine Operations (Multi-Worker Stress Test v2)', () => {
    let mainDbInstance: Indinis | null = null;
    let currentTestDir: string; // Root data directory for this specific test execution

    const COLLECTION_PATH = 'concurrent_docs_v2'; // Unique collection path
    const INDEX_PREFIX = 'idx_concurrent_v2';
    const NUM_WORKERS = process.env.CI ? 2 : 4; // Fewer workers on CI for stability/speed
    const OPS_PER_WORKER = process.env.CI ? 150 : 250;
    const NUM_SHARED_KEYS = 10;

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    }, JEST_HOOK_TIMEOUT);

    afterAll(async () => {
        console.log("[CONCURRENT ENGINE OPS SUITE END V2] Cleaning up base directory...");
        if (fs.existsSync(TEST_DATA_DIR_BASE)) {
            await rimraf(TEST_DATA_DIR_BASE).catch(err => console.error(`Base cleanup error:`, err));
        }
    }, JEST_HOOK_TIMEOUT);

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        currentTestDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(currentTestDir, { recursive: true });
        // The shared WAL directory will be created by the C++ layer if it doesn't exist
        // when the first Indinis instance (mainDbInstance) is created.
        console.log(`\n[CONCURRENT ENGINE OPS TEST V2 START] Using data directory: ${currentTestDir}`);
        
        mainDbInstance = new Indinis(currentTestDir, getSharedTestIndinisOptions(currentTestDir));
        await delay(500); 
    });

    afterEach(async () => {
        const dirToClean = currentTestDir; 
        console.log(`[CONCURRENT ENGINE OPS TEST V2 END - ${path.basename(dirToClean)}] Closing main DB...`);
        if (mainDbInstance) {
            try { await mainDbInstance.close(); } catch (e) { console.error("Error closing main DB:", e); }
            mainDbInstance = null;
        }
        await delay(1200); 
        if (dirToClean && fs.existsSync(dirToClean)) {
            console.log(`[CONCURRENT ENGINE OPS TEST V2 END - ${path.basename(dirToClean)}] Cleaning up:`, dirToClean);
            await rimraf(dirToClean, { maxRetries: 3, retryDelay: 1000 }).catch(err =>
                console.error(`Cleanup error for ${dirToClean}:`, err)
            );
        }
    }, JEST_HOOK_TIMEOUT);

    it('should handle concurrent transactions, DDL, and checkpoints from multiple workers', async () => {
        if (!mainDbInstance) throw new Error("Main DB not initialized for test");

        const sharedDataMapKeys: string[] = [];
        const finalExpectedState = new Map<string, any>(); // For more precise final verification

        for (let i = 0; i < NUM_SHARED_KEYS; i++) {
            const key = `${COLLECTION_PATH}/shared_item_${i}`;
            sharedDataMapKeys.push(key);
            const initialDoc = { content: "initial_shared_content", version: 0, writer: "main" };
            finalExpectedState.set(key, initialDoc); // Set initial expected state
        }
        await mainDbInstance.transaction(async tx => {
            for (const key of sharedDataMapKeys) {
                await tx.set(key, s(finalExpectedState.get(key)));
            }
        });
        console.log(`Initialized ${NUM_SHARED_KEYS} shared keys.`);

        const workerPromises: Promise<WorkerResult>[] = [];
        console.log(`Starting ${NUM_WORKERS} data worker threads...`);
        for (let i = 0; i < NUM_WORKERS; i++) {
            const workerInput: WorkerInput = {
                testDataDir: currentTestDir, // All workers use the same main data directory
                workerId: i,
                numOperations: OPS_PER_WORKER,
                collectionPath: COLLECTION_PATH,
                indexPrefix: INDEX_PREFIX,
                sharedDataMapKeys,
                // Options are constructed inside the worker thread using getSharedTestIndinisOptions
                // No need to pass indinisOptions in WorkerInput anymore if workers use shared helper
            };
            
            const worker = new Worker(__filename, { workerData: workerInput });
            workerPromises.push(new Promise((resolve, reject) => {
                worker.on('message', (result: WorkerResult) => {
                    // Update finalExpectedState based on worker's successful writes
                    // This is still a "last writer wins" view from the worker's perspective,
                    // but helps reconcile for final check.
                    // result.setData.forEach((value, key) => finalExpectedState.set(key, value));
                    resolve(result);
                });
                worker.on('error', reject);
                worker.on('exit', (code) => {
                    if (code !== 0) reject(new Error(`Worker ${i} stopped with exit code ${code}`));
                });
            }));
        }

        let ddlOperationsCompleted = 0;
        const ddlWorker = async () => {
            console.log("DDL & Checkpoint control operations starting...");
            // More DDL ops to stress index manager locking
            for (let i = 0; i < Math.max(5, NUM_WORKERS * OPS_PER_WORKER / 150); i++) { 
                if (!mainDbInstance) { console.warn("DDL Worker: mainDbInstance became null."); break; }
                try {
                    const idxName = `${INDEX_PREFIX}_dyn_field_${i}`;
                    const actionRoll = Math.random();

                    if (actionRoll < 0.5) { // Create index
                        console.log(`  DDL Control: Creating index ${idxName}`);
                        await mainDbInstance.createIndex(COLLECTION_PATH, idxName, { field: `dynamicField${i % 3}` });
                        ddlOperationsCompleted++;
                    } else if (actionRoll < 0.75 && ddlOperationsCompleted > 1) { // Delete an index
                        const indexes = await mainDbInstance.listIndexes(COLLECTION_PATH);
                        const dynIndexes = indexes.filter(idx => idx.name.startsWith(`${INDEX_PREFIX}_dyn_field_`));
                        if (dynIndexes.length > 0) {
                            const idxToDelete = dynIndexes[Math.floor(Math.random() * dynIndexes.length)].name;
                            console.log(`  DDL Control: Deleting index ${idxToDelete}`);
                            await mainDbInstance.deleteIndex(idxToDelete);
                            ddlOperationsCompleted++; // Count as an operation
                        }
                    } else if (mainDbInstance) { // Force checkpoint
                        console.log(`  DDL Control: Forcing checkpoint...`);
                        await mainDbInstance.forceCheckpoint();
                        ddlOperationsCompleted++;
                    }
                } catch (e: any) {
                    console.error(`  DDL Control: Error: ${e.message.substring(0,150)}`);
                }
                await delay(Math.random() * 400 + 200); 
            }
            console.log("DDL & Checkpoint control operations finished.");
        };
        const ddlPromise = ddlWorker();

        console.log("Waiting for all data worker threads to complete...");
        const results = await Promise.all(workerPromises);
        console.log("All data worker threads finished.");
        await ddlPromise;
        console.log("DDL/Checkpoint control operations also finished.");

        let totalSuccessfulCommits = 0;
        let totalFailedCommits = 0;
        let totalErrors = 0;
        results.forEach(res => {
            totalSuccessfulCommits += res.successfulCommits;
            totalFailedCommits += res.failedCommits;
            totalErrors += res.errors.length;
            if(res.errors.length > 0) console.warn(`Worker ${res.workerId} reported errors:`, res.errors.slice(0,2));
        });

        console.log(`\n--- Aggregated Worker Results ---`);
        console.log(`Total Ops Attempted (all workers): ${NUM_WORKERS * OPS_PER_WORKER}`);
        console.log(`Total Successful Commits: ${totalSuccessfulCommits}`);
        console.log(`Total Failed Commits: ${totalFailedCommits}`);
        console.log(`Total Worker Errors Logged (non-commit): ${totalErrors}`);
        expect(totalFailedCommits).toBeLessThan(totalSuccessfulCommits * 0.15); 
        expect(totalErrors).toBeLessThan(NUM_WORKERS * OPS_PER_WORKER * 0.05);

        console.log("\n--- Final Data Integrity Verification ---");
        if (!mainDbInstance) {
            console.log("Re-initializing main DB for final verification...");
            mainDbInstance = new Indinis(currentTestDir, getSharedTestIndinisOptions(currentTestDir));
            await delay(300);
        }

        // To verify, we need to know the *actual* final state.
        // Since workers update `finalExpectedState` locally, we need a final pass
        // through the DB to determine the *true* last committed state for each key.
        // This is hard without knowing commit order.
        // A simpler verification: read all keys written by workers and ensure they exist and are parseable.
        const allKeysAttemptedByWorkers = new Set<string>();
        for (let workerId = 0; workerId < NUM_WORKERS; workerId++) {
            for (let i = 0; i < OPS_PER_WORKER; i++) {
                 const localKeySuffix = `item_w${workerId}_op${i}`;
                 allKeysAttemptedByWorkers.add(`${COLLECTION_PATH}/${localKeySuffix}`);
            }
        }
        sharedDataMapKeys.forEach(k => allKeysAttemptedByWorkers.add(k));

        console.log(`Verifying a sample of potentially ${allKeysAttemptedByWorkers.size} keys...`);
        let checkedCount = 0;
        let foundAndValidCount = 0;
        let notFoundCount = 0;
        const sampleKeysToVerify = Array.from(allKeysAttemptedByWorkers).slice(0, Math.min(500, allKeysAttemptedByWorkers.size)); // Verify a subset

        await mainDbInstance.transaction(async tx => {
            for (const key of sampleKeysToVerify) {
                checkedCount++;
                const retrievedStr = await tx.get(key);
                if (retrievedStr) {
                    try {
                        const retrievedDoc = JSON.parse(retrievedStr as string);
                        expect(retrievedDoc).toHaveProperty('worker'); // Basic check
                        expect(retrievedDoc).toHaveProperty('data');
                        foundAndValidCount++;
                    } catch(e) {
                        console.error(`Error parsing JSON for key ${key} in final verification. Data: ${retrievedStr}`);
                        // Fail test immediately if data is unparsable
                        throw new Error(`Unparsable JSON for key ${key}`);
                    }
                } else {
                    notFoundCount++; // This key was likely deleted by another worker as the final op
                }
            }
        });
        console.log(`Final verification: Checked ${checkedCount} keys. Found and valid: ${foundAndValidCount}. Not found (potentially deleted): ${notFoundCount}.`);
        // We expect most keys to be either present and valid, or legitimately deleted.
        // A high number of foundAndValidCount compared to checkedCount is good.
        expect(foundAndValidCount + notFoundCount).toEqual(sampleKeysToVerify.length); // All checked keys should either be valid or null
        expect(foundAndValidCount).toBeGreaterThan(0); // At least some data should exist and be valid

        const cpHistory = await mainDbInstance.getCheckpointHistory();
        console.log(`Final Checkpoint History (last 5):`, cpHistory.slice(-5).map(c => `ID:${c.checkpoint_id} St:${c.status}`));
        expect(cpHistory.length).toBeGreaterThanOrEqual(Math.floor((NUM_WORKERS * OPS_PER_WORKER / 200) / 2)); // Heuristic: expect some checkpoints

    }, TEST_CASE_TIMEOUT);
});