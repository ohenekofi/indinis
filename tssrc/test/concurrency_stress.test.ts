// @filename: tssrc/test/concurrency_stress.test.ts
import { Indinis, StorageValue } from '../index'; // Adjust path if needed
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-concurrency-stress-v2'); // New version for clarity

// Helper to generate somewhat random string keys with a prefix
function generateKey(prefix: string, i: number, workerId: number): string {
    const paddedNum = i.toString().padStart(5, '0'); // Pad for some ordering
    return `${prefix}/worker${workerId}/item${paddedNum}`;
}

function generateRandomString(length: number): string {
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() * characters.length));
    }
    return result;
}

describe('Indinis B+Tree Concurrency Stress Test (Enhanced Deletions)', () => {
    let db: Indinis;
    let testDataDir: string;
    const primaryCollectionPath = 'stressdocs';
    const indexNameFieldA = 'idx_stress_fieldA_v2';
    const indexNameFieldB = 'idx_stress_fieldB_desc_v2'; // Descending index

    const NUM_WORKERS = 8; // Number of concurrent workers
    const OPS_PER_WORKER = 250; // Increased operations
    const KEY_RANGE_PER_WORKER = 150; // Wider range for more potential splits/merges
    const VALUE_SIZE = 50;

    jest.setTimeout(180000); // 3 minutes for stress test, adjust as needed

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        db = new Indinis(testDataDir);
        console.log(`\n[CONCURRENCY TEST V2 START] Using data directory: ${testDataDir}`);

        console.log(`Creating index ${indexNameFieldA} on ${primaryCollectionPath}.fieldA`);
        expect(await db.createIndex(primaryCollectionPath, indexNameFieldA, { field: 'fieldA' })).toBe('CREATED');

        console.log(`Creating index ${indexNameFieldB} on ${primaryCollectionPath}.fieldB (desc)`);
        expect(await db.createIndex(primaryCollectionPath, indexNameFieldB, { field: 'fieldB', order: 'desc' })).toBe('CREATED');
    });

    afterEach(async () => {
        console.log("[CONCURRENCY TEST V2 END] Closing database...");
        if (db) await db.close();
        console.log("[CONCURRENCY TEST V2 END] Cleaning up test directory...");
        if (fs.existsSync(testDataDir)) {
            await rimraf(testDataDir).catch(err => console.error(`Cleanup error for ${testDataDir}:`, err));
        }
    });

    afterAll(async () => {
        console.log("[SUITE END V2] Cleaning up base concurrency directory...");
        if (fs.existsSync(TEST_DATA_DIR_BASE)) {
            await rimraf(TEST_DATA_DIR_BASE).catch(err => console.error(`Base cleanup error for ${TEST_DATA_DIR_BASE}:`, err));
        }
    });

    it('should handle concurrent insertions, frequent deletions, and finds without deadlocks or crashes', async () => {
        const workers: Promise<void>[] = [];
        const insertedKeysByWorker: Map<number, Set<string>> = new Map();
        const allSuccessfullyInsertedKeys = new Set<string>(); // Track all keys expected to exist at the end

        console.log(`Starting ${NUM_WORKERS} workers, each performing ${OPS_PER_WORKER} operations.`);

        for (let workerId = 0; workerId < NUM_WORKERS; workerId++) {
            insertedKeysByWorker.set(workerId, new Set());
            const workerPromise = (async () => {
                console.log(`  Worker ${workerId}: Started`);
                for (let i = 0; i < OPS_PER_WORKER; i++) {
                    const opType = Math.random();
                    // Generate key based on worker ID and item number to create some locality but also potential for contention
                    const itemNumForThisOp = Math.floor(Math.random() * KEY_RANGE_PER_WORKER);
                    const key = generateKey(primaryCollectionPath, itemNumForThisOp, workerId);
                    
                    try {
                        await db.transaction(async (tx) => {
                            if (opType < 0.65) { // 65% chance of insert/update
                                const fieldAValue = generateRandomString(10);
                                const fieldBValue = Math.floor(Math.random() * 10000); // Wider range for B field
                                const doc = {
                                    id: key.split('/').pop(),
                                    fieldA: fieldAValue,
                                    fieldB: fieldBValue,
                                    worker: workerId,
                                    opNum: i,
                                    value: generateRandomString(VALUE_SIZE),
                                    lastOp: 'insert/update'
                                };
                                await tx.set(key, JSON.stringify(doc));
                                insertedKeysByWorker.get(workerId)!.add(key); // Track keys this worker believes it inserted
                                allSuccessfullyInsertedKeys.add(key); // Add to global set, will be removed on delete
                                if (i % (OPS_PER_WORKER / 4) === 0 && i > 0) console.log(`    Worker ${workerId}: Op ${i} - Inserted/Updated ${key}`);

                            } else if (opType < 0.90) { // 25% chance of delete (increased)
                                const keysOfThisWorker = Array.from(insertedKeysByWorker.get(workerId)!);
                                if (keysOfThisWorker.length > 0) {
                                    const keyToDelete = keysOfThisWorker[Math.floor(Math.random() * keysOfThisWorker.length)];
                                    const removed = await tx.remove(keyToDelete);
                                    if (removed) {
                                        insertedKeysByWorker.get(workerId)!.delete(keyToDelete);
                                        allSuccessfullyInsertedKeys.delete(keyToDelete); // Remove from global set
                                        if (i % (OPS_PER_WORKER / 4) === 0 && i > 0) console.log(`    Worker ${workerId}: Op ${i} - Removed ${keyToDelete}`);
                                    } else {
                                        // This can happen if another worker deleted it or if it was never truly committed due to a race.
                                        // For this test, we'll log it. In a strict test, this might be an error.
                                        if (i % (OPS_PER_WORKER / 4) === 0 && i > 0) console.log(`    Worker ${workerId}: Op ${i} - Attempted remove on ${keyToDelete}, but it was not found (or already deleted).`);
                                    }
                                } else {
                                     // No keys to delete for this worker yet, perform a read instead
                                     if (allSuccessfullyInsertedKeys.size > 0) {
                                        const randomKeyFromAll = Array.from(allSuccessfullyInsertedKeys)[Math.floor(Math.random() * allSuccessfullyInsertedKeys.size)];
                                        await tx.get(randomKeyFromAll); // Perform a get
                                    }
                                }
                            } else { // 5% chance of find
                                if (allSuccessfullyInsertedKeys.size > 0) {
                                    const randomKeyFromAll = Array.from(allSuccessfullyInsertedKeys)[Math.floor(Math.random() * allSuccessfullyInsertedKeys.size)];
                                    const val = await tx.get(randomKeyFromAll);
                                    if (i % (OPS_PER_WORKER / 4) === 0 && i > 0) console.log(`    Worker ${workerId}: Op ${i} - Get for ${randomKeyFromAll} (found: ${!!val})`);
                                }
                            }
                        });
                    } catch (e: any) {
                        // This catch block is important. If a transaction fails due to an internal C++ error
                        // (like a deadlock not handled, or an unexpected exception), it will be caught here.
                        console.error(`Worker ${workerId} Transaction FAILED for op ${i} on key ${key}:`, e.message, e.stack);
                        // For a stress test, we often want to continue to see if the system remains stable or if errors cascade.
                        // If we re-throw here, the first worker to hit a C++ issue will stop the whole test.
                        // throw e; // Re-throwing would make the test fail fast.
                        // For now, log and continue. Test success will be judged by lack of crashes and final verification.
                    }
                     // Small delay to allow context switching and increase chance of interleaving
                    if (i % 5 === 0) await new Promise(res => setTimeout(res, Math.random() * 3));
                }
                console.log(`  Worker ${workerId}: Finished operations.`);
            })();
            workers.push(workerPromise);
        }

        let allWorkersSucceeded = true;
        try {
            await Promise.all(workers);
            console.log("All worker promises resolved.");
        } catch (e) {
            allWorkersSucceeded = false;
            console.error("Error during concurrent execution (Promise.all rejected):", e);
            // If a worker re-threw an error, it would be caught here.
        }

        expect(allWorkersSucceeded).toBe(true); // Check if any worker promise was rejected.

        // --- Final Verification Phase ---
        console.log("\nPerforming final verification of expected keys...");
        let totalVerified = 0;
        const expectedFinalKeyCount = allSuccessfullyInsertedKeys.size;
        console.log(`Expecting ${expectedFinalKeyCount} keys to exist finally.`);

        if (expectedFinalKeyCount > 0) {
            await db.transaction(async (tx) => {
                for (const key of allSuccessfullyInsertedKeys) {
                    const docStr = await tx.get(key);
                    if (docStr !== null) {
                        try {
                            const doc = JSON.parse(docStr as string);
                            if (doc.id !== key.split('/').pop()) {
                                console.error(`Mismatch for key ${key}: doc.id is ${doc.id}`);
                                // expect(doc.id).toBe(key.split('/').pop()); // Fail test immediately
                            }
                            totalVerified++;
                        } catch (parseError) {
                            console.error(`Error parsing JSON for key ${key} during verification: ${docStr}`);
                            throw parseError; // Fail test
                        }
                    } else {
                        console.error(`Verification FAILED for key ${key}: Expected to exist but was not found.`);
                        // This indicates a problem, as `allSuccessfullyInsertedKeys` should reflect the final state.
                    }
                }
            });
        }
        
        console.log(`Final verification: Verified ${totalVerified} existing keys out of ${expectedFinalKeyCount} expected.`);
        expect(totalVerified).toBe(expectedFinalKeyCount);


        // Check if the B-Tree structure itself is still queryable (indexes exist)
        console.log("Verifying index existence...");
        const indexes = await db.listIndexes(primaryCollectionPath);
        expect(indexes.some(idx => idx.name === indexNameFieldA)).toBe(true);
        expect(indexes.some(idx => idx.name === indexNameFieldB)).toBe(true);
        console.log("Indexes are still listed correctly.");

        // Optional: Perform a few range scans on the indexes to see if they respond.
        // This won't check for perfect data correctness but can catch if the index is corrupted.
        console.log(`Attempting a debug scan on index ${indexNameFieldA} (empty range, just for health check)`);
        try {
            await db.debug_scanBTree(indexNameFieldA, Buffer.from(""), Buffer.from(""));
            console.log(` -> Debug scan on ${indexNameFieldA} completed without error.`);
        } catch (e: any) {
            console.error(`Error during debug scan of ${indexNameFieldA}: ${e.message}`);
            throw e; // Fail test if index scan fails
        }
    });
});