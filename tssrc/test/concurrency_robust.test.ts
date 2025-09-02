// @filename: tssrc/test/concurrency_robust.test.ts

/**
 * @file concurrency_robust.test.ts
 * @description A robust concurrency stress test for Indinis.
 *
 * This test is designed to solve the race condition present in the previous stress test.
 * It uses an "active operation counter" to ensure that the main test thread waits for
 * all asynchronous database transactions to fully complete before proceeding to the
 * teardown and final verification phase.
 *
 * It also introduces more rigorous data integrity checks:
 * 1. Read operations actively verify data against an expected state map.
 * 2. Final verification checks both the content of expected keys AND the total key count.
 * 3. A concentrated key space is used to intentionally provoke contention and conflicts.
 */
import { Indinis, StorageValue } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-concurrency-robust');

describe('Indinis Robust Concurrency & Data Integrity Stress Test', () => {
    let db: Indinis;
    let testDataDir: string;
    const primaryCollectionPath = 'stressdocs';

    // --- Test Parameters ---
    const NUM_WORKERS = 8;
    const TOTAL_OPS = 4000; // Increased total operations
    const KEY_SPACE_SIZE = 150; // Intentionally small to force high contention
    const OP_MIX = {
        WRITE: 0.60, // 60% chance to insert/update
        DELETE: 0.30, // 30% chance to delete
        READ_VERIFY: 0.10, // 10% chance to read and verify
    };

    // Give the test ample time to run under load
    jest.setTimeout(300000); // 5 minutes

    // --- Shared State for Test Coordination ---
    // This map represents the "ground truth" of what the database *should* contain.
    let finalState = new Map<string, StorageValue>();
    // This counter tracks in-flight async operations to solve the teardown race condition.
    let activeOperations = 0;
    const opStarted = () => activeOperations++;
    const opFinished = () => activeOperations--;
    // Array to collect any errors that occur within transactions.
    const errors: string[] = [];

    // --- Test Setup and Teardown ---
    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        // Reset shared state for each test run
        finalState.clear();
        activeOperations = 0;
        errors.length = 0;

        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        db = new Indinis(testDataDir);
        console.log(`\n[ROBUST STRESS TEST START] Using data directory: ${testDataDir}`);
        
        // No need to create indexes as this test doesn't use queries, only direct key operations.
    });

    afterEach(async () => {
        if (db) {
            console.log("[ROBUST STRESS TEST END] Closing database...");
            await db.close();
        }
        if (fs.existsSync(testDataDir)) {
            console.log("[ROBUST STRESS TEST END] Cleaning up test directory...");
            await rimraf(testDataDir).catch(err => console.error(`Cleanup error for ${testDataDir}:`, err));
        }
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) {
            console.log("[SUITE END] Cleaning up base concurrency directory...");
            await rimraf(TEST_DATA_DIR_BASE).catch(err => console.error(`Base cleanup error for ${TEST_DATA_DIR_BASE}:`, err));
        }
    });


    it('should handle high-contention writes, deletes, and reads without data loss, corruption, or deadlock', async () => {
        const opsPerWorker = Math.ceil(TOTAL_OPS / NUM_WORKERS);
        console.log(`Starting ${NUM_WORKERS} workers, each performing ~${opsPerWorker} operations on a key space of ${KEY_SPACE_SIZE}.`);

        // --- Worker Execution Phase ---
        const workerPromises = Array.from({ length: NUM_WORKERS }, (_, workerId) => (async () => {
            for (let i = 0; i < opsPerWorker; i++) {
                const keyIndex = Math.floor(Math.random() * KEY_SPACE_SIZE);
                const key = `${primaryCollectionPath}/${keyIndex.toString().padStart(4, '0')}`;
                const opType = Math.random();

                opStarted(); // Increment counter BEFORE starting the async operation
                try {
                    await db.transaction(async (tx) => {
                        if (opType < OP_MIX.WRITE) {
                            // --- WRITE Operation ---
                            const value = `worker_${workerId}_op_${i}_val_${generateId()}`;
                            await tx.set(key, value, { overwrite: true }); // Use overwrite to simplify logic
                            // Update our expected final state
                            finalState.set(key, value);

                        } else if (opType < OP_MIX.WRITE + OP_MIX.DELETE) {
                            // --- DELETE Operation ---
                            const removed = await tx.remove(key);
                            if (removed) {
                                // Update our expected final state only if it was successfully removed
                                finalState.delete(key);
                            }
                        } else {
                            // --- READ and VERIFY Operation ---
                            const expectedValue = finalState.get(key) ?? null;
                            const actualValue = await tx.get(key);

                            if (actualValue !== expectedValue) {
                                // This is an in-flight data integrity failure!
                                const errorMsg = `Data integrity failure on key ${key}! Worker ${workerId} expected '${expectedValue}' but read '${actualValue}'.`;
                                console.error(`\n---\n${errorMsg}\n---`);
                                errors.push(errorMsg);
                            }
                        }
                    });
                } catch (e: any) {
                    // Catch errors from failed transactions (e.g., C++ level)
                    const errorMsg = `Worker ${workerId} transaction failed: ${e.message}`;
                    console.error(`\n---\n${errorMsg}\n---`);
                    errors.push(errorMsg);
                } finally {
                    opFinished(); // Decrement counter AFTER the operation is fully complete (success or fail)
                }
                 // Small delay to encourage interleaving
                if (i % 20 === 0) await new Promise(res => setTimeout(res, 1));
            }
            console.log(`  Worker ${workerId}: Loop finished.`);
        })());

        await Promise.all(workerPromises);

        // --- Wait for Completion Phase ---
        console.log("All worker loops finished. Waiting for in-flight database operations to complete...");
        const waitStartTime = Date.now();
        while (activeOperations > 0) {
            console.log(`  Waiting... ${activeOperations} operations still in flight.`);
            if (Date.now() - waitStartTime > 60000) { // 1-minute timeout for operations to drain
                throw new Error(`Timed out waiting for ${activeOperations} in-flight operations to finish.`);
            }
            await new Promise(res => setTimeout(res, 250));
        }
        console.log("All in-flight operations have completed.");

        // --- Final Verification Phase ---
        console.log("\nPerforming final state verification...");

        // 1. Check for any errors recorded during the run.
        expect(errors).toEqual([]);

        // 2. Verify the total number of keys in the database.
        const allDocs = await db.store(primaryCollectionPath).take();
        console.log(`Expected final document count: ${finalState.size}. Actual count in DB: ${allDocs.length}.`);
        expect(allDocs.length).toBe(finalState.size);

        // 3. Verify the content of every expected key.
        if (finalState.size > 0) {
            await db.transaction(async (tx) => {
                let verifiedCount = 0;
                for (const [key, expectedValue] of finalState.entries()) {
                    const actualValue = await tx.get(key);
                    expect(actualValue).toEqual(expectedValue);
                    verifiedCount++;
                }
                console.log(`Successfully verified content of all ${verifiedCount} expected documents.`);
            });
        }
    });
});

/**
 * Helper to generate a Firestore-style random ID.
 */
function generateId(): string {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let id = '';
    for (let i = 0; i < 16; i++) {
        id += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return id;
}

function generateRandomString(length: number): string {
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() * characters.length));
    }
    return result;
}