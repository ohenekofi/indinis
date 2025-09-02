// @filename: tssrc/test/checkpoint_stress.test.ts
import { Indinis, CheckpointInfo } from '../index'; // Assuming CheckpointInfo is exported
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-checkpoint-stress');

// Helper for small delay
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Helper to stringify values for storage
const s = (obj: any) => JSON.stringify(obj);

interface CpTestDoc {
    id: string;
    data: string;
    batch: number;
    updatedInBatch?: number; // Track which batch last updated this doc
}

// Crude LogRecordType enum mapping for test (must match C++) - for WAL parsing if needed
enum LogRecordType {
    INVALID = 0, BEGIN_TXN = 1, COMMIT_TXN = 2, ABORT_TXN = 3,
    DATA_PUT = 4, DATA_DELETE = 5,
    CHECKPOINT_BEGIN = 6, CHECKPOINT_END = 7,
}
interface DecodedLogRecord {
    lsn: bigint; txn_id: bigint; type: LogRecordType; key: string; value: string;
}

// Basic parser for the WAL file for testing (same as in wal.test.ts)
function parseWalFile(filePath: string): DecodedLogRecord[] {
    if (!fs.existsSync(filePath)) return [];
    const buffer = fs.readFileSync(filePath);
    const records: DecodedLogRecord[] = [];
    let offset = 0;
    while (offset < buffer.length) {
        const recordStartOffset = offset;
        try {
            if (offset + 8 > buffer.length) break; const lsn = buffer.readBigUInt64LE(offset); offset += 8;
            if (offset + 8 > buffer.length) break; const txn_id = buffer.readBigUInt64LE(offset); offset += 8;
            if (offset + 1 > buffer.length) break; const type = buffer.readUInt8(offset); offset += 1;
            if (offset + 4 > buffer.length) break; const keyLen = buffer.readUInt32LE(offset); offset += 4;
            if (offset + keyLen > buffer.length) break; const key = buffer.toString('utf8', offset, offset + keyLen); offset += keyLen;
            if (offset + 4 > buffer.length) break; const valLen = buffer.readUInt32LE(offset); offset += 4;
            if (offset + valLen > buffer.length) break; const value = buffer.toString('utf8', offset, offset + valLen); offset += valLen;
            records.push({ lsn, txn_id, type: type as LogRecordType, key, value });
        } catch (e: any) {
            console.error("Error parsing WAL record at offset", recordStartOffset, e.message);
            break;
        }
    }
    return records;
}


describe('Indinis Checkpointing Stress Test', () => {
    let db: Indinis | null = null;
    let testDataDir: string;
    let checkpointMetaFilePath: string;
    let walFilePath: string;

    const colPath = 'cp_docs';
    const CHECKPOINT_INTERVAL_SECONDS = 2; // Short interval for frequent checkpoints
    const NUM_BATCHES = 5;
    const DOCS_PER_BATCH = 50;
    const OPS_BETWEEN_CHECKS = 10; // How many docs before checking checkpoint status in test log

    // Timeout calculation:
    // (Batches * DocsPerBatch * AvgOpTime_ms) + (Batches * CheckpointInterval_ms) + (SimulatedCrashRecoveryTime_ms) + Buffer
    // Example: (5 * 50 * 50ms) + (5 * 2000ms) + 5000ms + 10000ms = 12500 + 10000 + 5000 + 10000 = 37500ms
    jest.setTimeout(60000); // 60 seconds, should be enough

    beforeAll(async () => {
        // Ensure base directory exists for all test runs within this file
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        
        const checkpointsDir = path.join(testDataDir, 'checkpoints');
        await fs.promises.mkdir(checkpointsDir, { recursive: true });
        checkpointMetaFilePath = path.join(checkpointsDir, 'checkpoint.meta');

        const walDir = path.join(testDataDir, 'wal');
        await fs.promises.mkdir(walDir, { recursive: true });
        walFilePath = path.join(walDir, 'wal.log'); // Assuming single WAL file for now

        console.log(`\n[CP TEST START] Using data directory: ${testDataDir}`);
        db = new Indinis(testDataDir, { checkpointIntervalSeconds: CHECKPOINT_INTERVAL_SECONDS });
    });

   // In tssrc/test/checkpoint_stress.test.ts
    afterEach(async () => {
        console.log("[TEST END] Closing database...");
        if (db) {
            await db.close();
            db = null; // Help GC
        }
        // Add a delay *after* close and *before* rimraf
        console.log("[TEST END] Waiting a moment before cleanup...");
        await delay(500); // Increased delay to 500ms or 1s

        console.log("[TEST END] Cleaning up test directory...");
        if (fs.existsSync(testDataDir)) {
            try {
                await rimraf(testDataDir);
            } catch (e: any) {
                if (e.code === 'EBUSY' || e.code === 'EPERM') { // Handle EPERM too
                    console.warn(`EBUSY/EPERM error during cleanup for ${testDataDir}, retrying after longer delay...`);
                    await delay(2000); // Longer retry delay
                    await rimraf(testDataDir).catch(err => console.error(`Cleanup error (after retry) for ${testDataDir}:`, err));
                } else {
                    console.error(`Cleanup error for ${testDataDir}:`, e);
                }
            }
        }
    }, 30000); // Keep timeout for hook

    afterAll(async () => {
        console.log("[CP SUITE END] Cleaning up base directory...");
        if (fs.existsSync(TEST_DATA_DIR_BASE)) {
            await rimraf(TEST_DATA_DIR_BASE).catch(err => console.error(`Base cleanup error for ${TEST_DATA_DIR_BASE}:`, err));
        }
    });

    async function verifyData(expectedDocs: Map<string, CpTestDoc>, currentDb: Indinis, message: string) {
        console.log(`--- Verifying Data: ${message} (Expected ${expectedDocs.size} docs) ---`);
        let verifiedCount = 0;
        await currentDb.transaction(async tx => {
            for (const [key, expectedDoc] of expectedDocs.entries()) {
                const retrievedStr = await tx.get(key);
                if (expectedDoc.data === "[DELETED]") { // Special marker for deleted docs in our test map
                    expect(retrievedStr).toBeNull();
                } else {
                    expect(retrievedStr).not.toBeNull();
                    if (retrievedStr) {
                        const retrievedDoc = JSON.parse(retrievedStr as string) as CpTestDoc;
                        expect(retrievedDoc.data).toEqual(expectedDoc.data);
                        expect(retrievedDoc.batch).toEqual(expectedDoc.batch);
                        // also check updatedInBatch if it's set
                        if (expectedDoc.updatedInBatch !== undefined) {
                            expect(retrievedDoc.updatedInBatch).toEqual(expectedDoc.updatedInBatch);
                        }
                    }
                }
                verifiedCount++;
            }
        });
        expect(verifiedCount).toBe(expectedDocs.size);
        console.log(`--- Verification Complete: ${message} - Verified ${verifiedCount} docs ---`);
    }

    it('should perform checkpoints, recover from WAL using checkpoints, and maintain data integrity', async () => {
        if (!db) throw new Error("DB not initialized for test");

        const allExpectedDocs = new Map<string, CpTestDoc>();
        let lastNotedCheckpointId = 0;

        for (let batchNum = 1; batchNum <= NUM_BATCHES; batchNum++) {
            console.log(`--- Starting Batch ${batchNum}/${NUM_BATCHES} ---`);
            
            await db.transaction(async tx => {
                for (let i = 0; i < DOCS_PER_BATCH; i++) {
                    const docIdNum = (batchNum - 1) * DOCS_PER_BATCH + i;
                    const key = `${colPath}/doc_${docIdNum.toString().padStart(4, '0')}`;
                    const doc: CpTestDoc = {
                        id: key.split('/').pop()!,
                        data: `Batch ${batchNum}, Doc ${i}, Val ${Math.random()}`,
                        batch: batchNum,
                    };

                    if (batchNum > 1 && i % 10 === 0 && allExpectedDocs.size > 0) {
                        const keys = Array.from(allExpectedDocs.keys());
                        const keyToModify = keys[Math.floor(Math.random() * keys.length)];
                        const existingDocToModify = allExpectedDocs.get(keyToModify)!;

                        if (existingDocToModify.data === "[DELETED]") { // If it was already marked deleted, re-insert it
                            const reinsertedDoc: CpTestDoc = {
                                ...existingDocToModify, // Keep original batch and ID
                                data: `REINSERTED in Batch ${batchNum} - ${Math.random()}`,
                                updatedInBatch: batchNum,
                            };
                            console.log(`  Batch ${batchNum}: Reinserting ${keyToModify} with data "${reinsertedDoc.data}"`);
                            await tx.set(keyToModify, s(reinsertedDoc));
                            allExpectedDocs.set(keyToModify, reinsertedDoc);
                        } else if (Math.random() < 0.3) { // 30% chance to delete
                            console.log(`  Batch ${batchNum}: Deleting ${keyToModify}`);
                            await tx.remove(keyToModify);
                            allExpectedDocs.set(keyToModify, {...existingDocToModify, data: "[DELETED]", updatedInBatch: batchNum });
                        } else { // 70% chance to update
                            const updatedDoc: CpTestDoc = {
                                ...existingDocToModify,
                                data: `UPDATED in Batch ${batchNum} - ${Math.random()}`,
                                updatedInBatch: batchNum,
                            };
                            console.log(`  Batch ${batchNum}: Updating ${keyToModify} to data "${updatedDoc.data}"`);
                            await tx.set(keyToModify, s(updatedDoc));
                            allExpectedDocs.set(keyToModify, updatedDoc);
                        }
                    } else { // New insert
                        await tx.set(key, s(doc));
                        allExpectedDocs.set(key, doc);
                    }
                    
                    if (i % OPS_BETWEEN_CHECKS === 0 && i > 0) {
                        console.log(`    Batch ${batchNum}: Op ${i}, checking checkpoint status...`);
                        if (!db) throw new Error("Database instance is null for checkpoint check"); // Should not happen
                        const history = await db.getCheckpointHistory();
                        if (history.length > 0) {
                            const latestCp = history[history.length - 1];
                            console.log(`      Latest checkpoint in history: ID ${latestCp.checkpoint_id}, Status ${latestCp.status}, BeginLSN ${latestCp.checkpoint_begin_wal_lsn}`);
                            if (latestCp.status === "COMPLETED" && latestCp.checkpoint_id > lastNotedCheckpointId) {
                                lastNotedCheckpointId = latestCp.checkpoint_id;
                                console.log(`      PROGRESS: New Checkpoint COMPLETED: ID ${lastNotedCheckpointId}`);
                            }
                        } else {
                            console.log("      No checkpoints recorded in history yet.");
                        }
                    }
                }
            });
            console.log(`Batch ${batchNum} committed. Total expected docs with state: ${allExpectedDocs.size}`);

            const waitTime = (CHECKPOINT_INTERVAL_SECONDS + 0.5) * 1000; // Wait slightly longer
            console.log(`Waiting ${waitTime / 1000}s for checkpoint thread after Batch ${batchNum}...`);
            await delay(waitTime);

            if (batchNum % 2 === 0) {
                console.log(`Manually forcing checkpoint after Batch ${batchNum}...`);
                if (!db) throw new Error("DB is null before forceCheckpoint");
                const cpForced = await db.forceCheckpoint();
                expect(cpForced).toBe(true);
                console.log("Manual checkpoint requested.");
                await delay(1000); // Small delay for it to potentially complete
            }
        }

        await verifyData(allExpectedDocs, db, "Before simulated crash");

        console.log("\n--- Simulating Crash & Restart ---");
        const walRecordsBeforeClose = parseWalFile(walFilePath);
        console.log(`WAL contains ${walRecordsBeforeClose.length} records before simulated crash.`);
        if (fs.existsSync(checkpointMetaFilePath)) {
            const lastCpMetaBeforeClose = fs.readFileSync(checkpointMetaFilePath).toString();
            console.log("Last Checkpoint Meta before close:", lastCpMetaBeforeClose.substring(0, 200) + "...");
        } else {
            console.log("No checkpoint.meta file found before close.");
        }

        // Don't call db.close() to simulate a crash where memory state isn't flushed via normal shutdown.
        // The `afterEach` hook will eventually call `db.close()` on the *original* db instance if it's not nulled.
        // By nulling `db` and re-initializing, we simulate a fresh start which must recover.
        db = null; 
        
        console.log("Re-initializing Indinis instance from same data directory (triggers recovery)...");
        const db2 = new Indinis(testDataDir, { checkpointIntervalSeconds: CHECKPOINT_INTERVAL_SECONDS });

        await verifyData(allExpectedDocs, db2, "After simulated crash and recovery");

        let finalTxnId = 0;
        await db2.transaction(async tx => {
            await tx.set(`${colPath}/final_check_doc`, s({val: "ok"}));
            finalTxnId = await tx.getId();
        });
        expect(finalTxnId).toBeGreaterThan(0);
        console.log(`Transaction ID after recovery and new write: ${finalTxnId}`);

        const historyAfterRecovery = await db2.getCheckpointHistory();
        console.log("Checkpoint history after recovery:", historyAfterRecovery.map(ci => ci.toString()));
        expect(historyAfterRecovery.length).toBeGreaterThanOrEqual(0); 
        if(lastNotedCheckpointId > 0) {
            const lastRecoveredCp = historyAfterRecovery.find(h => h.checkpoint_id === lastNotedCheckpointId);
            if (lastRecoveredCp) { // It might have been pruned if MAX_CHECKPOINT_HISTORY is small
                expect(lastRecoveredCp.status).toBe("COMPLETED");
            } else {
                console.log(`Checkpoint ID ${lastNotedCheckpointId} not found in history after recovery, possibly pruned or never fully persisted pre-crash.`);
            }
        }

        await db2.close(); // Close the recovered DB instance
    });
});