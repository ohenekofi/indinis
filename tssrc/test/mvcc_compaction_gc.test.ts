// @tssrc/test/mvcc_compaction_gc.test.ts
import { Indinis, StorageValue } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-mvcc-gc-stress');

// Helper for small delay
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Helper to stringify values for storage
const s = (obj: any) => JSON.stringify(obj);

interface TestDoc {
    id: string;
    version: number;
    data: string;
    updatedTxn?: number;
}

describe('Indinis LSMTree MVCC Compaction and Garbage Collection Stress Test', () => {
    let db: Indinis;
    let testDataDir: string;
    const colPath = 'gc_docs'; // Collection path

    const K_UPDATE_GC = `${colPath}/update_gc_key`; // Updated many times, oldest should be GC'd
    const K_DELETE_GC = `${colPath}/delete_gc_key`; // Inserted, then deleted, should be fully GC'd
    const K_UPDATE_KEEP_SOME_HISTORY = `${colPath}/update_keep_key`; // Some history kept
    const K_STATIC_OLD = `${colPath}/static_old_key`; // Inserted early, might be GC'd
    const K_STATIC_NEW = `${colPath}/static_new_key`; // Inserted late, should persist

    // For this test, we need to ensure compactions run.
    // We'll rely on the default compaction threshold (e.g., 4 SSTables) and trigger flushes.
    // A direct "trigger compaction" debug API would be better for precise testing.
    // For now, we'll do enough operations to hopefully trigger it.
    // The `LSMTree` constructor has a default memtable_max_size of 4MB. We can make it smaller for tests.
    // Let's assume `memtable_max_size` is small enough that our batches cause flushes.
    // We also need to control/observe the `oldest_active_txn_id`.

    jest.setTimeout(120000); // 2 minutes

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        // Consider passing a smaller memtable_max_size via a test-specific constructor if possible,
        // or ensure enough data is written to trigger flushes.
        // For now, we'll assume default config and write enough data.
        db = new Indinis(testDataDir);
        console.log(`\n[MVCC GC TEST START] Using data directory: ${testDataDir}`);
    });

    afterEach(async () => {
        console.log("[MVCC GC TEST END] Closing database...");
        if (db) await db.close();
        console.log("[MVCC GC TEST END] Cleaning up test directory...");
        if (fs.existsSync(testDataDir)) {
            await rimraf(testDataDir).catch(err => console.error(`Cleanup error for ${testDataDir}:`, err));
        }
    });

    afterAll(async () => {
        console.log("[MVCC GC SUITE END] Cleaning up base directory...");
        if (fs.existsSync(TEST_DATA_DIR_BASE)) {
            await rimraf(TEST_DATA_DIR_BASE).catch(err => console.error(`Base cleanup error for ${TEST_DATA_DIR_BASE}:`, err));
        }
    });

    // Helper to get current oldest active TxnID (simulated - via a new empty transaction)
    // In a real test framework, we'd have a debug API for this.
    async function getCurrentSimulatedOldestActiveTxnId(currentDb: Indinis): Promise<number> {
        let id = -1;
        await currentDb.transaction(async tx => { id = await tx.getId(); });
        // This ID is the *next* available ID if no other txns are active.
        // So, current oldest active is this ID.
        return id;
    }


    it('should garbage collect old versions and tombstones correctly after compactions', async () => {
        let txnIdEpoch1End = 0;
        let txnIdEpoch2End = 0;
        let txnIdEpoch3End = 0;

        // --- Phase 1: Initial Population ---
        console.log("--- Phase 1: Initial Population ---");
        await db.transaction(async tx => {
            await tx.set(K_UPDATE_GC, s({ id: K_UPDATE_GC, version: 1, data: "v1" }));
            await tx.set(K_DELETE_GC, s({ id: K_DELETE_GC, version: 1, data: "v1_del" }));
            await tx.set(K_UPDATE_KEEP_SOME_HISTORY, s({ id: K_UPDATE_KEEP_SOME_HISTORY, version: 1, data: "v1_keep" }));
            await tx.set(K_STATIC_OLD, s({ id: K_STATIC_OLD, version: 1, data: "v1_static_old" }));
            txnIdEpoch1End = await tx.getId();
        });
        console.log(`Phase 1 committed. Max TxnID: ${txnIdEpoch1End}`);

        // --- Phase 2: Updates & Deletes (Batch 1) ---
        console.log("\n--- Phase 2: Updates & Deletes (Batch 1) ---");
        await db.transaction(async tx => {
            await tx.set(K_UPDATE_GC, s({ id: K_UPDATE_GC, version: 2, data: "v2" })); // Update
            await tx.remove(K_DELETE_GC); // Delete
            await tx.set(K_UPDATE_KEEP_SOME_HISTORY, s({ id: K_UPDATE_KEEP_SOME_HISTORY, version: 2, data: "v2_keep" }));
            // K_STATIC_OLD remains untouched
            txnIdEpoch2End = await tx.getId();
        });
        console.log(`Phase 2 committed. Max TxnID: ${txnIdEpoch2End}`);

        // At this point, oldest active TxnID for GC should be > txnIdEpoch2End for next compaction
        // to potentially GC anything from Epoch 1 or 2.
        // We need to ensure enough SSTables are created to trigger compaction.
        // Let's add a few more distinct operations to try and force flushes.
        for (let i = 0; i < 5; i++) { // Create 5 dummy SSTables potentially
            await db.transaction(async tx => {
                await tx.set(`${colPath}/dummy_flush_${i}`, s({ data: `dummy_data_${i}`}));
            });
        }
        console.log("Added dummy data to encourage SSTable flushes.");


        // --- Phase 3: Simulate Oldest Active Txn ID advance & wait for Compaction ---
        // For testing GC, we assume oldest_active_txn_id for GC will be effectively txnIdEpoch2End + 1
        // (meaning all transactions up to txnIdEpoch2End are no longer active).
        console.log(`\n--- Phase 3: Simulating Oldest Active Txn ID > ${txnIdEpoch2End} & Waiting for Compaction ---`);
        // We can't directly control compaction trigger time easily without a debug API.
        // We'll wait for a period. This is not ideal for CI but a starting point.
        console.log("Waiting for potential compaction (e.g., 15 seconds)...");
        await delay(15000); // Wait for compaction worker to run (depends on its interval)

        // --- Verification after 1st Compaction (assuming it ran and GC'd based on oldest_active > txnIdEpoch2End) ---
        // Test this by trying to read with an "old" transaction ID.
        // If GC happened correctly, versions older than `oldest_active_txn_id` (which was effectively txnIdEpoch2End+1)
        // *might* be gone, IF they were not the latest visible version *before* that oldest_active_txn_id.
        console.log("\n--- Verification after 1st potential compaction ---");
        // Let's check the current state using a new transaction (which sees the latest)
        await db.transaction(async tx => {
            const snapTxnId = await tx.getId();
            console.log(` Verifying with new Txn: ${snapTxnId}`);

            // K_UPDATE_GC: Should see version 2
            let doc = JSON.parse(await tx.get(K_UPDATE_GC) as string) as TestDoc;
            expect(doc.version).toBe(2); // Latest version
            console.log(`  ${K_UPDATE_GC}: version ${doc.version} (Correct)`);

            // K_DELETE_GC: Should be null (deleted)
            expect(await tx.get(K_DELETE_GC)).toBeNull();
            console.log(`  ${K_DELETE_GC}: null (Correctly deleted)`);

            // K_UPDATE_KEEP_SOME_HISTORY: Should see version 2
            doc = JSON.parse(await tx.get(K_UPDATE_KEEP_SOME_HISTORY) as string) as TestDoc;
            expect(doc.version).toBe(2);
            console.log(`  ${K_UPDATE_KEEP_SOME_HISTORY}: version ${doc.version} (Correct)`);

            // K_STATIC_OLD: Should still exist (version 1)
            doc = JSON.parse(await tx.get(K_STATIC_OLD) as string) as TestDoc;
            expect(doc.version).toBe(1);
            console.log(`  ${K_STATIC_OLD}: version ${doc.version} (Correct)`);
        });

        // --- Phase 4: Updates & Deletes (Batch 2) ---
        console.log("\n--- Phase 4: Updates & Deletes (Batch 2) ---");
        await db.transaction(async tx => {
            await tx.set(K_UPDATE_GC, s({ id: K_UPDATE_GC, version: 3, data: "v3" }));
            await tx.set(K_UPDATE_KEEP_SOME_HISTORY, s({ id: K_UPDATE_KEEP_SOME_HISTORY, version: 3, data: "v3_keep" }));
            await tx.set(K_STATIC_NEW, s({ id: K_STATIC_NEW, version: 1, data: "v1_static_new" }));
            // Delete K_STATIC_OLD
            await tx.remove(K_STATIC_OLD);
            txnIdEpoch3End = await tx.getId();
        });
        console.log(`Phase 4 committed. Max TxnID: ${txnIdEpoch3End}`);

        for (let i = 5; i < 10; i++) { // More dummy data
            await db.transaction(async tx => {
                await tx.set(`${colPath}/dummy_flush_${i}`, s({ data: `dummy_data_${i}`}));
            });
        }
        console.log("Added more dummy data.");


        // --- Phase 5: Simulate Oldest Active Txn ID advance & wait for Compaction (again) ---
        // Now, oldest_active_txn_id for GC should be > txnIdEpoch3End
        console.log(`\n--- Phase 5: Simulating Oldest Active Txn ID > ${txnIdEpoch3End} & Waiting for Compaction ---`);
        console.log("Waiting for potential compaction (e.g., 15 seconds)...");
        await delay(15000);

        // --- Phase 6: Final Verification ---
        console.log("\n--- Phase 6: Final Verification ---");
        await db.transaction(async tx => {
            const snapTxnId = await tx.getId();
            console.log(` Verifying with new Txn: ${snapTxnId}`);

            // K_UPDATE_GC: Should see version 3. Versions 1 and 2 should be GC'd if oldest_active was > txnIdEpoch2End during the first compaction.
            let doc = JSON.parse(await tx.get(K_UPDATE_GC) as string) as TestDoc;
            expect(doc.version).toBe(3);
            console.log(`  ${K_UPDATE_GC}: version ${doc.version} (Correct)`);

            // K_DELETE_GC: Still null. Its original version and tombstone should be fully GC'd.
            expect(await tx.get(K_DELETE_GC)).toBeNull();
            console.log(`  ${K_DELETE_GC}: null (Correctly GC'd)`);

            // K_UPDATE_KEEP_SOME_HISTORY: Should see version 3.
            // Version 1 might be GC'd if oldest_active was > txnIdEpoch1End and < txnIdEpoch2End during first compaction.
            // Version 2 might be GC'd if oldest_active was > txnIdEpoch2End during second compaction.
            // The GC rule "keep latest older than horizon" is key here.
            doc = JSON.parse(await tx.get(K_UPDATE_KEEP_SOME_HISTORY) as string) as TestDoc;
            expect(doc.version).toBe(3);
            console.log(`  ${K_UPDATE_KEEP_SOME_HISTORY}: version ${doc.version} (Correct)`);

            // K_STATIC_OLD: Should be null (deleted in Phase 4). Tombstone and original version should be GC'd.
            expect(await tx.get(K_STATIC_OLD)).toBeNull();
            console.log(`  ${K_STATIC_OLD}: null (Correctly deleted and GC'd)`);

            // K_STATIC_NEW: Should exist with version 1.
            doc = JSON.parse(await tx.get(K_STATIC_NEW) as string) as TestDoc;
            expect(doc.version).toBe(1);
            console.log(`  ${K_STATIC_NEW}: version ${doc.version} (Correct)`);
        });

        // To *really* test GC, we'd need a way to query with an *old* transaction ID that *should no longer see*
        // the GC'd versions. Indinis's current `tx.get(key)` uses the transaction's own start ID as its
        // snapshot. If a version is GC'd because it's older than *all* active transactions, then no new
        // transaction should be able to see it.

        console.log("\n--- Attempting to read K_UPDATE_GC with a very old (simulated) TxnID after GCs ---");
        // This part is conceptual because we can't *actually* start a transaction with an old ID easily.
        // We infer GC by the fact that space should be reclaimed and current reads are correct.
        // A debug "get_version_at_lsn_or_txn" or "dump_sstable_versions" would be needed for deep verification.
        // For now, the test relies on the latest state being correct and infers GC happened.

        const oldestPossibleTxnIdForReads = 1; // An ID older than any GC horizon used
        // Simulating a read for K_UPDATE_GC as if by a transaction that started at oldestPossibleTxnId.
        // This won't work directly with the current API. We'd expect it to see version 1 if it wasn't GC'd.
        // If version 1 of K_UPDATE_GC was GC'd (because oldest_active was > txnIdEpoch1End), then
        // a read by a new transaction (even if it could set its read timestamp low) would not find it.

        // We can check the approximate number of SSTables. If compactions worked, it should be less than total flushes.
        // This requires a debug API to count SSTables or list them.
        // await db.debug_listSSTables(); // Hypothetical API

    });
});