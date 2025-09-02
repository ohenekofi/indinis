// @filename test/mvcc_timetravel.test.ts
import { Indinis, StorageValue, ITransactionContext } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-mvcc');

// Helper function for small delay
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

describe('Indinis MVCC and Time Travel', () => {
    let db: Indinis;
    let testDataDir: string;

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        db = new Indinis(testDataDir);
        console.log(`\n[TEST START] Using data directory: ${testDataDir}`);
    });

    afterEach(async () => {
        console.log("[TEST END] Closing database...");
        if (db) await db.close();
        console.log("[TEST END] Cleaning up test directory...");
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir);
    }, 20000); // Longer timeout for cleanup

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    it('Scenario 1: Read sees snapshot, not concurrent committed write (Snapshot Isolation)', async () => {
        const key = 'mvccKey1';
        const valueV0 = 'valueV0'; // Initial value
        const valueV1 = 'valueV1'; // Value written by Txn B

        // Setup: Txn 0 writes initial value
        let txnIdT0 = -1;
        await db.transaction(async tx => {
            await tx.set(key, valueV0);
            txnIdT0 = await tx.getId();
        });
        console.log(`Setup: Committed initial value '${valueV0}' with Txn ID ${txnIdT0}`);

        let txnIdTA = -1, txnIdTB = -1, txnIdTC = -1;
        let read1TA: StorageValue | null = null;
        let read2TA: StorageValue | null = null;
        let resolveTxnAWait: () => void;
        const txnA_waitPromise = new Promise<void>(resolve => { resolveTxnAWait = resolve; });

        // Start Txn A
        console.log("Starting Txn A...");
        const txnA_Promise = db.transaction(async txA => {
            txnIdTA = await txA.getId();
            console.log(`  Txn A started (ID: ${txnIdTA})`);
            read1TA = await txA.get(key);
            console.log(`  Txn A: Read 1 = ${read1TA}`);
            console.log("  Txn A: Waiting...");
            await txnA_waitPromise; // Wait for external signal
            read2TA = await txA.get(key);
            console.log(`  Txn A: Read 2 = ${read2TA}`);
            console.log("  Txn A: Finishing commit.");
        });

        await delay(100); // Ensure Txn A performs its first read

        // Start Txn B, write, and commit *while Txn A is waiting*
        console.log("Starting Txn B...");
        await db.transaction(async txB => {
            txnIdTB = await txB.getId();
            console.log(`  Txn B started (ID: ${txnIdTB})`);
            await txB.set(key, valueV1);
            console.log(`  Txn B: Set key to '${valueV1}'. Committing...`);
        });
        console.log("Txn B committed.");

        // Allow Txn A to proceed and commit
        console.log("Allowing Txn A to continue...");
        resolveTxnAWait!();
        await txnA_Promise;
        console.log("Txn A finished.");

        // Assertions for Txn A's reads
        expect(read1TA).toBe(valueV0); // Should see the value committed *before* it started
        expect(read2TA).toBe(valueV0); // Should *still* see the value from its snapshot, not Txn B's write

        // Start Txn C *after* both A and B committed
        console.log("Starting Txn C...");
        let read1TC: StorageValue | null = null;
        await db.transaction(async txC => {
            txnIdTC = await txC.getId();
            console.log(`  Txn C started (ID: ${txnIdTC})`);
            read1TC = await txC.get(key);
            console.log(`  Txn C: Read 1 = ${read1TC}`);
        });
        console.log("Txn C finished.");

        // Assertion for Txn C's read
        expect(read1TC).toBe(valueV1); // Should see Txn B's committed write
    });

    it('Scenario 2: Read sees snapshot, not concurrent committed delete (Tombstone Visibility)', async () => {
        const key = 'mvccKeyDelete';
        const valueV0 = 'valueToDelete';

        // Setup: Txn 0 writes initial value
        let txnIdT0 = -1;
        await db.transaction(async tx => {
            await tx.set(key, valueV0);
            txnIdT0 = await tx.getId();
        });
        console.log(`Setup: Committed initial value '${valueV0}' with Txn ID ${txnIdT0}`);

        let txnIdTA = -1, txnIdTB = -1, txnIdTC = -1;
        let read1TA: StorageValue | null = null;
        let read2TA: StorageValue | null = null;
        let resolveTxnAWait: () => void;
        const txnA_waitPromise = new Promise<void>(resolve => { resolveTxnAWait = resolve; });

        // Start Txn A
        console.log("Starting Txn A...");
        const txnA_Promise = db.transaction(async txA => {
            txnIdTA = await txA.getId();
            console.log(`  Txn A started (ID: ${txnIdTA})`);
            read1TA = await txA.get(key);
            console.log(`  Txn A: Read 1 = ${read1TA}`);
            console.log("  Txn A: Waiting...");
            await txnA_waitPromise;
            read2TA = await txA.get(key);
            console.log(`  Txn A: Read 2 = ${read2TA}`);
            console.log("  Txn A: Finishing commit.");
        });

        await delay(100); // Ensure Txn A performs its first read

        // Start Txn B, *delete* the key, and commit *while Txn A is waiting*
        console.log("Starting Txn B...");
        await db.transaction(async txB => {
            txnIdTB = await txB.getId();
            console.log(`  Txn B started (ID: ${txnIdTB})`);
            const removed = await txB.remove(key);
            console.log(`  Txn B: Removed key (Success: ${removed}). Committing...`);
            expect(removed).toBe(true);
        });
        console.log("Txn B committed.");

        // Allow Txn A to proceed and commit
        console.log("Allowing Txn A to continue...");
        resolveTxnAWait!();
        await txnA_Promise;
        console.log("Txn A finished.");

        // Assertions for Txn A's reads
        expect(read1TA).toBe(valueV0); // Should see the value committed before it started
        expect(read2TA).toBe(valueV0); // Should *still* see the value from its snapshot, not the delete

        // Start Txn C *after* both A and B committed
        console.log("Starting Txn C...");
        let read1TC: StorageValue | null = null;
        await db.transaction(async txC => {
            txnIdTC = await txC.getId();
            console.log(`  Txn C started (ID: ${txnIdTC})`);
            read1TC = await txC.get(key);
            console.log(`  Txn C: Read 1 = ${read1TC}`);
        });
        console.log("Txn C finished.");

        // Assertion for Txn C's read
        expect(read1TC).toBeNull(); // Should see the delete (tombstone) from Txn B
    });

});