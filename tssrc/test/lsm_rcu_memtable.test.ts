// @tssrc/test/lsm_rcu_memtable.test.ts
import { Indinis, IndinisOptions, LsmOptions } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-rcu-memtable');

const s = (obj: any): string => JSON.stringify(obj);

describe('LSM-Tree RCU MemTable - Basic Functionality', () => {
    let db: Indinis | null;
    let testDataDir: string;
    const colPath = 'rcu_docs';

    jest.setTimeout(30000);

    // --- Standard Setup and Teardown ---
    // These hooks ensure each test runs in a clean, isolated environment.
    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) {
            await rimraf(TEST_DATA_DIR_BASE);
        }
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-run-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        console.log(`\n[RCU BASELINE TEST START] Using data directory: ${testDataDir}`);
    });

    afterEach(async () => {
        if (db) {
            await db.close();
            db = null;
        }
        if (fs.existsSync(testDataDir)) {
            await rimraf(testDataDir);
        }
    });

    // --- The Single, Simplified Test Case ---
    it('should perform basic CRUD and prefix scan operations when RCU memtable is selected', async () => {
        // Step 1: Initialize the database with the RCU memtable type specified.
        const lsmOptions: LsmOptions = { defaultMemTableType: 'RCU' };
        db = new Indinis(testDataDir, { lsmOptions });
        console.log("  [Step 1] Database initialized with RCU memtable.");

        const key1 = `${colPath}/item1`;
        const val1 = { version: 1, data: 'initial data' };
        const val2 = { version: 2, data: 'updated data' };
        
        // Step 2: Create a document and verify it can be read back.
        console.log("  [Step 2] Testing Create and Read...");
        await db.transaction(async tx => {
            await tx.set(key1, s(val1));
        });
        
        await db.transaction(async tx => {
            const retrieved = await tx.get(key1);
            expect(JSON.parse(retrieved as string)).toEqual(val1);
        });
        console.log("  Create and Read successful.");
        
        // Step 3: Update the document and verify the new value.
        console.log("  [Step 3] Testing Update...");
        await db.transaction(async tx => {
            await tx.set(key1, s(val2), { overwrite: true });
        });

        await db.transaction(async tx => {
            const retrieved = await tx.get(key1);
            expect(JSON.parse(retrieved as string)).toEqual(val2);
        });
        console.log("  Update successful.");

        // Step 4: Verify prefix scan functionality (proves iterator support).
        console.log("  [Step 4] Testing Prefix Scan...");
        const scanPrefix = `${colPath}/scan/`;
        const keyP1 = `${scanPrefix}a`;
        const keyP2 = `${scanPrefix}b`;
        await db.transaction(async tx => {
            await tx.set(keyP1, "scan_value_A");
            await tx.set(keyP2, "scan_value_B");
        });

        await db.transaction(async tx => {
            const results = await tx.getPrefix(scanPrefix);
            expect(results.length).toBe(2);
            expect(results.map(r => r.key).sort()).toEqual([keyP1, keyP2]);
        });
        console.log("  Prefix Scan successful.");
        
        // Step 5: Delete the original document and verify it is gone.
        console.log("  [Step 5] Testing Delete...");
        await db.transaction(async tx => {
            const wasRemoved = await tx.remove(key1);
            expect(wasRemoved).toBe(true);
        });

        await db.transaction(async tx => {
            const retrieved = await tx.get(key1);
            expect(retrieved).toBeNull();
        });
        console.log("  Delete successful.");
        console.log("  [SUCCESS] RCU memtable passed all basic functional checks.");
    });
});