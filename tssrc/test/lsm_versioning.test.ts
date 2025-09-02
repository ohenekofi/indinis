import { Indinis, IndinisOptions, LsmOptions, RcuStatsJs } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-rcu-memtable');

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const s = (obj: any): string => JSON.stringify(obj);

describe('LSM-Tree RCU MemTable Concurrency and Statistics', () => {
    let db: Indinis | null;
    let testDataDir: string;
    const colPath = 'rcu_docs';

    jest.setTimeout(60000);

    // This runs ONCE before any tests in this file execute.
    // Its purpose is to create the main parent directory for all test runs.
    beforeAll(async () => {
        // Ensure the base directory for this entire test suite exists.
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    // This runs ONCE after all tests in this file have completed.
    // Its purpose is to clean up the main parent directory.
    afterAll(async () => {
        console.log("[RCU SUITE END] Cleaning up base directory...");
        if (fs.existsSync(TEST_DATA_DIR_BASE)) {
            await rimraf(TEST_DATA_DIR_BASE);
        }
    });

    // This runs BEFORE EACH `it(...)` test case.
    // It creates a unique, isolated directory and a new database instance for every single test.
    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-run-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        
        // The DB instance is created here but the test case itself will provide the options.
        // We set `db` here so it's available in `afterEach`. The test will re-assign it.
        // This is a common pattern, but we'll adapt the test to not need this pre-assignment.
        console.log(`\n[RCU TEST START] Using data directory: ${testDataDir}`);
    });

    // This runs AFTER EACH `it(...)` test case.
    // It guarantees that the database is closed and the unique test directory is deleted.
    afterEach(async () => {
        if (db) {
            console.log(`[RCU TEST END] Closing database for dir: ${testDataDir}`);
            await db.close();
            db = null; // Clear the reference
        }
        if (fs.existsSync(testDataDir)) {
            console.log(`[RCU TEST END] Cleaning up test directory: ${testDataDir}`);
            await rimraf(testDataDir);
        }
    });

    // The test case itself, which now correctly uses the `db` variable scoped within it.
    it('should use RCU memtable and allow lock-free reads while exposing accurate stats', async () => {
        const lsmOptions: LsmOptions = {
            defaultMemTableType: 'RCU',
            rcuOptions: { gracePeriodMilliseconds: 200 }
        };
        // Initialize the `db` for this specific test run.
        db = new Indinis(testDataDir, { lsmOptions });
        
        // ... (the rest of the test case logic is unchanged) ...
        console.log("--- Phase 1: Writing initial data ---");
        await db.transaction(async tx => {
            for (let i = 0; i < 10; i++) {
                await tx.set(`${colPath}/doc_${i}`, s({ version: 1, data: `initial_${i}`}));
            }
        });
        
        let stats: RcuStatsJs | null = await db!.debug_getLsmRcuStats(colPath);
        console.log("Initial Stats:", stats);
        expect(stats).not.toBeNull();
        if (stats) {
            expect(stats.totalWrites).toBeGreaterThanOrEqual(10n);
        }

        console.log("\n--- Phase 2: Starting long-running read transaction ---");
        let longReadFinished = false;
        const longReadPromise = db.transaction(async tx => {
            const results = await tx.getPrefix(colPath);
            expect(results.length).toBe(10);
            const statsDuringRead = await db!.debug_getLsmRcuStats(colPath);
            expect(statsDuringRead?.activeReaders).toBeGreaterThanOrEqual(1);
            await delay(4000);
        }).then(() => { longReadFinished = true; });

        await delay(500);

        console.log("\n--- Phase 3: Starting concurrent writes ---");
        const writePromises = Array.from({ length: 20 }).map(() => 
            db!.transaction(async tx => {
                const keyIndex = Math.floor(Math.random() * 10);
                await tx.set(`${colPath}/doc_${keyIndex}`, s({ version: 2 }));
            })
        );
        
        await Promise.all(writePromises);
        await longReadPromise;
        console.log("All reads and writes have completed.");

        stats = await db!.debug_getLsmRcuStats(colPath);
        expect(stats?.activeReaders).toBe(0);

        console.log("\n--- Phase 4: Waiting for grace period and reclamation ---");
        await delay(500);
        await db.transaction(async tx => { await tx.set('final/key', 'final_val'); });
        await delay(200);

        stats = await db!.debug_getLsmRcuStats(colPath);
        expect(stats?.pendingReclamations).toBe(0);
    });
});