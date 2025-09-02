// @filename: tssrc/test/wal_recovery.test.ts
import { Indinis, StorageValue } from '../index'; // Main Indinis class import
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf'; // For cleaning up test directories

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-wal-recovery-v2'); // Use a new version suffix

// Helper function directly in the module scope for this test file
function testUtf8ToBuffer(str: string): Uint8Array {
    return new TextEncoder().encode(str);
}


describe('Indinis WAL Durability and Recovery Tests (v2)', () => {
    let testDataDir: string;
    let db: Indinis | null = null;

    const initializeDb = async (dirSuffix: string): Promise<Indinis> => {
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${dirSuffix}-${Date.now()}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        console.log(`[WAL TEST INIT] Using data directory: ${testDataDir}`);
        return new Indinis(testDataDir);
    };

    const closeDb = async () => {
        if (db) {
            console.log(`[WAL TEST CLOSE] Closing database instance...`);
            await db.close();
            db = null;
            console.log(`[WAL TEST CLOSE] Database instance closed.`);
        }
    };

    const cleanupTestDataDir = async () => {
        if (testDataDir && fs.existsSync(testDataDir)) {
            console.log(`[WAL TEST CLEANUP] Cleaning up test directory: ${testDataDir}`);
            await rimraf(testDataDir).catch(err => console.error(`Cleanup error for ${testDataDir}:`, err));
        }
    };

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    afterEach(async () => {
        await closeDb();
        await cleanupTestDataDir();
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) {
            await rimraf(TEST_DATA_DIR_BASE).catch(err => console.error(`Base cleanup error for ${TEST_DATA_DIR_BASE}:`, err));
        }
    });

    // --- Test Cases ---

    it('Scenario 1: Data from a committed transaction should be recovered after restart', async () => {
        db = await initializeDb('s1-commit-recover');
        const key1 = 'users/user1';
        const value1 = JSON.stringify({ name: 'Alice', email: 'alice@example.com' });
        const key2 = 'users/user2';
        const value2 = JSON.stringify({ name: 'Bob', city: 'New York' });

        await db.transaction(async tx => {
            await tx.set(key1, value1);
            await tx.set(key2, value2);
        });
        console.log('S1: Initial transaction committed.');

        await closeDb();
        db = new Indinis(testDataDir); // Reopen with the same path
        console.log('S1: Database reopened, WAL recovery should have run.');

        await db.transaction(async tx => {
            const retrievedValue1 = await tx.get(key1);
            const retrievedValue2 = await tx.get(key2);
            expect(retrievedValue1).toBe(value1);
            expect(retrievedValue2).toBe(value2);
        });
        console.log('S1: Data verified after restart.');
    });

    it('Scenario 2: Data from an aborted transaction should NOT be recovered', async () => {
        db = await initializeDb('s2-abort-norecover');
        const keyCommitted = 'config/settings';
        const valueCommitted = JSON.stringify({ theme: 'dark' });
        const keyAborted = 'temp/data';
        const valueAborted = 'this should not be saved';

        await db.transaction(async tx => {
            await tx.set(keyCommitted, valueCommitted);
        });
        console.log('S2: Committed transaction finished.');

        try {
            await db.transaction(async tx => {
                await tx.set(keyAborted, valueAborted);
                throw new Error("Simulated failure to abort transaction");
            });
        } catch (e: any) {
            // Ensure the error is the one we threw, or at least that an error occurred
            expect(e.message).toContain("Simulated failure");
        }
        console.log('S2: Aborted transaction attempt finished.');

        await closeDb();
        db = new Indinis(testDataDir);
        console.log('S2: Database reopened.');

        await db.transaction(async tx => {
            expect(await tx.get(keyCommitted)).toBe(valueCommitted);
            expect(await tx.get(keyAborted)).toBeNull();
        });
        console.log('S2: Data verified after restart.');
    });

    it('Scenario 3: Index updates from a committed transaction should be recovered', async () => {
        db = await initializeDb('s3-index-recover');
        const usersPath = 'r_users'; // Use distinct path to avoid conflict with other tests if run in parallel on same FS root
        const emailIndex = 'idx_r_email_v2';
        await db.createIndex(usersPath, emailIndex, { field: 'email' });
        console.log('S3: Email index created.');

        const key1 = `${usersPath}/u100`;
        const user1Data = { name: 'Charlie Recovery', email: 'charlie.recovery.v2@example.com' };
        const user1Value = JSON.stringify(user1Data);

        await db.transaction(async tx => {
            await tx.set(key1, user1Value);
        });
        console.log('S3: User data committed.');

        await closeDb();
        db = new Indinis(testDataDir);
        console.log('S3: Database reopened.');

        await db.transaction(async tx => {
            expect(await tx.get(key1)).toBe(user1Value);
        });

        // Use the local testUtf8ToBuffer helper
        const indexResults = await db.debug_scanBTree(
            emailIndex,
            Buffer.from(testUtf8ToBuffer(user1Data.email)), // Start key part
            Buffer.from(testUtf8ToBuffer(user1Data.email))  // End key part
        );
        expect(indexResults.length).toBeGreaterThanOrEqual(1);
        console.log(`S3: Debug scan for email '${user1Data.email}' found ${indexResults.length} raw BTree entries.`);
    });


    it('Scenario 4: Operations across multiple WAL file segments should be recovered', async () => {
        db = await initializeDb('s4-multi-segment');
        const numRecords = 200;
        const keys: string[] = [];

        console.log(`S4: Writing ${numRecords} records...`);
        for (let i = 0; i < numRecords; i++) {
            const key = `bulk_v2/item${i.toString().padStart(4, '0')}`;
            const value = JSON.stringify({ data: `value for item ${i} v2`, iteration: i });
            keys.push(key);
            await db.transaction(async tx => {
                await tx.set(key, value);
            });
            if (i % 50 === 0 && i > 0) console.log(`  S4: Committed up to item ${i}`);
        }
        console.log('S4: All bulk records committed.');

        await closeDb();
        db = new Indinis(testDataDir);
        console.log('S4: Database reopened.');

        await db.transaction(async tx => {
            expect(await tx.get(keys[0])).toBeDefined();
            expect(await tx.get(keys[Math.floor(numRecords / 2)])).toBeDefined();
            expect(await tx.get(keys[numRecords - 1])).toBeDefined();
            const valLast = await tx.get(keys[numRecords - 1]);
            expect(JSON.parse(valLast as string).iteration).toBe(numRecords - 1);
        });
        console.log('S4: Data subset verified after restart.');
    });


    it('Scenario 5: Recovery after clean appends (simulates multiple sessions)', async () => {
        db = await initializeDb('s5-clean-appends');
        const keyA = 'append/a';
        const valueA = 'value A - session 1';
        const keyB = 'append/b';
        const valueB = 'value B - session 2';

        await db.transaction(async tx => { await tx.set(keyA, valueA); });
        console.log('S5: Committed keyA.');
        await closeDb();
        console.log('S5: DB closed (session 1 end).');

        db = new Indinis(testDataDir); // Reopen for session 2
        console.log('S5: DB reopened (session 2 start).');
        await db.transaction(async tx => { await tx.set(keyB, valueB); });
        console.log('S5: Committed keyB.');
        await closeDb();
        console.log('S5: DB closed (session 2 end).');

        db = new Indinis(testDataDir); // Reopen for final verification
        console.log('S5: DB reopened for final verification.');
        await db.transaction(async tx => {
            expect(await tx.get(keyA)).toBe(valueA);
            expect(await tx.get(keyB)).toBe(valueB);
        });
        console.log('S5: Both keys verified across sessions.');
    });
});