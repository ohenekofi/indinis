// tssrc/test/batch_writes.test.ts
import { Indinis, increment } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-batch-writes-v2');
const JEST_HOOK_TIMEOUT = 30000; // 30 seconds for setup/teardown

interface User {
    id?: string;
    name: string;
    status: string;
    loginCount?: number;
}

describe('Indinis Batch Write API', () => {
    let db: Indinis | null = null; // Initialize to null
    let testDataDir: string;
    const usersPath = 'users';

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    // beforeEach remains the same, but it's important to not initialize db here
    // as we will do it within each test to control the lifecycle.
    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        // NOTE: db is NOT created here anymore. It's created inside each 'it' block.
    });

    // *** THE ROBUST TEARDOWN FIX ***
    afterEach(async () => {
        const dirToClean = testDataDir; // Capture the path before any async ops
        console.log(`[TEARDOWN for ${path.basename(dirToClean)}] Starting cleanup...`);
        try {
            if (db) {
                console.log("  -> Closing database instance...");
                await db.close();
                console.log("  -> Database closed successfully.");
            }
        } catch (e: any) {
            console.error(`  -> Error during db.close(): ${e.message}`);
        } finally {
            db = null; // Ensure the instance is cleared
            
            // Give the OS a moment to release file locks after C++ shutdown
            await new Promise(res => setTimeout(res, 500));

            if (fs.existsSync(dirToClean)) {
                console.log(`  -> Deleting test directory: ${dirToClean}`);
                await rimraf(dirToClean, { maxRetries: 3, retryDelay: 500 }).catch(err => {
                    console.error(`  -> rimraf cleanup failed for ${dirToClean}:`, err);
                });
            }
        }
        console.log(`[TEARDOWN for ${path.basename(dirToClean)}] Cleanup finished.`);
    }, JEST_HOOK_TIMEOUT); // Give the teardown hook a generous timeout

    // --- TEST CASES ---
/*
    it('Scenario BATCH-1: should execute a mixed batch of operations successfully', async () => {
        db = new Indinis(testDataDir); // Initialize DB for this test
        console.log("--- TEST START: BATCH-1 (Mixed Operations Success) ---");

        const usersStore = db.store<User>(usersPath);
        await usersStore.item('charlie').make({ name: 'Charles', status: 'pending', loginCount: 10 });
        await usersStore.item('diane').make({ name: 'Diane', status: 'active', loginCount: 5 });
        console.log("  Setup: Pre-populated 'charlie' and 'diane'.");

        const batch = db.batch();
        console.log("  Action: Building batch with make, modify, and remove operations.");

        batch
            .store<User>(usersPath).item('alice').make({ name: 'Alice', status: 'active', loginCount: 1 })
            .store<User>(usersPath).item('bob').make({ name: 'Bob', status: 'pending', loginCount: 0 })
            .store<User>(usersPath).item('charlie').modify({ status: 'active', loginCount: increment(1) })
            .store<User>(usersPath).item('diane').remove();

        console.log("  Action: Committing batch...");
        await batch.commit();
        console.log("  Action: Batch commit completed.");

        console.log("  Verification: Checking final state of all documents...");
        await db.transaction(async tx => {
            const alice = JSON.parse(await tx.get(`${usersPath}/alice`) as string);
            const bob = JSON.parse(await tx.get(`${usersPath}/bob`) as string);
            const charlie = JSON.parse(await tx.get(`${usersPath}/charlie`) as string);
            const diane = await tx.get(`${usersPath}/diane`);

            expect(alice.status).toBe('active');
            expect(bob.status).toBe('pending');
            expect(charlie.status).toBe('active');
            expect(charlie.loginCount).toBe(11);
            expect(diane).toBeNull();
        });
        console.log("  Verification: Success. All states are correct.");
        console.log("--- TEST END: BATCH-1 ---");
    });
*/
/*
    it('Scenario BATCH-2: should atomically fail and roll back the entire batch if one operation fails', async () => {
        db = new Indinis(testDataDir); // Initialize DB for this test
        console.log("--- TEST START: BATCH-2 (Atomic Failure) ---");

        const usersStore = db.store<User>(usersPath);
        await usersStore.item('alice').make({ name: 'Alice', status: 'active' });
        console.log("  Setup: Pre-populated 'alice'.");

        const batch = db.batch();
        console.log("  Action: Building batch with a conflicting operation.");

        batch.store<User>(usersPath).item('alice').make({ name: 'Alicia', status: 'pending' });
        console.log("    -> Queued: MAKE 'alice' (overwrite: false) -> This will fail.");
        
        batch.store<User>(usersPath).item('bob').make({ name: 'Bob', status: 'pending' });
        console.log("    -> Queued: MAKE 'bob' -> This should be rolled back.");
        
        console.log("  Action: Committing batch (expecting rejection)...");
        await expect(batch.commit()).rejects.toThrow(/Failed to create document.*already exists/);
        console.log("  Action: Batch commit rejected as expected.");

        console.log("  Verification: Checking that the database state is unchanged...");
        await db.transaction(async tx => {
            const alice = JSON.parse(await tx.get(`${usersPath}/alice`) as string);
            const bob = await tx.get(`${usersPath}/bob`);

            console.log("    - Verifying Alice (should be original):", alice);
            expect(alice.name).toBe('Alice');
            
            console.log("    - Verifying Bob (should not exist):", bob);
            expect(bob).toBeNull();
        });
        console.log("  Verification: Success. The batch was atomically rolled back.");
        console.log("--- TEST END: BATCH-2 ---");
    });
   */
    it('Scenario BATCH-3: should handle a large number of operations in a single batch', async () => {
        db = new Indinis(testDataDir); // Initialize DB for this test
        const BATCH_SIZE = 1000;
        console.log(`--- TEST START: BATCH-3 (Large Batch Performance) ---`);
        console.log(`  Action: Building a single batch with ${BATCH_SIZE} operations...`);

        const batch = db.batch();
        const usersBatchStore = batch.store<User>(usersPath);

        for (let i = 0; i < BATCH_SIZE; i++) {
            const name = `User-${i.toString().padStart(4, '0')}`;
            usersBatchStore.item(`user_${i}`).make({ name: name, status: 'imported' });
        }
        
        console.log(`  Action: Committing large batch of ${BATCH_SIZE} items...`);
        const startTime = Date.now();
        await batch.commit();
        const duration = Date.now() - startTime;
        console.log(`  Action: Large batch committed in ${duration}ms.`);
        expect(duration).toBeLessThan(5000);

        console.log("  Verification: Checking a subset and total count...");
        
        const allDocs = await db.store<User>(usersPath).take();
        expect(allDocs.length).toBe(BATCH_SIZE);
        
        const userLast = allDocs.find(d => d.id === `user_${BATCH_SIZE - 1}`);
        expect(userLast?.name).toBe(`User-${(BATCH_SIZE - 1).toString().padStart(4, '0')}`);
        
        console.log(`  Verification: Success. Found ${allDocs.length} documents as expected.`);
        console.log("--- TEST END: BATCH-3 ---");
    });
 
});