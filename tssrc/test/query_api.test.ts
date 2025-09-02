// tssrc/test/query_api.test.ts

import { Indinis, IndexOptions, LsmOptions, MemTableType } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

// Helper for small delay to allow async background tasks to run
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-query-api');

// Test Data Interfaces
interface User {
    id?: string;
    name: string;
    status: 'active' | 'pending' | 'archived';
    region: 'us-east' | 'us-west' | 'eu-central';
    level: number;
}

// --- START OF REFACTOR ---

const memTableTypesToTest: MemTableType[] = ['SkipList', 'Vector', 'PrefixHash', 'Hash', 'RCU'];

describe.each(memTableTypesToTest)('Indinis Fluent Query API (MemTable: %s)', (memTableType) => {
    let db: Indinis;
    let testDataDir: string;

    jest.setTimeout(40000); // Increased timeout per test case to handle setup

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${memTableType}-${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        
        const lsmOptions: LsmOptions = { defaultMemTableType: memTableType };
        db = new Indinis(testDataDir, { lsmOptions });
        
        console.log(`\n[QUERY API TEST START for ${memTableType}] Using data directory: ${testDataDir}`);
    });

    afterEach(async () => {
        if (db) await db.close();
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir);
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    // Helper to populate data and create necessary indexes for most tests
    async function setupInitialDataAndIndexes() {
        const usersStore = db.store<User>('users');
        await db.transaction(async tx => {
            await usersStore.item('user1').make({ name: 'Alice', status: 'active', region: 'us-east', level: 5 });
            await usersStore.item('user2').make({ name: 'Bob', status: 'pending', region: 'us-west', level: 3 });
            await usersStore.item('user3').make({ name: 'Charlie', status: 'active', region: 'us-west', level: 7 });
            await usersStore.item('user4').make({ name: 'Diane', status: 'archived', region: 'us-east', level: 5 });
        });
        
        await db.createIndex('users', 'idx_users_status', { field: 'status' });
        
        const backfillWaitTime = 5000;
        console.log(`Waiting ${backfillWaitTime}ms for index backfill to complete...`);
        await delay(backfillWaitTime);

        console.log("Test setup complete: 4 users created, 'name' and 'status' indexes are ready.");
    }

    // --- TEST CASES ---

    // Special case for Hash memtable which does not support indexed queries
    if (memTableType === 'Hash') {
        it('should throw an error when using getPrefix because ordered iteration is not supported', async () => {
            await setupInitialDataAndIndexes(); // Data is now in the HashMemTable
            const usersStore = db.store<User>('users');

            // The .take() method internally uses getPrefix, which requires an iterator.
            // This is the operation that must fail for the Hash memtable.
            const queryPromise = usersStore.take();
            
            await expect(queryPromise).rejects.toThrow(/Ordered iteration is not supported by HashMemTable/);
        });
    } else {
        // Run the full suite of query tests for all other memtable types
        it('should query using the automatically created default index on "name"', async () => {
            await setupInitialDataAndIndexes();
            const usersStore = db.store<User>('users');
            
            const foundUser = await usersStore.filter('name').equals('Bob').one();
            
            expect(foundUser).not.toBeNull();
            expect(foundUser?.id).toBe('user2');
            expect(foundUser?.name).toBe('Bob');
        });

        it('should successfully query on a user-defined index after backfill', async () => {
            await setupInitialDataAndIndexes();
            const usersStore = db.store<User>('users');
            
            const pendingUsers = await usersStore.filter('status').equals('pending').take();
            
            expect(pendingUsers).toHaveLength(1);
            expect(pendingUsers[0].name).toBe('Bob');
        });

        it('should return multiple results with take()', async () => {
            await setupInitialDataAndIndexes();
            const usersStore = db.store<User>('users');

            const activeUsers = await usersStore.filter('status').equals('active').take();
            
            expect(activeUsers).toHaveLength(2);
            activeUsers.sort((a, b) => (a.name || '').localeCompare(b.name || ''));
            
            expect(activeUsers[0].name).toBe('Alice');
            expect(activeUsers[1].name).toBe('Charlie');
        });
        
        it('should respect the limit parameter with take()', async () => {
            await setupInitialDataAndIndexes();
            const usersStore = db.store<User>('users');
            
            const limitedActiveUsers = await usersStore.filter('status').equals('active').take(1);
            
            expect(limitedActiveUsers).toHaveLength(1);
        });

        it('should return the first result with one()', async () => {
            await setupInitialDataAndIndexes();
            const usersStore = db.store<User>('users');
            
            const firstActiveUser = await usersStore.filter('status').equals('active').one();
            
            expect(firstActiveUser).not.toBeNull();
            expect(['Alice', 'Charlie']).toContain(firstActiveUser?.name);
        });

        it('should correctly handle post-filtering for multi-condition queries', async () => {
            await setupInitialDataAndIndexes();
            const usersStore = db.store<User>('users');

            const highLevelActiveUsers = await usersStore
                .filter('status').equals('active')         // Uses index
                .filter('level').greaterThanOrEqual(6)   // Post-filter
                .take();
            
            expect(highLevelActiveUsers).toHaveLength(1);
            expect(highLevelActiveUsers[0].name).toBe('Charlie');
            expect(highLevelActiveUsers[0].level).toBe(7);
        });
    }

    // This test does not rely on an index, so it should pass for all memtable types, including Hash.
    it('should fail to query on a field that is not indexed', async () => {
        await setupInitialDataAndIndexes();
        const usersStore = db.store<User>('users');
        let caughtError: Error | null = null;
        try {
            await usersStore.filter('region').equals('us-east').take();
        } catch (e: any) {
            caughtError = e;
        }
        expect(caughtError).not.toBeNull();
        expect(caughtError?.message).toContain('Query requires an index on at least one of the filter fields');
    });
});