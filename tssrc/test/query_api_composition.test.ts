/**
 * @file tssrc/test/query_api_composition.test.ts
 * @description Test suite for the query composition feature using `.use()`.
 */

import { Indinis, IQuery, QueryPart } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

// Helper for small delay to allow async background tasks (like index backfilling) to run
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-query-composition');

// --- Test Data and Reusable Query Parts ---

interface TestUser {
    id?: string;
    name: string;
    status: 'active' | 'pending';
    region: 'us-east' | 'us-west';
    level: number;
}

// A reusable part to find only active users.
const isActive: QueryPart<TestUser> = (query: IQuery<TestUser>) => query.filter('status').equals('active');

// A function that returns a query part for finding users in a specific region.
const inRegion = (region: TestUser['region']) => 
    (query: IQuery<TestUser>) => query.filter('region').equals(region);

// A reusable part for sorting by level.
const sortByLevel = (direction: 'asc' | 'desc' = 'asc') => 
    (query: IQuery<TestUser>) => query.sortBy('level', direction);

describe('Indinis Fluent Query API - Composition with .use()', () => {
    let db: Indinis;
    let testDataDir: string;
    const usersPath = 'users';

    jest.setTimeout(30000); // 30 seconds per test case

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        db = new Indinis(testDataDir);
        console.log(`\n[QUERY COMPOSITION TEST START] Using data directory: ${testDataDir}`);

        // Create all necessary indexes for the tests
        await db.createIndex(usersPath, 'idx_comp_status', { field: 'status' });
        await db.createIndex(usersPath, 'idx_comp_region', { field: 'region' });
        await db.createIndex(usersPath, 'idx_comp_level', { field: 'level' });

        // Pre-populate a diverse set of data
        const usersStore = db.store<TestUser>(usersPath);
        await db.transaction(async () => {
            await usersStore.item('user1').make({ name: 'Alice', status: 'active', region: 'us-east', level: 5 });
            await usersStore.item('user2').make({ name: 'Bob', status: 'pending', region: 'us-west', level: 3 });
            await usersStore.item('user3').make({ name: 'Charlie', status: 'active', region: 'us-west', level: 7 });
            await usersStore.item('user4').make({ name: 'Diane', status: 'active', region: 'us-east', level: 2 });
        });

        // Crucial Step: Wait for the asynchronous backfill to complete for the indexes.
        const backfillWaitTime = 4000;
        console.log(`Waiting ${backfillWaitTime}ms for index backfill to complete...`);
        await delay(backfillWaitTime);
        console.log("Test setup complete: Data populated and indexes are ready.");
    });

    afterEach(async () => {
        if (db) await db.close();
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir);
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    // Helper to get names from results for easy, deterministic comparison
    const getNames = (results: TestUser[]): string[] => results.map(r => r.name).sort();

    it('should apply a single query part with .use()', async () => {
        const activeUsers = await db.store<TestUser>(usersPath)
            .query()
            .use(isActive)
            .take();
            
        expect(getNames(activeUsers)).toEqual(['Alice', 'Charlie', 'Diane']);
    });

    it('should chain multiple query parts with .use()', async () => {
        const activeWestCoastUsers = await db.store<TestUser>(usersPath)
            .query()
            .use(isActive)
            .use(inRegion('us-west'))
            .take();
        
        expect(activeWestCoastUsers).toHaveLength(1);
        expect(activeWestCoastUsers[0].name).toBe('Charlie');
    });

    it('should correctly apply parameterized query parts', async () => {
        const eastCoastUsers = await db.store<TestUser>(usersPath)
            .query()
            .use(inRegion('us-east'))
            .take();

        expect(getNames(eastCoastUsers)).toEqual(['Alice', 'Diane']);
    });

    it('should correctly apply sorting query parts', async () => {
        const allUsersSorted = await db.store<TestUser>(usersPath)
            .query()
            .filter('level').greaterThan(0) // Get all documents to sort
            .use(sortByLevel('desc'))
            .take();

        const levels = allUsersSorted.map(u => u.level);
        expect(levels).toEqual([7, 5, 3, 2]); // Charlie, Alice, Bob, Diane
    });

    it('should allow mixing .use() and direct .filter() calls', async () => {
        // Find active users from the east coast with a level greater than 3
        const results = await db.store<TestUser>(usersPath)
            .query()
            .use(isActive)
            .use(inRegion('us-east'))
            .filter('level').greaterThan(3) // The extra direct filter
            .take();

        expect(results).toHaveLength(1);
        expect(results[0].name).toBe('Alice');
    });

    it('should return a query object from .use() to allow further chaining', async () => {
        const activeUserQuery = db.store<TestUser>(usersPath).query().use(isActive);
        
        // The result of .use() is another Query object, so we can chain .take()
        const results = await activeUserQuery.take();
        
        expect(getNames(results)).toEqual(['Alice', 'Charlie', 'Diane']);
    });
});