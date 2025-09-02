// tssrc/test/atomic_increment.test.ts

import { Indinis, increment, AtomicIncrementOperation } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-atomic-increment');

interface StatsDoc {
    id?: string;
    name: string;
    views: number;
    likes: number;
    version?: number;
    lastUpdater?: string;
}

describe('Indinis Atomic Increment Functionality', () => {
    let db: Indinis;
    let testDataDir: string;
    
    // --- START OF FIX ---
    // The store path must have an odd number of segments.
    const statsStorePath = 'stats'; // This is the collection/store.
    const postItemId = 'post123';   // This is the document ID.
    // The full key is constructed automatically by the API.
    // --- END OF FIX ---

    jest.setTimeout(30000);

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        db = new Indinis(testDataDir);
        console.log(`\n[ATOMIC TEST START] Using data directory: ${testDataDir}`);

        // --- FIX: Use the corrected paths here ---
        const initialDoc: StatsDoc = { name: 'My First Post', views: 0, likes: 10 };
        await db.store<StatsDoc>(statsStorePath).item(postItemId).make(initialDoc);
    });

    afterEach(async () => {
        if (db) await db.close();
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir);
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    // --- All test `it` blocks are updated to use the corrected paths ---

    it('should correctly increment a numeric field by a positive value', async () => {
        const post = db.store<StatsDoc>(statsStorePath).item(postItemId);
        await post.modify({ views: increment(1) });
        const updatedDoc = await post.one();
        expect(updatedDoc?.views).toBe(1);
    });

    it('should correctly increment and decrement numeric fields in the same operation', async () => {
        const post = db.store<StatsDoc>(statsStorePath).item(postItemId);
        await post.modify({ 
            views: increment(5),
            likes: increment(-2)
        });
        const updatedDoc = await post.one();
        expect(updatedDoc?.views).toBe(5);
        expect(updatedDoc?.likes).toBe(8);
    });

    it('should handle concurrent increments atomically without race conditions', async () => {
        const post = db.store<StatsDoc>(statsStorePath).item(postItemId);
        const numConcurrentUpdates = 25;
        const incrementsPerPromise = 5;

        console.log(`Spawning ${numConcurrentUpdates} concurrent writers, each incrementing 'views' by ${incrementsPerPromise}...`);
        const promises: Promise<void>[] = [];
        for (let i = 0; i < numConcurrentUpdates; i++) {
            const promise = post.modify({ views: increment(incrementsPerPromise) });
            promises.push(promise);
        }
        await Promise.all(promises);

        const finalDoc = await post.one();
        const expectedViews = numConcurrentUpdates * incrementsPerPromise;
        console.log(`Final document state after concurrent increments:`, finalDoc);
        expect(finalDoc?.views).toBe(expectedViews);
    });

    it('should correctly handle mixed atomic increments and simple merge updates in one modify call', async () => {
        const post = db.store<StatsDoc>(statsStorePath).item(postItemId);
        await post.modify({
            views: increment(10),
            lastUpdater: 'mixed_op_test'
        });
        const updatedDoc = await post.one();
        expect(updatedDoc?.views).toBe(10);
        expect(updatedDoc?.lastUpdater).toBe('mixed_op_test');
        expect(updatedDoc?.likes).toBe(10);
    });

    it('should throw an error when trying to increment a non-existent field', async () => {
        const post = db.store<StatsDoc>(statsStorePath).item(postItemId);
        const modifyPromise = post.modify({ nonExistentCounter: increment(1) } as any);
        await expect(modifyPromise).rejects.toThrow(/Cannot increment field 'nonExistentCounter'.*field does not exist or is not a number/);
    });

    it('should throw an error when trying to increment a non-numeric field', async () => {
        const post = db.store<StatsDoc>(statsStorePath).item(postItemId);
        await post.modify({ name: 'A Post With A Stringy Number Field', likes: 'five' as any });
        const modifyPromise = post.modify({ likes: increment(1) });
        await expect(modifyPromise).rejects.toThrow(/Cannot increment field 'likes'.*field does not exist or is not a number/);
    });

    it('should throw an error when trying to modify a non-existent document', async () => {
        const nonExistentPost = db.store<StatsDoc>(statsStorePath).item('nonexistent123');
        const modifyPromise = nonExistentPost.modify({ views: increment(1) });
        await expect(modifyPromise).rejects.toThrow(/Cannot apply atomic update: document with key 'stats\/nonexistent123' does not exist/);
    });

    it('should throw a TypeError if increment() is called with a non-numeric value', () => {
        expect(() => { increment('not a number' as any); }).toThrow(TypeError);
        expect(() => { increment(NaN); }).toThrow(TypeError);
        expect(() => { increment(Infinity); }).toThrow(TypeError);
    });
});