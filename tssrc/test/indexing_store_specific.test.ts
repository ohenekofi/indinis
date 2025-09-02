/**
 * @file tssrc/test/indexing_store_specific.test.ts
 * @description Test suite for store-specific index listing (`store.listIndexes()`).
 */

import { Indinis } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-indexing-scoped');

describe('Indinis Scoped Index Listing', () => {
    let db: Indinis;
    let testDataDir: string;
    const usersPath = 'users';
    const postsPath = 'users/user1/posts';
    const productsPath = 'products';

    jest.setTimeout(20000);

    beforeAll(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `suite-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        console.log(`[SCOPED INDEX TEST SUITE] Using data directory: ${testDataDir}`);
        
        db = new Indinis(testDataDir);

        // --- Setup: Create a variety of indexes across different stores ---
        console.log("Setting up indexes for tests...");
        await db.createIndex(usersPath, 'idx_users_email', { field: 'email' });
        await db.createIndex(usersPath, 'idx_users_level_desc', { field: 'level', order: 'desc' });
        await db.createIndex(postsPath, 'idx_posts_timestamp', { field: 'createdAt' });
        await db.createIndex(productsPath, 'idx_products_category', { field: 'category' });
        console.log("Setup complete.");
    });

    afterAll(async () => {
        if (db) await db.close();
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    it('should list only the indexes for a top-level store', async () => {
        const usersStore = db.store('users');
        const indexes = await usersStore.listIndexes();

        expect(indexes).toHaveLength(2);
        const indexNames = indexes.map(i => i.name).sort();
        expect(indexNames).toEqual(['idx_users_email', 'idx_users_level_desc']);
    });

    it('should list only the indexes for a nested sub-collection store', async () => {
        const postsStore = db.store(postsPath);
        const indexes = await postsStore.listIndexes();

        expect(indexes).toHaveLength(1);
        expect(indexes[0].name).toBe('idx_posts_timestamp');
        expect(indexes[0].storePath).toBe(postsPath);
    });

    it('should return an empty array for a store with no defined indexes', async () => {
        const newStorePath = 'invoices';
        // Ensure the store exists by writing a document, which also creates the default 'name' index.
        await db.store(newStorePath).item('inv1').make({ amount: 100, name: 'Invoice 1' });
        
        // This is a new store for our test context. Let's create a *different* store with no indexes.
        const emptyStore = db.store('empty_store');
        
        const indexes = await emptyStore.listIndexes();
        expect(indexes).toHaveLength(0);
    });

    it('should correctly distinguish between similar but different store paths', async () => {
        // Path 'products' exists, but 'product' does not.
        const similarButNonexistentStore = db.store('product');
        const indexes = await similarButNonexistentStore.listIndexes();
        expect(indexes).toHaveLength(0);
    });

    it('should still allow listing ALL indexes from the top-level db object', async () => {
        const allIndexes = await db.listIndexes();
        // Should contain all 4 created indexes + any default indexes created.
        const allIndexNames = allIndexes.map(i => i.name);
        
        expect(allIndexNames).toContain('idx_users_email');
        expect(allIndexNames).toContain('idx_posts_timestamp');
        expect(allIndexNames).toContain('idx_products_category');
        // Check for default index on 'invoices' store created in a previous test
        const invoicesDefaultIndex = allIndexes.find(i => i.storePath === 'invoices' && i.fields[0].name === 'name');
        expect(invoicesDefaultIndex).toBeDefined();
    });
});