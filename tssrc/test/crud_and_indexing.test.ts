// @filename: tssrc/test/crud_and_indexing.test.ts

import { Indinis, IndexOptions, StoreRef, ItemRef, IndexInfo } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-crud-and-auto-indexing');

// Helper to stringify values for storage
const s = (obj: any): string => JSON.stringify(obj);

// Helper to augment the Indinis interface for our debug method
declare module '../index' {
    interface Indinis {
        debug_scanBTree(indexName: string, startKeyBuffer: Buffer, endKeyBuffer: Buffer): Promise<any[]>;
    }
}

// --- Test Data Interfaces ---
interface User {
    id?: string;
    name: string;
    email: string;
    region?: string;
}

interface Product {
    id?: string;
    name: string;
    sku: string;
    price: number;
}


describe('Indinis Fluent API & Automatic Indexing', () => {
    let db: Indinis;
    let testDataDir: string;

    jest.setTimeout(30000); // Set timeout for tests

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        db = new Indinis(testDataDir);
        console.log(`\n[FLUENT/INDEX TEST START] Using data directory: ${testDataDir}`);
    });

    afterEach(async () => {
        if (db) await db.close();
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir);
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    describe('Fluent CRUD Operations (make, item)', () => {

        it('should create a document with an auto-generated ID using store.make()', async () => {
            const usersStore = db.store<User>('users');
            const newId = await usersStore.make({ name: 'Auto User', email: 'auto@example.com' });

            expect(typeof newId).toBe('string');
            expect(newId.length).toBe(20);

            const retrieved = await usersStore.item(newId).one();
            expect(retrieved).not.toBeNull();
            expect(retrieved?.name).toBe('Auto User');
            expect(retrieved?.id).toBe(newId);
        });

        it('should create a document with a specified ID using item.make()', async () => {
            const usersStore = db.store<User>('users');
            const userId = 'user-123';
            await usersStore.item(userId).make({ name: 'Specific User', email: 'specific@example.com' });

            const retrieved = await usersStore.item(userId).one();
            expect(retrieved).not.toBeNull();
            expect(retrieved?.name).toBe('Specific User');
            expect(retrieved?.id).toBe(userId);
        });

        it('should FAIL to create a document with overwrite:false if it already exists', async () => {
            const productsStore = db.store<Product>('products');
            const productId = 'prod-abc';

            // First creation should succeed
            await productsStore.item(productId).make({ name: 'Widget', sku: 'WGT-001', price: 19.99 });
            
            // Second attempt without overwrite should fail
            await expect(
                productsStore.item(productId).make({ name: 'Super Widget', sku: 'WGT-002', price: 29.99 }, false)
            ).rejects.toThrow(/A document with this ID already exists/);
        });

        it('should SUCCEED in overwriting a document with overwrite:true', async () => {
            const productsStore = db.store<Product>('products');
            const productId = 'prod-xyz';

            // First creation
            await productsStore.item(productId).make({ name: 'Thingamajig', sku: 'THG-001', price: 9.99 });
            const v1 = await productsStore.item(productId).one();
            expect(v1?.name).toBe('Thingamajig');

            // Overwrite it
            await productsStore.item(productId).make({ name: 'Super Thingamajig', sku: 'THG-002', price: 14.99 }, true);
            const v2 = await productsStore.item(productId).one();
            expect(v2?.name).toBe('Super Thingamajig'); // Should have the new name
            expect(v2?.price).toBe(14.99); // And the new price
        });
    });

    describe('Automatic and User-Defined Indexing', () => {
        
        it('should automatically create a default index on the "name" field for a new store', async () => {
            const newStorePath = 'organizations';
            
            // 1. Verify no indexes exist for this path initially
            let indexes = await db.listIndexes(newStorePath);
            expect(indexes).toHaveLength(0);

            // 2. Perform the first write to this new store path
            const orgsStore = db.store<{ id?: string, name: string, country: string }>('organizations');
            await orgsStore.item('org1').make({ name: 'Tech Corp', country: 'USA' });
            
            // 3. Verify the default index was created automatically
            indexes = await db.listIndexes(newStorePath);
            expect(indexes).toHaveLength(1);

            const defaultIndex = indexes[0];
            expect(defaultIndex.name).toContain('idx_default_organizations_name');
            expect(defaultIndex.storePath).toBe(newStorePath);
            expect(defaultIndex.fields).toEqual([{ name: 'name', order: 'asc' }]);
        });

        it('should update both default and user-defined indexes on document creation', async () => {
            const storePath = 'inventory';
            const userIndexName = 'idx_inventory_sku';
            
            // 1. Create a user-defined index on 'sku' BEFORE any data is written
            await db.createIndex(storePath, userIndexName, { field: 'sku' });

            // 2. Write a document to this store. This should:
            //    a) Trigger the creation of the default 'name' index.
            //    b) Update the user-defined 'sku' index.
            const inventoryStore = db.store<Product>('inventory');
            await inventoryStore.item('item-001').make({ name: 'Super Sprocket', sku: 'SPKT-9000', price: 100 });
            
            // 3. Verify both indexes now exist for this path
            const indexes = await db.listIndexes(storePath);
            expect(indexes.length).toBe(2);
            expect(indexes).toEqual(expect.arrayContaining([
                expect.objectContaining({ name: userIndexName }),
                expect.objectContaining({ name: expect.stringContaining('idx_default_inventory_name') })
            ]));

            // 4. (Low-level verification) Check B-Tree content for both indexes
            // We need a helper to create the binary key parts for the debug scan
            const createKeyPart = (val: string) => Buffer.from(val, 'utf8');

            // Check the default 'name' index
            const nameScanResults = await db.debug_scanBTree(
                indexes.find(i => i.name.includes('_name'))!.name,
                createKeyPart('Super Sprocket'),
                createKeyPart('Super Sprocket')
            );
            expect(nameScanResults.length).toBe(1);
            expect(nameScanResults[0].value.toString('utf8')).toBe(`${storePath}/item-001`);

            // Check the user-defined 'sku' index
            const skuScanResults = await db.debug_scanBTree(
                userIndexName,
                createKeyPart('SPKT-9000'),
                createKeyPart('SPKT-9000')
            );
            expect(skuScanResults.length).toBe(1);
            expect(skuScanResults[0].value.toString('utf8')).toBe(`${storePath}/item-001`);
        });

        it('should not re-create a default index if it already exists', async () => {
            const storePath = 'logs';
            const logsStore = db.store<{ id?: string, name: string, level: string }>('logs');

            // First write triggers default index creation
            await logsStore.item('log1').make({ name: 'log_event_1', level: 'info' });
            let indexes = await db.listIndexes(storePath);
            expect(indexes.length).toBe(1);

            // Second write should NOT create another index
            await logsStore.item('log2').make({ name: 'log_event_2', level: 'warn' });
            indexes = await db.listIndexes(storePath);
            expect(indexes.length).toBe(1); // Should still be just one default index
        });
    });
});