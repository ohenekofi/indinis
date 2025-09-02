/**
 * @file tssrc/test/query_api_multikey.test.ts
 * @description Test suite for the Fluent Query API's array operators (`arrayContains`, `arrayContainsAny`).
 */

import { Indinis, IndexOptions } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

// Helper for small delay to allow async background tasks (like index backfilling) to run
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-query-multikey');

// Test Data Interface
interface Product {
    id?: string;
    name: string;
    category: 'electronics' | 'office' | 'home';
    tags: string[]; // This will be indexed with a multikey index
    sizes: number[]; // A numeric array for multikey indexing
    inventory: number;
}

describe('Indinis Fluent Query API - Multikey and Array Operators', () => {
    let db: Indinis;
    let testDataDir: string;

    const productsPath = 'products';

    // Test data designed to cover various scenarios
    const testData: Omit<Product, 'id'>[] = [
        { name: 'Laptop', category: 'electronics', tags: ['electronics', 'computer', 'sale'], sizes: [13, 15], inventory: 50 },
        { name: 'Monitor', category: 'electronics', tags: ['electronics', 'display', 'bestseller'], sizes: [24, 27], inventory: 30 },
        { name: 'Desk Chair', category: 'office', tags: ['office', 'furniture'], sizes: [], inventory: 100 },
        { name: 'Mouse', category: 'electronics', tags: ['electronics', 'computer', 'accessory', 'bestseller', 'home'], sizes: [1, 2], inventory: 200 },
        // A document where 'tags' is intentionally not an array to test robustness
        { name: 'Bad Keyboard', category: 'electronics', tags: 'computer,accessory' as any, sizes: [1], inventory: 5 }
    ];

    jest.setTimeout(45000);

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        db = new Indinis(testDataDir);
        console.log(`\n[MULTIKEY QUERY TEST START] Using data directory: ${testDataDir}`);

        // --- Create necessary indexes ---
        // A multikey index on the 'tags' array field.
        await db.createIndex(productsPath, 'idx_prod_tags', { field: 'tags', multikey: true });
        // A multikey index on the 'sizes' numeric array field.
        await db.createIndex(productsPath, 'idx_prod_sizes', { field: 'sizes', multikey: true });
        // A standard (non-multikey) index on 'category' for negative testing.
        await db.createIndex(productsPath, 'idx_prod_category', { field: 'category' });

        // --- Pre-populate data ---
        const productsStore = db.store<Product>(productsPath);
        await db.transaction(async () => {
            for (let i = 0; i < testData.length; i++) {
                const docData = testData[i];
                await productsStore.item(`prod${i + 1}`).make(docData);
            }
        });

        // Wait for the asynchronous index backfill to complete.
        const backfillWaitTime = 5000;
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
    const getNames = (results: Product[]): string[] => results.map(r => r.name).sort();

    describe('.arrayContains()', () => {
        it('should find all documents where the tags array contains "electronics"', async () => {
            const results = await db.store<Product>(productsPath)
                .filter('tags').arrayContains('electronics')
                .take();
            
            expect(getNames(results)).toEqual(['Laptop', 'Monitor', 'Mouse']);
        });

        it('should find documents where the sizes array contains the number 24', async () => {
            const results = await db.store<Product>(productsPath)
                .filter('sizes').arrayContains(24)
                .take();

            expect(results).toHaveLength(1);
            expect(results[0].name).toBe('Monitor');
        });

        it('should return an empty array if no document contains the specified tag', async () => {
            const results = await db.store<Product>(productsPath)
                .filter('tags').arrayContains('apparel')
                .take();

            expect(results).toHaveLength(0);
        });

        it('should not match documents where the indexed field is not an array', async () => {
            // "Bad Keyboard" has tags: "computer,accessory" (a string, not an array)
            // This query should only return "Mouse", which has 'computer' in its array.
            const results = await db.store<Product>(productsPath)
                .filter('tags').arrayContains('computer')
                .take();
            
            expect(getNames(results)).toEqual(['Laptop', 'Mouse']);
        });
    });

    describe('.arrayContainsAny()', () => {
        it('should find all documents containing either "sale" or "bestseller"', async () => {
            const results = await db.store<Product>(productsPath)
                .filter('tags').arrayContainsAny(['sale', 'bestseller'])
                .take();

            expect(getNames(results)).toEqual(['Laptop', 'Monitor', 'Mouse']);
        });

        it('should find all documents with size 13 OR 27', async () => {
            const results = await db.store<Product>(productsPath)
                .filter('sizes').arrayContainsAny([13, 27])
                .take();

            expect(getNames(results)).toEqual(['Laptop', 'Monitor']);
        });

        it('should return a single document if it matches multiple values in the query', async () => {
            // The Mouse document has both 'bestseller' and 'home' tags.
            const results = await db.store<Product>(productsPath)
                .filter('tags').arrayContainsAny(['bestseller', 'home'])
                .take();
            
            // It should only appear once in the final result set.
            const mouseResult = results.filter(r => r.name === 'Mouse');
            expect(mouseResult).toHaveLength(1);
            expect(getNames(results)).toEqual(['Monitor', 'Mouse']);
        });
    });

    describe('Error Handling and Validation', () => {
        it('should throw an error when using arrayContains on a non-multikey index', async () => {
            const queryPromise = db.store<Product>(productsPath)
                .filter('category').arrayContains('electronics') // 'category' has a standard index
                .take();

            await expect(queryPromise).rejects.toThrow(/requires a multikey index/);
        });
        
        it('should throw an error when using arrayContainsAny on a non-multikey index', async () => {
            const queryPromise = db.store<Product>(productsPath)
                .filter('category').arrayContainsAny(['electronics', 'office'])
                .take();

            await expect(queryPromise).rejects.toThrow(/requires a multikey index/);
        });
        
        it('should throw a TypeError if arrayContains is given an array', () => {
            const store = db.store<Product>(productsPath);
            expect(() => {
                store.filter('tags').arrayContains(['sale'] as any);
            }).toThrow(TypeError);
        });
        
        it('should throw a TypeError if arrayContainsAny is not given an array', () => {
            const store = db.store<Product>(productsPath);
            expect(() => {
                store.filter('tags').arrayContainsAny('sale' as any);
            }).toThrow(TypeError);
        });
    });
});