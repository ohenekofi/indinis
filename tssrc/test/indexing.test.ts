//indexing.test.ts
import { Indinis, IndexInfo, IndexOptions } from '../index'; // Adjust path if needed
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-indexing-db'); // Slightly different name

// Helper interface for test data (can be simpler for indexing tests)
interface TestDoc {
    id?: string;
    fieldA?: string;
    fieldB?: number;
    fieldC?: boolean;
    nested?: { subField: string };
}

describe('Indinis DB Indexing Functionality', () => {
    let db: Indinis;
    let testDataDir: string;
    const usersPath = 'users'; // Example valid collection path (odd segments)
    const postsPath = 'users/user1/posts'; // Example valid sub-collection path (odd segments)
    const productsPath = 'products'; // Another valid collection path

    // Increase timeout for potential index operations/cleanup
    jest.setTimeout(25000); // 25 seconds

    beforeAll(async () => {
        // Create a single directory for all tests in this suite
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `suite-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        console.log(`[Test Setup] Using data directory: ${testDataDir}`);
        db = new Indinis(testDataDir);
    });

    afterAll(async () => {
        console.log("[Test Teardown] Closing database...");
        if (db) {
            await db.close();
        }
        console.log("[Test Teardown] Cleaning up test directory...");
        if (fs.existsSync(testDataDir)) {
            await rimraf(testDataDir, { maxRetries: 3, retryDelay: 500 }).catch(err => {
                console.error(`[Test Teardown] Failed to remove test suite directory ${testDataDir}:`, err);
            });
        }
         console.log("[Test Teardown] Complete.");
    });

    // --- Test Cases ---

    it('should create a simple index successfully', async () => {
        const indexName = 'idx_users_fieldA';
        const options: IndexOptions = { field: 'fieldA' };
        const result = await db.createIndex(usersPath, indexName, options);
        expect(result).toBe(true);

        // Verify it's listed for the correct path
        const indexes = await db.listIndexes(usersPath);
        const foundIndex = indexes.find(idx => idx.name === indexName);
        expect(foundIndex).toBeDefined();
        expect(foundIndex?.storePath).toBe(usersPath);
        expect(foundIndex?.fields).toEqual([{ name: 'fieldA', order: 'asc' }]);
    });

    it('should create a simple index with descending order', async () => {
        const indexName = 'idx_users_fieldB_desc';
        const options: IndexOptions = { field: 'fieldB', order: 'desc' };
        const result = await db.createIndex(usersPath, indexName, options);
        expect(result).toBe(true);

        const indexes = await db.listIndexes(usersPath);
        const foundIndex = indexes.find(idx => idx.name === indexName);
        expect(foundIndex).toBeDefined();
        expect(foundIndex?.fields).toEqual([{ name: 'fieldB', order: 'desc' }]);
    });

     it('should create a simple index on a subcollection path', async () => {
        const indexName = 'idx_posts_fieldC';
        const options: IndexOptions = { field: 'fieldC' };
        const result = await db.createIndex(postsPath, indexName, options);
        expect(result).toBe(true);

        const indexes = await db.listIndexes(postsPath);
        const foundIndex = indexes.find(idx => idx.name === indexName);
        expect(foundIndex).toBeDefined();
        expect(foundIndex?.storePath).toBe(postsPath);
    });

    it('should create a compound index successfully', async () => {
        const indexName = 'idx_users_A_B';
        const options: IndexOptions = { fields: ['fieldA', 'fieldB'] }; // Default order 'asc'
        const result = await db.createIndex(usersPath, indexName, options);
        expect(result).toBe(true);

        const indexes = await db.listIndexes(usersPath);
        const foundIndex = indexes.find(idx => idx.name === indexName);
        expect(foundIndex).toBeDefined();
        expect(foundIndex?.storePath).toBe(usersPath);
        expect(foundIndex?.fields).toEqual([
            { name: 'fieldA', order: 'asc' },
            { name: 'fieldB', order: 'asc' },
        ]);
    });

    it('should create a compound index with mixed orders', async () => {
        const indexName = 'idx_users_Bdesc_Casc';
        const options: IndexOptions = {
            fields: [
                { name: 'fieldB', order: 'desc' },
                { name: 'fieldC', order: 'asc' } // Assuming fieldC exists
            ]
        };
        const result = await db.createIndex(usersPath, indexName, options);
        expect(result).toBe(true);

        const indexes = await db.listIndexes(usersPath);
        const foundIndex = indexes.find(idx => idx.name === indexName);
        expect(foundIndex).toBeDefined();
        expect(foundIndex?.fields).toEqual([
            { name: 'fieldB', order: 'desc' },
            { name: 'fieldC', order: 'asc' },
        ]);
    });

    it('should return false when creating an index with an existing name', async () => {
        const indexName = 'idx_duplicate_name';
        await db.createIndex(usersPath, indexName, { field: 'field1' }); // Create first time
        const result = await db.createIndex(productsPath, indexName, { field: 'field2' }); // Attempt duplicate name on different path
        expect(result).toBe(false); // Index names are globally unique
    });

    it('should list all indexes when no path is provided', async () => {
        // Create indexes on different paths for this test
        const idxUser = 'list_all_user_idx';
        const idxPost = 'list_all_post_idx';
        const idxProd = 'list_all_prod_idx';
        await db.createIndex(usersPath, idxUser, { field: 'u' });
        await db.createIndex(postsPath, idxPost, { field: 'p' });
        await db.createIndex(productsPath, idxProd, { field: 'pr' });

        const allIndexes = await db.listIndexes(); // Get all

        // Check if the newly created indexes are present (plus potentially others from prior tests)
        expect(allIndexes.length).toBeGreaterThanOrEqual(3);
        expect(allIndexes).toEqual(expect.arrayContaining([
            expect.objectContaining({ name: idxUser, storePath: usersPath }),
            expect.objectContaining({ name: idxPost, storePath: postsPath }),
            expect.objectContaining({ name: idxProd, storePath: productsPath }),
        ]));
    });

     it('should list only indexes for the specific store path when provided', async () => {
        const idxUserSpecific = 'list_specific_user_idx';
        const idxProdSpecific = 'list_specific_prod_idx';
        await db.createIndex(usersPath, idxUserSpecific, { field: 'us' });
        await db.createIndex(productsPath, idxProdSpecific, { field: 'ps' });

        // List indexes for 'users' store
        const userIndexes = await db.listIndexes(usersPath);
        expect(userIndexes.some(idx => idx.name === idxUserSpecific)).toBe(true);
        expect(userIndexes.some(idx => idx.name === idxProdSpecific)).toBe(false);

        // List indexes for 'products' store
        const productIndexes = await db.listIndexes(productsPath);
        expect(productIndexes.some(idx => idx.name === idxUserSpecific)).toBe(false);
        expect(productIndexes.some(idx => idx.name === idxProdSpecific)).toBe(true);

        // List indexes for a path with no indexes
        const emptyIndexes = await db.listIndexes('nonexistent/path/ok');
        expect(emptyIndexes).toEqual([]);
     });


    it('should delete an existing index', async () => {
        const indexName = 'idx_to_delete_global';
        await db.createIndex(usersPath, indexName, { field: 'delete_field' });

        // Verify it exists (can check globally or by path)
        let indexes = await db.listIndexes(usersPath);
        expect(indexes.some(idx => idx.name === indexName)).toBe(true);

        // Delete it by name
        const deleteResult = await db.deleteIndex(indexName);
        expect(deleteResult).toBe(true);

        // Verify it's gone globally
        indexes = await db.listIndexes();
        expect(indexes.some(idx => idx.name === indexName)).toBe(false);
    });

    it('should return false when deleting a non-existent index', async () => {
        const indexName = 'idx_does_not_exist_global';
        const deleteResult = await db.deleteIndex(indexName);
        expect(deleteResult).toBe(false);
    });

    // --- Validation Tests ---

    it('should throw error if creating index with invalid storePath', async () => {
        const invalidPath = 'users/user1'; // Even segments, invalid for collection
        await expect(db.createIndex(invalidPath, 'idx_invalid_path', { field: 'a' }))
            .rejects.toThrow(/Invalid store path for index creation.*must have an odd number of segments/i);
        await expect(db.createIndex('', 'idx_invalid_path', { field: 'a' })) // Empty path
            .rejects.toThrow(/storePath must be a non-empty string/i); // CORRECTED EXPECTATION
    });

     it('should throw error if creating index with invalid name', async () => {
        await expect(db.createIndex(usersPath, '', { field: 'a' }))
            .rejects.toThrow(/Index name must be a non-empty string/i);
     });

    it('should throw error if creating index with invalid options', async () => {
        await expect(db.createIndex(usersPath, 'idx_invalid_opts1', {} as any))
            .rejects.toThrow(/Index options object must contain either a 'field' \(string\) or 'fields' \(array\) property/i);

        await expect(db.createIndex(usersPath, 'idx_invalid_opts2', { fields: [] }))
            .rejects.toThrow(/Index option 'fields' must be a non-empty array/i); // CORRECTED EXPECTATION (more precise)
    });

     it('should throw error if listing indexes with invalid storePath', async () => {
        const invalidPath = 'users/user1'; // Even segments
         await expect(db.listIndexes(invalidPath))
             .rejects.toThrow(/Invalid store path for listing indexes.*must have an odd number of segments/i);
     });

      it('should throw error if deleting index with invalid name', async () => {
        await expect(db.deleteIndex(''))
            .rejects.toThrow(/Index name must be a non-empty string/i);
     });

    // TODO: Add tests that verify index *usage* (query correctness/performance)
    // This requires query methods that can leverage indexes.
    // Example (pseudo-code):
    // it('should use index for equality query', async () => {
    //    await db.createIndex(usersPath, 'idx_email_usage', { field: 'email' });
    //    // Add data...
    //    const results = await db.query(usersPath).where('email', '==', 'test@example.com').get();
    //    // Assert results and potentially check query plan/logs if available
    // });

});