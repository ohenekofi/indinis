//tssrc/test/disk_freelist.test.ts
import { Indinis, IndinisOptions, StorageValue } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-disk-freelist');

// Define C++ constants needed for test logic
const CPP_PAGE_SIZE = 4096;
const CPP_METADATA_FIXED_AREA_SIZE = 4096;

// ... (getDbStats, createAndPopulateBTree, deleteBTreeAndData helpers remain the same) ...
async function createAndPopulateBTree(db: Indinis, indexName: string, numEntries: number, valueSize: number = 50): Promise<void> {
    await db.createIndex('freelist_test_col', indexName, { field: 'data' });
    for (let i = 0; i < numEntries; i++) {
        await db.transaction(async tx => {
            await tx.set(`freelist_test_col/item_${indexName}_${i.toString().padStart(5, '0')}`, JSON.stringify({ data: `val${i}`.padEnd(valueSize, 'X') }));
        });
    }
}
async function deleteBTreeAndData(db: Indinis, indexName: string, numEntries: number): Promise<void> {
    for (let i = 0; i < numEntries; i++) {
        await db.transaction(async tx => {
            await tx.remove(`freelist_test_col/item_${indexName}_${i.toString().padStart(5, '0')}`);
        });
    }
    // Deleting B-Tree pages happens when the B-Tree root is removed and its pages are no longer referenced
    // AND then those pages are deallocated by the BufferPoolManager.
    // To ensure B-Tree pages are truly deallocated, removing the index is a good step.
    await db.deleteIndex(indexName);
}

describe('DiskManager Freelist Functionality', () => {
    let db: Indinis;
    let testDataDir: string;
    // Estimate for pages used by initial metadata, a few WAL records, and an empty B-Tree for the collection
    const initialPageCountEstimate = 5;

    const getDbFileSize = (dir: string): number => {
        const dbFilePath = path.join(dir, 'primary.db');
        if (fs.existsSync(dbFilePath)) {
            return fs.statSync(dbFilePath).size;
        }
        return 0;
    };

     beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        db = new Indinis(testDataDir, { checkpointIntervalSeconds: 0 }); // Disable auto checkpoints for more control
        console.log(`\n[FREELIST TEST START] Using data directory: ${testDataDir}`);
    });

    afterEach(async () => {
        console.log("[FREELIST TEST END] Closing database...");
        if (db) await db.close();
        console.log("[FREELIST TEST END] Cleaning up test directory...");
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir);
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    it('Scenario FL1: Should grow file when allocating new pages beyond initial metadata', async () => {
        const sizeBefore = getDbFileSize(testDataDir);
        console.log(`FL1: Size before BTree creation: ${sizeBefore}`);

        await createAndPopulateBTree(db, "btree_grow", initialPageCountEstimate * 2, 100);
        await db.forceCheckpoint();
        await db.close();

        const sizeAfter = getDbFileSize(testDataDir);
        console.log(`FL1: Size after BTree creation & flush: ${sizeAfter}`);
        expect(sizeAfter).toBeGreaterThan(sizeBefore);
        // Expect growth to be at least a few pages
        expect(sizeAfter).toBeGreaterThanOrEqual(sizeBefore + CPP_PAGE_SIZE * initialPageCountEstimate);
    });

    it('Scenario FL2: Should reuse pages from freelist after deallocation', async () => {
        const numPageSetsToCreate = 2; // How many B-Trees we'll create in this test "set"
        const entriesPerBTreeForPages = 20; // Enough entries to ensure multiple pages per B-Tree
        const valueSizeForPages = 200; // Larger values to fill pages faster

        console.log(`FL2: Initializing first set of B-Trees...`);
        for(let i=0; i<numPageSetsToCreate; ++i) {
            await createAndPopulateBTree(db, `btree_set1_tree${i}`, entriesPerBTreeForPages, valueSizeForPages);
        }
        await db.forceCheckpoint();
        await db.close();
        const sizeAfterSet1 = getDbFileSize(testDataDir);
        console.log(`FL2: Size after Set1 B-Trees: ${sizeAfterSet1}`);
        expect(sizeAfterSet1).toBeGreaterThan(CPP_METADATA_FIXED_AREA_SIZE);

        db = new Indinis(testDataDir, { checkpointIntervalSeconds: 0 });

        console.log(`FL2: Deleting first set of B-Trees...`);
        for(let i=0; i<numPageSetsToCreate; ++i) {
            await deleteBTreeAndData(db, `btree_set1_tree${i}`, entriesPerBTreeForPages);
        }
        await db.forceCheckpoint(); // This is crucial to ensure B-Tree pages are deallocated and added to freelist
        await db.close();
        const sizeAfterDelete1 = getDbFileSize(testDataDir);
        console.log(`FL2: Size after deleting Set1 B-Trees & checkpoint: ${sizeAfterDelete1}`);
        // File size should generally remain the same as conservative deallocation doesn't shrink file

        db = new Indinis(testDataDir, { checkpointIntervalSeconds: 0 });

        console.log(`FL2: Creating second set of B-Trees (should reuse freelist pages)...`);
        for(let i=0; i<numPageSetsToCreate; ++i) {
            await createAndPopulateBTree(db, `btree_set2_tree${i}`, entriesPerBTreeForPages, valueSizeForPages);
        }
        await db.forceCheckpoint();
        await db.close();
        const sizeAfterSet2 = getDbFileSize(testDataDir);
        console.log(`FL2: Size after Set2 B-Trees: ${sizeAfterSet2}`);

        // If freelist was effectively used, sizeAfterSet2 should be very close to sizeAfterSet1 (or sizeAfterDelete1).
        // It might be slightly larger due to new metadata for new B-Tree roots, but not by the full amount of new data.
        const delta = Math.abs(sizeAfterSet2 - sizeAfterDelete1); // Compare against size after delete
        const allowedGrowthForNewRoots = CPP_PAGE_SIZE * numPageSetsToCreate; // Max a few new root pages for metadata
        console.log(`FL2: Delta between sizeAfterDelete1 and sizeAfterSet2: ${delta} (Allowed new growth for roots: ~${allowedGrowthForNewRoots})`);
        expect(delta).toBeLessThanOrEqual(allowedGrowthForNewRoots + (CPP_PAGE_SIZE * 2)); // Allow some slack
    });

    it('Scenario FL3: Test verifyFreeList (debug API)', async () => {
        if (!(db as any).debug_verifyFreeList) {
            console.warn("FL3: Skipping verifyFreeList test as debug_verifyFreeList NAPI is not exposed.");
            return;
        }
        console.log(`FL3: Verifying freelist on a new DB...`);
        let healthy = await (db as any).debug_verifyFreeList();
        expect(healthy).toBe(true);

        await createAndPopulateBTree(db, "btree_for_verifyFL1", 20, 50);
        await deleteBTreeAndData(db, "btree_for_verifyFL1", 20);
        await createAndPopulateBTree(db, "btree_for_verifyFL2", 15, 50);
        await db.forceCheckpoint();

        console.log(`FL3: Verifying freelist after some alloc/dealloc...`);
        healthy = await (db as any).debug_verifyFreeList();
        expect(healthy).toBe(true);
    });
});