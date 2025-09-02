//tssrc/test/disk_defragmentation.test.ts
import { Indinis, IndinisOptions } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-disk-defrag');

// Define C++ constants needed for test logic
const CPP_PAGE_SIZE = 4096;
const CPP_METADATA_FIXED_AREA_SIZE = 4096;

// --- Helper Interfaces for Stats (mirror C++ structs) ---
interface DefragmentationStatsJs {
    total_pages: number;
    allocated_pages: number;
    free_pages: number;
    fragmentation_gaps: number;
    largest_gap: number;
    potential_pages_saved: number;
    fragmentation_ratio: number;
}
interface DatabaseStatsJs {
    file_size_bytes: number;
    total_pages: number;
    allocated_pages: number;
    free_pages: number;
    next_page_id: number;
    num_btrees: number;
    utilization_ratio: number;
}
interface DefragmentationProgressJs {
    progress_percentage: number;
    status_message: string;
    current_stats: DefragmentationStatsJs;
}

// Assume these N-API methods are added to IndinisWrapper
declare module '../index' { // Augment existing Indinis interface
    interface Indinis {
        debug_analyzeFragmentation(): Promise<DefragmentationStatsJs>;
        debug_defragment(mode: 'CONSERVATIVE' | 'AGGRESSIVE'): Promise<boolean>; // Simplified callback for test
        debug_getDatabaseStats(): Promise<DatabaseStatsJs>;
    }
}

// Helper functions (createAndPopulateBTree, deleteBTreeAndData from freelist test can be reused)
async function createAndPopulateBTree(db: Indinis, indexName: string, numEntries: number, valueSize: number = 50): Promise<void> {
    await db.createIndex('defrag_col', indexName, { field: 'data' });
    for (let i = 0; i < numEntries; i++) {
        await db.transaction(async tx => {
            await tx.set(`defrag_col/item_${indexName}_${i.toString().padStart(5, '0')}`, JSON.stringify({ data: `val${i}`.padEnd(valueSize, 'X') }));
        });
    }
}
async function deleteBTreeAndData(db: Indinis, indexName: string, numEntries: number): Promise<void> {
    for (let i = 0; i < numEntries; i++) {
        await db.transaction(async tx => {
            await tx.remove(`defrag_col/item_${indexName}_${i.toString().padStart(5, '0')}`);
        });
    }
    await db.deleteIndex(indexName);
}


describe('DiskManager Defragmentation Functionality', () => {
    let db: Indinis;
    let testDataDir: string;

      const getDbFileSize = (dir: string): number => {
        const dbFilePath = path.join(dir, 'primary.db');
        return fs.existsSync(dbFilePath) ? fs.statSync(dbFilePath).size : 0;
    };

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        // Disable WAL and checkpoints for more direct disk observation, unless specific interaction is tested
        db = new Indinis(testDataDir, { checkpointIntervalSeconds: 0 /*, walOptions: { enabled: false } */ });
        console.log(`\n[DEFRAG TEST START] Using data directory: ${testDataDir}`);

        // Check if debug methods are present (skip tests if not)
        if (!(db as any).debug_analyzeFragmentation || !(db as any).debug_defragment || !(db as any).debug_getDatabaseStats) {
            throw new Error("Required debug N-API methods for defragmentation tests are not exposed.");
        }
    });

    afterEach(async () => {
        console.log("[DEFRAG TEST END] Closing database...");
        if (db) await db.close();
        console.log("[DEFRAG TEST END] Cleaning up test directory...");
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir);
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });


    it('Scenario DFG1: Should analyze fragmentation on a fragmented file', async () => {
        // ... (same setup as before) ...
        await createAndPopulateBTree(db, "btree1_frag", 100, 200);
        await createAndPopulateBTree(db, "btree2_frag", 100, 200);
        await deleteBTreeAndData(db, "btree1_frag", 100);
        await createAndPopulateBTree(db, "btree3_frag", 50, 200);
        await db.forceCheckpoint();

        const stats = await db.debug_analyzeFragmentation(); // Use augmented Indinis type
        console.log("DFG1: Fragmentation Stats:", stats);
        expect(stats.total_pages).toBeGreaterThan(0);
        expect(stats.allocated_pages).toBeGreaterThan(0);
        expect(stats.free_pages).toBeGreaterThan(0);
        expect(stats.fragmentation_gaps).toBeGreaterThan(0);
        expect(stats.fragmentation_ratio).toBeGreaterThan(0);
    });

    it('Scenario DFG2: Conservative defragmentation should reduce gaps without shrinking file', async () => {
        // ... (same setup as before) ...
        await createAndPopulateBTree(db, "c_btree1", 100, 100);
        await createAndPopulateBTree(db, "c_btree2", 100, 100);
        await deleteBTreeAndData(db, "c_btree1", 100);
        await createAndPopulateBTree(db, "c_btree3", 70, 100);
        await db.forceCheckpoint();

        const initialStats = await db.debug_analyzeFragmentation();
        const initialDbStats = await db.debug_getDatabaseStats(); // Get full DB stats for file size
        console.log("DFG2: Initial Frag Stats:", initialStats, "Initial DB Size:", initialDbStats.file_size_bytes);
        expect(initialStats.fragmentation_gaps).toBeGreaterThan(0);

        const success = await db.debug_defragment('CONSERVATIVE');
        expect(success).toBe(true);

        const finalStats = await db.debug_analyzeFragmentation();
        const finalDbStats = await db.debug_getDatabaseStats();
        console.log("DFG2: Final Frag Stats:", finalStats, "Final DB Size:", finalDbStats.file_size_bytes);

        expect(finalStats.fragmentation_gaps).toBe(0);
        expect(finalStats.allocated_pages).toBe(initialStats.allocated_pages);
        expect(finalStats.free_pages).toBe(initialStats.free_pages);
        expect(finalDbStats.file_size_bytes).toBe(initialDbStats.file_size_bytes);
        // ... (data integrity verification) ...
                await db.transaction(async tx => {
            let val = await tx.get("defrag_col/item_c_btree2_00000");
            expect(val).not.toBeNull();
            val = await tx.get("defrag_col/item_c_btree3_00000");
            expect(val).not.toBeNull();
        });
    });

    it('Scenario DFG3: Aggressive defragmentation should reduce gaps AND shrink file', async () => {
        // ... (same setup as before) ...
        await createAndPopulateBTree(db, "a_btree1", 150, 150);
        await createAndPopulateBTree(db, "a_btree2", 150, 150);
        await deleteBTreeAndData(db, "a_btree1", 150);
        await db.forceCheckpoint();

        const initialStats = await db.debug_analyzeFragmentation();
        const initialDbStats = await db.debug_getDatabaseStats();
        console.log("DFG3: Initial Frag Stats:", initialStats, "Initial DB Size:", initialDbStats.file_size_bytes);
        expect(initialStats.fragmentation_gaps).toBeGreaterThan(0);
        expect(initialStats.free_pages).toBeGreaterThan(initialStats.allocated_pages * 0.3);

        const success = await db.debug_defragment('AGGRESSIVE');
        expect(success).toBe(true);

        const finalStats = await db.debug_analyzeFragmentation();
        const finalDbStats = await db.debug_getDatabaseStats();
        console.log("DFG3: Final Frag Stats:", finalStats, "Final DB Size:", finalDbStats.file_size_bytes);

        expect(finalStats.fragmentation_gaps).toBe(0);
        expect(finalStats.allocated_pages).toBe(initialStats.allocated_pages);
        expect(finalStats.free_pages).toBe(0);
        expect(finalDbStats.file_size_bytes).toBeLessThan(initialDbStats.file_size_bytes);
        
        const expectedMinSize = CPP_METADATA_FIXED_AREA_SIZE + finalStats.allocated_pages * CPP_PAGE_SIZE;
        expect(finalDbStats.file_size_bytes).toBe(expectedMinSize);
        // ... (data integrity verification) ...
        await db.transaction(async tx => {
            const val = await tx.get("defrag_col/item_a_btree2_00000");
            expect(val).not.toBeNull();
        });
    });
});