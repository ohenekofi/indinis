// tssrc/test/lsm_adaptive_compaction.test.ts
import { Indinis, IndinisOptions, ThreadPoolStatsJs } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-adaptive-compaction');

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const s = (obj: any): string => JSON.stringify(obj);

interface CompactionTestDoc {
    id: string;
    data: string;
    batch: number;
}

// Helper to parse SSTable filenames to check levels
function parseSSTableFilename(filename: string): { level: number; id: number } | null {
    const match = filename.match(/^sstable_L(\d+)_(\d+)\.dat$/);
    if (match) {
        return { level: parseInt(match[1], 10), id: parseInt(match[2], 10) };
    }
    return null;
}

// Helper to list SSTables on disk by their level
async function listSSTablesByLevel(lsmDataDir: string): Promise<Map<number, string[]>> {
    const levelsMap = new Map<number, string[]>();
    if (!fs.existsSync(lsmDataDir)) return levelsMap;
    const files = await fs.promises.readdir(lsmDataDir);
    for (const file of files) {
        const parsed = parseSSTableFilename(file);
        if (parsed) {
            if (!levelsMap.has(parsed.level)) {
                levelsMap.set(parsed.level, []);
            }
            levelsMap.get(parsed.level)!.push(file);
        }
    }
    return levelsMap;
}

describe('LSM-Tree Adaptive and Prioritized Compaction', () => {
    let db: Indinis | null = null;
    let testDataDir: string;
    let lsmDataDir: string;

    const colPath = 'adaptive_compaction_docs';
    const L0_COMPACTION_TRIGGER_FROM_CPP = 4;
    const CPP_SCHEDULER_INTERVAL_SECONDS = 5;

    const testOptions: IndinisOptions = {
        checkpointIntervalSeconds: 2,
        minCompactionThreads: 1, // Start with a minimum of 1
        maxCompactionThreads: 4, // Allow scaling up to 4
        // Use a small segment size to avoid WAL files getting too large during the test
        walOptions: { segment_size: 128 * 1024 }
    };

    jest.setTimeout(90000); // 90 seconds for this I/O heavy test

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        lsmDataDir = path.join(testDataDir, 'data', colPath); // Path to the specific LSM store's data
        await fs.promises.mkdir(testDataDir, { recursive: true });
        console.log(`\n[ADAPTIVE COMPACTION TEST START] Using data directory: ${testDataDir}`);
    });

    afterEach(async () => {
        if (db) await db.close();
        db = null;
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir);
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    it('should use the adaptive thread pool, trigger L0->L1 compaction, and maintain data integrity', async () => {
        db = new Indinis(testDataDir, testOptions);
        
        const allWrittenDocs = new Map<string, CompactionTestDoc>();
        const docsPerFlush = 10; // Enough data to ensure memtable is not empty

        console.log(`--- Phase 1: Creating ${L0_COMPACTION_TRIGGER_FROM_CPP} L0 SSTables to meet the compaction threshold ---`);

        for (let i = 0; i < L0_COMPACTION_TRIGGER_FROM_CPP; i++) {
            const batchNum = i + 1;
            await db.transaction(async tx => {
                for (let j = 0; j < docsPerFlush; j++) {
                    const key = `${colPath}/batch${batchNum}_doc${j}`;
                    const doc: CompactionTestDoc = { id: key, data: `Data for ${key}`, batch: batchNum };
                    await tx.set(key, s(doc));
                    allWrittenDocs.set(key, doc);
                }
            });
            // Force a checkpoint, which also flushes the active memtable, creating an L0 SSTable.
            await db.forceCheckpoint();
            console.log(`  Batch ${batchNum}: Data committed and memtable flushed via checkpoint.`);
        }
        
        let levelsState = await listSSTablesByLevel(lsmDataDir);
        expect(levelsState.get(0)?.length ?? 0).toBe(L0_COMPACTION_TRIGGER_FROM_CPP);
        console.log(`  Verified: ${L0_COMPACTION_TRIGGER_FROM_CPP} L0 SSTables now exist on disk.`);

        // --- Phase 2: Verify Thread Pool Stats and Trigger Compaction ---
        let poolStats = await db.debug_getThreadPoolStats();
        console.log("  Initial thread pool stats:", poolStats);
        expect(poolStats).not.toBeNull();
        if (poolStats) {
            expect(poolStats.minThreads).toBe(testOptions.minCompactionThreads);
            expect(poolStats.maxThreads).toBe(testOptions.maxCompactionThreads);
            expect(poolStats.currentThreadCount).toBe(testOptions.minCompactionThreads);
        }

        console.log("\n--- Phase 2: Writing one more batch to trigger the compaction scheduler ---");
        await db.transaction(async tx => {
            const key = `${colPath}/trigger_doc`;
            const doc: CompactionTestDoc = { id: key, data: "trigger data", batch: 99 };
            await tx.set(key, s(doc));
            allWrittenDocs.set(key, doc);
        });
        await db.forceCheckpoint();
        
        levelsState = await listSSTablesByLevel(lsmDataDir);
        const l0countBeforeCompaction = levelsState.get(0)?.length ?? 0;
        console.log(`  Trigger batch flushed. L0 SSTable count is now ${l0countBeforeCompaction}.`);
        expect(l0countBeforeCompaction).toBeGreaterThanOrEqual(L0_COMPACTION_TRIGGER_FROM_CPP);

        // --- Phase 3: Wait for Compaction and Verify ---
        const waitTime = (CPP_SCHEDULER_INTERVAL_SECONDS * 1000) + 3000; // Wait for scheduler + some execution time
        console.log(`\n--- Phase 3: Waiting ${waitTime / 1000}s for the compaction to run... ---`);
        await delay(waitTime);

        poolStats = await db.debug_getThreadPoolStats();
        console.log("  Thread pool stats during/after compaction:", poolStats);
        // We can't deterministically know if it scaled up/down in this short time,
        // but we can check it's still within bounds.
        if (poolStats) {
            expect(poolStats.currentThreadCount).toBeGreaterThanOrEqual(testOptions.minCompactionThreads!);
            expect(poolStats.currentThreadCount).toBeLessThanOrEqual(testOptions.maxCompactionThreads!);
        }

        levelsState = await listSSTablesByLevel(lsmDataDir);
        console.log("  SSTable levels after waiting:", levelsState);
        const l0FilesAfter = levelsState.get(0) || [];
        const l1FilesAfter = levelsState.get(1) || [];

        expect(l1FilesAfter.length).toBeGreaterThan(0); // The key result: L1 files were created.
        expect(l0FilesAfter.length).toBeLessThan(l0countBeforeCompaction); // L0 files were consumed.
        
        console.log(`  Verified: Compaction occurred. L0 files: ${l0FilesAfter.length}, L1 files: ${l1FilesAfter.length}.`);

        // --- Phase 4: Final Data Integrity Check ---
        console.log("\n--- Phase 4: Verifying data integrity after compaction ---");
        await db.transaction(async tx => {
            for (const [key, expectedDoc] of allWrittenDocs.entries()) {
                const retrievedStr = await tx.get(key);
                if (!retrievedStr) {
                    throw new Error(`Data integrity failure: Key "${key}" not found after compaction.`);
                }
                const retrievedDoc = JSON.parse(retrievedStr as string) as CompactionTestDoc;
                expect(retrievedDoc.data).toEqual(expectedDoc.data);
            }
        });
        console.log(`  All ${allWrittenDocs.size} documents verified successfully.`);
    });
});