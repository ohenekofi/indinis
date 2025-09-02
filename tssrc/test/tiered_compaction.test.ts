// @tssrc/test/tiered_compaction.test.ts
import { Indinis, IndinisOptions, StorageValue } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-tiered-compaction-v2');

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const s = (obj: any): string => JSON.stringify(obj);

interface TieredDoc {
    id: string;
    data: string;
    version: number;
    batchInfo: string;
}

function parseSSTableFilename(filename: string): { level: number; id: number } | null {
    const match = filename.match(/^sstable_L(\d+)_(\d+)\.dat$/);
    if (match) {
        return { level: parseInt(match[1], 10), id: parseInt(match[2], 10) };
    }
    return null;
}

async function listSSTablesByLevel(lsmDataDir: string): Promise<Map<number, { filename: string, id: number }[]>> {
    const levelsMap = new Map<number, { filename: string, id: number }[]>();
    if (!fs.existsSync(lsmDataDir)) {
        console.warn(`LSM data directory not found for listing SSTables: ${lsmDataDir}`);
        return levelsMap;
    }

    try {
        const files = await fs.promises.readdir(lsmDataDir);
        for (const file of files) {
            const parsed = parseSSTableFilename(file);
            if (parsed) {
                if (!levelsMap.has(parsed.level)) {
                    levelsMap.set(parsed.level, []);
                }
                levelsMap.get(parsed.level)!.push({ filename: file, id: parsed.id });
            }
        }
        for (const levelFiles of levelsMap.values()) {
            levelFiles.sort((a, b) => a.id - b.id);
        }
    } catch (error) {
        console.error(`Error listing SSTables in ${lsmDataDir}:`, error);
    }
    return levelsMap;
}

describe('Indinis LSMTree Tiered Compaction (v2 Test)', () => {
    let db: Indinis | null = null;
    let testDataDir: string;
    let lsmDataDir: string;
    let walDir: string;

    const colPath = 'compaction_test_docs';
    const L0_COMPACTION_TRIGGER_FROM_CPP = 4;
    const CHECKPOINT_INTERVAL_SECONDS_FOR_TEST = 2;
    // Define the C++ scheduler interval here as a constant for the test logic
    const CPP_SCHEDULER_INTERVAL_SECONDS = 5;


    const getTestOptions = (): IndinisOptions => ({
        checkpointIntervalSeconds: CHECKPOINT_INTERVAL_SECONDS_FOR_TEST,
        sstableDataBlockUncompressedSizeKB: 4,
        sstableCompressionType: "NONE",
        walOptions: {
            wal_directory: walDir,
            segment_size: 256 * 1024,
            wal_file_prefix: "tiered_comp_wal",
        }
    });

    jest.setTimeout(180000);

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        lsmDataDir = path.join(testDataDir, 'data');
        walDir = path.join(testDataDir, 'custom_wal_dir_for_tiered_comp');
        
        await fs.promises.mkdir(testDataDir, { recursive: true });
        console.log(`\n[TIERED COMP TEST V2 START] Using data directory: ${testDataDir}`);
    });

    afterEach(async () => {
        if (db) {
            try { await db.close(); } catch (e) { console.error("Error closing DB:", e); }
            db = null;
        }
        await delay(700);
        if (fs.existsSync(testDataDir)) {
            console.log(`[TIERED COMP TEST V2 END] Cleaning up: ${testDataDir}`);
            await rimraf(testDataDir, { maxRetries: 3, retryDelay: 1000 }).catch(err =>
                console.error(`Cleanup error for ${testDataDir}:`, err)
            );
        }
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) {
            await rimraf(TEST_DATA_DIR_BASE).catch(err => console.error(`Base cleanup error:`, err));
        }
    });

    it('should trigger L0->L1 compaction after L0 fills and verify data integrity', async () => {
        const testDbOptions = getTestOptions();
        db = new Indinis(testDataDir, testDbOptions);
        await delay(300);

        const docsPerIndividualSSTable = 3; // Reduced to make memtable flushing more likely with fewer docs
        const numL0SSTablesToCreate = L0_COMPACTION_TRIGGER_FROM_CPP;
        const allWrittenDocs = new Map<string, TieredDoc>();

        console.log(`--- Test Goal: Create ${L0_COMPACTION_TRIGGER_FROM_CPP} L0 SSTables, then one more to trigger L0->L1 compaction ---`);

        for (let sstableIdx = 0; sstableIdx < numL0SSTablesToCreate; sstableIdx++) {
            const batchInfo = `L0_SSTABLE_${sstableIdx}`;
            console.log(`  Preparing data for intended L0 SSTable #${sstableIdx + 1}`);
            await db.transaction(async tx => {
                for (let docIdx = 0; docIdx < docsPerIndividualSSTable; docIdx++) {
                    const key = `${colPath}/${batchInfo}_doc${docIdx}`;
                    const doc: TieredDoc = { id: key, data: `Data for ${key}`, version: 1, batchInfo };
                    await tx.set(key, s(doc));
                    allWrittenDocs.set(key, doc);
                }
            });
            console.log(`    Data for SSTable #${sstableIdx + 1} committed. Forcing checkpoint to flush memtable...`);
            const cpForced = await db.forceCheckpoint();
            expect(cpForced).toBe(true);
            await delay( (CHECKPOINT_INTERVAL_SECONDS_FOR_TEST * 1000) + 1500);

            const levelsState = await listSSTablesByLevel(lsmDataDir);
            const l0FilesCount = levelsState.get(0)?.length || 0; // Corrected: use .length for array
            console.log(`    L0 SSTables after creating #${sstableIdx + 1}: ${l0FilesCount}`);
            expect(l0FilesCount).toBe(sstableIdx + 1);
        }
        
        let levelsStateBeforeTrigger = await listSSTablesByLevel(lsmDataDir);
        expect(levelsStateBeforeTrigger.get(0)?.length || 0).toBe(L0_COMPACTION_TRIGGER_FROM_CPP); // Corrected: use .length
        console.log(`  L0 now has ${L0_COMPACTION_TRIGGER_FROM_CPP} SSTables (at trigger point).`);

        const triggerBatchInfo = `L0_SSTABLE_TRIGGER`;
        console.log(`  Writing one more batch (${triggerBatchInfo}) to trigger L0 compaction...`);
        await db.transaction(async tx => {
            for (let docIdx = 0; docIdx < docsPerIndividualSSTable; docIdx++) {
                const key = `${colPath}/${triggerBatchInfo}_doc${docIdx}`;
                const doc: TieredDoc = { id: key, data: `Data for ${key}`, version: 1, batchInfo: triggerBatchInfo };
                await tx.set(key, s(doc));
                allWrittenDocs.set(key, doc);
            }
        });
        console.log(`    Trigger batch committed. Forcing checkpoint to flush to L0...`);
        const cpForcedLast = await db.forceCheckpoint();
        expect(cpForcedLast).toBe(true);
        await delay( (CHECKPOINT_INTERVAL_SECONDS_FOR_TEST * 1000) + 1500);
        
        let levelsStateAfterTriggerFlush = await listSSTablesByLevel(lsmDataDir);
        console.log("SSTable levels immediately after flushing the triggering L0 table:", levelsStateAfterTriggerFlush);
        expect(levelsStateAfterTriggerFlush.get(0)?.length || 0).toBeGreaterThanOrEqual(L0_COMPACTION_TRIGGER_FROM_CPP); // Corrected: use .length

        console.log("  Waiting for L0->L1 compaction to occur (e.g., up to 15 seconds)...");
        // Use the CPP_SCHEDULER_INTERVAL_SECONDS constant defined in this test file

        console.log("  Waiting for L0->L1 compaction to occur AND file system to sync (e.g., up to 20-25 seconds)...");
        //await delay( (CPP_SCHEDULER_INTERVAL_SECONDS * 1000) + 10000);
        await delay(25000);

        const levelsStateAfterCompaction = await listSSTablesByLevel(lsmDataDir);
        console.log("SSTable levels after waiting for L0->L1 compaction:", levelsStateAfterCompaction);

        const l0FilesAfterCompaction = levelsStateAfterCompaction.get(0) || [];
        const l1FilesAfterCompaction = levelsStateAfterCompaction.get(1) || [];

        expect(l0FilesAfterCompaction.length).toBeLessThan(L0_COMPACTION_TRIGGER_FROM_CPP);
        expect(l1FilesAfterCompaction.length).toBeGreaterThan(0);
        console.log(`  Compaction Result: L0 files: ${l0FilesAfterCompaction.length}, L1 files: ${l1FilesAfterCompaction.length}`);

        console.log("  Verifying data integrity after L0->L1 compaction...");
        await db.transaction(async tx => {
            let verifiedCount = 0;
            for (const [key, expectedDoc] of allWrittenDocs.entries()) {
                const retrievedStr = await tx.get(key);
                if (!retrievedStr) {
                    throw new Error(`Data integrity check failed: Key "${key}" (batch: ${expectedDoc.batchInfo}) not found after compaction.`);
                }
                const retrievedDoc = JSON.parse(retrievedStr as string) as TieredDoc;
                expect(retrievedDoc.data).toEqual(expectedDoc.data);
                expect(retrievedDoc.batchInfo).toEqual(expectedDoc.batchInfo);
                verifiedCount++;
            }
            expect(verifiedCount).toBe(allWrittenDocs.size);
        });
        console.log(`  All ${allWrittenDocs.size} documents verified successfully after L0->L1 compaction.`)
    });
});