//tssrc/test/lsm_compaction_strategies.test.ts 
import { Indinis, IndinisOptions, LeveledCompactionOptionsJs, UniversalCompactionOptionsJs } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-compaction-strategies');

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const s = (obj: any): string => JSON.stringify(obj);

interface CompactionStrategyDoc {
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
            if (!levelsMap.has(parsed.level)) levelsMap.set(parsed.level, []);
            levelsMap.get(parsed.level)!.push(file);
        }
    }
    return levelsMap;
}

describe('Indinis LSM-Tree Compaction Strategies', () => {
    let db: Indinis | null = null;
    let testDataDir: string;
    let lsmDataDir: string;

    const colPath = 'comp_strategy_docs';
    // Use a small memtable size in C++ via a non-exposed option, or write enough data
    // to force flushes frequently. We will force flushes with checkpoints.
    const docsPerFlush = 15; // Number of docs to write to trigger one flush

    jest.setTimeout(120000); // 2 minutes for I/O heavy tests

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    afterEach(async () => {
        if (db) await db.close();
        db = null;
        if (testDataDir && fs.existsSync(testDataDir)) await rimraf(testDataDir);
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    // --- Leveled Compaction Test ---
    describe('Leveled Compaction Strategy (Default)', () => {
        const leveledOptions: IndinisOptions = {
            checkpointIntervalSeconds: 2,
            lsmOptions: {
                compactionStrategy: 'LEVELED',
                leveledCompactionOptions: {
                    l0CompactionTrigger: 4, // Trigger compaction when L0 has 4 files
                }
            }
        };

        beforeEach(async () => {
            const randomSuffix = `leveled-${Date.now()}`;
            testDataDir = path.join(TEST_DATA_DIR_BASE, randomSuffix);
            lsmDataDir = path.join(testDataDir, 'data', colPath);
            await fs.promises.mkdir(testDataDir, { recursive: true });
            db = new Indinis(testDataDir, leveledOptions);
            console.log(`\n[LEVELED TEST START] Using data directory: ${testDataDir}`);
        });

        it('should compact L0 files down to L1 when the trigger is reached', async () => {
            if (!db) fail('DB not initialized');
            
            const numL0FilesToCreate = leveledOptions.lsmOptions!.leveledCompactionOptions!.l0CompactionTrigger!;
            const allWrittenDocs = new Map<string, CompactionStrategyDoc>();

            console.log(`--- Leveled: Creating ${numL0FilesToCreate} L0 SSTables ---`);
            for (let i = 0; i < numL0FilesToCreate; i++) {
                await db.transaction(async tx => {
                    for (let j = 0; j < docsPerFlush; j++) {
                        const key = `${colPath}/batch${i}_doc${j}`;
                        const doc = { id: key, data: `Leveled data for ${key}`, batch: i };
                        await tx.set(key, s(doc));
                        allWrittenDocs.set(key, doc);
                    }
                });
                await db.forceCheckpoint(); // This flushes the memtable to an L0 SSTable
            }

            let levelsState = await listSSTablesByLevel(lsmDataDir);
            expect(levelsState.get(0)?.length ?? 0).toBe(numL0FilesToCreate);
            console.log(`  Verified ${numL0FilesToCreate} L0 files exist. Waiting for compaction scheduler...`);

            // Wait for the compaction scheduler to run (e.g., C++ scheduler interval + execution time)
            await delay(8000);

            levelsState = await listSSTablesByLevel(lsmDataDir);
            console.log("  SSTable levels after waiting for compaction:", levelsState);
            const l0FilesAfter = levelsState.get(0) || [];
            const l1FilesAfter = levelsState.get(1) || [];

            expect(l1FilesAfter.length).toBeGreaterThan(0); // The key result: L1 files were created
            expect(l0FilesAfter.length).toBe(0); // All L0 files should be consumed
            console.log(`  Verified: Compaction occurred. L0 files: ${l0FilesAfter.length}, L1 files: ${l1FilesAfter.length}.`);

            // Final data integrity check
            await db.transaction(async tx => {
                for (const [key, expectedDoc] of allWrittenDocs.entries()) {
                    const retrievedStr = await tx.get(key);
                    if (!retrievedStr) throw new Error(`Key "${key}" not found after leveled compaction.`);
                    const retrievedDoc = JSON.parse(retrievedStr as string);
                    expect(retrievedDoc.data).toEqual(expectedDoc.data);
                }
            });
            console.log(`  All ${allWrittenDocs.size} documents verified after leveled compaction.`);
        });
    });

    // --- Universal Compaction Test ---
    describe('Universal Compaction Strategy', () => {
        const universalOptions: IndinisOptions = {
            checkpointIntervalSeconds: 2,
            lsmOptions: {
                compactionStrategy: 'UNIVERSAL',
                universalCompactionOptions: {
                    l0CompactionTrigger: 3,
                    minMergeWidth: 2,
                    maxMergeWidth: 3,
                    sizeRatioPercent: 150, // Compact runs that are < 150% of the next run's size
                }
            }
        };

        beforeEach(async () => {
            const randomSuffix = `universal-${Date.now()}`;
            testDataDir = path.join(TEST_DATA_DIR_BASE, randomSuffix);
            lsmDataDir = path.join(testDataDir, 'data', colPath);
            await fs.promises.mkdir(testDataDir, { recursive: true });
            db = new Indinis(testDataDir, universalOptions);
            console.log(`\n[UNIVERSAL TEST START] Using data directory: ${testDataDir}`);
        });

        it('should merge runs of similarly-sized files within a level', async () => {
            if (!db) fail('DB not initialized');

            const numSSTablesToCreate = universalOptions.lsmOptions!.universalCompactionOptions!.l0CompactionTrigger! * 2;
            const allWrittenDocs = new Map<string, CompactionStrategyDoc>();

            console.log(`--- Universal: Creating ${numSSTablesToCreate} L0 SSTables to trigger multiple L0->L1 compactions ---`);
            for (let i = 0; i < numSSTablesToCreate; i++) {
                await db.transaction(async tx => {
                    for (let j = 0; j < docsPerFlush; j++) {
                        const key = `${colPath}/batch${i}_doc${j}`;
                        const doc = { id: key, data: `Universal data for ${key}`, batch: i };
                        await tx.set(key, s(doc));
                        allWrittenDocs.set(key, doc);
                    }
                });
                await db.forceCheckpoint();
                // Wait for scheduler to potentially run after each L0 trigger is met
                if ((i + 1) % universalOptions.lsmOptions!.universalCompactionOptions!.l0CompactionTrigger! === 0) {
                    console.log(`  L0 trigger potentially met after batch ${i}. Waiting for compaction...`);
                    await delay(8000);
                }
            }

            let levelsState = await listSSTablesByLevel(lsmDataDir);
            console.log("  SSTable levels after all writes:", levelsState);

            const l1Files = levelsState.get(1) || [];
            // We forced multiple L0->L1 compactions, so we should have several files in L1
            // which can now be candidates for a size-tiered merge.
            expect(l1Files.length).toBeGreaterThanOrEqual(1);

            console.log("  Waiting for potential size-tiered (L1+) compaction...");
            await delay(10000);
            
            let finalLevelsState = await listSSTablesByLevel(lsmDataDir);
            console.log("  SSTable levels after final wait:", finalLevelsState);
            const finalL1Files = finalLevelsState.get(1) || [];

            // This is the key assertion for universal compaction: the number of files in a level
            // should decrease as smaller files are merged into larger ones.
            if (l1Files.length >= universalOptions.lsmOptions!.universalCompactionOptions!.minMergeWidth!) {
                expect(finalL1Files.length).toBeLessThan(l1Files.length);
                console.log(`  Verified: Universal compaction occurred in L1. File count reduced from ${l1Files.length} to ${finalL1Files.length}.`);
            } else {
                console.log(`  Skipping universal compaction check in L1, not enough files created (${l1Files.length}).`);
            }

            // Final data integrity check
            await db.transaction(async tx => {
                for (const [key, expectedDoc] of allWrittenDocs.entries()) {
                    const retrievedStr = await tx.get(key);
                    if (!retrievedStr) throw new Error(`Key "${key}" not found after universal compaction.`);
                    const retrievedDoc = JSON.parse(retrievedStr as string);
                    expect(retrievedDoc.data).toEqual(expectedDoc.data);
                }
            });
            console.log(`  All ${allWrittenDocs.size} documents verified after universal compaction.`);
        });
    });
});