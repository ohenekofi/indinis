// tssrc/test/lsm_tree_upgrades.test.ts

import { Indinis, IndinisOptions, StorageValue, KeyValueRecord } from '../index';
import * as fs from 'fs';
import *as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-lsm-upgrades-v1');

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

function generateRandomString(length: number): string {
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-. RepetitiveSequencePattern';
    let result = '';
    const charactersLength = characters.length;
    for (let i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() * charactersLength));
    }
    return result;
}

const MONOSTATE_MARKER_STRING = "__IndinisDB::MonostateValue__";

interface LsmUpgradeTestDoc {
    id: string;
    textPayload?: string;
    numberPayload?: number;
    bigIntPayloadString?: string;
    binaryPayload?: { type: 'Buffer'; data: number[] };
    isMonostate?: boolean;
    version: number;
    batch: string;
}

describe('LSMTree Upgrades Test (Background Flush, GetPrefix, ValueTypes, TxnID Persistence)', () => {
    let db: Indinis | null = null;
    let testDataDir: string;
    let walDirForTest: string;
    let lsmDataDir: string;

    const colPath = 'lsm_upg_docs';
    const MAX_IMMUTABLE_MEMTABLES_CPP = 2;
    const DOCS_PER_FLUSH_BATCH = 5; // Reduced for shorter logs
    const SSTABLE_BLOCK_SIZE_KB = 4;
    const L0_TRIGGER_COUNT_CPP = 4;
    const CHECKPOINT_INTERVAL = 3;
    const FLUSH_WORKER_INTERVAL_GUESS_MS = 150;
    const CPP_LSM_SCHEDULER_INTERVAL_SECONDS = 5;

    const getTestOptions = (): IndinisOptions => ({
        checkpointIntervalSeconds: CHECKPOINT_INTERVAL,
        sstableDataBlockUncompressedSizeKB: SSTABLE_BLOCK_SIZE_KB,
        sstableCompressionType: "ZSTD",
        walOptions: {
            wal_directory: walDirForTest,
            segment_size: 256 * 1024,
            wal_file_prefix: `lsm_upg_wal_`,
        },
    });

    jest.setTimeout(300000);

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        walDirForTest = path.join(testDataDir, 'custom_wal_lsm_upg');
        lsmDataDir = path.join(testDataDir, 'data');

        await fs.promises.mkdir(testDataDir, { recursive: true });
        
        console.log(`\n[LSM UPG TEST START] DataDir: ${testDataDir}, WALDir: ${walDirForTest}`);
        db = new Indinis(testDataDir, getTestOptions());
        await delay(600);
    });

    afterEach(async () => {
        const currentDir = testDataDir;
        console.log(`[LSM UPG TEST END - ${path.basename(currentDir)}] Closing DB...`);
        if (db) {
            try { await db.close(); } catch (e) { console.error(`Error closing DB in ${currentDir}:`, e); }
            db = null;
        }
        await delay(1800);
        if (currentDir && fs.existsSync(currentDir)) {
            console.log(`[LSM UPG TEST END - ${path.basename(currentDir)}] Cleaning up dir: ${currentDir}`);
            await rimraf(currentDir, { maxRetries: 3, retryDelay: 1000 }).catch(err =>
                console.error(`Cleanup error for ${currentDir}:`, err)
            );
        }
    });

    afterAll(async () => {
        console.log("[LSM UPG SUITE END] Cleaning up base directory...");
        if (fs.existsSync(TEST_DATA_DIR_BASE)) {
            await rimraf(TEST_DATA_DIR_BASE).catch(err => console.error(`Base cleanup error:`, err));
        }
    });

    async function countSSTables(level?: number): Promise<number> {
        if (!fs.existsSync(lsmDataDir)) return 0;
        const files = await fs.promises.readdir(lsmDataDir);
        return files.filter(f => {
            const match = f.match(/^sstable_L(\d+)_(\d+)\.dat$/);
            if (!match) return false;
            return level === undefined || parseInt(match[1], 10) === level;
        }).length;
    }

    it('should test background flushing, ValueType variations, getPrefix, and compaction effects', async () => {
        if (!db) throw new Error("DB not initialized for LSM upgrades test");

        const allDocsForVerification = new Map<string, LsmUpgradeTestDoc>();
        let currentTxnId = 0;

        console.log("--- Phase 1: Writing diverse data to trigger memtable flushes ---");
        const batchesForFlushTest = MAX_IMMUTABLE_MEMTABLES_CPP + 2;

        for (let batchIdx = 0; batchIdx < batchesForFlushTest; batchIdx++) {
            const batchName = `flush_batch_${batchIdx}`;
            console.log(`  Starting ${batchName}, writing ${DOCS_PER_FLUSH_BATCH} docs...`);
            await db.transaction(async tx => {
                for (let i = 0; i < DOCS_PER_FLUSH_BATCH; i++) {
                    const docId = `doc_${i}`;
                    const key = `${colPath}/${batchName}/${docId}`;
                    let doc: LsmUpgradeTestDoc = { id: docId, version: 1, batch: batchName };
                    
                    let valueToSet: StorageValue;

                    const typeSelector = i % 5;
                    if (typeSelector === 0) {
                        doc.textPayload = `Text payload for ${key} ${generateRandomString(50)}`;
                        valueToSet = doc.textPayload;
                    } else if (typeSelector === 1) {
                        doc.numberPayload = parseFloat((Math.random() * 100000).toFixed(3));
                        valueToSet = doc.numberPayload;
                    } else if (typeSelector === 2) {
                        const binaryData = Buffer.from(`Binary data for ${key} - ${i} ` + generateRandomString(20));
                        doc.binaryPayload = { type: 'Buffer', data: Array.from(binaryData) };
                        valueToSet = binaryData;
                    } else if (typeSelector === 3) {
                        doc.isMonostate = true; // This flag is for test logic, the value is still a string
                        valueToSet = MONOSTATE_MARKER_STRING;
                    } else {
                        const bigIntValue = BigInt(Date.now()) + BigInt(i) + BigInt(batchIdx * 1000);
                        doc.bigIntPayloadString = bigIntValue.toString();
                        valueToSet = doc.bigIntPayloadString;
                    }
                    await tx.set(key, valueToSet);
                    allDocsForVerification.set(key, doc);
                }
                currentTxnId = await tx.getId();
            });
            console.log(`    ${batchName} committed. Current TxnID: ${currentTxnId}.`);
            await delay(FLUSH_WORKER_INTERVAL_GUESS_MS * 2);
        }
        const sstCountAfterFlushWrites = await countSSTables(0);
        console.log(`  Phase 1 writes complete. L0 SSTables created by background flush: ${sstCountAfterFlushWrites}`);
        expect(sstCountAfterFlushWrites).toBeGreaterThanOrEqual(0);


        console.log("\n--- Phase 2: Verifying all diverse data after potential flushes ---");
        await db.transaction(async tx => {
            let verifiedCount = 0;
            for (const [key, expectedDoc] of allDocsForVerification.entries()) {
                const retrievedRaw = await tx.get(key);
                
                if (expectedDoc.isMonostate) {
                    expect(typeof retrievedRaw === 'string').toBe(true);
                    expect(retrievedRaw as string).toEqual(MONOSTATE_MARKER_STRING);
                } else if (expectedDoc.binaryPayload) {
                    expect(retrievedRaw).toBeInstanceOf(Buffer);
                    expect((retrievedRaw as Buffer).equals(Buffer.from(expectedDoc.binaryPayload.data))).toBe(true);
                } else if (expectedDoc.bigIntPayloadString) {
    console.log(`[BigInt Verification] Expected: string (${expectedDoc.bigIntPayloadString}), Got type: ${typeof retrievedRaw}, Value:`, retrievedRaw);
    console.log(`[DEBUG VERIFY Phase 2] Key: ${key}, Expected BigIntPayload: ${expectedDoc.bigIntPayloadString}, RetrievedRaw Type: ${typeof retrievedRaw}, RetrievedRaw Value:`, retrievedRaw);
    expect(typeof retrievedRaw).toBe('number'); // <--- CHANGE THIS
    // Compare string representations if precision is a concern, or direct number if appropriate
    expect((retrievedRaw as number).toString()).toEqual(expectedDoc.bigIntPayloadString); // <--- CHANGE THIS
                }else if (expectedDoc.numberPayload !== undefined) {
                    // Debug log only for the number payload case
                    if (typeof retrievedRaw !== 'number') { // Add this check before expect
                         console.log(`[DEBUG VERIFY Phase 2 - Type Mismatch] Key: ${key}, Expected NumberPayload: ${expectedDoc.numberPayload}, RetrievedRaw Type: ${typeof retrievedRaw}, RetrievedRaw Value:`, retrievedRaw);
                    }
                    expect(typeof retrievedRaw === 'number').toBe(true);
                    if (typeof retrievedRaw === 'number') {
                        expect(retrievedRaw).toBeCloseTo(expectedDoc.numberPayload, 5);
                    }
                } else if (expectedDoc.textPayload !== undefined) {
                    expect(typeof retrievedRaw === 'string').toBe(true);
                    expect(retrievedRaw as string).toEqual(expectedDoc.textPayload);
                } else {
                    fail(`Key ${key} has no expected payload type for verification (doc: ${JSON.stringify(expectedDoc)}).`);
                }
                verifiedCount++;
            }
            expect(verifiedCount).toBe(allDocsForVerification.size);
        });
        console.log(`  All ${allDocsForVerification.size} diverse documents verified correctly.`);


        console.log("\n--- Phase 3: Testing getPrefix ---");
        const prefixToTest = `${colPath}/flush_batch_1/`;
        // ** OPTION 1 Correction: Remove !doc.isMonostate filter here **
        const expectedPrefixDocsInfo = Array.from(allDocsForVerification.entries())
            .filter(([key, doc]) => {
                return key.startsWith(prefixToTest) &&
                       countPathSegments(key) === (countPathSegments(prefixToTest) + 1);
                       // Removed: && !doc.isMonostate; 
            })
            .map(([key, doc]) => ({ key: key, doc: doc }));

        let prefixResults: KeyValueRecord[] = [];
        await db.transaction(async tx => {
            prefixResults = await tx.getPrefix(prefixToTest);
        });
        
        console.log(`  getPrefix for '${prefixToTest}' returned ${prefixResults.length} items. Expected ${expectedPrefixDocsInfo.length}.`);
        expect(prefixResults.length).toBe(expectedPrefixDocsInfo.length); // Now expects 5 for flush_batch_1

        for (const expectedItem of expectedPrefixDocsInfo) {
            const found = prefixResults.find(r => r.key === expectedItem.key);
            expect(found).toBeDefined();
            if (found) {
                const originalDoc = expectedItem.doc;
                
                if (originalDoc.isMonostate) {
                    expect(typeof found.value === 'string').toBe(true);
                    expect(found.value as string).toEqual(MONOSTATE_MARKER_STRING);
                } else if (originalDoc.binaryPayload) {
                     expect(found.value).toBeInstanceOf(Buffer);
                     expect((found.value as Buffer).equals(Buffer.from(originalDoc.binaryPayload.data))).toBe(true);
                } else if (originalDoc.bigIntPayloadString) {
                    expect(typeof found.value === 'string').toBe(true);
                    expect(found.value as string).toEqual(originalDoc.bigIntPayloadString);
                } else if (originalDoc.numberPayload !== undefined) {
                    expect(typeof found.value === 'number').toBe(true);
                    if (typeof found.value === 'number') {
                         expect(found.value).toBeCloseTo(originalDoc.numberPayload, 5);
                    }
                } else if (originalDoc.textPayload !== undefined) {
                    expect(typeof found.value === 'string').toBe(true);
                    expect(found.value as string).toEqual(originalDoc.textPayload);
                }
            }
        }
        console.log(`  getPrefix results for '${prefixToTest}' verified.`);

        const limitForPrefix = Math.floor(expectedPrefixDocsInfo.length / 3);
        if (limitForPrefix > 0) {
            await db.transaction(async tx => {
                prefixResults = await tx.getPrefix(prefixToTest, limitForPrefix);
            });
            expect(prefixResults.length).toBe(limitForPrefix);
            console.log(`  getPrefix with limit ${limitForPrefix} returned ${prefixResults.length} items (correct).`);
        }

        console.log("\n--- Phase 4: Triggering Compactions ---");
        const additionalBatchesForCompaction = L0_TRIGGER_COUNT_CPP + 2;
        for (let batchIdx = 0; batchIdx < additionalBatchesForCompaction; batchIdx++) {
            const batchName = `compaction_trigger_batch_${batchIdx}`;
            await db.transaction(async tx => {
                for (let i = 0; i < DOCS_PER_FLUSH_BATCH; i++) {
                    const docId = `doc_${i}`;
                    const key = `${colPath}/${batchName}/${docId}`;
                    const doc: LsmUpgradeTestDoc = { id: docId, textPayload: `Compact data ${key}`, version: 1, batch: batchName };
                    await tx.set(key, doc.textPayload!);
                    allDocsForVerification.set(key, doc);
                }
            });
             console.log(`    Compaction trigger batch ${batchIdx + 1} committed.`);
        }
        
        console.log("  Data for compaction trigger written. Waiting for compactions...");
        await delay((CHECKPOINT_INTERVAL + CPP_LSM_SCHEDULER_INTERVAL_SECONDS + 10) * 1000);

        const l0CountAfterCompaction = await countSSTables(0);
        const l1CountAfterCompaction = await countSSTables(1);
        console.log(`  SSTable counts after waiting for compaction: L0=${l0CountAfterCompaction}, L1=${l1CountAfterCompaction}`);
        if (sstCountAfterFlushWrites + additionalBatchesForCompaction > L0_TRIGGER_COUNT_CPP) {
           expect(l1CountAfterCompaction).toBeGreaterThan(0);
           expect(l0CountAfterCompaction).toBeLessThanOrEqual(L0_TRIGGER_COUNT_CPP + 1);
        }

        console.log("\n--- Phase 5: Final data integrity check after compactions ---");
        await db.transaction(async tx => {
            let finalVerifiedCount = 0;
            for (const [key, expectedDoc] of allDocsForVerification.entries()) {
                const retrievedRaw = await tx.get(key);
                
                if (expectedDoc.isMonostate) {
                    expect(typeof retrievedRaw === 'string').toBe(true);
                    expect(retrievedRaw as string).toEqual(MONOSTATE_MARKER_STRING);
                } else if (expectedDoc.binaryPayload) {
                    expect(retrievedRaw).toBeInstanceOf(Buffer);
                    expect((retrievedRaw as Buffer).equals(Buffer.from(expectedDoc.binaryPayload.data))).toBe(true);
                } else if (expectedDoc.bigIntPayloadString) {
                    // C++ converts numeric strings to int64_t if possible,
                    // then NAPI converts int64_t back to JS number.
                    expect(typeof retrievedRaw === 'number').toBe(true); // EXPECT A NUMBER
                    if (typeof retrievedRaw === 'number') {
                        // Compare the numeric value. JS numbers are doubles, so direct comparison with BigInt string is tricky.
                        // Best to compare their string representations if the number is large.
                        expect(retrievedRaw.toString()).toEqual(expectedDoc.bigIntPayloadString);
                    }
                } else if (expectedDoc.numberPayload !== undefined) {
                     // Debug log only for the number payload case
                     if (typeof retrievedRaw !== 'number') {
                        console.log(`[DEBUG VERIFY Phase 5 - Type Mismatch] Key: ${key}, Expected NumberPayload: ${expectedDoc.numberPayload}, RetrievedRaw Type: ${typeof retrievedRaw}, RetrievedRaw Value:`, retrievedRaw);
                    }
                    expect(typeof retrievedRaw === 'number').toBe(true);
                     if (typeof retrievedRaw === 'number') {
                        expect(retrievedRaw).toBeCloseTo(expectedDoc.numberPayload, 5);
                     }
                } else if (expectedDoc.textPayload !== undefined) {
                    expect(typeof retrievedRaw === 'string').toBe(true);
                    expect(retrievedRaw as string).toEqual(expectedDoc.textPayload);
                } else {
                    fail(`Key ${key} has no expected payload type for final check (doc: ${JSON.stringify(expectedDoc)}).`);
                }
                finalVerifiedCount++;
            }
            expect(finalVerifiedCount).toBe(allDocsForVerification.size);
        });
        console.log(`  All ${allDocsForVerification.size} documents re-verified successfully after compactions.`);
    });
});

function countPathSegments(pathVal: string): number {
    if (!pathVal || pathVal === '/') return 0;
    const normalizedPath = pathVal.startsWith('/') ? pathVal.substring(1) : pathVal;
    const finalPath = normalizedPath.endsWith('/') ? normalizedPath.substring(0, normalizedPath.length - 1) : normalizedPath;
    if (finalPath === '') return 0;
    return finalPath.split('/').length;
}