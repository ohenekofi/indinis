// filename: tssrc/test/wal_segment_checkpoint_stress.test.ts

import { Indinis, IndinisOptions, CheckpointInfo } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

// Base directory for this specific test suite
const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-sharded-wal-validation');

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const s = (obj: any): string => JSON.stringify(obj);

interface SegmentTestDoc {
    id: string;
    batch: number;
    docInBatch: number;
    data: string;
}

// --- Log Parsing Utilities (for debugging and validation) ---
// These are crucial for inspecting the state of the WAL on disk.
enum LogRecordType {
    INVALID = 0, BEGIN_TXN = 1, COMMIT_TXN = 2, ABORT_TXN = 3,
    DATA_PUT = 4, DATA_DELETE = 5,
    CHECKPOINT_BEGIN = 6, CHECKPOINT_END = 7,
}
interface DecodedLogRecord {
    lsn: bigint; txn_id: bigint; type: LogRecordType; key: string; value: string;
}

// Helper to parse the segment ID from a sharded filename
function parseSegmentIdFromFilename(filename: string, prefix: string): { shardId: number, segmentId: number } | null {
    const match = filename.match(new RegExp(`^${prefix}_shard_(\\d+)_(\\d+)\\.log$`));
    if (match) {
        return { shardId: parseInt(match[1], 10), segmentId: parseInt(match[2], 10) };
    }
    return null;
}

// A robust parser for a single WAL segment file
function parseSingleWalSegment(filePath: string): DecodedLogRecord[] {
    if (!fs.existsSync(filePath)) return [];
    const buffer = fs.readFileSync(filePath);
    const records: DecodedLogRecord[] = [];
    let offset = 34; // Skip C++ WALSegmentHeader

    while (offset < buffer.length) {
        try {
            if (offset + 4 > buffer.length) break;
            const payloadLen = buffer.readUInt32LE(offset); offset += 4;
            const contentStart = offset;
            if (offset + payloadLen > buffer.length) break;
            const contentBuffer = buffer.subarray(contentStart, contentStart + payloadLen); offset += payloadLen;
            if (offset + 4 > buffer.length) break;
            const crc = buffer.readUInt32LE(offset); offset += 4;

            let contentOffset = 0;
            const lsn = contentBuffer.readBigUInt64LE(contentOffset); contentOffset += 8;
            const txn_id = contentBuffer.readBigUInt64LE(contentOffset); contentOffset += 8;
            const type = contentBuffer.readUInt8(contentOffset); contentOffset += 1;
            const keyLen = contentBuffer.readUInt32LE(contentOffset); contentOffset += 4;
            const key = contentBuffer.toString('utf8', contentOffset, contentOffset + keyLen); contentOffset += keyLen;
            const valLen = contentBuffer.readUInt32LE(contentOffset); contentOffset += 4;
            const value = contentBuffer.toString('utf8', contentOffset, contentOffset + valLen); contentOffset += valLen;
            
            records.push({ lsn, txn_id, type: type as LogRecordType, key, value });
        } catch (e: any) {
            console.error(`Error parsing WAL record in ${path.basename(filePath)} at offset ${offset}: ${e.message}`);
            break;
        }
    }
    return records;
}

// Main parser that finds, sorts, and reads all segment files for all shards
function parseAllWalSegments(walDirectoryPath: string, walFilePrefix: string): DecodedLogRecord[] {
    if (!fs.existsSync(walDirectoryPath)) return [];
    let allRecords: DecodedLogRecord[] = [];
    try {
        const files = fs.readdirSync(walDirectoryPath);
        for (const file of files) {
            if (parseSegmentIdFromFilename(file, walFilePrefix)) {
                allRecords.push(...parseSingleWalSegment(path.join(walDirectoryPath, file)));
            }
        }
    } catch (err) {
        console.error(`Error reading WAL directory ${walDirectoryPath}:`, err);
    }
    // CRITICAL: Sort all records from all shards by LSN to get the global timeline
    allRecords.sort((a, b) => (a.lsn < b.lsn ? -1 : a.lsn > b.lsn ? 1 : 0));
    return allRecords;
}
// --- End Log Parsing Utilities ---

describe('Sharded WAL Integration and Recovery Stress Test', () => {
    let db: Indinis | null = null;
    let testDataDir: string;
    let walDir: string;

    const colPath = 'sharded_wal_docs';
    const CHECKPOINT_INTERVAL_SECONDS = 2;
    const NUM_BATCHES = 8;
    const DOCS_PER_BATCH = 150;
    const WAL_FILE_PREFIX = "sharded_wal";

    // Configuration for the database instance, forcing small segments to test rotation
    const testOptions: IndinisOptions = {
        checkpointIntervalSeconds: CHECKPOINT_INTERVAL_SECONDS,
        walOptions: {
            segment_size: 32 * 1024, // Tiny 32KB segments to force frequent rotation
            wal_file_prefix: WAL_FILE_PREFIX,
        }
    };

    jest.setTimeout(240000); // 4 minutes

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        walDir = path.join(testDataDir, 'wal'); // C++ default
        // The testOptions will be passed to the constructor
        await fs.promises.mkdir(testDataDir, { recursive: true });
        console.log(`\n[SHARDED WAL TEST START] Using data directory: ${testDataDir}`);
    });

    afterEach(async () => {
        if (db) await db.close();
        db = null;
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir);
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    it('should handle high-volume concurrent writes, segment rolls, checkpoints, and recover correctly', async () => {
        // --- Phase 1: High-Volume Concurrent Writes ---
        db = new Indinis(testDataDir, testOptions);
        
        const allExpectedDocs = new Map<string, SegmentTestDoc>();
        const writePromises: Promise<any>[] = [];
        let totalDocsWritten = 0;

        console.log(`--- Starting ${NUM_BATCHES} batches of ${DOCS_PER_BATCH} concurrent writes ---`);
        for (let batchNum = 1; batchNum <= NUM_BATCHES; batchNum++) {
            for (let i = 0; i < DOCS_PER_BATCH; i++) {
                totalDocsWritten++;
                const key = `${colPath}/doc_${totalDocsWritten.toString().padStart(5, '0')}`;
                const doc: SegmentTestDoc = {
                    id: key,
                    batch: batchNum,
                    docInBatch: i,
                    data: `Data for ${key}, batch ${batchNum}, val ${Math.random()}`,
                };
                
                const promise = db.transaction(async tx => {
                    await tx.set(key, s(doc));
                }).then(() => {
                    // This is thread-safe because Map operations are atomic for single set/get
                    allExpectedDocs.set(key, doc); 
                }).catch(err => {
                    console.error(`Transaction failed for key ${key}: ${err.message}`);
                });
                writePromises.push(promise);
            }
            // Wait for a batch to complete before starting the next to manage memory
            await Promise.all(writePromises.splice(0, writePromises.length));
            console.log(`Batch ${batchNum} committed. Total docs: ${allExpectedDocs.size}`);
        }
        
        await Promise.all(writePromises);
        console.log(`--- All ${allExpectedDocs.size} documents committed. ---`);

        // Give time for the final checkpoint to run and potentially truncate logs
        console.log(`Waiting for final checkpoint and truncation...`);
        await delay((CHECKPOINT_INTERVAL_SECONDS + 1) * 1000);

        // --- Phase 2: Verify state BEFORE simulating a crash ---
        const walSegmentsPreCrash = fs.readdirSync(walDir).filter(f => parseSegmentIdFromFilename(f, WAL_FILE_PREFIX));
        console.log(`WAL segments on disk before crash: ${walSegmentsPreCrash.length} files.`);
        expect(walSegmentsPreCrash.length).toBeGreaterThan(1); // Expect multiple segments due to small size

        const allWalRecords = parseAllWalSegments(walDir, WAL_FILE_PREFIX);
        const lastLsn = allWalRecords.length > 0 ? allWalRecords[allWalRecords.length - 1].lsn : BigInt(0);
        console.log(`Last LSN found in WAL before crash: ${lastLsn}`);

        // --- Phase 3: Simulate Crash and Recover ---
        console.log("\n--- Simulating Crash & Restart ---");
        db = null; // Let the instance be garbage collected without calling close()
        
        if (global.gc) global.gc(); // Force GC to help ensure file handles are released
        await delay(1000);

        console.log("Re-initializing Indinis instance to trigger recovery...");
        db = new Indinis(testDataDir, testOptions);
        await delay(1000); // Give recovery time

        // --- Phase 4: Verify Data Integrity After Recovery ---
        console.log(`\n--- Verifying all ${allExpectedDocs.size} documents after recovery ---`);
        let verifiedCount = 0;
        await db.transaction(async tx => {
            for (const [key, expectedDoc] of allExpectedDocs.entries()) {
                const retrieved = await tx.get(key);
                if (!retrieved) {
                    throw new Error(`Data integrity failure: Key "${key}" was not found after recovery.`);
                }
                const retrievedDoc = JSON.parse(retrieved as string) as SegmentTestDoc;
                expect(retrievedDoc.data).toEqual(expectedDoc.data);
                verifiedCount++;
            }
        });
        expect(verifiedCount).toBe(allExpectedDocs.size);
        console.log(`--- Successfully verified all ${verifiedCount} documents. ---`);

        // --- Phase 5: Ensure WAL continues correctly ---
        const keyAfterRecovery = `${colPath}/post_recovery_doc`;
        await db.transaction(async tx => {
            await tx.set(keyAfterRecovery, s({ data: "I exist" }));
        });
        const finalWalRecords = parseAllWalSegments(walDir, WAL_FILE_PREFIX);
        const finalLsn = finalWalRecords.length > 0 ? finalWalRecords[finalWalRecords.length - 1].lsn : BigInt(0);
        
        console.log(`Last LSN after new write: ${finalLsn} (was ${lastLsn} before crash)`);
        expect(finalLsn).toBeGreaterThan(lastLsn);
        console.log("--- WAL sequence correctly continued after recovery. Test successful. ---");
    });
});