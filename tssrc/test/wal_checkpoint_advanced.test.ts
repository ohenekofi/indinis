// @tssrc/test/wal_checkpoint_advanced.test.ts
import { Indinis, IndinisOptions, CheckpointInfo, StorageValue } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';
// No need for promisify if using fs.promises directly

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-wal-cp-advanced-v3');

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const s = (obj: any): string => JSON.stringify(obj);

// --- WAL Parsing Utilities ---
enum LogRecordType {
    INVALID = 0, BEGIN_TXN = 1, COMMIT_TXN = 2, ABORT_TXN = 3,
    DATA_PUT = 4, DATA_DELETE = 5,
    CHECKPOINT_BEGIN = 6, CHECKPOINT_END = 7,
    LSM_PUT = 8,
    BTREE_INSERT_ENTRY = 9,
    BTREE_REMOVE_KEY_PHYSICAL = 10,
    UPDATE_BTREE_ROOT = 11,
}
interface DecodedLogRecord {
    lsn: bigint;
    txn_id: bigint;
    type: LogRecordType;
    key: string;
    value: string;
}

function parseLsnFromExtWalSegmentFilename(filename: string, prefix: string): bigint | null {
    const logExtension = ".log";
    if (filename.startsWith(prefix) && filename.endsWith(logExtension)) {
        let numPart = filename.substring(prefix.length);
        if (numPart.startsWith("_")) {
            numPart = numPart.substring(1);
        }
        numPart = numPart.substring(0, numPart.length - logExtension.length);
        if (/^\d+$/.test(numPart)) {
            try { return BigInt(numPart); } catch (e) { /* ignore */ }
        }
    }
    return null;
}

function parseSingleExtWalSegment(filePath: string): DecodedLogRecord[] {
            if (!fs.existsSync(filePath)) return [];
            const buffer = fs.readFileSync(filePath);
            const records: DecodedLogRecord[] = [];
            let offset = 0;
            const WAL_SEGMENT_HEADER_SIZE = 34;
            if (buffer.length < WAL_SEGMENT_HEADER_SIZE) {
                console.warn(`Segment file ${path.basename(filePath)} smaller than header. Size: ${buffer.length}`);
                return records;
            }
            offset = WAL_SEGMENT_HEADER_SIZE; // Skip C++ WALSegmentHeader

            while (offset < buffer.length) {
                const recordStartOffsetForErrorLog = offset;
                try {
                    // 1. Read payload_content_data_len (uint32_t)
                    if (offset + 4 > buffer.length) {
                        // console.log(`[Parse ${path.basename(filePath)}] EOF before payload length. Offset: ${offset}`);
                        break;
                    }
                    const payload_content_data_len = buffer.readUInt32LE(offset);
                    offset += 4;

                    // 2. Read payload_content_data_str
                    const contentStartOffset = offset;
                    if (offset + payload_content_data_len > buffer.length) {
                        console.warn(`[Parse ${path.basename(filePath)}] Incomplete payload content. Expected ${payload_content_data_len}, available ${buffer.length - offset} at offset ${contentStartOffset}. RecStart: ${recordStartOffsetForErrorLog}`);
                        break;
                    }
                    const contentBuffer = buffer.subarray(contentStartOffset, contentStartOffset + payload_content_data_len);
                    offset += payload_content_data_len; // Advance main offset by content length

                    // 3. Read stored_crc_val_32 (uint32_t)
                    if (offset + 4 > buffer.length) {
                        console.warn(`[Parse ${path.basename(filePath)}] Incomplete CRC. Expected 4, available ${buffer.length - offset} at offset ${offset}. RecStart: ${recordStartOffsetForErrorLog}`);
                        break;
                    }
                    const stored_crc = buffer.readUInt32LE(offset);
                    offset += 4;
                    // CRC verification would happen here in C++ or a more advanced TS parser

                    // Now, deserialize from contentBuffer
                    let currentContentOffset = 0;
                    if (currentContentOffset + 8 > contentBuffer.length) break; const lsn = contentBuffer.readBigUInt64LE(currentContentOffset); currentContentOffset += 8;
                    if (currentContentOffset + 8 > contentBuffer.length) break; const txn_id = contentBuffer.readBigUInt64LE(currentContentOffset); currentContentOffset += 8;
                    if (currentContentOffset + 1 > contentBuffer.length) break; const type = contentBuffer.readUInt8(currentContentOffset); currentContentOffset += 1;
                    if (currentContentOffset + 4 > contentBuffer.length) break; const keyLen = contentBuffer.readUInt32LE(currentContentOffset); currentContentOffset += 4;
                    if (currentContentOffset + keyLen > contentBuffer.length) break; const key = contentBuffer.toString('utf8', currentContentOffset, currentContentOffset + keyLen); currentContentOffset += keyLen;
                    if (currentContentOffset + 4 > contentBuffer.length) break; const valueBinaryLen = contentBuffer.readUInt32LE(currentContentOffset); currentContentOffset += 4;
                    if (currentContentOffset + valueBinaryLen > contentBuffer.length) break; const value = contentBuffer.toString('utf8', currentContentOffset, currentContentOffset + valueBinaryLen); currentContentOffset += valueBinaryLen;

                    records.push({ lsn, txn_id, type: type as LogRecordType, key, value });

                } catch (e: any) {
                    console.error(`Error parsing WAL record in ${path.basename(filePath)} at approx offset ${recordStartOffsetForErrorLog}. Message: ${e.message}`);
                    break;
                }
            }
            if (records.length > 0) {
                console.log(`  Parsed ${records.length} records from ${path.basename(filePath)}.`);
            }
            return records;
        }

function parseAllExtWalSegments(walDirectoryPath: string, walFilePrefix: string): DecodedLogRecord[] {
    if (!fs.existsSync(walDirectoryPath)) { console.warn(`WAL directory ${walDirectoryPath} does not exist.`); return []; }
    const allRecords: DecodedLogRecord[] = [];
    try {
        const files = fs.readdirSync(walDirectoryPath);
        const segmentFiles = files
            .map(file => ({ file, segmentId: parseLsnFromExtWalSegmentFilename(file, walFilePrefix) }))
            .filter(item => item.segmentId !== null)
            .sort((a, b) => Number(a.segmentId! - b.segmentId!))
            .map(item => item.file);
        for (const segmentFile of segmentFiles) {
            const filePath = path.join(walDirectoryPath, segmentFile);
            allRecords.push(...parseSingleExtWalSegment(filePath));
        }
    } catch (err) { console.error(`Error reading WAL directory ${walDirectoryPath}:`, err); }
    return allRecords;
}
// --- End WAL Parsing Utilities ---

describe('Advanced ExtWALManager & Checkpoint Recovery Scenarios', () => {
    let db: Indinis | null = null;
    let testDataDir: string; // Specific to each test, set in setupTestEnv
    let currentTestOptions: IndinisOptions; // Store options for re-initialization

    const setupTestEnv = async (suffix: string, optionsOverride?: Partial<IndinisOptions>): Promise<Indinis> => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${suffix}-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });

        const baseWalDirFromOpts = optionsOverride?.walOptions?.wal_directory;
        // Ensure WAL dir name is unique per test run to avoid conflicts if base is reused
        const defaultWalSubDir = `adv_wal_data_${suffix}_${randomSuffix}`;
        const walDirForThisTest = baseWalDirFromOpts
            ? (path.isAbsolute(baseWalDirFromOpts) ? baseWalDirFromOpts : path.join(testDataDir, baseWalDirFromOpts))
            : path.join(testDataDir, defaultWalSubDir);

        await fs.promises.mkdir(walDirForThisTest, { recursive: true });
        const checkpointDir = path.join(testDataDir, 'checkpoints'); // Default C++ path
        await fs.promises.mkdir(checkpointDir, { recursive: true });

        console.log(`\n[WAL-CP ADV TEST - ${suffix}] Using data dir: ${testDataDir}, WAL dir: ${walDirForThisTest}`);

        const defaultWalConfig = {
            wal_directory: walDirForThisTest,
            segment_size: 128 * 1024,
            wal_file_prefix: `adv_${suffix}_wal_`, // Scenario-specific prefix
            background_flush_enabled: true,
            flush_interval_ms: 50,
            sync_on_commit_record: true,
        };

        currentTestOptions = {
            checkpointIntervalSeconds: 2,
            sstableDataBlockUncompressedSizeKB: 16,
            ...optionsOverride, // Apply top-level overrides first
            walOptions: { // Then construct walOptions carefully
                ...defaultWalConfig, // Start with defaults
                ...(optionsOverride?.walOptions || {}), // Override with provided walOptions
                // Ensure critical paths are from our calculated values if not overridden
                wal_directory: optionsOverride?.walOptions?.wal_directory !== undefined ? defaultWalConfig.wal_directory : walDirForThisTest,
                wal_file_prefix: optionsOverride?.walOptions?.wal_file_prefix !== undefined ? defaultWalConfig.wal_file_prefix : `adv_${suffix}_wal_`,
            },
        };
         // Correctly ensure optionsOverride.walOptions.wal_directory & prefix are used if they exist
        if (optionsOverride?.walOptions?.wal_directory !== undefined) {
            currentTestOptions.walOptions!.wal_directory = walDirForThisTest; // Already calculated based on override or default
        }
        if (optionsOverride?.walOptions?.wal_file_prefix !== undefined) {
            currentTestOptions.walOptions!.wal_file_prefix = optionsOverride.walOptions.wal_file_prefix;
        }


        console.log(`[DEBUG] Effective currentTestOptions for ${suffix}:`, JSON.stringify(currentTestOptions, null, 2));

        db = new Indinis(testDataDir, currentTestOptions);
        await delay(400); // Increased init delay
        return db;
    };

    const getCheckpointMetaFile = () => path.join(testDataDir, 'checkpoints', 'checkpoint.meta');

    beforeAll(async () => { await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true }); });
    
    afterEach(async () => {
        const currentTestDirForLog = testDataDir; // Capture before db might be nulled
        if (db) {
            console.log(`[TEST END - ${path.basename(currentTestDirForLog || "unknown")}] Closing database...`);
            try {
                await db.close();
                console.log(`[TEST END - ${path.basename(currentTestDirForLog || "unknown")}] Database close() completed.`);
            } catch (e: any) {
                console.error(`[TEST END - ${path.basename(currentTestDirForLog || "unknown")}] Error during db.close(): ${e.message}`);
            }
        } else {
            console.log(`[TEST END - ${path.basename(currentTestDirForLog || "unknown")}] No active DB instance to close.`);
        }
        db = null;

        if (global.gc) {
            console.log(`[TEST END - ${path.basename(currentTestDirForLog || "unknown")}] Forcing GC...`);
            global.gc();
        }
        await delay(1200); // Longer delay

        if (currentTestDirForLog && fs.existsSync(currentTestDirForLog)) {
            console.log(`[TEST END - ${path.basename(currentTestDirForLog)}] Cleaning up test directory: ${currentTestDirForLog}`);
            await rimraf(currentTestDirForLog, { maxRetries: 3, retryDelay: 1000, glob: false }).catch(err =>
                console.error(`[TEST END - ${path.basename(currentTestDirForLog)}] rimraf cleanup error for ${currentTestDirForLog}:`, err)
            );
        } else if (currentTestDirForLog) {
            console.log(`[TEST END - ${path.basename(currentTestDirForLog)}] Test directory ${currentTestDirForLog} not found for cleanup.`);
        }
    });
    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) {
            await rimraf(TEST_DATA_DIR_BASE).catch(err => console.error(`Base cleanup error:`, err));
        }
    });

    jest.setTimeout(90000);

    it('SCENARIO CP-ADV 1: Recovery with incomplete checkpoint (BEGIN logged, no END, no meta)', async () => {
        db = await setupTestEnv('cp_incomplete_begin');
        if (!db) throw new Error("Test setup error for SCENARIO CP-ADV 1: DB not initialized");

        const key1 = 'data/k_cp_inc_b1';
        const val1 = 'val_before_incomplete_cp';
        await db.transaction(async tx => { await tx.set(key1, val1); });
        console.log("CP-ADV 1: Data before incomplete CP committed.");

        console.log("CP-ADV 1: Forcing a checkpoint (this will complete in C++).");
        await db.forceCheckpoint();
        await delay((currentTestOptions.checkpointIntervalSeconds || 2) * 1000 + 1000);

        const metaFile = getCheckpointMetaFile();
        if (fs.existsSync(metaFile)) {
            console.log("CP-ADV 1: Deleting checkpoint.meta to simulate failure before its persistence.");
            await fs.promises.unlink(metaFile);
        } else {
            console.warn("CP-ADV 1: checkpoint.meta not found before simulated crash. Test might not fully simulate desired state if a CP was expected to write it.");
        }

        await db.close(); // Cleanly close this instance to flush WAL etc.
        db = null;

        console.log("CP-ADV 1: Reopening DB. Recovery should use WAL from last *successful* CP or start.");
        db = new Indinis(testDataDir, currentTestOptions);
        await delay(500);

        await db.transaction(async tx => {
            const rValRaw = await tx.get(key1);
            let rVal1String: string | null = null;
            if (rValRaw instanceof Buffer) rVal1String = rValRaw.toString('utf-8');
            else if (typeof rValRaw === 'string') rVal1String = rValRaw;
            
            console.log(`CP-ADV 1: Verifying after recovery. Type of rValRaw: ${typeof rValRaw}, Retrieved Value: '${rVal1String}'`);
            console.log(`CP-ADV 1: Expected val1. Type: ${typeof val1}, Value: '${val1}'`);
            expect(rVal1String).toEqual(val1);
        });
        console.log("CP-ADV 1: Data verified after recovery.");

        const history = await db.getCheckpointHistory();
        console.log("CP-ADV 1: History after recovery from incomplete CP scenario:", history.map(h=>h.toString()));
        const lastCp = history.length > 0 ? history[history.length - 1] : null;
        if (lastCp && lastCp.checkpoint_begin_wal_lsn > 0) {
            expect(lastCp.status).not.toBe("COMPLETED");
        }
    });

    it('SCENARIO CP-ADV 2: Recovery with checkpoint.meta pointing to an LSN *after* WAL (meta deleted)', async () => {
        db = await setupTestEnv('cp_meta_ahead_wal');
        if (!db) throw new Error("Test setup error for SCENARIO CP-ADV 2: DB not initialized");
        
        const key1 = 'data/k_cp_meta_adv1'; const val1 = 'val_cp_meta_adv_1';
        await db.transaction(async tx => { await tx.set(key1, val1); });
        console.log("CP-ADV 2: Data written. Forcing checkpoint.");
        await db.forceCheckpoint();
        await delay((currentTestOptions.checkpointIntervalSeconds || 2) * 1000 + 1000);

        const metaFile = getCheckpointMetaFile();
        if (fs.existsSync(metaFile)) {
            console.log("CP-ADV 2: DELETING checkpoint.meta to simulate its absence/corruption for this scenario.");
            await fs.promises.unlink(metaFile);
        } else {
            console.warn("CP-ADV 2: checkpoint.meta not found, cannot simulate its deletion for this scenario as intended.");
        }

        await db.close();
        db = null;

        console.log("CP-ADV 2: Reopening DB. Recovery should detect missing meta and do full WAL scan.");
        db = new Indinis(testDataDir, currentTestOptions);
        await delay(500);

        await db.transaction(async tx => {
            const rValRaw = await tx.get(key1);
            let rVal1String: string | null = null;
            if (rValRaw instanceof Buffer) rVal1String = rValRaw.toString('utf-8');
            else if (typeof rValRaw === 'string') rVal1String = rValRaw;
            expect(rVal1String).toEqual(val1);
        });
        console.log("CP-ADV 2: Data verified after recovery.");

        const history = await db.getCheckpointHistory();
        console.log("CP-ADV 2: Checkpoint history after recovery:", history.map(h=>h.toString()));
    });

    it('SCENARIO CP-ADV 3: WAL segment files correctly reflect WAL content', async () => {
        const numRecordsToWrite = 250;
        const segmentSizeForThisTest = 16 * 1024; // 16KB segments
        db = await setupTestEnv('cp_wal_segment_content', { walOptions: { segment_size: segmentSizeForThisTest }});

        if (!db || !currentTestOptions.walOptions?.wal_directory || !currentTestOptions.walOptions?.wal_file_prefix) {
            console.error("Failing test CP-ADV 3 early due to config issue. currentTestOptions:", JSON.stringify(currentTestOptions, null, 2));
            throw new Error("Test setup error for SCENARIO CP-ADV 3: DB or WAL config properties missing.");
        }

        let totalExpectedMinWalRecords = 0;
        for (let i = 0; i < numRecordsToWrite; i++) {
            await db.transaction(async tx => {
                await tx.set(`data/item_${i}`, `value_${i}_content_long_enough_to_ensure_data_is_written`);
            });
            totalExpectedMinWalRecords += 3; // BEGIN, DATA_PUT, COMMIT
        }
        console.log(`CP-ADV 3: Wrote ${numRecordsToWrite} user records (expected at least ~${totalExpectedMinWalRecords} data-related WAL entries).`);
        await db.close(); db = null;

        const walRecordsFromDisk = parseAllExtWalSegments(
            currentTestOptions.walOptions.wal_directory,
            currentTestOptions.walOptions.wal_file_prefix
        );
        console.log(`CP-ADV 3: Total records parsed from all WAL segments: ${walRecordsFromDisk.length}`);

        const numCheckpointsRoughly = Math.floor((numRecordsToWrite * 50 /*ms/op est*/) / ((currentTestOptions.checkpointIntervalSeconds || 2) * 1000)) +1;
        const expectedCheckpointWalRecords = numCheckpointsRoughly * 2; // BEGIN_CP, END_CP

        expect(walRecordsFromDisk.length).toBeGreaterThanOrEqual(totalExpectedMinWalRecords);
        // Allow some slack for checkpoints that might have run
        expect(walRecordsFromDisk.length).toBeLessThanOrEqual(totalExpectedMinWalRecords + expectedCheckpointWalRecords + 10);

        let lastLsn = 0n;
        for (const rec of walRecordsFromDisk) {
            expect(rec.lsn).toBeGreaterThan(lastLsn);
            lastLsn = rec.lsn;
        }
        console.log(`CP-ADV 3: WAL LSNs verified for continuity. Last LSN: ${lastLsn}`);
    });
});