// @tssrc/test/wal.test.ts
import { Indinis } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-wal-tests');

// Crude LogRecordType enum mapping for test (must match C++)
enum LogRecordType {
    INVALID = 0,
    BEGIN_TXN = 1,
    COMMIT_TXN = 2,
    ABORT_TXN = 3,
    DATA_PUT = 4,
    DATA_DELETE = 5,
}

interface DecodedLogRecord {
    lsn: bigint;
    txn_id: bigint;
    type: LogRecordType;
    key: string;
    value: string;
}

// Basic parser for the WAL file for testing
function parseWalFile(filePath: string): DecodedLogRecord[] {
    if (!fs.existsSync(filePath)) {
        return [];
    }
    const buffer = fs.readFileSync(filePath);
    const records: DecodedLogRecord[] = [];
    let offset = 0;

    while (offset < buffer.length) {
        const recordStartOffset = offset; // For debugging on error
        try {
            if (offset + 8 > buffer.length) break; // Not enough for LSN
            const lsn = buffer.readBigUInt64LE(offset); offset += 8;

            if (offset + 8 > buffer.length) break; // Not enough for TxnID
            const txn_id = buffer.readBigUInt64LE(offset); offset += 8;

            if (offset + 1 > buffer.length) break; // Not enough for type
            const type = buffer.readUInt8(offset); offset += 1;

            if (offset + 4 > buffer.length) break; // Not enough for keyLen
            const keyLen = buffer.readUInt32LE(offset); offset += 4;
            if (offset + keyLen > buffer.length) break; // Not enough for key data
            const key = buffer.toString('utf8', offset, offset + keyLen); offset += keyLen;

            if (offset + 4 > buffer.length) break; // Not enough for valLen
            const valLen = buffer.readUInt32LE(offset); offset += 4;
            if (offset + valLen > buffer.length) break; // Not enough for value data
            const value = buffer.toString('utf8', offset, offset + valLen); offset += valLen;
            
            records.push({ lsn, txn_id, type: type as LogRecordType, key, value });
        } catch (e: any) {
            console.error("Error parsing WAL record starting at offset", recordStartOffset, "current offset", offset, "error:", e.message);
            // Log the problematic part of the buffer
            const errorContextLength = Math.min(32, buffer.length - recordStartOffset);
            console.error("Buffer around error (hex):", buffer.subarray(recordStartOffset, recordStartOffset + errorContextLength).toString('hex'));
            break; // Stop parsing on error
        }
    }
    return records;
}


describe('Indinis Basic WAL Functionality', () => {
    let db: Indinis;
    let testDataDir: string;
    let walFilePath: string;

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        
        const walDir = path.join(testDataDir, 'wal'); // Mirror C++ structure
        await fs.promises.mkdir(walDir, {recursive: true});
        walFilePath = path.join(walDir, 'wal.log');

        db = new Indinis(testDataDir);
        console.log(`\n[WAL TEST START] Using data directory: ${testDataDir}`);
    });

    afterEach(async () => {
        console.log("[WAL TEST END] Closing database...");
        if (db) await db.close();
        console.log("[WAL TEST END] Cleaning up test directory...");
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir);
    });

    afterAll(async () => {
        console.log("[WAL SUITE END] Cleaning up base directory...");
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    it('should create a WAL file on initialization', () => {
        expect(fs.existsSync(walFilePath)).toBe(true);
    });

    it('should log BEGIN_TXN, DATA_PUT, and COMMIT_TXN for a simple transaction', async () => {
        const key = 'walTestKey1';
        const value = 'walTestValue1';
        let txnId = -1;

        await db.transaction(async tx => {
            txnId = await tx.getId();
            await tx.set(key, value);
        });

        const records = parseWalFile(walFilePath);
        expect(records.length).toBeGreaterThanOrEqual(3);

        const beginRecord = records.find(r => r.txn_id === BigInt(txnId) && r.type === LogRecordType.BEGIN_TXN);
        const putRecord = records.find(r => r.txn_id === BigInt(txnId) && r.type === LogRecordType.DATA_PUT && r.key === key);
        const commitRecord = records.find(r => r.txn_id === BigInt(txnId) && r.type === LogRecordType.COMMIT_TXN);

        expect(beginRecord).toBeDefined();
        expect(putRecord).toBeDefined();
        expect(putRecord?.value).toContain(value); // Basic check, serializer adds type prefix
        expect(commitRecord).toBeDefined();
        
        // Check LSN ordering (basic)
        if (beginRecord && putRecord && commitRecord) {
            expect(putRecord.lsn).toBeGreaterThan(beginRecord.lsn);
            expect(commitRecord.lsn).toBeGreaterThan(putRecord.lsn);
        }
    });

    it('should log DATA_DELETE for a remove operation', async () => {
        const key = 'walTestKey2';
        const value = 'initialValue';
        let setupTxnId = -1;
        let deleteTxnId = -1;

        await db.transaction(async tx => {
            setupTxnId = await tx.getId();
            await tx.set(key, value);
        });

        await db.transaction(async tx => {
            deleteTxnId = await tx.getId();
            await tx.remove(key);
        });

        const records = parseWalFile(walFilePath);
        
        const deleteLogRecord = records.find(r => r.txn_id === BigInt(deleteTxnId) && r.type === LogRecordType.DATA_DELETE && r.key === key);
        expect(deleteLogRecord).toBeDefined();
        expect(deleteLogRecord?.value).toBe('[TOMBSTONE]');
    });

    it('should log ABORT_TXN if a transaction is aborted', async () => {
        const key = 'walTestKeyAbort';
        const value = 'neverCommitted';
        let abortTxnId = -1;
        let txCtx: any;

        try {
            await db.transaction(async tx => {
                abortTxnId = await tx.getId();
                txCtx = tx; // Save context to call abort
                await tx.set(key, value);
                throw new Error("Intentional abort");
            });
        } catch (e) {
            // Expected error, now check logs.
            // NAPI binding might call abort automatically. If not, call it here explicitly.
            if (txCtx && typeof txCtx.abort === 'function' && !txCtx.isAborted()) {
                 console.log("Manually calling abort on Txn ", abortTxnId);
                 await txCtx.abort(); // Ensure abort is called if error happens before internal auto-abort
            }
        }

        const records = parseWalFile(walFilePath);
        console.log("WAL Records after abort attempt:", records.filter(r => r.txn_id === BigInt(abortTxnId)));

        const abortLogRecord = records.find(r => r.txn_id === BigInt(abortTxnId) && r.type === LogRecordType.ABORT_TXN);
        expect(abortLogRecord).toBeDefined();

        // Ensure no commit record for this txn
        const commitLogRecord = records.find(r => r.txn_id === BigInt(abortTxnId) && r.type === LogRecordType.COMMIT_TXN);
        expect(commitLogRecord).toBeUndefined();
    });

    it('should recover next LSN correctly after restart', async () => {
        let initialLsnRecords: DecodedLogRecord[] = [];
        await db.transaction(async tx => { await tx.set("key1", "val1"); });
        await db.transaction(async tx => { await tx.set("key2", "val2"); });
        await db.close(); // Close DB to flush WAL

        initialLsnRecords = parseWalFile(walFilePath);
        const lastInitialLsn = initialLsnRecords.length > 0 ? initialLsnRecords[initialLsnRecords.length - 1].lsn : 0n;
        console.log("Last LSN before restart: ", lastInitialLsn);

        // Reopen DB (simulates restart)
        db = new Indinis(testDataDir);
        let txnIdAfterRestart = -1;
        await db.transaction(async tx => {
            txnIdAfterRestart = await tx.getId();
            await tx.set("key3", "val3");
        });
        await db.close();

        const recordsAfterRestart = parseWalFile(walFilePath);
        console.log("WAL Records after restart:", recordsAfterRestart);

        const expectedLsnAfterRestart = lastInitialLsn + 1n;
        const firstRecordOfNewTxn = recordsAfterRestart.find(r => r.lsn === expectedLsnAfterRestart && r.type === LogRecordType.BEGIN_TXN);
        expect(firstRecordOfNewTxn).toBeDefined();
        if (firstRecordOfNewTxn) {
            console.log("First LSN after restart: ", firstRecordOfNewTxn.lsn);
            expect(firstRecordOfNewTxn.lsn).toBe(lastInitialLsn + 1n);
        }
    });

    it('should recover committed data after a simulated crash', async () => {
        const key1 = 'recoverKey1';
        const value1 = 'recoveredValue1';
        const key2 = 'recoverKey2';
        const value2 = { msg: "recovered object" };
        let txn1Id = -1;

        // --- Phase 1: Write data with first DB instance ---
        console.log("[RecoveryTest] Phase 1: Writing initial data.");
        await db.transaction(async tx => {
            txn1Id = await tx.getId();
            await tx.set(key1, value1);
            await tx.set(key2, JSON.stringify(value2));
        });
        console.log(`[RecoveryTest] Phase 1: Txn ${txn1Id} committed.`);

        // CRITICAL: To simulate a crash, we ensure the WAL's COMMIT record is flushed,
        // but the main data structures (LSMTree SSTables, BTree pages via BPM) might not be.
        // Our StorageEngine::commitTransaction flushes the COMMIT WAL record.
        // We *don't* call db.close() here to simulate that caches weren't flushed.

        const recordsBeforeCrash = parseWalFile(walFilePath);
        const commitRecordExists = recordsBeforeCrash.some(
            r => r.txn_id === BigInt(txn1Id) && r.type === LogRecordType.COMMIT_TXN
        );
        expect(commitRecordExists).toBe(true); // Ensure commit was logged
        console.log(`[RecoveryTest] Phase 1: WAL contains COMMIT for Txn ${txn1Id}. Simulating crash...`);

        // --- Phase 2: "Restart" - Create a new DB instance pointing to the same directory ---
        // The previous `db` instance will be GC'd without its explicit `close()`
        // (though in Jest's test environment, afterEach might call it anyway,
        // the key is that we are testing the startup of a *new* instance).
        console.log("[RecoveryTest] Phase 2: Creating new DB instance to trigger recovery.");
        db = new Indinis(testDataDir); // This will trigger performRecovery()
        console.log("[RecoveryTest] Phase 2: New DB instance created.");

        // --- Phase 3: Verify data ---
        console.log("[RecoveryTest] Phase 3: Verifying recovered data.");
        await db.transaction(async tx => {
            const rVal1 = await tx.get(key1);
            expect(rVal1).toBe(value1);
            console.log(`  [Verify] ${key1} -> ${rVal1} (Expected: ${value1})`);

            const rVal2Str = await tx.get(key2);
            expect(rVal2Str).not.toBeNull();
            if (rVal2Str) {
                expect(JSON.parse(rVal2Str as string)).toEqual(value2);
                console.log(`  [Verify] ${key2} -> Parsed object matches (Expected: ${JSON.stringify(value2)})`);
            }
        });
        console.log("[RecoveryTest] Phase 3: Verification complete.");

        // Bonus: Write another transaction to ensure LSN/TxnID continues correctly
        let txn2Id_afterRecovery = -1;
        await db.transaction(async tx => {
            txn2Id_afterRecovery = await tx.getId();
            await tx.set("afterRecoveryKey", "new data");
        });
        expect(txn2Id_afterRecovery).toBeGreaterThan(txn1Id); // Txn IDs should progress
        console.log(`[RecoveryTest] New Txn ID after recovery: ${txn2Id_afterRecovery} (Original Txn ID: ${txn1Id})`);

        const recordsAfterRecoveryAndNewTxn = parseWalFile(walFilePath);
        const lastLsnAfterNewTxn = recordsAfterRecoveryAndNewTxn.length > 0 ?
            recordsAfterRecoveryAndNewTxn[recordsAfterRecoveryAndNewTxn.length - 1].lsn : 0n;
        
        const originalCommitLsn = recordsBeforeCrash.find(r => r.txn_id === BigInt(txn1Id) && r.type === LogRecordType.COMMIT_TXN)!.lsn;
        expect(lastLsnAfterNewTxn).toBeGreaterThan(originalCommitLsn);
        console.log(`[RecoveryTest] Last LSN in WAL after new Txn: ${lastLsnAfterNewTxn} (Original commit LSN: ${originalCommitLsn})`);

    });
});