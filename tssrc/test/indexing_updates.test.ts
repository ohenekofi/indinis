// tssrc/test/indexing_updates.test.ts

import { Indinis } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-indexing-updates-v3');

// --- TypeScript Order-Preserving Encoders ---

function tsEncodeInt64OrderPreserving(value: bigint | number): Buffer {
    const valToEncode = BigInt(value);
    const buffer = Buffer.alloc(8);
    // Write it as signed first to get the correct 64-bit two's complement pattern
    buffer.writeBigInt64BE(valToEncode);
    // Now read it back as unsigned to work with the bits, then XOR with sign bit
    const uBits = buffer.readBigUInt64BE();
    const transformedBits = uBits ^ (1n << 63n); // XOR with 0x8000...
    buffer.writeBigUInt64BE(transformedBits);
    return buffer;
}

function tsEncodeDoubleOrderPreserving(value: number): Buffer {
    const tempBuffer = Buffer.alloc(8);
    tempBuffer.writeDoubleBE(value, 0);
    let bits = tempBuffer.readBigUInt64BE(0);

    if ((bits >> 63n) === 0n) { // Positive or +0.0 (sign bit is 0 on original double)
        bits |= (1n << 63n);    // Set MSB of the bit pattern to 1
    } else { // Negative or -0.0 (sign bit is 1 on original double)
        bits = bits ^ 0xFFFFFFFFFFFFFFFFn; // Bitwise NOT for all 64 bits
    }
    tempBuffer.writeBigUInt64BE(bits); // Write final transformed bits back
    return tempBuffer;
}

function tsEncodeStringOrderPreserving(value: string): Buffer {
    return Buffer.from(value, 'utf8');
}

function createTsEncodedFieldKeyPart(
    fieldValue: string | number | bigint | undefined | null,
    fieldType: 'string' | 'int64' | 'double'
): Buffer {
    if (fieldValue === null || fieldValue === undefined) {
        return Buffer.from([0x00]);
    }
    switch (fieldType) {
        case 'string':
            if (typeof fieldValue !== 'string') throw new Error(`Mismatched type for string encoding. Expected string, got ${typeof fieldValue}`);
            return tsEncodeStringOrderPreserving(fieldValue);
        case 'int64':
            if (typeof fieldValue !== 'number' && typeof fieldValue !== 'bigint') throw new Error(`Mismatched type for int64 encoding. Expected number/bigint, got ${typeof fieldValue}`);
            return tsEncodeInt64OrderPreserving(fieldValue);
        case 'double':
            if (typeof fieldValue !== 'number') throw new Error(`Mismatched type for double encoding. Expected number, got ${typeof fieldValue}`);
            return tsEncodeDoubleOrderPreserving(fieldValue);
        default:
            const _exhaustiveCheck: never = fieldType;
            throw new Error(`Unsupported fieldType for encoding: ${_exhaustiveCheck}`);
    }
}

function encodeTxnIdDescendingJS(txnId: number): Buffer {
    const txnIdBigInt = BigInt(txnId);
    const buffer = Buffer.alloc(8);
    buffer.writeBigUInt64BE(txnIdBigInt);
    const beValBigInt = buffer.readBigUInt64BE();
    const invertedBigInt = beValBigInt ^ 0xFFFFFFFFFFFFFFFFn;
    const invertedBuffer = Buffer.alloc(8);
    invertedBuffer.writeBigUInt64BE(invertedBigInt);
    return invertedBuffer;
}

function createBTreeCompositeKeyForTest(
    tsEncodedFieldKeyPart: Buffer,
    txnId: number
): Buffer {
    const encodedTxnIdBuffer = encodeTxnIdDescendingJS(txnId);
    return Buffer.concat([tsEncodedFieldKeyPart, encodedTxnIdBuffer]);
}

function formatBufferForPrint(buf: Buffer): string {
    return buf.toString('hex');
}

function expectEntriesToContainKey(entries: Buffer[], expectedKey: Buffer, message: string) {
    const found = entries.some(entry => entry.equals(expectedKey));
    if (!found) {
        console.error(`${message} - Expected key not found (hex): ${formatBufferForPrint(expectedKey)}`);
        console.error(`  Entries scanned (hex): [${entries.map(e => formatBufferForPrint(e)).join(', ')}]`);
    }
    expect(found).toBe(true);
}

// --- Test Suite ---

describe('Indinis Indexing Updates (B+Tree with Order-Preserving Encoding)', () => {
    let db: Indinis;
    let testDataDir: string;
    const usersPath = 'users';
    const emailIndexName = 'idx_users_email';
    const ageIndexName = 'idx_users_age';
    const scoreIndexName = 'idx_users_score';
    const ageDescIndexName = 'idx_users_age_desc';

    async function debugScanBTreeForFieldValue(
        valueToScan: string | number | bigint | undefined | null,
        valueTypeForEncoding: 'string' | 'int64' | 'double',
        targetIndexName: string,
        isFieldDescending: boolean = false
    ): Promise<Buffer[]> {
        if (!db) throw new Error("DB not initialized for debug scan");
    
        let tsEncodedFieldScanPart = createTsEncodedFieldKeyPart(valueToScan, valueTypeForEncoding);
    
        if (isFieldDescending) {
            const invertedBytes = tsEncodedFieldScanPart.map(b => ~b & 0xFF);
            tsEncodedFieldScanPart = Buffer.from(invertedBytes);
            // console.log(`  Field is DESC, inverted TsEncodedFieldScanPart for scan (hex): '${formatBufferForPrint(tsEncodedFieldScanPart)}'`);
        }
        
        // tsEncodedFieldScanPart IS ALREADY A BUFFER. Pass it directly.
        // const keyPartForBTreeScan = tsEncodedFieldScanPart.toString('binary'); // REMOVE THIS LINE
    
        console.log(`DEBUG SCAN: Index='${targetIndexName}', ValueToScan='${valueToScan}' (type: ${valueTypeForEncoding}), Desc: ${isFieldDescending}`);
        console.log(`  Final KeyPartForBTreeScan (Buffer to be passed to NAPI) (hex): '${formatBufferForPrint(tsEncodedFieldScanPart)}'`);
    
        const resultsAsBuffers: Buffer[] = await db.debug_scanBTree(
            targetIndexName,
            tsEncodedFieldScanPart, // PASS THE BUFFER DIRECTLY
            tsEncodedFieldScanPart  // PASS THE BUFFER DIRECTLY
        );
        console.log(`DEBUG SCAN Results (raw count: ${resultsAsBuffers.length}): [${resultsAsBuffers.map(r => formatBufferForPrint(r)).join(', ')}]`);
        return resultsAsBuffers;
    }

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        db = new Indinis(testDataDir);
        console.log(`\n[TEST START] Using data directory: ${testDataDir}`);

        expect(await db.createIndex(usersPath, emailIndexName, { field: 'email' })).toBe(true);
        console.log(`Index '${emailIndexName}' created for field 'email'.`);
        expect(await db.createIndex(usersPath, ageIndexName, { field: 'age' })).toBe(true);
        console.log(`Index '${ageIndexName}' created for field 'age'.`);
        expect(await db.createIndex(usersPath, scoreIndexName, { field: 'score' })).toBe(true);
        console.log(`Index '${scoreIndexName}' created for field 'score'.`);
        expect(await db.createIndex(usersPath, ageDescIndexName, { field: 'age', order: 'desc' })).toBe(true);
        console.log(`Index '${ageDescIndexName}' created for field 'age' (descending).`);
    });

    afterEach(async () => {
        console.log("[TEST END] Closing database...");
        if (db) await db.close();
        console.log("[TEST END] Cleaning up test directory...");
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir);
    }, 20000);

    afterAll(async () => {
        console.log("[SUITE END] Cleaning up base directory...");
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    // --- Original Scenarios Adapted ---

    it('SCENARIO 1 (String): Should correctly index a newly inserted document (email)', async () => {
        const userId = 'user1';
        const email = 'alice@example.com';
        const age = 30;
        const key = `${usersPath}/${userId}`;
        let insertTxnId = -1;

        await db.transaction(async tx => {
            await tx.set(key, JSON.stringify({ name: 'Alice', email: email, age: age }));
            insertTxnId = await tx.getId();
        });
        console.log(`Inserted ${key} (email: ${email}, age: ${age}) in Txn ${insertTxnId}`);

        const indexEntriesEmail = await debugScanBTreeForFieldValue(email, 'string', emailIndexName);
        const tsEncodedEmail = createTsEncodedFieldKeyPart(email, 'string');
        const expectedBTreeKeyEmail = createBTreeCompositeKeyForTest(tsEncodedEmail, insertTxnId);

        expect(indexEntriesEmail).toHaveLength(1);
        expectEntriesToContainKey(indexEntriesEmail, expectedBTreeKeyEmail, "SCENARIO 1: Email index entry");
    });

    it('SCENARIO 2 (String): Should handle index update when indexed string field changes', async () => {
        const userId = 'user2';
        const key = `${usersPath}/${userId}`;
        const initialEmail = 'bob_initial@example.com';
        const updatedEmail = 'bob_updated@example.com';
        let insertTxnId = -1, updateTxnId = -1;

        await db.transaction(async tx => { await tx.set(key, JSON.stringify({ email: initialEmail })); insertTxnId = await tx.getId(); });
        await db.transaction(async tx => { await tx.set(key, JSON.stringify({ email: updatedEmail })); updateTxnId = await tx.getId(); });

        const oldEmailEntries = await debugScanBTreeForFieldValue(initialEmail, 'string', emailIndexName);
        const tsEncInitialEmail = createTsEncodedFieldKeyPart(initialEmail, 'string');
        expectEntriesToContainKey(oldEmailEntries, createBTreeCompositeKeyForTest(tsEncInitialEmail, insertTxnId), "S2: Old email original");
        expectEntriesToContainKey(oldEmailEntries, createBTreeCompositeKeyForTest(tsEncInitialEmail, updateTxnId), "S2: Old email tombstone");
        expect(oldEmailEntries).toHaveLength(2);

        const newEmailEntries = await debugScanBTreeForFieldValue(updatedEmail, 'string', emailIndexName);
        const tsEncUpdatedEmail = createTsEncodedFieldKeyPart(updatedEmail, 'string');
        expectEntriesToContainKey(newEmailEntries, createBTreeCompositeKeyForTest(tsEncUpdatedEmail, updateTxnId), "S2: New email entry");
        expect(newEmailEntries).toHaveLength(1);
    });

    it('SCENARIO 3 (String): Should not change index when non-indexed field is updated', async () => {
        const userId = 'user3';
        const key = `${usersPath}/${userId}`;
        const email = 'charlie@example.com';
        let insertTxnId = -1; // updateTxnId not needed for verification here

        await db.transaction(async tx => { await tx.set(key, JSON.stringify({ email: email, name: 'Charlie' })); insertTxnId = await tx.getId(); });
        await db.transaction(async tx => { await tx.set(key, JSON.stringify({ email: email, name: 'Charlie Brown' })); /* updateTxnId = await tx.getId(); */ });

        const indexEntries = await debugScanBTreeForFieldValue(email, 'string', emailIndexName);
        const tsEncEmail = createTsEncodedFieldKeyPart(email, 'string');
        expectEntriesToContainKey(indexEntries, createBTreeCompositeKeyForTest(tsEncEmail, insertTxnId), "S3: Email original entry");
        expect(indexEntries).toHaveLength(1);
    });

    it('SCENARIO 4 (String): Should add a tombstone when a document is deleted', async () => {
        const userId = 'user4';
        const key = `${usersPath}/${userId}`;
        const email = 'diane@example.com';
        let insertTxnId = -1, deleteTxnId = -1;

        await db.transaction(async tx => { await tx.set(key, JSON.stringify({ email: email })); insertTxnId = await tx.getId(); });
        await db.transaction(async tx => { await tx.remove(key); deleteTxnId = await tx.getId(); });

        const indexEntries = await debugScanBTreeForFieldValue(email, 'string', emailIndexName);
        const tsEncEmail = createTsEncodedFieldKeyPart(email, 'string');
        expectEntriesToContainKey(indexEntries, createBTreeCompositeKeyForTest(tsEncEmail, insertTxnId), "S4: Original entry");
        expectEntriesToContainKey(indexEntries, createBTreeCompositeKeyForTest(tsEncEmail, deleteTxnId), "S4: Tombstone entry");
        expect(indexEntries).toHaveLength(2);
    });

    it('SCENARIO 5 (Null/String): Should handle indexing when field is initially null, then set to string', async () => {
        const userId = 'user5';
        const key = `${usersPath}/${userId}`;
        const finalEmail = 'ed@example.com';
        let insertTxnId = -1, updateTxnId = -1;

        await db.transaction(async tx => { await tx.set(key, JSON.stringify({ name: 'Ed' })); insertTxnId = await tx.getId(); });
        await db.transaction(async tx => { await tx.set(key, JSON.stringify({ name: 'Ed', email: finalEmail })); updateTxnId = await tx.getId(); });

        const nullEmailEntries = await debugScanBTreeForFieldValue(null, 'string', emailIndexName);
        const tsEncNull = createTsEncodedFieldKeyPart(null, 'string');
        expectEntriesToContainKey(nullEmailEntries, createBTreeCompositeKeyForTest(tsEncNull, insertTxnId), "S5: Original null entry");
        expectEntriesToContainKey(nullEmailEntries, createBTreeCompositeKeyForTest(tsEncNull, updateTxnId), "S5: Null tombstone");
        expect(nullEmailEntries).toHaveLength(2);

        const emailEntries = await debugScanBTreeForFieldValue(finalEmail, 'string', emailIndexName);
        const tsEncFinalEmail = createTsEncodedFieldKeyPart(finalEmail, 'string');
        expectEntriesToContainKey(emailEntries, createBTreeCompositeKeyForTest(tsEncFinalEmail, updateTxnId), "S5: New email entry");
        expect(emailEntries).toHaveLength(1);
    });

    it('SCENARIO 6 (String/Null): Should handle indexing when field is set to string, then removed (becomes null)', async () => {
        const userId = 'user6';
        const key = `${usersPath}/${userId}`;
        const initialEmail = 'fran@example.com';
        let insertTxnId = -1, updateTxnId = -1;

        await db.transaction(async tx => { await tx.set(key, JSON.stringify({ email: initialEmail })); insertTxnId = await tx.getId(); });
        await db.transaction(async tx => { await tx.set(key, JSON.stringify({ name: 'Fran' })); updateTxnId = await tx.getId(); });

        const emailEntries = await debugScanBTreeForFieldValue(initialEmail, 'string', emailIndexName);
        const tsEncInitialEmail = createTsEncodedFieldKeyPart(initialEmail, 'string');
        expectEntriesToContainKey(emailEntries, createBTreeCompositeKeyForTest(tsEncInitialEmail, insertTxnId), "S6: Original email entry");
        expectEntriesToContainKey(emailEntries, createBTreeCompositeKeyForTest(tsEncInitialEmail, updateTxnId), "S6: Email tombstone");
        expect(emailEntries).toHaveLength(2);

        const nullEmailEntries = await debugScanBTreeForFieldValue(null, 'string', emailIndexName);
        const tsEncNull = createTsEncodedFieldKeyPart(null, 'string');
        expectEntriesToContainKey(nullEmailEntries, createBTreeCompositeKeyForTest(tsEncNull, updateTxnId), "S6: New null entry");
        expect(nullEmailEntries).toHaveLength(1);
    });


    // --- New Robust Tests for Numeric Types and Order ---

    it('NUMERIC (Int64): Should correctly index and find various int64 values', async () => {
        const values = [
            { id: 'int_user_pos', age: 100, txnId: 0 },
            { id: 'int_user_neg', age: -50, txnId: 0 },
            { id: 'int_user_zero', age: 0, txnId: 0 },
            { id: 'int_user_max_safe', age: Number.MAX_SAFE_INTEGER, txnId: 0 },
            { id: 'int_user_min_safe', age: Number.MIN_SAFE_INTEGER, txnId: 0 },
            // Test with BigInts for full 64-bit range
            { id: 'int_user_large_pos_big', age: (1n << 60n), txnId: 0 }, // Large positive BigInt
            { id: 'int_user_large_neg_big', age: -(1n << 60n), txnId: 0 }, // Large negative BigInt
        ];

        for (const item of values) {
            await db.transaction(async tx => {
                // OLD: await tx.set(`${usersPath}/${item.id}`, JSON.stringify({ age: Number(item.age) }));
                // NEW: Store BigInt as a string in JSON to preserve precision
                await tx.set(`${usersPath}/${item.id}`, JSON.stringify({ age: item.age.toString() }));
                item.txnId = await tx.getId();
            });
        }

        for (const item of values) {
            const entries = await debugScanBTreeForFieldValue(item.age, 'int64', ageIndexName);
            const tsEncAge = createTsEncodedFieldKeyPart(item.age, 'int64');
            expectEntriesToContainKey(entries, createBTreeCompositeKeyForTest(tsEncAge, item.txnId), `Int64: ${item.age}`);
            expect(entries).toHaveLength(1);
        }
    });

    it('NUMERIC (Double): Should correctly index and find various double values', async () => {
        const values = [
            { id: 'dbl_user_pos', score: 123.456, txnId: 0 },
            { id: 'dbl_user_neg', score: -78.901, txnId: 0 },
            { id: 'dbl_user_p_zero', score: 0.0, txnId: 0 }, // Test with +0.0
            // { id: 'dbl_user_n_zero', score: -0.0, txnId: 0 }, // -0.0 becomes 0 in JSON.stringify
            { id: 'dbl_user_small_pos', score: 0.000000123, txnId: 0 },
            { id: 'dbl_user_small_neg', score: -0.000000456, txnId: 0 },
            { id: 'dbl_user_large_pos', score: 1.23e+100, txnId: 0 },
            { id: 'dbl_user_large_neg', score: -4.56e+100, txnId: 0 },
        ];

        for (const item of values) {
            await db.transaction(async tx => {
                await tx.set(`${usersPath}/${item.id}`, JSON.stringify({ score: item.score }));
                item.txnId = await tx.getId();
            });
        }

        for (const item of values) {
            const entries = await debugScanBTreeForFieldValue(item.score, 'double', scoreIndexName);
            const tsEncScore = createTsEncodedFieldKeyPart(item.score, 'double');
            expectEntriesToContainKey(entries, createBTreeCompositeKeyForTest(tsEncScore, item.txnId), `Double: ${item.score}`);
            expect(entries).toHaveLength(1);
        }
    });
    
    it('DESCENDING ORDER (Int64): Should handle descending order for age', async () => {
        const keyA = `${usersPath}/desc_A`; const ageA = 20; let txnA = -1;
        const keyB = `${usersPath}/desc_B`; const ageB = 30; let txnB = -1;
        const keyC = `${usersPath}/desc_C`; const ageC = 20; let txnC = -1; // Same age as A, different txn

        await db.transaction(async tx => { await tx.set(keyA, JSON.stringify({ age: ageA })); txnA = await tx.getId(); });
        await db.transaction(async tx => { await tx.set(keyB, JSON.stringify({ age: ageB })); txnB = await tx.getId(); });
        await db.transaction(async tx => { await tx.set(keyC, JSON.stringify({ age: ageC })); txnC = await tx.getId(); });

        // Helper to get expected BTree key for DESC order
        const getDescExpectedBTreeKey = (age: number, txnId: number) => {
            let tsEncPart = createTsEncodedFieldKeyPart(age, 'int64');
            // C++ side inverts the bytes of the field part for DESC
            let invertedTsEncPart = Buffer.from(tsEncPart.map(b => ~b & 0xFF));
            return createBTreeCompositeKeyForTest(invertedTsEncPart, txnId);
        };

        // When scanning for a value in a DESC index, we provide the ORIGINAL value.
        // The debugScanBTreeForFieldValue helper will then apply the inversion
        // because we pass `isFieldDescending: true`.
        const entriesAgeA_descScan = await debugScanBTreeForFieldValue(ageA, 'int64', ageDescIndexName, true);
        expectEntriesToContainKey(entriesAgeA_descScan, getDescExpectedBTreeKey(ageA, txnA), "DESC Order: age 20 (TxnA)");
        expectEntriesToContainKey(entriesAgeA_descScan, getDescExpectedBTreeKey(ageC, txnC), "DESC Order: age 20 (TxnC)");
        expect(entriesAgeA_descScan).toHaveLength(2);

        const entriesAgeB_descScan = await debugScanBTreeForFieldValue(ageB, 'int64', ageDescIndexName, true);
        expectEntriesToContainKey(entriesAgeB_descScan, getDescExpectedBTreeKey(ageB, txnB), "DESC Order: age 30 (TxnB)");
        expect(entriesAgeB_descScan).toHaveLength(1);

        // Verify lexicographical order of the *final BTree keys*
        const btreeKeyA_desc = getDescExpectedBTreeKey(ageA, txnA); // age 20, desc
        const btreeKeyB_desc = getDescExpectedBTreeKey(ageB, txnB); // age 30, desc
        // Since 20 < 30, for descending order, key(20) should sort AFTER key(30).
        // So, Buffer.compare(keyA_desc, keyB_desc) should be > 0.
        expect(btreeKeyA_desc.compare(btreeKeyB_desc)).toBeGreaterThan(0);
    });
});