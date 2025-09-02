// @filename: tssrc/test/btree_page_layout.test.ts

import { Indinis, IndexOptions } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

// Helper to stringify values for storage
const s = (obj: any): string => JSON.stringify(obj);

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-btree-page-layout');

// --- TypeScript Type Augmentation & Helpers ---


// --- TypeScript Type Augmentation & Helpers ---

interface BTreeDebugScanEntry {
    key: Buffer;
    value: Buffer;
}

declare module '../index' {
    interface Indinis {
        debug_scanBTree(indexName: string, startKeyBuffer: Buffer, endKeyBuffer: Buffer): Promise<BTreeDebugScanEntry[]>;
    }
}

function createTsEncodedFieldKeyPart(fieldValue: string | null): Buffer {
    if (fieldValue === null) return Buffer.from([0x00]);
    return Buffer.from(fieldValue, 'utf8');
}

// --- Test Data Interface ---
interface LayoutTestDoc {
    name: string;
    email: string;
}

describe('B-Tree Page Layout and MVCC Integrity Test', () => {
    let db: Indinis;
    let testDataDir: string;
    const colPath = 'layout_test_users';
    const indexName = 'idx_layout_email';
    const indexOptions: IndexOptions = { field: 'email' };

    jest.setTimeout(20000);

    // --- Setup & Teardown (unchanged) ---
    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        db = new Indinis(testDataDir);
        // Create the index once before all tests in this suite run.
        await db.createIndex(colPath, indexName, indexOptions);
        console.log(`\n[PAGE LAYOUT TEST START] Using data directory: ${testDataDir}`);
    });

    afterEach(async () => {
        if (db) await db.close();
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir);
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    /**
     * Test helper to verify the index. THIS IS THE FIX.
     * It now correctly processes raw, multi-versioned data from the debug scan
     * to determine the final, visible state of each document in the index.
     */
    async function verifyIndexForEmail(email: string | null, expectedPrimaryKeys: string[]) {
        const encodedEmail = createTsEncodedFieldKeyPart(email);
        // debug_scanBTree returns ALL raw versions for the given key part.
        const rawIndexEntries = await db.debug_scanBTree(indexName, encodedEmail, encodedEmail);

        // --- START OF CORRECTED MVCC LOGIC ---

        // Step 1: Group all found versions by their primary key.
        const versionsByPk = new Map<string, { btreeValue: Buffer, txnId: number }[]>();

        for (const entry of rawIndexEntries) {
            // The value in the B-Tree index is the primary key (or empty for a tombstone).
            // A tombstone entry still refers to a primary key via its composite B-Tree key.
            // Let's decode the composite key to reliably get the PK.
            
            // For a robust test, we would need a NAPI binding for decodeCompositeKey.
            // For this test, we'll make a simplifying assumption that the value field contains the PK.
            const primaryKey = entry.value.toString('utf8');

            // If the primaryKey is empty, it's a tombstone. We need to find the PK it belongs to
            // by decoding the composite B-Tree key. Since we don't have that decoder in JS,
            // we'll make another assumption for this test: a tombstone's composite key will match
            // a live entry's composite key except for the TxnId. This is too complex for a clean test.
            
            // Let's simplify: A tombstone's *value* is an empty buffer.
            // A tombstone's *key* still contains the primary key.
            // The `decodeCompositeKey` is needed. Let's assume we can get the PK from the B-Tree value.
            
            // Let's try a different, more robust approach.
            // Let's find the primary key from the composite key itself.
            const compositeKeyStr = entry.key.toString('utf-8');
            // key format: <email>\0<primary_key><desc_txn_id>
            const nullSeparatorIndex = compositeKeyStr.indexOf('\0');
            if (nullSeparatorIndex === -1) continue; // Malformed key, skip
            // The PK is between the null separator and the final 8-byte TxnId.
            const pkFromKey = compositeKeyStr.substring(nullSeparatorIndex + 1, compositeKeyStr.length - 8);

            const txnIdBuffer = entry.key.slice(-8);
            const invertedTxnId = txnIdBuffer.readBigUInt64BE(0);
            const txnId = Number(0xFFFFFFFFFFFFFFFFn - invertedTxnId);

            if (!versionsByPk.has(pkFromKey)) {
                versionsByPk.set(pkFromKey, []);
            }
            versionsByPk.get(pkFromKey)!.push({ btreeValue: entry.value, txnId });
        }

        const livePrimaryKeys = new Set<string>();

        // Step 2: For each document's versions, find the latest one and check its state.
        for (const [primaryKey, versions] of versionsByPk.entries()) {
            if (versions.length === 0) continue;

            // Find the version with the highest transaction ID (the most recent one).
            const latestVersion = versions.reduce((latest, current) => 
                current.txnId > latest.txnId ? current : latest
            );

            // A live entry has a non-empty buffer for its value. A tombstone has a zero-length buffer.
            if (latestVersion.btreeValue.length > 0) {
                livePrimaryKeys.add(primaryKey);
            }
        }
        
        const foundPrimaryKeys = Array.from(livePrimaryKeys).sort();
        expectedPrimaryKeys.sort();
        // --- END OF CORRECTED MVCC LOGIC ---

        console.log(`  Verifying index for email '${email}': Found live keys [${foundPrimaryKeys.join(', ')}], Expected [${expectedPrimaryKeys.join(', ')}]`);
        expect(foundPrimaryKeys).toEqual(expectedPrimaryKeys);
    }

    // The test case `it(...)` block remains identical to the previous version.
    it('should correctly handle multiple inserts, updates, and deletes on a single B-Tree leaf page', async () => {
        // Step 1: Insert first item ('c@test.com')
        console.log("--- Step 1: Insert first item ---");
        await db.transaction(async tx => {
            await tx.set(`${colPath}/user_c`, s({ name: 'Charlie', email: 'c@test.com' }));
        });
        await verifyIndexForEmail('c@test.com', [`${colPath}/user_c`]);
        await verifyIndexForEmail('a@test.com', []);

        // Step 2: Insert a second item that sorts BEFORE the first ('a@test.com')
        console.log("\n--- Step 2: Insert item that sorts BEFORE existing item ---");
        await db.transaction(async tx => {
            await tx.set(`${colPath}/user_a`, s({ name: 'Alice', email: 'a@test.com' }));
        });
        await verifyIndexForEmail('c@test.com', [`${colPath}/user_c`]);
        await verifyIndexForEmail('a@test.com', [`${colPath}/user_a`]);
        console.log("  Successfully inserted second item, B-Tree remains consistent.");

        // Step 3: Insert a third item that sorts in the MIDDLE ('b@test.com')
        console.log("\n--- Step 3: Insert item that sorts in the MIDDLE ---");
        await db.transaction(async tx => {
            await tx.set(`${colPath}/user_b`, s({ name: 'Bob', email: 'b@test.com' }));
        });
        await verifyIndexForEmail('a@test.com', [`${colPath}/user_a`]);
        await verifyIndexForEmail('b@test.com', [`${colPath}/user_b`]);
        await verifyIndexForEmail('c@test.com', [`${colPath}/user_c`]);
        console.log("  Successfully inserted third item.");

        // Step 4: Update an item, which changes its indexed value
        console.log("\n--- Step 4: Update item, changing indexed value ---");
        await db.transaction(async tx => {
            await tx.set(`${colPath}/user_b`, s({ name: 'Robert', email: 'z@test.com' }));
        });
        await verifyIndexForEmail('b@test.com', []); // Old email should now be gone.
        await verifyIndexForEmail('z@test.com', [`${colPath}/user_b`]);
        console.log("  Successfully updated item's indexed field.");

        // Step 5: Delete an item
        console.log("\n--- Step 5: Delete an item ---");
        await db.transaction(async tx => {
            await tx.remove(`${colPath}/user_a`);
        });
        await verifyIndexForEmail('a@test.com', []);
        await verifyIndexForEmail('z@test.com', [`${colPath}/user_b`]);
        await verifyIndexForEmail('c@test.com', [`${colPath}/user_c`]);
        console.log("  Successfully deleted item, index updated correctly.");
    });
});