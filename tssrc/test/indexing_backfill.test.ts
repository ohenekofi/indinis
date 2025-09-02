// @filename: tssrc/test/indexing_backfill.test.ts

import { Indinis, IndexOptions, StorageValue } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

// Helper for small delay to allow async background tasks to run
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Helper to stringify values for storage
const s = (obj: any): string => JSON.stringify(obj);

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-indexing-backfill');

// --- TypeScript Type Augmentation & Helpers ---
// These ensure the test file's types match the C++ addon's actual return values.

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
interface BackfillTestDoc {
    id: string;
    status: 'pending' | 'active' | 'archived';
    region: string;
    createdAt: number;
    payload: string;
}

describe('Indinis Asynchronous Index Backfilling', () => {
    let db: Indinis;
    let testDataDir: string;
    const colPath = 'backfill_docs';

    const indexByStatus: IndexOptions = { field: 'status' };
    const indexName = 'idx_status';

    jest.setTimeout(45000);

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        db = new Indinis(testDataDir, { checkpointIntervalSeconds: 5 });
        console.log(`\n[BACKFILL TEST START] Using data directory: ${testDataDir}`);
    });

    afterEach(async () => {
        console.log("[BACKFILL TEST END] Closing database...");
        if (db) await db.close();
        console.log("[BACKFILL TEST END] Cleaning up test directory...");
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir);
    });

    afterAll(async () => {
        console.log("[BACKFILL SUITE END] Cleaning up base directory...");
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    async function queryByStatus(status: BackfillTestDoc['status']): Promise<BackfillTestDoc[]> {
        const tsEncodedFieldPart = createTsEncodedFieldKeyPart(status);
        const results: BackfillTestDoc[] = [];

        await db.transaction(async (tx) => {
            const indexEntries = await db.debug_scanBTree(indexName, tsEncodedFieldPart, tsEncodedFieldPart);
            
            for (const entry of indexEntries) {
                const primaryKey = entry.value.toString('utf8');
                if (primaryKey) {
                    const docStr = await tx.get(primaryKey);
                    if (docStr) {
                        results.push(JSON.parse(docStr as string));
                    }
                }
            }
        });
        return results;
    }

    it('should create an index asynchronously and correctly backfill historical data', async () => {
        // Phase 1: Write data BEFORE index exists
        console.log("--- Phase 1: Writing data before index exists ---");
        const preIndexDocs: BackfillTestDoc[] = [
            { id: 'doc1', status: 'active', region: 'us-east', createdAt: Date.now(), payload: 'pre-index doc 1' },
            { id: 'doc2', status: 'pending', region: 'us-west', createdAt: Date.now(), payload: 'pre-index doc 2' },
            { id: 'doc3', status: 'active', region: 'us-west', createdAt: Date.now(), payload: 'pre-index doc 3' },
        ];
        await db.transaction(async tx => {
            for (const doc of preIndexDocs) {
                await tx.set(`${colPath}/${doc.id}`, s(doc));
            }
        });
        console.log(`${preIndexDocs.length} documents written.`);

        // Phase 2: Create the index
        console.log("\n--- Phase 2: Creating index 'idx_status' (should return immediately) ---");
        const createResult = await db.createIndex(colPath, indexName, indexByStatus);
        expect(createResult).toBe('CREATED');
        
        // Phase 3: Write new data WHILE backfill is running
        console.log("\n--- Phase 3: Writing new data while backfill is in progress ---");
        const postIndexDoc: BackfillTestDoc = { id: 'doc4_post_idx', status: 'active', region: 'eu-central', createdAt: Date.now(), payload: 'post-index doc 4' };
        await db.transaction(async tx => {
            await tx.set(`${colPath}/${postIndexDoc.id}`, s(postIndexDoc));
        });
        console.log("New document 'doc4_post_idx' written.");

        // Phase 4: Wait for backfill to complete and verify
        const backfillWaitTime = 5000;
        console.log(`\n--- Phase 4: Waiting ${backfillWaitTime / 1000}s for backfill to complete... ---`);
        await delay(backfillWaitTime);

        console.log("Verifying index content after backfill...");
        const activeDocs = await queryByStatus('active');
        const pendingDocs = await queryByStatus('pending');

        expect(activeDocs).toHaveLength(3);
        expect(activeDocs).toEqual(expect.arrayContaining([
            expect.objectContaining({ id: 'doc1' }),
            expect.objectContaining({ id: 'doc3' }),
            expect.objectContaining({ id: 'doc4_post_idx' }),
        ]));

        expect(pendingDocs).toHaveLength(1);
        expect(pendingDocs[0]).toEqual(expect.objectContaining({ id: 'doc2' }));
    });

    it('should be idempotent and not re-backfill an existing index', async () => {
        // Phase 1: First call
        console.log("\n--- Idempotency Test: First call to createIndex ---");
        const createResult1 = await db.createIndex(colPath, indexName, indexByStatus);
        expect(createResult1).toBe('CREATED');

        const docToTest = { id: 'item1', status: 'active', region: 'us-east', payload: 'test', createdAt: Date.now() };
        await db.transaction(async tx => {
            await tx.set(`${colPath}/item1`, s(docToTest));
        });
        console.log("Data written after first index creation.");
        await delay(3000);

        // Phase 2: Second call
        console.log("--- Idempotency Test: Second call to createIndex ---");
        const startTime = Date.now();
        const createResult2 = await db.createIndex(colPath, indexName, indexByStatus);
        const duration = Date.now() - startTime;

        expect(createResult2).toBe('EXISTED');
        expect(duration).toBeLessThan(100);
        console.log(`Second createIndex call returned '${createResult2}' in ${duration}ms.`);

        // Phase 3: Verification
        console.log("Verifying index content after idempotent call...");
        const activeDocs = await queryByStatus('active');
        expect(activeDocs.length).toBe(1);
        expect(activeDocs[0]).toEqual(expect.objectContaining({ id: 'item1' }));
        console.log("Data integrity verified after idempotent call.");
    });
});