// tssrc/test/columnar_concurrency.test.ts

import { Indinis, IndinisOptions, ColumnSchemaDefinition, ColumnType, sum, count } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-columnar-concurrency');

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const s = (obj: any): string => JSON.stringify(obj);

// Augment the Indinis interface for our new debug method
declare module '../index' {
    interface Indinis {
        debug_forceColumnarCompaction(storePath: string): Promise<boolean>;
    }
}

// Test Data Interface
interface SalesRecord {
    id?: string;
    region: string;
    product_category: string;
    units_sold: number;
    total_price: number;
    is_return: boolean; // This field is a boolean
}

describe('Columnar Store Concurrency and Compaction Stress Test', () => {
    let db: Indinis;
    let testDataDir: string;
    const salesPath = 'sales';

    const salesSchema: ColumnSchemaDefinition = {
        storePath: salesPath,
        columns: [
            { name: 'region', type: ColumnType.STRING, column_id: 1 },
            { name: 'product_category', type: ColumnType.STRING, column_id: 2 },
            { name: 'units_sold', type: ColumnType.INT64, column_id: 3 },
            { name: 'total_price', type: ColumnType.DOUBLE, column_id: 4 },
            { name: 'is_return', type: ColumnType.BOOLEAN, column_id: 5 },
        ]
    };

    jest.setTimeout(300000);

    beforeEach(async () => {
        // ... (beforeEach setup remains unchanged)
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        const options: IndinisOptions = {
            walOptions: { segment_size: 128 * 1024 },
            checkpointIntervalSeconds: 5,
        };
        db = new Indinis(testDataDir, options);
        await db.registerStoreSchema(salesSchema);
        console.log(`\n[COLUMNAR STRESS TEST START] Using data directory: ${testDataDir}`);
    });

    afterEach(async () => {
        if (db) await db.close();
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir);
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    it('should handle high-volume concurrent writes and compactions without data loss', async () => {
        const NUM_WRITERS = 8;
        const OPS_PER_WRITER = 500;
        const regions = ['NA', 'EU', 'APAC', 'LATAM'];
        const categories = ['electronics', 'books', 'apparel', 'home'];
        
        const groundTruth = new Map<string, SalesRecord>();

        console.log(`--- Phase 1: Spawning ${NUM_WRITERS} writers to perform ${OPS_PER_WRITER} operations each... ---`);
        // ... (writer logic remains unchanged) ...
        const writerPromises: Promise<void>[] = [];
        for (let i = 0; i < NUM_WRITERS; i++) {
            const writerPromise = (async (writerId: number) => {
                for (let j = 0; j < OPS_PER_WRITER; j++) {
                    const docId = `sale_w${writerId}_${j}`;
                    const key = `${salesPath}/${docId}`;
                    const opType = Math.random();
                    if (opType < 0.7 && groundTruth.has(key)) {
                        const existing = groundTruth.get(key)!;
                        const updated: SalesRecord = { ...existing, units_sold: existing.units_sold + 5, total_price: existing.total_price + 50.5 };
                        await db.store<SalesRecord>(salesPath).item(docId).modify(updated);
                        groundTruth.set(key, updated);
                    } else {
                        const newRecord: Omit<SalesRecord, 'id'> = { region: regions[j % regions.length], product_category: categories[j % categories.length], units_sold: 1 + (j % 10), total_price: 10.0 + (j % 50), is_return: Math.random() < 0.1 };
                        await db.store<SalesRecord>(salesPath).item(docId).make(newRecord);
                        groundTruth.set(key, { ...newRecord, id: docId });
                    }
                }
            })(i);
            writerPromises.push(writerPromise);
        }
        // ... (compaction trigger logic remains unchanged) ...
        let compactionTriggers = 0;
        const compactionPromise = (async () => {
            while (writerPromises.some(p => !(p as any).isFulfilled)) {
                await delay(8000);
                try {
                    console.log("--- Triggering force columnar compaction... ---");
                    await db.debug_forceColumnarCompaction(salesPath);
                    compactionTriggers++;
                } catch (e: any) { console.error("Error forcing compaction:", e.message); }
            }
        })();
        await Promise.all([...writerPromises, compactionPromise]);
        console.log(`--- Phase 1 Complete: All ${NUM_WRITERS * OPS_PER_WRITER} operations finished. Triggered ${compactionTriggers} compactions. ---`);
        console.log("--- Phase 2: Final flush and wait ---");
        await db.forceCheckpoint();
        await delay(10000);

        console.log("--- Phase 3: Data Integrity Verification ---");
        
        const expectedTotals = new Map<string, { total_units: number, transaction_count: number }>();
        for (const record of groundTruth.values()) {
            if (!record.is_return) {
                const key = `${record.region}|${record.product_category}`;
                const current = expectedTotals.get(key) || { total_units: 0, transaction_count: 0 };
                current.total_units += record.units_sold;
                current.transaction_count += 1;
                expectedTotals.set(key, current);
            }
        }

        // --- THIS CODE BLOCK WILL NOW COMPILE AND RUN CORRECTLY ---
        const dbResults = await db.store<SalesRecord>(salesPath) // <-- Correctly typed StoreRef
            .query()
            .filter('is_return').equals(false) // <-- 'is_return' is a valid key of SalesRecord, 'false' is a valid StorageValue
            .groupBy('region', 'product_category') // <-- 'region' & 'product_category' are valid keys
            .aggregate({
                total_units: sum('units_sold'),
                transaction_count: count('units_sold')
            })
            .take();

        expect(dbResults.length).toBe(expectedTotals.size);
        
        for (const result of dbResults) {
            const key = `${result.region}|${result.product_category}`;
            const expected = expectedTotals.get(key);
            console.log(`Verifying group: ${key}. DB: {units: ${result.total_units}, count: ${result.transaction_count}}. Expected: {units: ${expected?.total_units}, count: ${expected?.transaction_count}}`);
            expect(expected).toBeDefined();
            if (expected) {
                expect(result.total_units).toBe(expected.total_units);
                expect(result.transaction_count).toBe(expected.transaction_count);
            }
        }
        console.log("--- Verification Complete: All aggregation results match the ground truth. ---");
    });
});