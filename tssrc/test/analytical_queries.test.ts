//tssrc/test/analytical_queries.test.ts
import { Indinis, IndinisOptions, ColumnSchemaDefinition, ColumnType, sum, count } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const s = (obj: any): string => JSON.stringify(obj);

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-analytical-queries');

// Test Data Interface
interface SalesRecord {
    id?: string;
    region: 'NA' | 'EU' | 'APAC';
    product_category: 'electronics' | 'books' | 'apparel';
    units_sold: number;
    total_price: number;
}

describe('Indinis Analytical Queries (groupBy, aggregate)', () => {
    let db: Indinis;
    let testDataDir: string;
    const salesPath = 'sales';

    const salesSchema: ColumnSchemaDefinition = {
        storePath: salesPath,
        schemaVersion: 1,
        columns: [
            { name: 'region', type: ColumnType.STRING, column_id: 1 },
            { name: 'product_category', type: ColumnType.STRING, column_id: 2 },
            { name: 'units_sold', type: ColumnType.INT64, column_id: 3 },
            { name: 'total_price', type: ColumnType.DOUBLE, column_id: 4 },
            { name: 'product_id', type: ColumnType.INT64, column_id: 5 }
        ]
    };

    // Sample data designed for easy-to-verify aggregations
    const testData: (Omit<SalesRecord, 'id'> & { product_id: number })[] = [
        { region: 'NA', product_category: 'electronics', units_sold: 10, total_price: 1200.50, product_id: 101 },
        { region: 'NA', product_category: 'books', units_sold: 100, total_price: 1500.00, product_id: 401 },
        { region: 'EU', product_category: 'electronics', units_sold: 5, total_price: 800.00, product_id: 201 },
        { region: 'EU', product_category: 'apparel', units_sold: 50, total_price: 2500.00, product_id: 501 },
        { region: 'NA', product_category: 'electronics', units_sold: 8, total_price: 950.50, product_id: 102 },
        { region: 'APAC', product_category: 'books', units_sold: 200, total_price: 2200.00, product_id: 402 },
        { region: 'EU', product_category: 'electronics', units_sold: 7, total_price: 990.00, product_id: 202 },
    ];

    jest.setTimeout(45000);

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        
        console.log(`\n[ANALYTICAL TEST START] Using data directory: ${testDataDir}`);
        
        // --- START OF FIX: Corrected Test Setup ---

        // 1. Create the DB instance
        db = new Indinis(testDataDir, { checkpointIntervalSeconds: 0 }); // Disable auto checkpoints for control

        // 2. Register the schema BEFORE writing data. This is the most common use case
        //    and prevents the backfill race condition in this test.
        await db.registerStoreSchema(salesSchema);

        // 3. Write the data. Because a schema exists, `shadowInsert` will be called
        //    during this transaction, populating the columnar store's active buffer.
        const salesStore = db.store<SalesRecord>(salesPath);
        await db.transaction(async (tx) => {
            for (let i = 0; i < testData.length; i++) {
                // Add the missing product_id to the data being made
                const docData = { ...testData[i], product_id: (i + 1) * 100 };
                await salesStore.item(`sale${i + 1}`).make(docData);
            }
        });
        
        // 4. Force a flush. In C++, this would be a debug API call. In this test,
        //    closing the database is the most reliable way to ensure the columnar
        //    write buffer is flushed to disk.
        console.log("Setup: Closing DB to force flush of columnar write buffer...");
        await db.close();

        // 5. Re-open the database for the actual test. It will now load the
        //    `.cstore` file created in the previous step.
        console.log("Setup: Re-opening DB for test execution...");
        db = new Indinis(testDataDir);
        await delay(1000); // Allow time for reopening and background threads to settle.
        
        console.log("Test setup complete: Data written to both LSM and Columnar stores.");
        // --- END OF FIX ---
    });

    afterEach(async () => {
        if (db) await db.close();
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir);
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    it('should perform a simple aggregation (SUM) over the whole collection', async () => {
        console.log("--- TEST: Total SUM of units_sold ---");
        const results = await db.store<SalesRecord>(salesPath)
            .query()
            .aggregate({
                total_units: sum('units_sold')
            })
            .take();

        // Expected sum: 10 + 100 + 5 + 50 + 8 + 200 + 7 = 380
        expect(results).toHaveLength(1);
        expect(results[0].total_units).toBe(380);
        console.log("  Verified: Correct total sum calculated.");
    });

    it('should perform a GROUP BY with SUM and COUNT aggregations', async () => {
        console.log("--- TEST: GROUP BY region with SUM and COUNT ---");
        const results = await db.store<SalesRecord>(salesPath)
            .query()
            .groupBy('region')
            .aggregate({
                total_revenue: sum('total_price'),
                num_transactions: count('product_id') // Count on any non-null field
            })
            .take();

        // Sort for deterministic test
        results.sort((a, b) => a.region.localeCompare(b.region));

        expect(results).toHaveLength(3);
    
        // --- CORRECTED ASSERTIONS ---
        
        // APAC: 1 transaction, total_price 2200.00
        expect(results[0]).toEqual({ region: 'APAC', total_revenue: 2200.00, num_transactions: 1 });
        
        // EU: 3 transactions, total_price 800.00 + 2500.00 + 990.00 = 4290.00
        expect(results[1]).toEqual({ region: 'EU', total_revenue: 4290.00, num_transactions: 3 });

        // NA: 3 transactions, total_price 1200.50 + 1500.00 + 950.50 = 3651.00
        expect(results[2]).toEqual({ region: 'NA', total_revenue: 3651.00, num_transactions: 3 });
        
        console.log("  Verified: Correct GROUP BY results for all regions.");
    });

 

    it('should apply a filter BEFORE performing the aggregation', async () => {
        console.log("--- TEST: Filter before GROUP BY ---");
        // Get total sales for only the 'electronics' category, grouped by region
        const results = await db.store<SalesRecord>(salesPath)
            .query()
            .filter('product_category').equals('electronics') // This filter is applied first
            .groupBy('region')
            .aggregate({
                electronics_revenue: sum('total_price')
            })
            .take();
        
        results.sort((a, b) => a.region.localeCompare(b.region));

        expect(results).toHaveLength(2); // Only EU and NA have electronics sales

        // EU electronics revenue: 800.00 + 990.00 = 1790.00
        expect(results[0]).toEqual({ region: 'EU', electronics_revenue: 1790.00 });

        // NA electronics revenue: 1200.50 + 950.50 = 2151.00
        expect(results[1]).toEqual({ region: 'NA', electronics_revenue: 2151.00 });

        console.log("  Verified: Filter was correctly applied before aggregation.");
    });

    it('should throw an error if trying to use .one() with an aggregation', async () => {
        const query = db.store<SalesRecord>(salesPath)
            .query()
            .groupBy('region')
            .aggregate({ total: sum('units_sold') });

        await expect(query.one()).rejects.toThrow(
            ".one() cannot be used with an aggregation query. Use .take() to get aggregation results."
        );
        console.log("  Verified: Correctly threw error for .one() on aggregation.");
    });

});