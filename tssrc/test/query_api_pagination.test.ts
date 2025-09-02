// @filename: tssrc/test/query_api_pagination.test.ts

import { Indinis, StoreRef } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

// Helper for small delay
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-query-pagination');

// --- Test Data Interface ---
interface Employee {
    id?: string;
    name: string;
    level: number;
    department: 'eng' | 'sales' | 'hr';
    hireDate: number; // Unix timestamp
}

describe('Indinis Fluent Query API - Pagination with .get()', () => {
    let db: Indinis;
    let testDataDir: string;
    let empStore: StoreRef<Employee>;
    const employeesPath = 'employees';

    // A diverse set of employees for testing sorting and filtering
    const testData: Omit<Employee, 'id'>[] = [
        { name: 'Alice',   level: 7, department: 'eng',   hireDate: 1672531200 }, // Jan 1 2023
        { name: 'Bob',     level: 5, department: 'sales', hireDate: 1675209600 }, // Feb 1 2023
        { name: 'Charlie', level: 7, department: 'eng',   hireDate: 1677628800 }, // Mar 1 2023
        { name: 'Diane',   level: 5, department: 'sales', hireDate: 1680307200 }, // Apr 1 2023
        { name: 'Eve',     level: 8, department: 'eng',   hireDate: 1682899200 }, // May 1 2023
        { name: 'Frank',   level: 3, department: 'hr',    hireDate: 1685577600 }, // Jun 1 2023
        { name: 'Grace',   level: 7, department: 'eng',   hireDate: 1688169600 }, // Jul 1 2023
    ];

    jest.setTimeout(45000);

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        db = new Indinis(testDataDir);
        console.log(`\n[PAGINATION TEST START] Using data directory: ${testDataDir}`);

        // Create indexes on all fields we'll be sorting by
        await db.createIndex(employeesPath, 'idx_emp_name_asc', { field: 'name', order: 'asc' });
        await db.createIndex(employeesPath, 'idx_emp_level_desc', { field: 'level', order: 'desc' });
        await db.createIndex(employeesPath, 'idx_emp_hireDate_asc', { field: 'hireDate', order: 'asc' });

        // Pre-populate data
        empStore = db.store<Employee>(employeesPath);
        await db.transaction(async () => {
            for (let i = 0; i < testData.length; i++) {
                await empStore.item(`emp${i + 1}`).make(testData[i]);
            }
        });
        
        // Wait for index backfill to complete
        await delay(5000);
        console.log("Test setup complete: Data populated and indexes are ready.");
    });

    afterEach(async () => {
        if (db) await db.close();
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir);
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    // Helper to extract names for easy, deterministic comparison
    const getNames = (results: Employee[]): string[] => results.map(r => r.name);

    it('should fetch the first page of results with a simple orderBy and limit', async () => {
        const firstPage = await empStore.query()
            .orderBy('name', 'asc')
            .limit(3)
            .get();

        expect(getNames(firstPage.docs)).toEqual(['Alice', 'Bob', 'Charlie']);
        expect(firstPage.hasNextPage).toBe(true);
        expect(firstPage.hasPrevPage).toBe(false); // Not implemented yet, should be false
        expect(firstPage.endCursor).toEqual(['Charlie']);
    });

    it('should fetch the next page using the endCursor from the previous page', async () => {
        const firstPage = await empStore.query()
            .orderBy('name', 'asc')
            .limit(3)
            .get();

        expect(firstPage.hasNextPage).toBe(true);
        expect(firstPage.endCursor).toBeDefined();

        const nextPage = await empStore.query()
            .orderBy('name', 'asc')
            .limit(3)
            .startAfter(...firstPage.endCursor!)
            .get();
        
        expect(getNames(nextPage.docs)).toEqual(['Diane', 'Eve', 'Frank']);
        expect(nextPage.hasNextPage).toBe(true);
    });

    it('should correctly report hasNextPage as false on the last page', async () => {
        // Fetch page 1 (3 docs)
        const page1 = await empStore.query().orderBy('name', 'asc').limit(3).get();
        // Fetch page 2 (3 docs)
        const page2 = await empStore.query().orderBy('name', 'asc').limit(3).startAfter(...page1.endCursor!).get();
        // Fetch page 3 (should have 1 doc)
        const page3 = await empStore.query().orderBy('name', 'asc').limit(3).startAfter(...page2.endCursor!).get();

        expect(getNames(page3.docs)).toEqual(['Grace']);
        expect(page3.hasNextPage).toBe(false);
        expect(page3.endCursor).toEqual(['Grace']);
    });

    it('should handle a compound orderBy and generate a compound cursor', async () => {
        // --- Test Goal ---
        // This test verifies that a query with multiple .orderBy() clauses returns documents
        // sorted correctly by the primary key first, and then by the secondary key for ties.
        // It also verifies that the generated `endCursor` is a compound array containing
        // values for all orderBy fields from the last document in the result set.

        const firstPage = await empStore.query()
            .orderBy('level', 'desc') // Primary sort: highest level first
            .orderBy('name', 'asc')   // Secondary sort: alphabetical for ties in level
            .limit(4)
            .get();

        // --- Verification ---

        // 1. Verify the ORDER of the returned documents.
        // The database should return the documents in this exact order:
        // - Eve (level 8)
        // - Alice (level 7, name 'A')
        // - Charlie (level 7, name 'C')
        // - Grace (level 7, name 'G')
        const receivedNames = firstPage.docs.map(doc => doc.name);
        const receivedLevels = firstPage.docs.map(doc => doc.level);
        
        console.log("Received names for compound sort:", receivedNames);
        console.log("Received levels for compound sort:", receivedLevels);

        expect(receivedNames).toEqual(['Eve', 'Alice', 'Charlie', 'Grace']);
        expect(receivedLevels).toEqual([8, 7, 7, 7]);

        // 2. Verify the pagination state.
        // Since we have 7 total documents and only fetched 4, there should be a next page.
        expect(firstPage.hasNextPage).toBe(true);

        // 3. Verify the compound cursor.
        // The cursor should be an array of values from the *last* document ('Grace')
        // matching the orderBy fields: [Grace.level, Grace.name].
        expect(firstPage.endCursor).toEqual([7, 'Grace']);
        
        console.log("  Verified: Compound sort order and cursor are correct.");
    });

    it('should use a compound cursor to fetch the next page correctly', async () => {
        const firstPage = await empStore.query()
            .orderBy('level', 'desc')
            .orderBy('name', 'asc')
            .limit(4)
            .get();

        expect(firstPage.endCursor).toEqual([7, 'Grace']);
        
        const nextPage = await empStore.query()
            .orderBy('level', 'desc')
            .orderBy('name', 'asc')
            .limit(4)
            .startAfter(...firstPage.endCursor!) // startAfter(7, 'Grace')
            .get();

        // Expected order on next page:
        // 5. Bob (level 5)
        // 6. Diane (level 5)
        // 7. Frank (level 3)
        expect(getNames(nextPage.docs)).toEqual(['Bob', 'Diane', 'Frank']);
        const levels = nextPage.docs.map(d => d.level);
        expect(levels).toEqual([5, 5, 3]);
        expect(nextPage.hasNextPage).toBe(false);
    });

    it('should accept a full document object as a cursor for startAfter', async () => {
        const firstPage = await empStore.query()
            .orderBy('hireDate', 'asc')
            .limit(2)
            .get();

        expect(getNames(firstPage.docs)).toEqual(['Alice', 'Bob']);
        
        const lastDocOnPage = firstPage.docs[firstPage.docs.length - 1]; // This is Bob's document
        expect(lastDocOnPage.name).toBe('Bob');
        
        // Instead of using firstPage.endCursor, pass the whole document object.
        const nextPage = await empStore.query()
            .orderBy('hireDate', 'asc')
            .limit(2)
            .startAfter(lastDocOnPage) // Using the document snapshot as the cursor
            .get();

        expect(getNames(nextPage.docs)).toEqual(['Charlie', 'Diane']);
    });

    it('should return an empty result if the cursor points past the end of the data', async () => {
        const lastPage = await empStore.query()
            .orderBy('name', 'asc')
            .startAfter('Zebra') // Start after the last possible name
            .limit(10)
            .get();

        expect(lastPage.docs).toHaveLength(0);
        expect(lastPage.hasNextPage).toBe(false);
    });
});
