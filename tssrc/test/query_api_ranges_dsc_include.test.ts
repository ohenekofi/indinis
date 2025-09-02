/**
 * @file tssrc/test/query_api_ranges_dsc_include.test.ts
 * @description Rigorous test suite for range queries on both ASC and DESC indexes.
 */

import { Indinis, IndexOptions, StoreRef } from '../index'; // StoreRef is needed for type hints
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

// Helper for small delay
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-query-ranges-v2');

// Test Data Interface
interface Employee {
    id?: string;
    name: string;
    level: number;
    department: 'eng' | 'sales' | 'hr';
    salary: number;
}

describe('Indinis Fluent Query API - Ascending and Descending Range Queries', () => {
    let db: Indinis;
    let testDataDir: string;
    const employeesPath = 'employees';

    // This data is intentionally ordered by name, not by level, to test sorting.
    const testData: Omit<Employee, 'id'>[] = [ // Type the source data correctly
        { name: 'Alice', level: 5, department: 'eng', salary: 100000 },
        { name: 'Bob', level: 3, department: 'sales', salary: 80000 },
        { name: 'Charlie', level: 7, department: 'eng', salary: 140000 },
        { name: 'Diane', level: 5, department: 'sales', salary: 110000 },
        { name: 'Eve', level: 8, department: 'eng', salary: 180000 },
        { name: 'Frank', level: 2, department: 'hr', salary: 75000 },
    ];

    jest.setTimeout(45000);

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        db = new Indinis(testDataDir);
        console.log(`\n[RANGE QUERY TEST v2 START] Using data directory: ${testDataDir}`);

        // Create necessary indexes
        await db.createIndex(employeesPath, 'idx_emp_level', { field: 'level', order: 'asc' });
        await db.createIndex(employeesPath, 'idx_emp_level_desc', { field: 'level', order: 'desc' });
        await db.createIndex(employeesPath, 'idx_emp_salary', { field: 'salary' });

        // Pre-populate data
        const empStore: StoreRef<Employee> = db.store<Employee>(employeesPath);
        await db.transaction(async () => {
            for (let i = 0; i < testData.length; i++) {
                const docData = testData[i];
                const docId = `emp${i + 1}`; // Create a deterministic ID like emp1, emp2, etc.

                // --- FIX APPLIED HERE ---
                // The `item.make()` method expects an object *without* an 'id' property.
                // The `id` is taken from the `item(docId)` reference itself.
                await empStore.item(docId).make(docData);
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

    // Helper to get names from results for easy comparison
    const getNames = (results: Employee[]): string[] => results.map(r => r.name).sort();

    // --- All test cases (`it` blocks) remain exactly the same as before ---
    
    describe('Ascending Index (idx_level_asc)', () => {
        it('should handle > correctly', async () => {
            const results = await db.store<Employee>(employeesPath).filter('level').greaterThan(5).take();
            expect(getNames(results)).toEqual(['Charlie', 'Eve']);
        });

        it('should handle >= correctly', async () => {
            const results = await db.store<Employee>(employeesPath).filter('level').greaterThanOrEqual(5).take();
            expect(getNames(results)).toEqual(['Alice', 'Charlie', 'Diane', 'Eve']);
        });

        it('should handle < correctly', async () => {
            const results = await db.store<Employee>(employeesPath).filter('level').lessThan(5).take();
            expect(getNames(results)).toEqual(['Bob', 'Frank']);
        });

        it('should handle <= correctly', async () => {
            const results = await db.store<Employee>(employeesPath).filter('level').lessThanOrEqual(5).take();
            expect(getNames(results)).toEqual(['Alice', 'Bob', 'Diane', 'Frank']);
        });
    });

    describe('Descending Index (idx_level_desc)', () => {
        it('should handle > correctly', async () => {
            const results = await db.store<Employee>(employeesPath).filter('level').greaterThan(5).take();
            expect(getNames(results)).toEqual(['Charlie', 'Eve']);
        });

        it('should handle <= correctly', async () => {
            const results = await db.store<Employee>(employeesPath).filter('level').lessThanOrEqual(5).take();
            expect(getNames(results)).toEqual(['Alice', 'Bob', 'Diane', 'Frank']);
        });
        
        it('should return results in descending order when specified', async () => {
            const empStore = db.store<Employee>(employeesPath);

            // This query should select all employees with level >= 5.
            // The .orderBy() call hints to the planner that the descending index is preferred.
            const seniorEmployees = await empStore
                .filter('level').greaterThanOrEqual(5)
                .sortBy('level', 'desc') // <<< FIX: EXPLICITLY REQUEST DESCENDING ORDER
                .take();

            // Extract just the levels to check the order
            const levels = seniorEmployees.map((e: { level: any; }) => e.level);

            // The C++ layer now ensures the results are ordered correctly.
            expect(levels).toEqual([8, 7, 5, 5]);
        });
    });

    it('should handle a range query combined with a post-filter', async () => {
        const empStore = db.store<Employee>(employeesPath);
        const seniorEngineers = await empStore
            .filter('level').greaterThanOrEqual(5) // Uses index
            .filter('department').equals('eng')  // Post-filter
            .take();
        
        expect(getNames(seniorEngineers)).toEqual(['Alice', 'Charlie', 'Eve']);
    });

    it('should handle an unbounded range query (all items)', async () => {
        const empStore = db.store<Employee>(employeesPath);
        const allEmployees = await empStore.filter('level').greaterThanOrEqual(0).take();
        expect(getNames(allEmployees)).toEqual(['Alice', 'Bob', 'Charlie', 'Diane', 'Eve', 'Frank']);
    });

    it('should return an empty array for a range with no matches', async () => {
        const empStore = db.store<Employee>(employeesPath);
        const impossibleLevel = await empStore.filter('level').greaterThan(10).take();
        expect(impossibleLevel).toHaveLength(0);

        const impossibleSalary = await empStore.filter('salary').lessThan(50000).take();
        expect(impossibleSalary).toHaveLength(0);
    });
});