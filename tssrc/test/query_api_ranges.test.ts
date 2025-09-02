/**
 * @file tssrc/test/query_api_ranges.test.ts
 * @description Test suite for the new Range Query functionality (<, <=, >, >=).
 */

import { Indinis, IndexOptions } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

// Helper for small delay to allow async background tasks to run
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-query-ranges');

// Test Data Interface
interface Employee {
    id?: string;
    name: string;
    level: number;
    department: 'eng' | 'sales' | 'hr';
    salary: number;
}

describe('Indinis Fluent Query API - Range Queries', () => {
    let db: Indinis;
    let testDataDir: string;

    const employeesPath = 'employees';

    jest.setTimeout(45000);

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        db = new Indinis(testDataDir);
        console.log(`\n[RANGE QUERY TEST START] Using data directory: ${testDataDir}`);

        // Create necessary indexes
        await db.createIndex(employeesPath, 'idx_emp_level', { field: 'level' });
        await db.createIndex(employeesPath, 'idx_emp_level_desc', { field: 'level', order: 'desc' });
        await db.createIndex(employeesPath, 'idx_emp_salary', { field: 'salary' });

        // Pre-populate data
        const empStore = db.store<Employee>(employeesPath);
        await db.transaction(async () => { // Use a single transaction for setup
            await empStore.item('emp1').make({ name: 'Alice', level: 5, department: 'eng', salary: 100000 });
            await empStore.item('emp2').make({ name: 'Bob', level: 3, department: 'sales', salary: 80000 });
            await empStore.item('emp3').make({ name: 'Charlie', level: 7, department: 'eng', salary: 140000 });
            await empStore.item('emp4').make({ name: 'Diane', level: 5, department: 'sales', salary: 110000 });
            await empStore.item('emp5').make({ name: 'Eve', level: 8, department: 'eng', salary: 180000 });
        });

        // Wait for index backfill to complete
        await delay(5000);
        console.log("Test setup complete: 5 employees created and indexes are ready.");
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

    it('should handle greaterThan (>) correctly', async () => {
        const empStore = db.store<Employee>(employeesPath);
        const seniorEmployees = await empStore.filter('level').greaterThan(5).take();
        
        expect(getNames(seniorEmployees)).toEqual(['Charlie', 'Eve']);
    });

    it('should handle greaterThanOrEqual (>=) correctly', async () => {
        const empStore = db.store<Employee>(employeesPath);
        const midAndSeniorEmployees = await empStore.filter('level').greaterThanOrEqual(5).take();
        
        expect(getNames(midAndSeniorEmployees)).toEqual(['Alice', 'Charlie', 'Diane', 'Eve']);
    });

    it('should handle lessThan (<) correctly', async () => {
        const empStore = db.store<Employee>(employeesPath);
        const juniorEmployees = await empStore.filter('level').lessThan(5).take();

        expect(getNames(juniorEmployees)).toEqual(['Bob']);
    });

    it('should handle lessThanOrEqual (<=) correctly', async () => {
        const empStore = db.store<Employee>(employeesPath);
        const nonSeniorEmployees = await empStore.filter('level').lessThanOrEqual(5).take();

        expect(getNames(nonSeniorEmployees)).toEqual(['Alice', 'Bob', 'Diane']);
    });

    it('should handle a range query combined with a post-filter', async () => {
        const empStore = db.store<Employee>(employeesPath);

        // Find all employees with level >= 5 who are in the 'eng' department.
        // The index on 'level' will be used, and 'department' will be a post-filter.
        const seniorEngineers = await empStore
            .filter('level').greaterThanOrEqual(5) // Uses index
            .filter('department').equals('eng')  // Post-filter
            .take();
        
        expect(getNames(seniorEngineers)).toEqual(['Alice', 'Charlie', 'Eve']);
    });

    it('should handle range queries on a DESCENDING index', async () => {
        const empStore = db.store<Employee>(employeesPath);

        // When using a DESC index, the operators still mean the same thing logically.
        // Find employees with level < 5 using the descending index.
        const juniorEmployees = await empStore.filter('level').lessThan(5).take();

        // The result set should be identical regardless of index direction.
        expect(getNames(juniorEmployees)).toEqual(['Bob']);
    });

    it('should handle an unbounded range query (all items)', async () => {
        const empStore = db.store<Employee>(employeesPath);
        
        // A filter that should match all documents.
        const allEmployees = await empStore.filter('level').greaterThanOrEqual(0).take();

        expect(getNames(allEmployees)).toEqual(['Alice', 'Bob', 'Charlie', 'Diane', 'Eve']);
    });

    it('should return an empty array for a range with no matches', async () => {
        const empStore = db.store<Employee>(employeesPath);

        const impossibleLevel = await empStore.filter('level').greaterThan(10).take();
        expect(impossibleLevel).toHaveLength(0);

        const impossibleSalary = await empStore.filter('salary').lessThan(50000).take();
        expect(impossibleSalary).toHaveLength(0);
    });

});