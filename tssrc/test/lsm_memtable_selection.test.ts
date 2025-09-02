// tssrc/test/memtable_selection.test.ts

import { Indinis, IndinisOptions } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-memtable-selection');

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const s = (obj: any): string => JSON.stringify(obj);

describe('LSM-Tree MemTable Type Selection', () => {
    let db: Indinis | null = null;
    let testDataDir: string;
    const colPath = 'memtable_test';

    jest.setTimeout(45000);

    const cleanup = async () => {
        if (db) await db.close();
        db = null;
        if (testDataDir && fs.existsSync(testDataDir)) await rimraf(testDataDir);
    };

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });
    
    afterEach(async () => await cleanup());

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    const runBasicCrudTest = async (dbInstance: Indinis, prefixScansShouldWork: boolean) => {
        const key1 = `${colPath}/item1`;
        const val1 = { data: 'value1' };

        // Test set/get
        await dbInstance.transaction(async tx => {
            await tx.set(key1, s(val1));
            const retrieved = await tx.get(key1);
            expect(JSON.parse(retrieved as string)).toEqual(val1);
        });

        // Test persistence and remove
        await dbInstance.transaction(async tx => {
            const retrieved = await tx.get(key1);
            expect(JSON.parse(retrieved as string)).toEqual(val1);
            await tx.remove(key1);
            expect(await tx.get(key1)).toBeNull();
        });

        // Test prefix scan behavior
        const keyP1 = `${colPath}/p/1`;
        const keyP2 = `${colPath}/p/2`;
        await dbInstance.transaction(async tx => {
            await tx.set(keyP1, "prefix_val1");
            await tx.set(keyP2, "prefix_val2");
        });

        if (prefixScansShouldWork) {
            await dbInstance.transaction(async tx => {
                const results = await tx.getPrefix(`${colPath}/p`);
                expect(results.length).toBe(2);
                expect(results.map(r => r.key).sort()).toEqual([keyP1, keyP2]);
            });
        } else {
            // Hash table does not support getPrefix. The transaction should fail.
            await expect(
                dbInstance.transaction(tx => tx.getPrefix(`${colPath}/p`))
            ).rejects.toThrow(/Ordered iteration is not supported/i);
        }
    };

    it('should use SkipList by default and support all operations', async () => {
        const randomSuffix = `default-${Date.now()}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, randomSuffix);
        db = new Indinis(testDataDir); // No options, should default to SkipList
        console.log(`\n[MemTable Test: Default (SkipList)] Using data directory: ${testDataDir}`);
        await runBasicCrudTest(db, true);
    });

    it('should use Vector memtable when specified and support all operations', async () => {
        const randomSuffix = `vector-${Date.now()}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, randomSuffix);
        const options: IndinisOptions = { lsmOptions: { defaultMemTableType: 'Vector' } };
        db = new Indinis(testDataDir, options);
        console.log(`\n[MemTable Test: Vector] Using data directory: ${testDataDir}`);
        await runBasicCrudTest(db, true);
    });

    it('should use PrefixHash memtable when specified and support all operations', async () => {
        const randomSuffix = `prefixhash-${Date.now()}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, randomSuffix);
        const options: IndinisOptions = { lsmOptions: { defaultMemTableType: 'PrefixHash' } };
        db = new Indinis(testDataDir, options);
        console.log(`\n[MemTable Test: PrefixHash] Using data directory: ${testDataDir}`);
        await runBasicCrudTest(db, true);
    });

    it('should use Hash memtable when specified and throw on prefix scan', async () => {
        const randomSuffix = `hash-${Date.now()}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, randomSuffix);
        const options: IndinisOptions = { lsmOptions: { defaultMemTableType: 'Hash' } };
        db = new Indinis(testDataDir, options);
        console.log(`\n[MemTable Test: Hash] Using data directory: ${testDataDir}`);
        await runBasicCrudTest(db, false); // Pass `false` to expect prefix scans to fail
    });

    // --- START OF NEW TEST CASE ---
    it('should use RCU memtable when specified and support all operations', async () => {
        const randomSuffix = `rcu-${Date.now()}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, randomSuffix);
        const options: IndinisOptions = { lsmOptions: { defaultMemTableType: 'RCU' } };
        db = new Indinis(testDataDir, options);
        console.log(`\n[MemTable Test: RCU] Using data directory: ${testDataDir}`);
        // RCU memtable is ordered (based on std::map) and supports iterators, so prefix scans should work.
        await runBasicCrudTest(db, true); 
    });
    // --- END OF NEW TEST CASE ---
});