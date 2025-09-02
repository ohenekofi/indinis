// @tssrc/test/encryption_password_change.test.ts

import { Indinis, IndinisOptions, StorageValue } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-pw-change-tests-v1');

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const s = (obj: any): string => JSON.stringify(obj);

interface PwChangeTestDoc { id: string; data: string; }

describe('Indinis Password Change and KDF Iteration Persistence Test', () => {
    let testDataDir: string;
    const initialPassword = "InitialPassword123!";
    const newPassword = "NewStrongerPassword456$";
    const initialKdfIterations = 700000; // Custom high for initial
    const newKdfIterations = 800000;     // Custom high for new
    const defaultKdfIterations = 600000; // Assumed C++ default

    const colPath = 'pw_change_docs';
    let db: Indinis | null = null;

    const getOptions = (password?: string, kdfIters?: number): IndinisOptions => ({
        checkpointIntervalSeconds: 5,
        sstableDataBlockUncompressedSizeKB: 8, // Small blocks
        encryptionOptions: {
            password: password,
            scheme: password ? "AES256_GCM_PBKDF2" : "NONE",
            kdfIterations: kdfIters,
        },
        walOptions: { wal_directory: path.join(testDataDir, 'wal_pw_change') }
    });

    jest.setTimeout(60000); // 1 minute

    beforeAll(async () => { await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true }); });
    afterAll(async () => { if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE); });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        // WAL dir created by Indinis constructor
        console.log(`\n[PW-CHANGE TEST START] Using data directory: ${testDataDir}`);
    });

    afterEach(async () => {
        if (db) { try { await db.close(); } catch (e) {} db = null; }
        await delay(200);
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir, { maxRetries: 2, retryDelay: 500 });
    });

    const key1 = `${colPath}/doc1`;
    const doc1: PwChangeTestDoc = { id: "doc1", data: "Data under initial password" };

    it('should allow password change and persist KDF iterations', async () => {
        // 1. Create DB with initial password and KDF iterations
        console.log(`--- Creating DB with initial password and KDF iterations: ${initialKdfIterations} ---`);
        db = new Indinis(testDataDir, getOptions(initialPassword, initialKdfIterations));
        await delay(100);
        await db.transaction(async tx => { await tx.set(key1, s(doc1)); });
        console.log("  Initial data written.");
        await db.close(); db = null; await delay(200);

        // 2. Reopen with initial password and KDF iterations - verify data
        console.log("--- Reopening with initial password and KDF iterations ---");
        db = new Indinis(testDataDir, getOptions(initialPassword, initialKdfIterations));
        await delay(100);
        await db.transaction(async tx => {
            const r = await tx.get(key1);
            expect(r).not.toBeNull();
            expect(JSON.parse(r as string).data).toEqual(doc1.data);
        });
        console.log("  Data verified with initial credentials.");

        // 3. Change password
        console.log(`--- Changing password to new password with KDF iterations: ${newKdfIterations} ---`);
        const changeSuccess = await db.changeDatabasePassword(initialPassword, newPassword, newKdfIterations);
        expect(changeSuccess).toBe(true);
        console.log("  Password change successful.");
        await db.close(); db = null; await delay(200);

        // 4. Attempt to open with OLD password (should fail or not decrypt DEK correctly)
        console.log("--- Attempting to open with OLD password (expect failure) ---");
        let oldPassOpenThrew = false;
        try {
            db = new Indinis(testDataDir, getOptions(initialPassword, initialKdfIterations)); // Old creds
            await delay(100);
            // Accessing data should fail if KEK validation fails due to wrong KEK
            await db.transaction(async tx => { await tx.get(key1); }); // This might throw from C++ or return null
        } catch (e: any) {
            console.log(`  Caught expected error opening with old password: ${e.message}`);
            oldPassOpenThrew = true;
        }
        // More robust check: initializeEncryptionState should throw from C++ if password invalid
        expect(oldPassOpenThrew || (db === null || (await db.transaction(async tx=>tx.get(key1))) === null )).toBe(true);
        if (db) { await db.close(); db = null; }
        await delay(200);


        // 5. Open with NEW password and NEW KDF iterations - verify data
        console.log(`--- Opening with NEW password and NEW KDF iterations: ${newKdfIterations} ---`);
        db = new Indinis(testDataDir, getOptions(newPassword, newKdfIterations));
        await delay(100);
        await db.transaction(async tx => {
            const r = await tx.get(key1);
            expect(r).not.toBeNull();
            expect(JSON.parse(r as string).data).toEqual(doc1.data);
        });
        console.log("  Data verified with new credentials.");

        // 6. Write NEW data with new password setup
        const key2 = `${colPath}/doc2_new_pass`;
        const doc2: PwChangeTestDoc = { id: "doc2", data: "Data under new password" };
        await db.transaction(async tx => { await tx.set(key2, s(doc2)); });
        console.log("  New data written under new password setup.");
        await db.close(); db = null; await delay(200);

        // 7. Open with NEW password but OMIT KDF iterations (should use STORED newKdfIterations)
        console.log("--- Opening with NEW password and OMITTED KDF iterations (should use stored) ---");
        db = new Indinis(testDataDir, getOptions(newPassword, undefined)); // Undefined iterations
        await delay(100);
        await db.transaction(async tx => {
            const r1 = await tx.get(key1);
            expect(r1).not.toBeNull();
            expect(JSON.parse(r1 as string).data).toEqual(doc1.data);

            const r2 = await tx.get(key2);
            expect(r2).not.toBeNull();
            expect(JSON.parse(r2 as string).data).toEqual(doc2.data);
        });
        console.log("  Data verified with new password and implicitly used stored KDF iterations.");
        await db.close(); db = null;
    });
});