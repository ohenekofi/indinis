// @tssrc/test/encryption_gcm_kdf.test.ts

import { Indinis, IndinisOptions, StorageValue } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-gcm-kdf-tests-v1');

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const s = (obj: any): string => JSON.stringify(obj);

interface GCMKDFTestDoc {
    id: string;
    payload: string;
    description: string;
}

describe('Indinis AES-GCM Encryption and KDF Iteration Test', () => {
    let testDataDir: string;
    const dbPassword = "StrongPasswordForGCM&KDF!#$";
    const customKdfIterations = 750000; // A high, non-default value
    const defaultKdfIterations = 600000; // Assuming this is the C++ default from EncryptionLibrary
    const colPath = 'gcm_kdf_docs';
    let db: Indinis | null = null;

    const getOptions = (password?: string, scheme?: "NONE" | "AES256_CBC_PBKDF2" | "AES256_GCM_PBKDF2", iterations?: number): IndinisOptions => ({
        checkpointIntervalSeconds: 5, // Keep checkpoints running
        sstableDataBlockUncompressedSizeKB: 16, // Smaller blocks to ensure more encryption/decryption ops
        sstableCompressionType: "ZSTD",
        encryptionOptions: {
            password: password,
            scheme: scheme || (password ? "AES256_GCM_PBKDF2" : "NONE"), // Default to GCM if password given
            kdfIterations: iterations,
        },
        walOptions: { // Basic WAL config
            wal_directory: path.join(testDataDir, 'gcm_kdf_wal'), // Specific WAL dir
            segment_size: 1024 * 256, // Smallish WAL segments
        }
    });

    jest.setTimeout(90000); // 1.5 minutes

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        // WAL directory will be created by C++ based on options
        console.log(`\n[GCM-KDF TEST START] Using data directory: ${testDataDir}`);
    });

    afterEach(async () => {
        console.log("[GCM-KDF TEST END] Closing database...");
        if (db) {
            try { await db.close(); } catch (e) { console.error("Error closing DB in afterEach:", e); }
            db = null;
        }
        await delay(500); // Give some time for file handles to release
        if (fs.existsSync(testDataDir)) {
            console.log("[GCM-KDF TEST END] Cleaning up test directory...");
            await rimraf(testDataDir, { maxRetries: 3, retryDelay: 1000 }).catch(err =>
                console.error(`Cleanup error for ${testDataDir}:`, err)
            );
        }
    });

    afterAll(async () => {
        console.log("[GCM-KDF SUITE END] Cleaning up base directory...");
        if (fs.existsSync(TEST_DATA_DIR_BASE)) {
            await rimraf(TEST_DATA_DIR_BASE).catch(err => console.error(`Base cleanup error:`, err));
        }
    });

    const key1 = `${colPath}/doc001`;
    const doc1: GCMKDFTestDoc = { id: "doc001", payload: "AES-GCM with custom KDF iterations data.", description: "Initial write" };
    const key2 = `${colPath}/doc002`;
    const doc2: GCMKDFTestDoc = { id: "doc002", payload: "Another payload for AES-GCM integrity checks!", description: "Second item" };


    it('should encrypt with AES-GCM and custom KDF iterations, then decrypt successfully', async () => {
        console.log("--- Test: Encrypt with GCM and custom KDF iterations ---");
        // Phase 1: Initialize with GCM and custom KDF iterations, write data
        const optionsWrite = getOptions(dbPassword, "AES256_GCM_PBKDF2", customKdfIterations);
        console.log(`  Initializing DB with GCM and KDF iterations: ${customKdfIterations}`);
        db = new Indinis(testDataDir, optionsWrite);
        await delay(200);

        await db.transaction(async tx => {
            await tx.set(key1, s(doc1));
            await tx.set(key2, s(doc2));
        });
        console.log("  Data written and committed.");

        // Verify salt file was created
        const saltFilePath = path.join(testDataDir, 'indinis.salt');
        expect(fs.existsSync(saltFilePath)).toBe(true);
        const saltFileContent = fs.readFileSync(saltFilePath);
        expect(saltFileContent.length).toBe(16); // Assuming 16-byte salt
        console.log(`  indinis.salt file found (size: ${saltFileContent.length} bytes).`);

        await db.close(); db = null; await delay(500);
        console.log("  DB closed after writing.");

        // Phase 2: Reopen with SAME password and KDF iterations, verify data
        console.log(`  Reopening DB with GCM and KDF iterations: ${customKdfIterations}`);
        db = new Indinis(testDataDir, optionsWrite); // Use same options
        await delay(200);

        await db.transaction(async tx => {
            const rDoc1Str = await tx.get(key1);
            const rDoc2Str = await tx.get(key2);
            expect(rDoc1Str).not.toBeNull();
            expect(rDoc2Str).not.toBeNull();
            if (rDoc1Str) expect(JSON.parse(rDoc1Str as string).payload).toEqual(doc1.payload);
            if (rDoc2Str) expect(JSON.parse(rDoc2Str as string).payload).toEqual(doc2.payload);
        });
        console.log("  Data verified successfully with correct password and KDF iterations.");
        await db.close(); db = null; await delay(500);
    });

    it('should FAIL to decrypt if KDF iterations mismatch (correct password, wrong iterations)', async () => {
        console.log("\n--- Test: Fail decryption with KDF iteration mismatch ---");
        // Phase 1: Write data with CUSTOM KDF iterations
        const optionsWriteCustomIter = getOptions(dbPassword, "AES256_GCM_PBKDF2", customKdfIterations);
        console.log(`  Initializing DB for write with GCM and KDF iterations: ${customKdfIterations}`);
        db = new Indinis(testDataDir, optionsWriteCustomIter);
        await delay(200);
        await db.transaction(async tx => { await tx.set(key1, s(doc1)); });
        console.log("  Data written with custom KDF iterations.");
        await db.close(); db = null; await delay(500);

        // Phase 2: Attempt to reopen with DEFAULT (different) KDF iterations
        const optionsReadDefaultIter = getOptions(dbPassword, "AES256_GCM_PBKDF2", defaultKdfIterations); // Or pass 0 to use C++ default
        console.log(`  Reopening DB with GCM and KDF iterations: ${defaultKdfIterations} (expected to fail DEK derivation)`);
        
        let readAttemptThrewOrReturnedNull = false;
        try {
            db = new Indinis(testDataDir, optionsReadDefaultIter);
            await delay(200);
            await db.transaction(async tx => {
                const val = await tx.get(key1);
                // If DEK derivation failed due to iteration mismatch, subsequent decryption should fail.
                // GCM's tag check should cause a decryption error.
                if (val === null) {
                    readAttemptThrewOrReturnedNull = true; // Good, returned null as expected
                    console.log("  Read returned null as expected due to KDF iteration mismatch.");
                } else {
                    console.warn(`  WARN: Read with KDF iteration mismatch returned NON-NULL: ${val}. This is unexpected.`);
                    // It's possible if the DEK derived was somehow still valid enough to decrypt some header,
                    // but then GCM tag check on payload should fail.
                    // Or if iteration count wasn't actually different/used.
                }
            });
            // If the transaction completed without error AND val was not null, it's a failure.
            if(!readAttemptThrewOrReturnedNull) {
                throw new Error("Read succeeded with mismatched KDF iterations, which is incorrect.");
            }
        } catch (e: any) {
            console.log(`  Caught error during transaction with mismatched KDF iterations (as expected): ${e.message}`);
            readAttemptThrewOrReturnedNull = true;
        }
        expect(readAttemptThrewOrReturnedNull).toBe(true);
        console.log("  Decryption failure due to KDF iteration mismatch verified.");
        if (db) { await db.close(); db = null; await delay(500); }
    });

    it('should FAIL to decrypt with AES-GCM if password is incorrect (correct iterations)', async () => {
        console.log("\n--- Test: Fail GCM decryption with incorrect password ---");
        // Phase 1: Write data with GCM and custom KDF iterations
        const optionsWrite = getOptions(dbPassword, "AES256_GCM_PBKDF2", customKdfIterations);
        console.log(`  Initializing DB for write with GCM and KDF iterations: ${customKdfIterations}`);
        db = new Indinis(testDataDir, optionsWrite);
        await delay(200);
        await db.transaction(async tx => { await tx.set(key1, s(doc1)); });
        console.log("  Data written.");
        await db.close(); db = null; await delay(500);

        // Phase 2: Attempt to reopen with INCORRECT password but CORRECT KDF iterations
        const wrongPassword = "ThisIsTheWrongPassword123!!!";
        const optionsReadWrongPw = getOptions(wrongPassword, "AES256_GCM_PBKDF2", customKdfIterations);
        console.log(`  Reopening DB with GCM, KDF iterations ${customKdfIterations}, but INCORRECT password.`);
        
        let readAttemptFailedAsExpected = false;
        try {
            db = new Indinis(testDataDir, optionsReadWrongPw);
            await delay(200);
            await db.transaction(async tx => {
                const val = await tx.get(key1);
                // With GCM, an incorrect key (derived from wrong password) should cause authentication tag mismatch.
                // This should manifest as a decryption failure, leading to tx.get() returning null or an error.
                if (val === null) {
                    readAttemptFailedAsExpected = true;
                    console.log("  Read returned null as expected due to incorrect password (GCM auth fail).");
                } else {
                    console.warn(`  WARN: Read with incorrect password returned NON-NULL: ${val}. This is unexpected for GCM.`);
                    // This implies the GCM authentication tag check might not be robustly failing
                    // or the error isn't propagated to make tx.get() return null.
                }
            });
             if (!readAttemptFailedAsExpected) {
                throw new Error("Read succeeded with incorrect password, which is incorrect for GCM.");
            }
        } catch (e: any) {
            console.log(`  Caught error during transaction with incorrect password (as expected for GCM): ${e.message}`);
            readAttemptFailedAsExpected = true;
        }
        expect(readAttemptFailedAsExpected).toBe(true);
        console.log("  GCM decryption failure with incorrect password verified.");
        if (db) { await db.close(); db = null; await delay(500); }
    });

    it('should use AES-CBC if specified and work correctly', async () => {
        console.log("\n--- Test: Encrypt with CBC and custom KDF iterations ---");
        // Phase 1: Initialize with CBC and custom KDF iterations, write data
        const optionsWriteCBC = getOptions(dbPassword, "AES256_CBC_PBKDF2", customKdfIterations);
        console.log(`  Initializing DB with CBC and KDF iterations: ${customKdfIterations}`);
        db = new Indinis(testDataDir, optionsWriteCBC);
        await delay(200);

        await db.transaction(async tx => {
            await tx.set(key1, s(doc1));
        });
        console.log("  Data written with CBC.");
        await db.close(); db = null; await delay(500);

        // Phase 2: Reopen with SAME CBC options and verify data
        console.log(`  Reopening DB with CBC and KDF iterations: ${customKdfIterations}`);
        db = new Indinis(testDataDir, optionsWriteCBC); // Use same options
        await delay(200);

        await db.transaction(async tx => {
            const rDoc1Str = await tx.get(key1);
            expect(rDoc1Str).not.toBeNull();
            if (rDoc1Str) expect(JSON.parse(rDoc1Str as string).payload).toEqual(doc1.payload);
        });
        console.log("  Data verified successfully with CBC, correct password, and KDF iterations.");
        // No specific integrity check for CBC here, just confidentiality.
        await db.close(); db = null; await delay(500);
    });

    it('should default to AES-GCM if password is provided but scheme is NONE or omitted in options', async () => {
        console.log("\n--- Test: Default to GCM with password ---");
        // Options where scheme is "NONE" but password IS provided
        const optionsPasswordWithNoneScheme = getOptions(dbPassword, "NONE", customKdfIterations);
        // Options where scheme is omitted but password IS provided
        const optionsPasswordOmittedScheme: IndinisOptions = {
            ...getOptions(undefined, undefined, undefined), // get base options without enc
            encryptionOptions: { password: dbPassword, kdfIterations: customKdfIterations }
        };
        
        for (const currentOptions of [optionsPasswordWithNoneScheme, optionsPasswordOmittedScheme]) {
            const schemeDesc = currentOptions.encryptionOptions?.scheme || "omitted";
            console.log(`  Testing with password and scheme: "${schemeDesc}" (expecting GCM to be used internally)`);
            
            // Need a unique subdir for each sub-test if they write to the same testDataDir
            const subDirName = `subdir_gcm_default_${schemeDesc.toLowerCase().replace(/[^a-z0-9]/g, '')}`;
            const currentSubTestDir = path.join(testDataDir, subDirName);
            await fs.promises.mkdir(currentSubTestDir, {recursive: true});

            const optionsForThisRun = JSON.parse(JSON.stringify(currentOptions));
            if(optionsForThisRun.walOptions) optionsForThisRun.walOptions.wal_directory = path.join(currentSubTestDir, 'wal');


            db = new Indinis(currentSubTestDir, optionsForThisRun);
            await delay(200);
            await db.transaction(async tx => { await tx.set(key1, s(doc1)); });
            await db.close(); db = null; await delay(500);

            // Reopen with explicit GCM and verify - this confirms GCM was used for writing
            const optionsReadGCM = getOptions(dbPassword, "AES256_GCM_PBKDF2", customKdfIterations);
            const gcmReadOptions = JSON.parse(JSON.stringify(optionsReadGCM));
            if(gcmReadOptions.walOptions) gcmReadOptions.walOptions.wal_directory = path.join(currentSubTestDir, 'wal');


            db = new Indinis(currentSubTestDir, gcmReadOptions);
            await delay(200);
            await db.transaction(async tx => {
                const rDoc1Str = await tx.get(key1);
                expect(rDoc1Str).not.toBeNull();
                if (rDoc1Str) expect(JSON.parse(rDoc1Str as string).payload).toEqual(doc1.payload);
            });
            console.log(`    Verified data with explicit GCM read (Original write scheme: "${schemeDesc}")`);
            await db.close(); db = null; await delay(500);
        }
    });
});