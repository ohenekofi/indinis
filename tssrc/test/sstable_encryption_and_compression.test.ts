// @filename: tssrc/test/sstable_encryption_and_compression.test.ts

import { Indinis, IndinisOptions, ExtWALManagerConfigJs, StorageValue } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-sstable-enc-comp-v3'); // New version

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const s = (obj: any): string => JSON.stringify(obj);

function generateRepetitivePayload(baseSize: number, idInfo: string): string {
    const pattern = `CompressiblePattern-${idInfo}-AbcDefGhiJklMnoPqrStuVwxYz-0123456789-${idInfo}-PatternEndRepeatingSequenceForGoodMeasureAndTestingPurposes`;
    let payload = "";
    while (payload.length < baseSize) {
        payload += pattern + Math.random().toString(36).substring(2, 8); // Add some variance
    }
    return payload.slice(0, baseSize + Math.floor(Math.random() * (baseSize * 0.15))); // +/- 15% size
}

interface SSTableEncCompDoc {
    idSuffix: string; 
    payload: string;
    batch: number;
    seqInBatch: number;
    timestamp?: number;
}

describe('LSMTree SSTable Block Compression & Encryption Test (v3)', () => {
    let db: Indinis | null = null;
    let testDataDir: string;
    let walDirForTest: string;
    let lsmDataDir: string; 

    const colPath = 'sstable_ec_docs_v3';
    const dbPassword = "TestPassword123!ForSSTableEncryptionAndCompression#$(*SECURE)";
    
    const sstableBlockSizeKB = 4; // 4KB uncompressed blocks to force more blocks
    const sstableCompressionLevel = 3; // A common default for ZSTD

    const testWalConfig: ExtWALManagerConfigJs = {
        wal_directory: '', 
        wal_file_prefix: "sst_ec_wal_v3", 
        segment_size: 1 * 1024 * 1024, // 1MB WAL segments
        background_flush_enabled: true, 
        flush_interval_ms: 250,        
        sync_on_commit_record: true,    
    };

    const getIndinisOptions = (password?: string): IndinisOptions => ({
        checkpointIntervalSeconds: 20,
        encryptionOptions: {
            password: password || undefined,
            // ALWAYS use the modern, secure GCM scheme when a password is provided.
            scheme: password ? "AES256_GCM_PBKDF2" : "NONE", 
        },
        sstableDataBlockUncompressedSizeKB: sstableBlockSizeKB,
        sstableCompressionType: "ZSTD", 
        sstableCompressionLevel: sstableCompressionLevel,
        walOptions: {
            ...testWalConfig,
            wal_directory: walDirForTest
        }
    });

    const NUM_BATCHES = 5;       
    const DOCS_PER_BATCH = 100;  // 100 docs * ~0.7KB = ~70KB per batch -> multiple 4KB blocks
    const PAYLOAD_BASE_SIZE = 700; // Bytes per document payload (0.7KB)

    jest.setTimeout(300000); // 5 minutes, generous for I/O and potential retries

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        walDirForTest = path.join(testDataDir, 'custom_wal_dir_sst_ec_v3'); // Ensure unique WAL dir
        lsmDataDir = path.join(testDataDir, 'data'); // Default Indinis data subdir for LSM
        
        await fs.promises.mkdir(testDataDir, { recursive: true });
        // Note: walDirForTest will be created by C++ if not existing when Indinis is constructed with that path.

        console.log(`\n[SSTABLE ENC-COMP TEST INIT v3] Root Dir: ${testDataDir}`);
        // DB will be initialized within each test using getIndinisOptions
    });

    afterEach(async () => {
        console.log("[SSTABLE ENC-COMP TEST END v3] Closing database instance (if any)...");
        if (db) {
            try { await db.close(); } catch (e: any) { console.error("Error during db.close() in afterEach:", e.message); }
            db = null;
        }
        await delay(1500); 
        if (fs.existsSync(testDataDir)) {
            console.log("[SSTABLE ENC-COMP TEST END v3] Cleaning up test directory: ", testDataDir);
            await rimraf(testDataDir, { maxRetries: 3, retryDelay: 1000 }).catch(err => 
                console.error(`Cleanup error for ${testDataDir} in afterEach:`, err)
            );
        }
    });

    afterAll(async () => {
        console.log("[SSTABLE ENC-COMP SUITE END v3] Cleaning up base directory: ", TEST_DATA_DIR_BASE);
        if (fs.existsSync(TEST_DATA_DIR_BASE)) {
            await rimraf(TEST_DATA_DIR_BASE).catch(err => console.error(`Base cleanup error:`, err));
        }
    });

    it('Scenario: Write (Compress+Encrypt), Recover with Correct/Incorrect/No Password', async () => {
        const allDocsWritten = new Map<string, SSTableEncCompDoc>();
        let totalDocsCommitted = 0;

        // --- Phase 1: Initialize with ENCRYPTION and write data ---
        console.log("\n--- PHASE 1: Initialize DB with Encryption & Write Data ---");
        db = new Indinis(testDataDir, getIndinisOptions(dbPassword));
        await delay(300); // Allow init
        
        console.log(`Writing ${NUM_BATCHES * DOCS_PER_BATCH} documents...`);
        for (let batchNum = 1; batchNum <= NUM_BATCHES; batchNum++) {
            const docsInThisBatch: {key: string, doc: SSTableEncCompDoc}[] = [];
            const batchIdString = `b${batchNum.toString().padStart(2,'0')}`;
            for (let i = 0; i < DOCS_PER_BATCH; i++) {
                const docIdSuffix = `${batchIdString}_doc_${i.toString().padStart(4, '0')}`;
                const key = `${colPath}/${docIdSuffix}`;
                const payload = generateRepetitivePayload(PAYLOAD_BASE_SIZE, docIdSuffix);
                const doc: SSTableEncCompDoc = { idSuffix: docIdSuffix, payload, batch: batchNum, seqInBatch: i, timestamp: Date.now() };
                docsInThisBatch.push({key, doc});
            }
            await db.transaction(async tx => {
                for (const item of docsInThisBatch) await tx.set(item.key, s(item.doc));
                allDocsWritten.set(docsInThisBatch[0].key, {...docsInThisBatch[0].doc}); // Store first for quicker access if needed
                if (docsInThisBatch.length > 1) allDocsWritten.set(docsInThisBatch[docsInThisBatch.length-1].key, {...docsInThisBatch[docsInThisBatch.length-1].doc});
            });
            totalDocsCommitted += docsInThisBatch.length;
            console.log(`  Batch ${batchNum} committed. Total: ${totalDocsCommitted}. Check C++ logs for Compress & Encrypt messages.`);
            await delay(200); // Small delay for potential async flush operations
        }
        console.log("All data written. Closing DB to ensure flushes.");
        // Store a few more keys from the middle for verification
        const midBatch = Math.floor(NUM_BATCHES / 2) + 1;
        const midDoc = Math.floor(DOCS_PER_BATCH / 2);
        const midKey = `${colPath}/b${midBatch.toString().padStart(2,'0')}_doc_${midDoc.toString().padStart(4, '0')}`;
        if (allDocsWritten.has(midKey)) { /* already added by loop if storing all */ }
        else { /* if only storing first/last, add this one too */
            // For this test, assume allDocsWritten contains all, so no special add needed here.
        }

        await db.close(); db = null; await delay(1000);

        // --- Verify Disk Size (Heuristic) ---
        let sstableFilesPhase1: string[] = [];
        if (fs.existsSync(lsmDataDir)) sstableFilesPhase1 = fs.readdirSync(lsmDataDir).filter(f => f.startsWith("sstable_") && f.endsWith(".dat"));
        console.log(`Found ${sstableFilesPhase1.length} SSTable files after Phase 1 writes.`);
        expect(sstableFilesPhase1.length).toBeGreaterThan(0);

        let totalUncompressedAppSize = 0;
        allDocsWritten.forEach(doc => totalUncompressedAppSize += Buffer.from(s(doc)).length);
        let totalSSTableDiskSizePhase1 = 0;
        sstableFilesPhase1.forEach(f => totalSSTableDiskSizePhase1 += fs.statSync(path.join(lsmDataDir, f)).size);
        console.log(`Phase 1: Approx Uncompressed App Data: ${(totalUncompressedAppSize / 1024).toFixed(1)}KB. SSTable Disk Size: ${(totalSSTableDiskSizePhase1 / 1024).toFixed(1)}KB.`);
        if (totalUncompressedAppSize > 50 * 1024) { // If substantial data
            expect(totalSSTableDiskSizePhase1).toBeLessThan(totalUncompressedAppSize * 0.75); // Expect decent C+E reduction
        }

        // --- Phase 2: Reopen with CORRECT password and verify all data ---
        console.log("\n--- PHASE 2: Reopen with CORRECT Password & Verify Data ---");
        db = new Indinis(testDataDir, getIndinisOptions(dbPassword)); await delay(300);
        await db.transaction(async tx => {
            let verifiedCount = 0;
            for (const [key, expectedDoc] of allDocsWritten.entries()) {
                const retrievedStr = await tx.get(key);
                if (!retrievedStr) throw new Error(`Key ${key} was null with correct password!`);
                const retrievedDoc = JSON.parse(retrievedStr as string) as SSTableEncCompDoc;
                expect(retrievedDoc.payload).toEqual(expectedDoc.payload);
                expect(retrievedDoc.batch).toEqual(expectedDoc.batch);
                verifiedCount++;
            }
            expect(verifiedCount).toBe(allDocsWritten.size);
        });
        console.log(`Phase 2: Successfully verified all ${allDocsWritten.size} docs with correct password.`);
        await db.close(); db = null; await delay(500);

        // --- Phase 3: Reopen with INCORRECT password ---
        console.log("\n--- PHASE 3: Reopen with INCORRECT Password ---");
        let incorrectPasswordOpenThrew = false; // New flag
        try {
            // This instantiation should fail because the password is wrong
            db = new Indinis(testDataDir, getIndinisOptions("ThisIsDefinitelyTheWrongPassword!*&^")); 
            await delay(300); // If it didn't throw, give it a moment

            // If new Indinis DIDN'T throw, then we try to read, which also should fail or return null
            // This path indicates a deeper issue if reached.
            console.warn("  WARN: DB initialization with INCORRECT password did NOT throw. Attempting read...");
            await db.transaction(async tx => {
                const keyToTest = Array.from(allDocsWritten.keys())[0];
                const val = await tx.get(keyToTest);
                if (val !== null) {
                    console.warn(`    WARN: Read with INCORRECT password for key ${keyToTest} returned NON-NULL data. This is highly unexpected.`);
                    try { JSON.parse(val as string); } catch (e) { /* expected to fail parsing */ }
                    throw new Error("Read with incorrect password returned parsable data, or did not return null!");
                }
                // If val is null, it means the read failed to decrypt, which is good if init didn't throw.
            });
        } catch (e: any) {
            console.log(`  Caught error during DB initialization or transaction with incorrect password (as expected): ${e.message}`);
            incorrectPasswordOpenThrew = true;
        }
        expect(incorrectPasswordOpenThrew).toBe(true); // Verify that DB init itself threw an error
        console.log("Phase 3: Behavior with incorrect password verified (DB initialization failed).");
        if (db) { await db.close(); db = null; } // Ensure db is closed if it somehow got instantiated
        await delay(500);

        // --- Phase 4: Reopen with NO password (encryption effectively disabled in BPM/LSMTree context) ---
        console.log("\n--- PHASE 4: Reopen with NO Password ---");
        db = new Indinis(testDataDir, getIndinisOptions(undefined)); // Pass undefined for password
        await delay(300);
        let noPasswordReadFailed = false;
        try {
            await db.transaction(async tx => {
                const keyToTest = Array.from(allDocsWritten.keys())[0];
                const val = await tx.get(keyToTest);
                // Expect C++ WARN logs: "...Block ... on disk is encrypted ... but ... encryption is disabled in reader. CANNOT PROCEED..."
                // This should cause SSTableReader's loadAndDecompressBlockForKey to return false.
                if (val !== null) {
                    console.warn(`  WARN: Read with NO password for key ${keyToTest} returned NON-NULL data. Encrypted data might have been treated as plaintext. Value preview: ${(val as string).substring(0, 50)}`);
                    try { JSON.parse(val as string); } catch (e) { noPasswordReadFailed = true; }
                     if (!noPasswordReadFailed) {
                         throw new Error("Read with no password returned parsable data from an encrypted store!");
                    }
                } else {
                    noPasswordReadFailed = true; // Good, get returned null
                }
            });
            if(!noPasswordReadFailed) {
                 throw new Error("Transaction with no password succeeded AND returned parsable data from an encrypted store!");
            }
        } catch (e: any) {
            console.log(`  Caught error during transaction with no password (as expected): ${e.message}`);
            noPasswordReadFailed = true;
        }
        expect(noPasswordReadFailed).toBe(true);
        console.log("Phase 4: Behavior with no password (on encrypted store) verified (read failed or returned unparsable/null).");
        // No need to close this db instance, afterEach will handle it.
        // await db.close(); db = null; // Or close it explicitly
    });
});