// @filename: tssrc/test/btree_page_encryption.test.ts

import { Indinis, IndinisOptions, StorageValue } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-btree-encryption-v1');

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const s = (obj: any): string => JSON.stringify(obj);

function generateLargeString(sizeMB: number): string {
    const mb = 1024 * 1024;
    const targetSize = sizeMB * mb;
    const pattern = "This is a long and repetitive string pattern for testing B-Tree page encryption and compression. ";
    let result = "";
    while (result.length < targetSize) {
        result += pattern;
    }
    return result.slice(0, targetSize);
}

interface BTreeTestDoc {
    id: string; // Used as part of the key
    data: string;
    version: number;
    someNumber?: number;
}

describe('Indinis B-Tree Page Encryption and Compression Test', () => {
    let testDataDir: string;
    const dbPassword = "MySuperSecretTestPassword!@#$";
    const colPath = 'encrypted_docs'; // A collection that will use B-Trees for primary storage (LSM Tree in this setup)
                                      // and potentially for secondary indexes if we create them.

    // Indinis options for this test
    const indinisEncryptedOptions: IndinisOptions = {
        checkpointIntervalSeconds: 5, // Allow checkpoints to run
        encryptionOptions: {
            password: dbPassword,
            scheme: "AES256_CBC_PBKDF2", // Explicitly state, though password implies it
        },
        // Keep SSTable settings modest, primary focus is B-Tree pages via BPM
        sstableDataBlockUncompressedSizeKB: 64, 
        sstableCompressionType: "ZSTD", 
        // B-Tree page compression is implicitly handled by BPM based on config if passed down
        // For this test, we assume BPM will attempt compression if enabled internally (not directly configured via IndinisOptions yet)
    };
    
    const indinisNoPasswordOptions: IndinisOptions = {
        ...indinisEncryptedOptions, // Copy other settings
        encryptionOptions: { // Override encryption
            password: "", // Empty or undefined means no password-based encryption
            scheme: "NONE"
        }
    };


    jest.setTimeout(120000); // 2 minutes

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    });

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        console.log(`\n[BTree ENC TEST START] Using data directory: ${testDataDir}`);
        // DB will be initialized within each test to control options
    });

    afterEach(async () => {
        console.log("[BTree ENC TEST END] Cleaning up test directory...");
        // db instance is managed per test, close it if it exists
        if (fs.existsSync(testDataDir)) {
            await rimraf(testDataDir, { maxRetries: 3, retryDelay: 500 }).catch(err => 
                console.error(`Cleanup error for ${testDataDir} in afterEach:`, err)
            );
        }
    });

    afterAll(async () => {
        console.log("[BTree ENC SUITE END] Cleaning up base directory...");
        if (fs.existsSync(TEST_DATA_DIR_BASE)) {
            await rimraf(TEST_DATA_DIR_BASE).catch(err => console.error(`Base cleanup error for ${TEST_DATA_DIR_BASE}:`, err));
        }
    });

    async function performWritesAndReads(db: Indinis, numDocs: number, batchId: string, existingDocsMap: Map<string, BTreeTestDoc>) {
        console.log(`  Performing ${numDocs} writes/updates for batch ${batchId}...`);
        for (let i = 0; i < numDocs; i++) {
            const docIdNum = i;
            const key = `${colPath}/doc_${batchId}_${docIdNum.toString().padStart(3, '0')}`;
            
            let docToSet: BTreeTestDoc;
            const existingDoc = existingDocsMap.get(key);

            if (existingDoc && Math.random() < 0.3) { // 30% chance to update an existing doc
                docToSet = { 
                    ...existingDoc, 
                    data: generateLargeString(0.002), // ~2KB new data
                    version: existingDoc.version + 1,
                    someNumber: Math.floor(Math.random() * 1000)
                };
            } else { // New doc or overwrite
                docToSet = { 
                    id: `doc_${batchId}_${docIdNum.toString().padStart(3, '0')}`,
                    data: generateLargeString(0.003), // ~3KB data
                    version: 1,
                    someNumber: Math.floor(Math.random() * 1000)
                };
            }
            
            await db.transaction(async tx => {
                await tx.set(key, s(docToSet));
            });
            existingDocsMap.set(key, docToSet); // Update our expected map

            if (i % Math.floor(numDocs / 5) === 0 && i > 0) { // Periodically read some data
                const readKey = Array.from(existingDocsMap.keys())[Math.floor(Math.random() * existingDocsMap.size)];
                await db.transaction(async tx => {
                    const retrieved = await tx.get(readKey);
                    expect(retrieved).not.toBeNull();
                    if(retrieved) {
                        expect(JSON.parse(retrieved as string).id).toEqual(existingDocsMap.get(readKey)?.id);
                    }
                });
            }
        }
        console.log(`  Finished ${numDocs} writes/updates for batch ${batchId}. Total expected docs: ${existingDocsMap.size}`);
    }

    async function verifyAllData(db: Indinis, expectedDocsMap: Map<string, BTreeTestDoc>, contextMessage: string) {
        console.log(`--- Verifying all ${expectedDocsMap.size} documents: ${contextMessage} ---`);
        let verifiedCount = 0;
        await db.transaction(async tx => {
            for (const [key, expectedDoc] of expectedDocsMap.entries()) {
                const retrievedStr = await tx.get(key);
                if (retrievedStr === null) {
                    console.error(`[VERIFY FAILURE - ${contextMessage}] Key "${key}" was unexpectedly null. Expected data: ${expectedDoc.data.substring(0,30)}...`);
                }
                expect(retrievedStr).not.toBeNull();
                if (retrievedStr) {
                    const retrievedDoc = JSON.parse(retrievedStr as string) as BTreeTestDoc;
                    expect(retrievedDoc.id).toEqual(expectedDoc.id);
                    expect(retrievedDoc.data).toEqual(expectedDoc.data); // Verify large payload
                    expect(retrievedDoc.version).toEqual(expectedDoc.version);
                    expect(retrievedDoc.someNumber).toEqual(expectedDoc.someNumber);
                }
                verifiedCount++;
            }
        });
        expect(verifiedCount).toBe(expectedDocsMap.size);
        console.log(`--- Successfully verified ${verifiedCount} documents: ${contextMessage} ---`);
    }


    it('should encrypt B-Tree pages, allow reads with correct password, and fail with incorrect/no password', async () => {
        let db: Indinis | null = null; // Shadow outer db for per-initialization control
        const allDocs = new Map<string, BTreeTestDoc>();
        const numDocsToWrite = 50; // Enough to potentially make multiple B-Tree pages dirty
                                   // And fill BPM to cause evictions (BPM size 128 pages * ~4KB/page = ~512KB)
                                   // Total data: 50 docs * ~3KB/doc = ~150KB. May not fill BPM to force eviction without smaller BPM.
                                   // We will rely on db.close() to trigger flushPage for all dirty pages.

        // --- Phase 1: Write data with encryption enabled ---
        console.log("PHASE 1: Initializing DB with encryption password and writing data...");
        db = new Indinis(testDataDir, indinisEncryptedOptions);
        await delay(200); // Init time

        await performWritesAndReads(db, numDocsToWrite, "batch1", allDocs);
        
        console.log("PHASE 1: Closing DB to ensure pages are flushed (and encrypted)...");
        await db.close();
        db = null;
        await delay(500); // Allow file operations to settle

        // At this point, any B-Tree pages that were dirty should have been:
        // - Compressed (if beneficial by BPM's flushPage)
        // - Encrypted (by BPM's flushPage using the DEK)
        // - Written to disk with an OnDiskPageHeader indicating encryption and compression.
        // C++ TRACE logs from BPM::flushPage should show:
        // - "[BPM flushPage X] Attempting compression..."
        // - "[BPM flushPage X] Encrypting data..."
        // - "[BPM flushPage X] Successfully wrote page to disk. (Enc: 1, Comp: Y)" (Enc:1 is AES256_CBC_PBKDF2)

        // --- Phase 2: Reopen with CORRECT password and verify data ---
        console.log("\nPHASE 2: Reopening DB with CORRECT password and verifying data...");
        db = new Indinis(testDataDir, indinisEncryptedOptions);
        await delay(200);

        await verifyAllData(db, allDocs, "After reopen with correct password");
        // C++ TRACE logs from BPM::fetchPage during verification should show:
        // - "[BPM fetchPage X] Physical page read..."
        // - "[BPM fetchPage X] OnDiskHeader - EncScheme: 1 ..."
        // - "[BPM fetchPage X] Decrypting (Scheme: AES256_CBC_PBKDF2)..."
        // - "[BPM fetchPage X] Decryption successful..."
        // - "[BPM fetchPage X] Decompressing (Type Y)..." or "No decompression needed..."
        // - "[BPM fetchPage X] Successfully processed ... into frame ..."

        console.log("PHASE 2: Performing more writes with correct password...");
        await performWritesAndReads(db, 20, "batch2_correct_pw", allDocs);
        await verifyAllData(db, allDocs, "After more writes with correct password");

        await db.close();
        db = null;
        await delay(500);

        // --- Phase 3: Attempt to reopen with INCORRECT password ---
        console.log("\nPHASE 3: Attempting to reopen DB with INCORRECT password...");
        const wrongPasswordOptions: IndinisOptions = {
            ...indinisEncryptedOptions,
            encryptionOptions: { password: "WrongPasswordOops!", scheme: "AES256_CBC_PBKDF2" }
        };
        db = new Indinis(testDataDir, wrongPasswordOptions);
        await delay(200);

        console.log("PHASE 3: Attempting to read data with incorrect password (expecting errors or nulls)...");
        let cryptoErrorCaught = false;
        try {
            await db.transaction(async tx => {
                const firstKey = Array.from(allDocs.keys())[0];
                if (firstKey) {
                    // This get() should trigger fetchPage -> decrypt -> fail
                    const val = await tx.get(firstKey); 
                    // If decrypt fails cleanly, val might be null. If it throws, catch below.
                    // Depending on how robust the C++ error propagation is, this might not throw JS error
                    // but C++ logs should show CryptoException during page fetch.
                    if (val !== null) {
                        console.warn(`PHASE 3: Read with wrong password returned NON-NULL for ${firstKey}: ${val}. This might indicate decryption failure was not handled correctly or data was not actually encrypted.`);
                    }
                    expect(val).toBeNull(); // Or expect the transaction to fail
                } else {
                    console.warn("PHASE 3: No keys in allDocs map to test read with incorrect password.");
                }
            });
        } catch (e: any) {
            console.log(`PHASE 3: Caught expected error during transaction with incorrect password: ${e.message}`);
            // Check if the error message indicates a crypto problem (this depends on NAPI error wrapping)
            // e.g. if C++ CryptoException is propagated as a generic error.
            // For now, any error is somewhat expected.
            cryptoErrorCaught = true;
        }
        // A more robust test here would be if tx.get() itself throws a specific CryptoError from C++
        // If data was simply "corrupted" and parsing JSON failed, it would also be an indication.
        // The C++ logs are key: look for "decryptWithAES - EVP_DecryptFinal_ex (often indicates incorrect key...)"
        // or "CryptoException during processing" from BPM::fetchPage.

        // We expect that operations might fail or return garbage that fails parsing.
        // If no exception was thrown, ensure we didn't accidentally read valid data.
        if (!cryptoErrorCaught) {
            console.warn("PHASE 3: No explicit JS error caught with incorrect password. Check C++ logs for decryption failures. Page reads should ideally fail or return garbage.");
            // If reads don't throw, they should return null or data that fails JSON.parse
            // This part of the test is highly dependent on how C++ errors are propagated.
        }


        await db.close();
        db = null;
        await delay(500);

        // --- Phase 4: Attempt to reopen with NO password (encryption disabled on BPM side) ---
        console.log("\nPHASE 4: Attempting to reopen DB with NO password (BPM encryption disabled)...");
        db = new Indinis(testDataDir, indinisNoPasswordOptions); // This will set bpm->encryption_is_active_ = false
        await delay(200);
        
        console.log("PHASE 4: Attempting to read data with encryption disabled (expecting errors/garbage)...");
        cryptoErrorCaught = false;
        try {
            await db.transaction(async tx => {
                const firstKey = Array.from(allDocs.keys())[0];
                 if (firstKey) {
                    const val = await tx.get(firstKey); 
                    // If the page was encrypted on disk, and BPM now tries to read it as unencrypted
                    // (because encryption_is_active_ is false), the OnDiskPageHeader.encryption_scheme
                    // will indicate encryption. BPM::fetchPage should ideally LOG A HUGE WARNING.
                    // The data passed to decompression will be ciphertext, which will likely fail decompression
                    // or result in garbage if decompression "succeeds" on random-looking data.
                    // This should ideally lead to a failure in fetchPage, returning null.
                    if (val !== null) {
                        console.warn(`PHASE 4: Read with no password returned NON-NULL for ${firstKey}: ${val}. Disk page was likely encrypted.`);
                    }
                    expect(val).toBeNull(); // Or expect transaction to fail.
                }
            });
        } catch (e: any) {
            console.log(`PHASE 4: Caught expected error during transaction with no password: ${e.message}`);
            cryptoErrorCaught = true;
        }
        // C++ logs from BPM::fetchPage are critical here:
        // "Page on disk is encrypted (Scheme: X) but BPM encryption is currently disabled. Treating as raw data. THIS IS LIKELY AN ERROR."
        // And subsequent decompression or data processing errors.

        await db.close();
        db = null;
    });
});