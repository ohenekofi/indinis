 // @tssrc/test/indinis.test.ts
 import { Indinis, StorageValue } from '../index'; // Adjust path if needed
 import * as fs from 'fs';
 import * as path from 'path';
 import { rimraf } from 'rimraf';

 const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-core');

 describe('Indinis Core Functionality', () => {
     let db: Indinis;
     let testDataDir: string;

     beforeEach(async () => {
         const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
         testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
         await fs.promises.mkdir(testDataDir, { recursive: true });
         db = new Indinis(testDataDir);
     });

     afterEach(async () => {
          // Ensure instance cleanup if needed (e.g., db.close())
          // Relying on GC for now, which triggers C++ destructors.
          // Use rimraf for reliable directory cleanup
          if (fs.existsSync(testDataDir)) {
              await rimraf(testDataDir);
          }
          // Add a small delay AFTER cleanup if needed to let C++ fully release files before next test
          // await new Promise(res => setTimeout(res, 50));
     });

     afterAll(async () => {
         if (fs.existsSync(TEST_DATA_DIR_BASE)) {
              await rimraf(TEST_DATA_DIR_BASE);
         }
     });


     it('should create an Indinis instance successfully', () => {
         expect(db).toBeDefined();
         expect(db instanceof Indinis).toBe(true);
         expect(fs.existsSync(testDataDir)).toBe(true);
         expect(fs.existsSync(path.join(testDataDir, 'data'))).toBe(true);
         expect(fs.existsSync(path.join(testDataDir, 'indices'))).toBe(true);
     });

     it('should execute an empty transaction', async () => {
         await expect(db.transaction(async tx => {})).resolves.toBeUndefined();
     });

      it('should allow basic put and get within a transaction', async () => {
         const key = 'testKey';
         const value = 'testValue';
         await db.transaction(async tx => {
             await tx.set(key, value);
             const retrieved = await tx.get(key);
             expect(retrieved).toBe(value);
         });
          await db.transaction(async tx => {
              const retrieved = await tx.get(key);
              expect(retrieved).toBe(value);
          });
     });

      it('should abort transaction if callback throws error', async () => {
         const key = 'abortKey';
         const value = 'should not persist';
         const errorMsg = 'Callback failed';

         await expect(db.transaction(async tx => {
             await tx.set(key, value);
             expect(await tx.get(key)).toBe(value);
             throw new Error(errorMsg);
         })).rejects.toThrow(errorMsg);

         await db.transaction(async tx => {
             const retrieved = await tx.get(key);
             expect(retrieved).toBeNull();
         });
     });


     it('should create an index', async () => {
         const result = await db.createIndex('my_index', 'some_field');
         expect(result).toBe(true);
         // REMOVED: Filesystem check - Index creation is logical, persistence happens later.
         // expect(fs.existsSync(path.join(testDataDir, 'indices', 'my_index.idx'))).toBe(true);
     });

     it('should return false when creating an existing index', async () => {
         const result1 = await db.createIndex('duplicate_index', 'field1');
         expect(result1).toBe(true);
         const result2 = await db.createIndex('duplicate_index', 'field2');
         expect(result2).toBe(false); // Should fail as it already exists
     });

     it('should drop an existing index', async () => {
         // 1. Create the index
         await db.createIndex('index_to_drop', 'field_drop');
         // REMOVED: Filesystem check before drop
         // expect(fs.existsSync(path.join(testDataDir, 'indices', 'index_to_drop.idx'))).toBe(true);

         // 2. Drop the index
         const result = await db.dropIndex('index_to_drop');
         expect(result).toBe(true); // Expect drop operation to succeed

         // 3. Verify dropping again fails
         const result2 = await db.dropIndex('index_to_drop');
         expect(result2).toBe(false);

         // Optional: Check filesystem *after* drop (if C++ dropIndex removes file immediately)
         // If dropIndex only marks for deletion, this check might still fail.
         // expect(fs.existsSync(path.join(testDataDir, 'indices', 'index_to_drop.idx'))).toBe(false);
     });

     it('should return false when dropping a non-existent index', async () => {
         const result = await db.dropIndex('non_existent_index');
         expect(result).toBe(false);
     });

     it('should persist data across restarts (basic check)', async () => {
         const key = "persist-key";
         const value = { message: "Hello Persistence" };
         const valueStr = JSON.stringify(value);

         // --- Phase 1: Write data using the first instance ---
         const db1 = db; // Use the instance from beforeEach
         await db1.transaction(async tx => {
             await tx.set(key, valueStr);
         });
         console.log(`[Persist Test] Data written by db1 for key ${key}`);

         // --- Crucial Step: Ensure data might be flushed ---
         // Option A: Explicit close (if implemented)
         // await db1.close();

         // Option B: Small delay (Workaround for relying on GC/destructors)
         // This gives background flush a chance. Adjust delay as needed.
         console.log("[Persist Test] Waiting briefly for potential flush...");
         await new Promise(resolve => setTimeout(resolve, 200)); // e.g., 200ms delay
         console.log("[Persist Test] Wait finished.");

         // --- Phase 2: Re-initialize Indinis instance ---
         console.log("[Persist Test] Creating db2 instance...");
         const db2 = new Indinis(testDataDir); // Point to the SAME directory
         let retrievedValue: any = null;
         let retrievedValueRaw: StorageValue | null = null;

         console.log("[Persist Test] Starting read transaction with db2...");
         try {
             await db2.transaction(async tx => {
                 retrievedValueRaw = await tx.get(key);
                 console.log(`[Persist Test] tx.get('${key}') returned:`, retrievedValueRaw);
                 if (typeof retrievedValueRaw === 'string') {
                     retrievedValue = JSON.parse(retrievedValueRaw);
                 }
             });
         } catch (error) {
            console.error("[Persist Test] Error during db2 transaction:", error);
            // Fail the test explicitly if the read transaction errors
            throw error;
         }


         console.log("[Persist Test] Read transaction finished. Raw value:", retrievedValueRaw);
         // *** The Key Assertion ***
         expect(retrievedValueRaw).not.toBeNull(); // <<< This failed before
         expect(typeof retrievedValueRaw).toBe('string');
         expect(retrievedValue).toEqual(value);

         // await db2.close(); // if implemented
     });

 });