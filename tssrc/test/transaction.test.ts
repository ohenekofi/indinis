import { Indinis, StorageValue, ITransactionContext, KeyValueRecord } from '../index'; // Add ITransactionContext here
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-transaction'); // Changed name

describe('Indinis Transaction Operations and MVCC', () => {
    let db: Indinis;
    let testDataDir: string;

    jest.setTimeout(15000); // 15 seconds

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        await fs.promises.mkdir(testDataDir, { recursive: true });
        db = new Indinis(testDataDir);
    });

    afterEach(async () => {
        if (db) { // Ensure db exists before trying to close
            await db.close(); // Add an explicit close method call
        }
        if (fs.existsSync(testDataDir)) {
            try {
                console.log(`[Cleanup] Attempting to remove ${testDataDir}`);
                await rimraf(testDataDir); // Add options: { maxRetries: 3, retryDelay: 500 } if needed
                console.log(`[Cleanup] Successfully removed ${testDataDir}`);
            } catch (err) {
                console.error(`[Cleanup] Error removing ${testDataDir}:`, err);
                // Decide whether to re-throw or just log
                // throw err; // Re-throwing might hide the original test failure location
            }
        }
    }, 30000); // Timeout specifically for this hook

     // Cleanup base directory after all tests in this file run
    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) {
             await rimraf(TEST_DATA_DIR_BASE);
        }
    });

    describe('Basic CRUD Operations', () => {
        it('should set and get a string value within a transaction', async () => {
            const key = 'myKey';
            const value = 'myValue';

            // Perform set and get within one transaction
            await db.transaction(async tx => {
                const setResult = await tx.set(key, value);
                expect(setResult).toBe(true); // Assuming set returns boolean success
                expect(await tx.get(key)).toBe(value);
            });

            // Verify it's visible after commit in a new transaction
            await db.transaction(async tx => {
                 expect(await tx.get(key)).toBe(value);
            });
        });

        it('should set and get a number value', async () => {
            const key = 'numKey';
            const value = 12345;
            await db.transaction(async tx => {
                await tx.set(key, value);
                expect(await tx.get(key)).toBe(value);
            });
            await db.transaction(async tx => { expect(await tx.get(key)).toBe(value); });
        });

        it('should set and get a buffer value', async () => {
            const key = 'bufKey';
            const value = Buffer.from('hello buffer');
            await db.transaction(async tx => {
                await tx.set(key, value);
                const retrieved = await tx.get(key);
                expect(retrieved).toBeInstanceOf(Buffer);
                expect((retrieved as Buffer).equals(value)).toBe(true);
            });
            await db.transaction(async tx => {
                const retrieved = await tx.get(key);
                expect(retrieved).toBeInstanceOf(Buffer);
                expect((retrieved as Buffer).equals(value)).toBe(true);
             });
        });

         it('should set and get an object (as JSON string)', async () => {
            const key = 'objKey';
            const value = { a: 1, b: 'test' };
            const valueStr = JSON.stringify(value);

            await db.transaction(async tx => {
                await tx.set(key, valueStr);
                const retrievedStr = await tx.get(key);
                expect(retrievedStr).toBe(valueStr);
                expect(JSON.parse(retrievedStr as string)).toEqual(value);
            });
             await db.transaction(async tx => {
                 const retrievedStr = await tx.get(key);
                expect(retrievedStr).toBe(valueStr);
             });
        });

        it('should return null for a non-existent key', async () => {
            await db.transaction(async tx => {
                expect(await tx.get('nonExistentKey')).toBeNull();
            });
        });

        it('should overwrite an existing value on set', async () => {
            const key = 'overwriteKey';
            const initialValue = 'value1';
            const newValue = 'value2';

            // Set initial value
            await db.transaction(async tx => { await tx.set(key, initialValue); });

            // Overwrite
            await db.transaction(async tx => {
                const setResult = await tx.set(key, newValue);
                expect(setResult).toBe(true);
                expect(await tx.get(key)).toBe(newValue); // See own write
            });

            // Verify final value
            await db.transaction(async tx => {
                expect(await tx.get(key)).toBe(newValue);
            });
        });

        it('should remove an existing key', async () => {
            const key = 'removeKey';
            const value = 'toBeRemoved';

            // Set initial value
            await db.transaction(async tx => { await tx.set(key, value); });

            // Verify it exists
            await db.transaction(async tx => { expect(await tx.get(key)).toBe(value); });

            // Remove
            await db.transaction(async tx => {
                 const removeResult = await tx.remove(key);
                 expect(removeResult).toBe(true);
                 expect(await tx.get(key)).toBeNull(); // Should see own delete
            });

            // Verify it's gone
            await db.transaction(async tx => {
                 expect(await tx.get(key)).toBeNull();
            });
        });

        it('should return false when removing a non-existent key', async () => {
             await db.transaction(async tx => {
                 const removeResult = await tx.remove('nonExistentRemoveKey');
                 expect(removeResult).toBe(false);
             });
        });

         it('should automatically abort changes if callback throws', async () => {
            const key = 'abortKeySet';
            const value = 'abortedValue';

            await expect(db.transaction(async tx => {
                await tx.set(key, value);
                throw new Error("Force abort");
            })).rejects.toThrow("Force abort");

            // Verify it's not visible
            await db.transaction(async tx => {
                 expect(await tx.get(key)).toBeNull();
            });
        });

         it('should automatically abort removes if callback throws', async () => {
            const key = 'abortKeyRemove';
            const value = 'keptValue';

            // Set initial value
            await db.transaction(async tx => { await tx.set(key, value); });

            // Attempt remove but abort
             await expect(db.transaction(async tx => {
                 expect(await tx.remove(key)).toBe(true);
                 expect(await tx.get(key)).toBeNull(); // Sees own remove
                 throw new Error("Force abort remove");
             })).rejects.toThrow("Force abort remove");

            // Verify it still exists
            await db.transaction(async tx => {
                 expect(await tx.get(key)).toBe(value);
            });
        });

         it('should return a valid transaction ID via context', async () => {
              await db.transaction(async tx => {
                 const id = await tx.getId();
                 expect(typeof id).toBe('number');
                 expect(id).toBeGreaterThan(0);
             });
         });
    });

    describe('MVCC (Snapshot Isolation/Read Committed) Tests', () => {
        // NOTE: These tests will now reflect the OBSERVED behavior (Read Committed)
        // If Snapshot Isolation is implemented later, these tests might need adjustment.

         it('should provide Read Committed isolation', async () => {
            const key = 'mvccKey1';
            const value1 = 'valueV1';

            // Start Txn1 (but don't finish callback yet)
             let txn1Promise: Promise<any> | null = null;
             let txn1Resolver: (value?: any) => void;
             let txn1Tx: ITransactionContext | null = null;

             const txn1CallbackDone = new Promise<void>(resolve => { txn1Resolver = resolve; });

             txn1Promise = db.transaction(async tx => {
                txn1Tx = tx;
                await tx.set(key, value1);
                // Don't resolve/commit yet, wait externally
                await txn1CallbackDone;
             });


             // Wait briefly to ensure set is likely in Txn1's writeset
             await new Promise(res => setTimeout(res, 50)); // Small delay

             // Txn2 starts *after* Txn1 writes, but *before* Txn1 commits
             let valTxn2BeforeCommit: StorageValue | null = null;
             await db.transaction(async tx => {
                  valTxn2BeforeCommit = await tx.get(key);
             });
             // With Read Committed or Snapshot Isolation, Txn2 should NOT see Txn1's uncommitted write
             expect(valTxn2BeforeCommit).toBeNull();


             // Allow Txn1 callback to finish and commit
             txn1Resolver!();
             await txn1Promise; // Wait for Txn1 to commit


             // Txn3 starts *after* Txn1 commits
             let valTxn3AfterCommit: StorageValue | null = null;
             await db.transaction(async tx => {
                 valTxn3AfterCommit = await tx.get(key);
             });
             expect(valTxn3AfterCommit).toBe(value1); // Txn3 SHOULD see the committed change
        });

        it('should demonstrate repeatable reads (Snapshot Isolation behavior)', async () => { // <-- RENAMED Description
            const key = 'repeatReadKey';
            const value1 = 'firstRead';
            const value2 = 'newValueWritten';

            // Establish initial value
            await db.transaction(async tx => { await tx.set(key, value1); });

            // Txn A starts
            let read1TxnA: StorageValue | null = null;
            let read2TxnA: StorageValue | null = null;
            let txnAResolver: (value?: any) => void;
            const txnACallbackDone = new Promise<void>(resolve => { txnAResolver = resolve; });

            const txnAPromise = db.transaction(async tx => {
                read1TxnA = await tx.get(key); // First read
                await txnACallbackDone;        // Wait
                read2TxnA = await tx.get(key); // Second read
            });

            await new Promise(res => setTimeout(res, 50)); // Ensure TxnA gets first read
            expect(read1TxnA).toBe(value1);

            // Txn B starts, writes a new value, and commits
            await db.transaction(async tx => {
                await tx.set(key, value2);
            }); // TxnB commits here

            // Allow TxnA to proceed to its second read
            txnAResolver!();
            await txnAPromise; // Wait for Txn A to finish

            // *** Verify Snapshot Isolation behavior ***
            expect(read1TxnA).toBe(value1); // First read saw original value
            // In Snapshot Isolation, the second read MUST see the same value as the first
            expect(read2TxnA).toBe(value1); // <<< CORRECTED ASSERTION

            // Txn C starts after Txn B committed, should see the new value
            await db.transaction(async tx => {
                expect(await tx.get(key)).toBe(value2);
            });
        });


        it('should allow a transaction to see its own writes', async () => {
             const key = 'ownWriteKey';
             const value = 'tempValue';

             await db.transaction(async tx => {
                 expect(await tx.get(key)).toBeNull(); // Initially null
                 await tx.set(key, value);
                 expect(await tx.get(key)).toBe(value); // Should see own write immediately
             });
        });

        it('should allow a transaction to see its own deletes', async () => {
            const key = 'ownDeleteKey';
            const value = 'initialValue';

            // Setup initial value
            await db.transaction(async tx => { await tx.set(key, value); });

             await db.transaction(async tx => {
                 expect(await tx.get(key)).toBe(value); // See initial value
                 expect(await tx.remove(key)).toBe(true);
                 expect(await tx.get(key)).toBeNull(); // Should see own delete immediately
             });

             // Verify delete was committed
              await db.transaction(async tx => {
                 expect(await tx.get(key)).toBeNull();
             });
        });
    });

    describe('StoreRef.take() and Subcollections', () => {
        // Helper to populate data for these tests
        async function populateTestData(dbInstance: Indinis) {
            await dbInstance.transaction(async tx => {
                // Top level docs
                await tx.set('users/user1', JSON.stringify({ id: 'user1', name: 'Alice', email: 'alice@example.com' }));
                await tx.set('users/user2', JSON.stringify({ id: 'user2', name: 'Bob', email: 'bob@example.com' }));
                await tx.set('config/settings', JSON.stringify({ theme: 'dark', notifications: true }));

                // Subcollection for user1
                await tx.set('users/user1/posts/post1', JSON.stringify({ id: 'post1', title: 'Alice Post 1', userId: 'user1', published: true }));
                await tx.set('users/user1/posts/post2', JSON.stringify({ id: 'post2', title: 'Alice Post 2', userId: 'user1', published: false }));
                await tx.set('users/user1/posts/post3', JSON.stringify({ id: 'post3', title: 'Alice Post 3', userId: 'user1', published: true }));

                // Subcollection for user2
                await tx.set('users/user2/posts/postA', JSON.stringify({ id: 'postA', title: 'Bob Post A', userId: 'user2', published: true }));

                // Deeper subcollection (comments for post1)
                await tx.set('users/user1/posts/post1/comments/comment1', JSON.stringify({ id: 'comment1', text: 'Great post!', postId: 'post1' }));
                await tx.set('users/user1/posts/post1/comments/comment2', JSON.stringify({ id: 'comment2', text: 'Interesting...', postId: 'post1' }));

                 // Add a key that shares prefix but isn't a direct child document
                 await tx.set('users/user1/posts_meta', JSON.stringify({ info: 'metadata about posts' }));
            });
        }

        beforeEach(async () => {
            // Populate data before each test in this block
            await populateTestData(db);
        });

        it('should fetch all documents in a top-level collection', async () => {
            const usersStore = db.store<{ id: string, name: string, email: string }>('users');
            const users = await usersStore.take();

            expect(users).toHaveLength(2);
            expect(users).toEqual(expect.arrayContaining([
                expect.objectContaining({ id: 'user1', name: 'Alice' }),
                expect.objectContaining({ id: 'user2', name: 'Bob' })
            ]));
            // Ensure config/settings wasn't fetched
            expect(users).not.toEqual(expect.arrayContaining([
                 expect.objectContaining({ theme: 'dark' })
            ]));
        });

         it('should fetch documents with limit in a top-level collection', async () => {
            // Note: Order depends on underlying C++ prefix scan sort order (should be key order)
            const usersStore = db.store<{ id: string, name: string, email: string }>('users');
            const users = await usersStore.take(1); // Get only 1

            expect(users).toHaveLength(1);
            // Assuming keys 'user1' comes before 'user2'
            expect(users[0]).toEqual(expect.objectContaining({ id: 'user1', name: 'Alice' }));
        });


        it('should fetch all documents in a subcollection', async () => {
            const user1PostsStore = db.store<{ id: string, title: string, userId: string, published: boolean }>('users/user1/posts');
            const posts = await user1PostsStore.take();

            expect(posts).toHaveLength(3);
            expect(posts).toEqual(expect.arrayContaining([
                expect.objectContaining({ id: 'post1', title: 'Alice Post 1' }),
                expect.objectContaining({ id: 'post2', title: 'Alice Post 2' }),
                expect.objectContaining({ id: 'post3', title: 'Alice Post 3' })
            ]));
             // Ensure comments were not fetched
            expect(posts).not.toEqual(expect.arrayContaining([
                 expect.objectContaining({ text: 'Great post!' })
            ]));
             // Ensure user2's post wasn't fetched
             expect(posts).not.toEqual(expect.arrayContaining([
                expect.objectContaining({ id: 'postA' })
           ]));
            // Ensure 'users/user1/posts_meta' wasn't fetched
            expect(posts).not.toEqual(expect.arrayContaining([
                expect.objectContaining({ info: 'metadata about posts' })
           ]));
        });

        it('should fetch documents with limit in a subcollection', async () => {
            const user1PostsStore = db.store<{ id: string, title: string, userId: string, published: boolean }>('users/user1/posts');
             // Assuming 'post1', 'post2', 'post3' key order
            const posts = await user1PostsStore.take(2);

            expect(posts).toHaveLength(2);
            expect(posts).toEqual(expect.arrayContaining([
                 expect.objectContaining({ id: 'post1' }),
                 expect.objectContaining({ id: 'post2' })
            ]));
             expect(posts).not.toEqual(expect.arrayContaining([
                 expect.objectContaining({ id: 'post3' })
            ]));
        });


        it('should fetch documents in a deeper subcollection', async () => {
            const post1CommentsStore = db.store<{ id: string, text: string, postId: string }>('users/user1/posts/post1/comments');
            const comments = await post1CommentsStore.take();

            expect(comments).toHaveLength(2);
            expect(comments).toEqual(expect.arrayContaining([
                expect.objectContaining({ id: 'comment1', text: 'Great post!' }),
                expect.objectContaining({ id: 'comment2', text: 'Interesting...' })
            ]));
        });


        it('should return an empty array for an empty or non-existent collection', async () => {
            // Use a path with an ODD number of segments
            const emptyStore = db.store<any>('nonexistent'); // 1 segment (valid collection path)
            const items = await emptyStore.take();
            expect(items).toEqual([]);

            // This path is valid (3 segments)
            const user1Messages = db.store<any>('users/user1/messages'); // Exists but no docs added
            const messages = await user1Messages.take();
            expect(messages).toEqual([]);
        });

        it('should throw error if StoreRef path is invalid (even segments)', () => {
            expect(() => {
                 db.store('users/user1'); // Even path, invalid for StoreRef
            }).toThrow(/Invalid path for StoreRef.*must have an odd number of segments/);
        });

        it('should throw error if ItemRef path is invalid (odd segments)', () => {
            expect(() => {
                 db.store('users').item('user1/posts'); // Odd path for document ID
            }).toThrow(/Invalid path for ItemRef.*must have an even number of segments/);
             expect(() => {
                // Valid store, but invalid item ID creating an odd path overall
                db.store('users/user1/posts').item('post1/comments');
           }).toThrow(/Invalid path for ItemRef.*must have an even number of segments/);
        });

         // Test the underlying getPrefix method (via transaction context)
         it('should retrieve correct key-value pairs using tx.getPrefix', async () => {
             const prefix = 'users/user1/posts/';
             let fetchedData: KeyValueRecord[] = [];

             await db.transaction(async tx => {
                 fetchedData = await tx.getPrefix(prefix);
             });

             console.log("[FETCH TEST FOR TEST]",fetchedData);

             expect(fetchedData).toHaveLength(3); // post1, post2, post3
             expect(fetchedData).toEqual(expect.arrayContaining([
                 expect.objectContaining({ key: 'users/user1/posts/post1' }),
                 expect.objectContaining({ key: 'users/user1/posts/post2' }),
                 expect.objectContaining({ key: 'users/user1/posts/post3' })
             ]));
             // Verify one of the values
              const post1Record = fetchedData.find(r => r.key === 'users/user1/posts/post1');
              expect(post1Record).toBeDefined();
              expect(JSON.parse(post1Record!.value.toString())).toEqual(expect.objectContaining({ id: 'post1', title: 'Alice Post 1' }));

              // Ensure comments or parent docs aren't included
             expect(fetchedData).not.toEqual(expect.arrayContaining([
                expect.objectContaining({ key: 'users/user1' }),
                expect.objectContaining({ key: 'users/user1/posts/post1/comments/comment1' })
            ]));
         });

         it('should respect limit using tx.getPrefix', async () => {
            const prefix = 'users/user1/posts/';
            let fetchedData: KeyValueRecord[] = [];

            await db.transaction(async tx => {
                fetchedData = await tx.getPrefix(prefix, 2);
            });

            expect(fetchedData).toHaveLength(2);
             // Assuming 'post1', 'post2' key order
            expect(fetchedData).toEqual(expect.arrayContaining([
                 expect.objectContaining({ key: 'users/user1/posts/post1' }),
                 expect.objectContaining({ key: 'users/user1/posts/post2' })
            ]));
        });

        // Test the underlying getPrefix method (via transaction context)
        it('should retrieve correct key-value pairs using tx.getPrefix (now filtered)', async () => {
            const prefix = 'users/user1/posts/';
            let fetchedData: KeyValueRecord[] = [];

            await db.transaction(async tx => {
                fetchedData = await tx.getPrefix(prefix);
            });

            // *** ADJUSTED EXPECTATION: C++ filtering applied ***
            expect(fetchedData).toHaveLength(3); // Should ONLY get post1, post2, post3 now
            expect(fetchedData).toEqual(expect.arrayContaining([
                expect.objectContaining({ key: 'users/user1/posts/post1' }),
                expect.objectContaining({ key: 'users/user1/posts/post2' }),
                expect.objectContaining({ key: 'users/user1/posts/post3' })
            ]));
             // Verify one of the values
             const post1Record = fetchedData.find(r => r.key === 'users/user1/posts/post1');
             expect(post1Record).toBeDefined();
             expect(JSON.parse(post1Record!.value.toString())).toEqual(expect.objectContaining({ id: 'post1', title: 'Alice Post 1' }));

             // Ensure comments or parent docs aren't included (This check remains valid)
            expect(fetchedData).not.toEqual(expect.arrayContaining([
               // Keys below should NOT be returned by the filtered getPrefix
               expect.objectContaining({ key: 'users/user1' }),
               expect.objectContaining({ key: 'users/user1/posts/post1/comments/comment1' }),
                expect.objectContaining({ key: 'users/user1/posts_meta' }) // Also not a direct child document
           ]));
        });

        it('should respect limit using tx.getPrefix (now filtered)', async () => {
           const prefix = 'users/user1/posts/';
           let fetchedData: KeyValueRecord[] = [];

           await db.transaction(async tx => {
               fetchedData = await tx.getPrefix(prefix, 2);
           });

            // *** ADJUSTED EXPECTATION: C++ filtering applied ***
           expect(fetchedData).toHaveLength(2);
            // Assuming 'post1', 'post2' key order
           expect(fetchedData).toEqual(expect.arrayContaining([
                expect.objectContaining({ key: 'users/user1/posts/post1' }),
                expect.objectContaining({ key: 'users/user1/posts/post2' })
           ]));
            // Ensure post3 is not included due to limit
            expect(fetchedData).not.toEqual(expect.arrayContaining([
                 expect.objectContaining({ key: 'users/user1/posts/post3' })
            ]));
            // Ensure comments are not included due to C++ filtering
            expect(fetchedData).not.toEqual(expect.arrayContaining([
               expect.objectContaining({ key: 'users/user1/posts/post1/comments/comment1' })
           ]));
       });


    }); // End describe StoreRef.take()
});