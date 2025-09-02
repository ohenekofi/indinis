// @tssrc/crud_examples.ts

// Note: This file demonstrates usage, it doesn't contain automated tests.
// Use `npm test` to run Jest tests.

import { Indinis } from './index'; // Assuming index.ts is in the same dir or adjust path
import * as path from 'path';
import * as fs from 'fs';
import { rimraf } from 'rimraf'; // Ensure @types/rimraf is installed

const EXAMPLE_DATA_DIR = "./indinis_fluent_example_data"; // Changed dir name

// Helper to ensure clean state for examples
async function cleanupDataDir() {
    if (fs.existsSync(EXAMPLE_DATA_DIR)) {
        console.log("Cleaning up example data directory...");
        await rimraf(EXAMPLE_DATA_DIR);
    }
    await fs.promises.mkdir(EXAMPLE_DATA_DIR, { recursive: true });
}

// --- Define Data Structure ---
interface User {
    id?: string;
    name: string;
    email: string;
    status?: 'active' | 'inactive';
    createdAt?: number;
    profileViews?: number;
    tags?: string[];
    lastLogin?: number; // Added for upsert/modify examples
}

interface Post {
    id?: string;
    userId: string;
    title: string;
    content: string;
    published?: boolean;
    createdAt?: number;
}

interface Comment {
    id?: string;
    postId: string;
    userId: string; // User who commented
    text: string;
    createdAt?: number;
}

// Example 1: Demonstrating Fluent API for Basic CRUD
async function fluentCrudExample() {
    console.log("\n--- Running Fluent CRUD Example ---");
    const db = new Indinis(EXAMPLE_DATA_DIR);
    const usersStore = db.store<User>('users'); // Path 'users' is valid
    console.log("Got StoreRef for path:", usersStore.storePath);
  
    // Create Alice
    const aliceData = { name: "Alice", email: "alice@example.com", createdAt: Date.now() };
    const aliceId = await usersStore.make(aliceData); // Auto ID
    console.log(`Created Alice with ID: ${aliceId}`);
  
    // Create Bob with specific ID
    const bobData = { name: "Bob", email: "bob@example.com", createdAt: Date.now() };
    await usersStore.item("userBob").make(bobData); // Path 'users/userBob' is valid for item
    console.log(`Created Bob with ID: userBob`);
  
    // Read Alice
    const retrievedAlice = await usersStore.item(aliceId).one();
    console.log(`Retrieved Alice (${aliceId}):`, retrievedAlice);
  
    // Modify Bob
    await usersStore.item("userBob").modify({ status: 'active', lastLogin: Date.now() });
    const modifiedBob = await usersStore.item("userBob").one();
    console.log(`Modified Bob (userBob):`, modifiedBob);
  
    // Delete Alice
    await usersStore.item(aliceId).remove();
    console.log(`Deleted Alice (${aliceId})`);
    const exists = await usersStore.item(aliceId).exists();
    console.log(`Does Alice (${aliceId}) exist?`, exists); // Should be false
  
    console.log("--- Fluent CRUD Example Finished ---");
  }
  
  
  // --- Example 2: MVCC (Keep as is) ---
  async function mvccExample() {
      console.log("\n--- Running MVCC Example (using db.transaction) ---");
      const db = new Indinis(EXAMPLE_DATA_DIR);
      console.log("Setting initial counter value to '0'");
      await db.transaction(async tx => { await tx.set("counter", "0"); });
  
      let txnA_read1: any = null;
      let txnA_read2: any = null;
      let resolveTxnAWait: () => void;
      const txnA_waitPromise = new Promise<void>(resolve => { resolveTxnAWait = resolve; });
  
      console.log("Starting Txn A");
      const txnA_Promise = db.transaction(async txA => {
          console.log("  Txn A started (Tx ID:", await txA.getId(), ")");
          txnA_read1 = await txA.get("counter");
          console.log("  Txn A: Read 1 =", txnA_read1);
          console.log("  Txn A: Waiting...");
          await txnA_waitPromise;
          txnA_read2 = await txA.get("counter");
          console.log("  Txn A: Read 2 =", txnA_read2);
          console.log("  Txn A: Finishing.");
      });
  
      await new Promise(res => setTimeout(res, 150)); // Give Txn A time for first read
  
      console.log("Starting Txn B (will write '1' and commit)");
      await db.transaction(async txB => {
           console.log("  Txn B started (Tx ID:", await txB.getId(), ")");
           await txB.set("counter", "1");
           console.log("  Txn B: Set counter to '1'.");
      });
       console.log("Txn B committed.");
  
      console.log("Allowing Txn A to proceed...");
      resolveTxnAWait!();
      await txnA_Promise;
      console.log("Txn A finished.");
  
      console.log("\n--- MVCC Results ---");
      console.log("Txn A - First Read:", txnA_read1); // Should be '0'
      console.log("Txn A - Second Read:", txnA_read2); // Should ALSO be '0' (Snapshot Isolation)
  
      await db.transaction(async txC => {
          const finalValue = await txC.get("counter");
          console.log("Txn C - Final Read:", finalValue); // Should be '1'
      });
      console.log("--- MVCC Example Finished ---");
  }
  
  // --- NEW Example 3: Subcollections and take() ---
  async function subcollectionTakeExample() {
      console.log("\n--- Running Subcollection and take() Example ---");
      const db = new Indinis(EXAMPLE_DATA_DIR);
  
      // --- Populate Data ---
      console.log("Populating data...");
      const user1Id = 'user1';
      const user2Id = 'user2';
      await db.transaction(async tx => {
          // Users
          await tx.set(`users/${user1Id}`, JSON.stringify({ id: user1Id, name: 'Charlie' }));
          await tx.set(`users/${user2Id}`, JSON.stringify({ id: user2Id, name: 'Diane' }));
  
          // Posts for User 1
          await tx.set(`users/${user1Id}/posts/post1`, JSON.stringify({ id: 'post1', userId: user1Id, title: 'Charlie Post 1' }));
          await tx.set(`users/${user1Id}/posts/post2`, JSON.stringify({ id: 'post2', userId: user1Id, title: 'Charlie Post 2' }));
  
          // Comments for Post 1
          await tx.set(`users/${user1Id}/posts/post1/comments/commentA`, JSON.stringify({ id: 'commentA', postId: 'post1', userId: user2Id, text: 'First!' }));
          await tx.set(`users/${user1Id}/posts/post1/comments/commentB`, JSON.stringify({ id: 'commentB', postId: 'post1', userId: user1Id, text: 'My own comment' }));
  
          // Another top-level collection
           await tx.set(`products/prod1`, JSON.stringify({ id: 'prod1', name: 'Thingamajig' }));
      });
      console.log("Data populated.");
  
      // --- Using take() ---
  
      // 1. Get all posts for user1
      console.log(`\nFetching all posts for user '${user1Id}'...`);
      const user1PostsStore = db.store<Post>(`users/${user1Id}/posts`); // Path is valid collection path
      const allUser1Posts = await user1PostsStore.take();
      console.log(`  Found ${allUser1Posts.length} posts:`);
      allUser1Posts.forEach(p => console.log(`    - ${p.id}: ${p.title}`));
      if (allUser1Posts.length !== 2) throw new Error("Verification failed: Expected 2 posts for user1");
  
      // 2. Get posts for user1 with limit
      console.log(`\nFetching 1 post for user '${user1Id}'...`);
      const limitedUser1Posts = await user1PostsStore.take(1);
      console.log(`  Found ${limitedUser1Posts.length} post:`);
      limitedUser1Posts.forEach(p => console.log(`    - ${p.id}: ${p.title}`));
       if (limitedUser1Posts.length !== 1) throw new Error("Verification failed: Expected 1 post with limit");
  
      // 3. Get all comments for post1
      console.log(`\nFetching all comments for post 'post1' under user '${user1Id}'...`);
      const post1CommentsStore = db.store<Comment>(`users/${user1Id}/posts/post1/comments`); // Valid collection path
      const allPost1Comments = await post1CommentsStore.take();
      console.log(`  Found ${allPost1Comments.length} comments:`);
      allPost1Comments.forEach(c => console.log(`    - ${c.id}: ${c.text}`));
       if (allPost1Comments.length !== 2) throw new Error("Verification failed: Expected 2 comments for post1");
  
      // 4. Get all users (top level) - Should NOT include posts/comments
      console.log(`\nFetching all users (top-level)...`);
      const usersStore = db.store<User>('users');
      const allUsers = await usersStore.take();
      console.log(`  Found ${allUsers.length} users:`);
      allUsers.forEach(u => console.log(`    - ${u.id}: ${u.name}`));
       if (allUsers.length !== 2) throw new Error("Verification failed: Expected 2 users at top level");
       // Simple check to ensure no posts were included
       const containsPosts = allUsers.some(item => 'title' in item);
       if (containsPosts) throw new Error("Verification failed: Top-level user fetch included post data!");
  
  
      console.log("--- Subcollection and take() Example Finished ---");
  }
  
  
  // --- Run All Examples ---
  async function runAllExamples() {
    await cleanupDataDir();
    try {
      await fluentCrudExample(); // Run original fluent example
      await mvccExample();       // Run original MVCC example
      await subcollectionTakeExample(); // Run new subcollection example
      console.log("\n✅ All examples completed successfully.");
    } catch (error) {
      console.error("\n❌ Error running examples:", error);
      process.exitCode = 1; // Indicate failure
    } finally {
        console.log("\nExamples finished. Compaction threads will stop on process exit or GC.");
        // Optional: await cleanupDataDir(); // Uncomment to clean up after run
    }
  }

// --- Execute ---
runAllExamples();