// @tssrc/indexing_examples.ts

/**
 * Examples demonstrating the Indinis Index Management API.
 *
 * Note: These examples focus on CREATING, LISTING, and DELETING indexes.
 * The *usage* of indexes happens implicitly when you modify data in indexed
 * collections and will be leveraged by future query capabilities.
 */

import { Indinis, IndexOptions } from './index';
import * as path from 'path';
import * as fs from 'fs';
import { rimraf } from 'rimraf';

const EXAMPLE_DATA_DIR = "./indinis_indexing_example_data";

// Helper to ensure clean state for examples
async function setupExampleDir() {
    if (fs.existsSync(EXAMPLE_DATA_DIR)) {
        console.log(`Cleaning up previous example data directory: ${EXAMPLE_DATA_DIR}`);
        await rimraf(EXAMPLE_DATA_DIR);
    }
    await fs.promises.mkdir(EXAMPLE_DATA_DIR, { recursive: true });
    console.log(`Created example data directory: ${EXAMPLE_DATA_DIR}`);
}

async function runIndexingExamples() {
    console.log("\n--- Running Indinis Indexing Examples ---");
    await setupExampleDir();
    const db = new Indinis(EXAMPLE_DATA_DIR);

    try {
        // --- Define Store Paths ---
        const usersPath = 'users'; // Valid collection path
        const productsPath = 'products'; // Valid collection path
        const ordersPath = 'customers/customer123/orders'; // Valid nested collection path

        // --- 1. Creating Indexes ---
        console.log("\n--- Example 1: Creating Indexes ---");

        // Simple index on users.email (ascending by default)
        console.log(`Creating simple index 'usersByEmail' on path '${usersPath}' for field 'email'...`);
        let success = await db.createIndex(usersPath, 'usersByEmail', { field: 'email' });
        console.log(`  Success: ${success}`);

        // Simple index on products.price (descending)
        console.log(`Creating simple index 'productsByPriceDesc' on path '${productsPath}' for field 'price' (descending)...`);
        success = await db.createIndex(productsPath, 'productsByPriceDesc', { field: 'price', order: 'desc' });
        console.log(`  Success: ${success}`);

        // Compound index on orders (status asc, date desc)
        console.log(`Creating compound index 'ordersByStatusDate' on path '${ordersPath}'...`);
        const compoundOptions: IndexOptions = {
            fields: [
                { name: 'status', order: 'asc' }, // Explicit asc
                { name: 'orderDate', order: 'desc' } // Explicit desc
            ]
        };
        success = await db.createIndex(ordersPath, 'ordersByStatusDate', compoundOptions);
        console.log(`  Success: ${success}`);

        // Attempt to create a duplicate index name (should fail)
        console.log(`Attempting to create duplicate index name 'usersByEmail' on path '${productsPath}'...`);
        success = await db.createIndex(productsPath, 'usersByEmail', { field: 'sku' });
        console.log(`  Success: ${success}`); // Expect: false

        // Attempt to create index on invalid path (should throw)
        try {
             console.log(`Attempting to create index on invalid path 'users/user123'...`);
             await db.createIndex('users/user123', 'invalidPathIndex', { field: 'test' });
        } catch (error: any) {
             console.log(`  Caught expected error: ${error.message}`);
        }


        // --- 2. Listing Indexes ---
        console.log("\n--- Example 2: Listing Indexes ---");

        // List all indexes in the database
        console.log("Listing all indexes:");
        const allIndexes = await db.listIndexes();
        if (allIndexes.length > 0) {
            allIndexes.forEach(idx => console.log(`  - Name: ${idx.name}, Path: ${idx.storePath}, Fields: ${JSON.stringify(idx.fields)}`));
        } else {
            console.log("  (No indexes found)");
        }

        // List indexes specifically for the 'users' path
        console.log(`\nListing indexes for path '${usersPath}':`);
        const userIndexes = await db.listIndexes(usersPath);
         if (userIndexes.length > 0) {
             userIndexes.forEach(idx => console.log(`  - Name: ${idx.name}`)); // Fields already logged above
         } else {
             console.log("  (No indexes found for this path)");
         }

        // List indexes for a path with no indexes
        console.log(`\nListing indexes for path 'nonexistent/path/ok':`);
        const nonExistentIndexes = await db.listIndexes('nonexistent/path/ok'); // Valid path structure
        console.log(`  Found ${nonExistentIndexes.length} indexes.`); // Expect 0


        // --- 3. Deleting Indexes ---
        console.log("\n--- Example 3: Deleting Indexes ---");

        // Delete an existing index
        const indexToDelete = 'productsByPriceDesc';
        console.log(`Deleting index '${indexToDelete}'...`);
        success = await db.deleteIndex(indexToDelete);
        console.log(`  Success: ${success}`); // Expect: true

        // Verify deletion by listing all again
        console.log("Listing all indexes after deletion:");
         const indexesAfterDelete = await db.listIndexes();
         if (indexesAfterDelete.length > 0) {
            indexesAfterDelete.forEach(idx => console.log(`  - Name: ${idx.name}, Path: ${idx.storePath}`));
         } else {
             console.log("  (No indexes found)");
         }
         const wasDeleted = !indexesAfterDelete.some(idx => idx.name === indexToDelete);
         console.log(`  Index '${indexToDelete}' successfully removed: ${wasDeleted}`);

        // Attempt to delete a non-existent index
        console.log(`Attempting to delete non-existent index 'noSuchIndex'...`);
        success = await db.deleteIndex('noSuchIndex');
        console.log(`  Success: ${success}`); // Expect: false

    } catch (error: any) {
        console.error("\n❌ An error occurred during the examples:", error);
        process.exitCode = 1;
    } finally {
        console.log("\nClosing database...");
        await db.close();
        console.log("Database closed.");
         // Optional: await rimraf(EXAMPLE_DATA_DIR); // Uncomment to clean up after run
        console.log("\n--- Indexing Examples Finished ---");
    }
}

// --- Execute ---
runIndexingExamples();