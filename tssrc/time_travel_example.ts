// @filename examples/time_travel_example.ts
/**
 * Example demonstrating Time Travel / Snapshot Isolation.
 */

import { Indinis, StorageValue } from './index'; // Adjust path if needed
import * as path from 'path';
import * as fs from 'fs';
import { rimraf } from 'rimraf';

const EXAMPLE_DATA_DIR = "./indinis_timetravel_example_data";

// Helper to ensure clean state for examples
async function setupExampleDir() {
    if (fs.existsSync(EXAMPLE_DATA_DIR)) {
        console.log("Cleaning up example data directory...");
        await rimraf(EXAMPLE_DATA_DIR);
    }
    await fs.promises.mkdir(EXAMPLE_DATA_DIR, { recursive: true });
    console.log(`Created example data directory: ${EXAMPLE_DATA_DIR}`);
}

// Helper for small delay
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

async function runTimeTravelExample() {
    console.log("\n--- Running Time Travel Example ---");
    await setupExampleDir();
    const db = new Indinis(EXAMPLE_DATA_DIR);

    const key = 'shared_resource';

    try {
        // --- Initial State ---
        let txnIdT0 = -1;
        console.log("Setting initial value to 'Version 0'...");
        await db.transaction(async tx => {
            await tx.set(key, 'Version 0');
            txnIdT0 = await tx.getId();
        });
        console.log(` -> Committed Txn ${txnIdT0}`);


        // --- Concurrent Transactions ---
        let txnIdTA = -1, txnIdTB = -1, txnIdTC = -1;
        let read1TA: StorageValue | null = null;
        let read2TA: StorageValue | null = null;
        let resolveTxnAWait: () => void;
        const txnA_waitPromise = new Promise<void>(resolve => { resolveTxnAWait = resolve; });

        // Start Transaction A
        console.log("\nStarting Transaction A...");
        const txnA_Promise = db.transaction(async txA => {
            txnIdTA = await txA.getId();
            console.log(` -> Txn A Started (ID: ${txnIdTA})`);
            read1TA = await txA.get(key);
            console.log(` -> Txn A: First read of '${key}': ${read1TA}`);
            console.log(" -> Txn A: Waiting...");
            await txnA_waitPromise; // Pause execution
            console.log(" -> Txn A: Resumed.");
            read2TA = await txA.get(key);
            console.log(` -> Txn A: Second read of '${key}': ${read2TA}`);
            console.log(" -> Txn A: Attempting commit...");
        });

        // Give Txn A time to perform its first read
        await delay(150);

        // Start Transaction B while A is paused
        console.log("\nStarting Transaction B...");
        await db.transaction(async txB => {
            txnIdTB = await txB.getId();
            console.log(` -> Txn B Started (ID: ${txnIdTB})`);
            await txB.set(key, 'Version 1 by B');
            console.log(` -> Txn B: Set '${key}' to 'Version 1 by B'.`);
            console.log(" -> Txn B: Committing...");
        });
        console.log(` -> Txn B Committed (ID: ${txnIdTB})`);

        // Unpause Transaction A
        console.log("\nResuming Transaction A...");
        resolveTxnAWait!();
        await txnA_Promise;
        console.log(` -> Txn A Committed (ID: ${txnIdTA})`);


        // --- Verification ---
        console.log("\n--- Verification ---");
        console.log(`Txn A First Read Result: ${read1TA}`);
        console.log(`Txn A Second Read Result: ${read2TA}`);
        if (read1TA === read2TA && read1TA === 'Version 0') {
            console.log("✅ SUCCESS: Txn A maintained its snapshot view ('Version 0') across reads.");
        } else {
            console.error(`❌ FAILURE: Txn A did not maintain snapshot isolation! Read 1: ${read1TA}, Read 2: ${read2TA}`);
        }

        // Start Transaction C to see the final state
        console.log("\nStarting Transaction C to see final state...");
        await db.transaction(async txC => {
            txnIdTC = await txC.getId();
            const finalRead = await txC.get(key);
            console.log(` -> Txn C Started (ID: ${txnIdTC})`);
            console.log(` -> Txn C: Read of '${key}': ${finalRead}`);
            if (finalRead === 'Version 1 by B') {
                 console.log("✅ SUCCESS: Txn C sees the committed change from Txn B.");
            } else {
                console.error(`❌ FAILURE: Txn C did not see the committed change! Saw: ${finalRead}`);
            }
        });
        console.log(` -> Txn C Committed (ID: ${txnIdTC})`);


    } catch (error) {
        console.error("\n❌ An error occurred during the example:", error);
    } finally {
        console.log("\nClosing database...");
        await db.close();
        console.log("Database closed.");
        // Optional: await rimraf(EXAMPLE_DATA_DIR);
        console.log("\n--- Time Travel Example Finished ---");
    }
}

// --- Execute ---
runTimeTravelExample();