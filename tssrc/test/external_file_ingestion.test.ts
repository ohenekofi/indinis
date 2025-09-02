// tssrc/tes/external_file_ingestion.test.ts
import { Indinis, IndinisOptions } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-file-ingestion-v1');

const delay = (ms: number): Promise<void> => {
    return new Promise(resolve => setTimeout(resolve, ms));
}; 

// Augment the Indinis interface for our test-only methods
declare module '../index' {
    interface Indinis {
        debug_getSSTableBuilder(filepath: string, options: any): any;
        debug_serializeValue(value: any): Buffer;
                    add(record: any): Promise<void>;
            finish(): Promise<void>;
            close(): Promise<void>; 
    }
}

/**
 * Test helper to create a valid SSTable file.
 * It now correctly stringifies object values before serializing them
 * into the C++ binary format.
 * @param db The Indinis instance.
 * @param filepath The full path where the SSTable file will be created.
 * @param records An array of records to add to the SSTable.
 */
async function createTestSSTable(db: Indinis, filepath: string, records: { key: string; value: any }[]): Promise<void> {
    const builder = (db as any).debug_getSSTableBuilder(filepath, {
        compressionType: 'ZSTD'
    });

    try {
        console.log(`[createTestSSTable] Starting to build SSTable at: ${filepath}`);
        for (const record of records) {
            const valueToSerialize = JSON.stringify(record.value);
            const serializedValueBuffer = (db as any).debug_serializeValue(valueToSerialize);
            const commit_txn_id = BigInt(Date.now() + Math.floor(Math.random() * 10000));

            // Log the exact data being passed to the C++ addon
            console.log(`  -> Adding to builder: Key='${record.key}', Value (JSON)='${valueToSerialize}', CommitTxnID=${commit_txn_id}`);

            await builder.add({
                key: record.key,
                value: serializedValueBuffer,
                deleted: false,
                commit_txn_id: commit_txn_id,
            });
        }
        await builder.finish();
        console.log(`[createTestSSTable] Finished building SSTable successfully.`);
    } finally {
        builder.close();
        console.log(`[createTestSSTable] Builder closed and resources released.`);
    }
}


describe('Indinis External File Ingestion', () => {
    let db: Indinis;
    let testDataDir: string;
    let externalFilesDir: string;

    const colPath = 'ingested_docs';

    jest.setTimeout(45000);

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `db-${randomSuffix}`);
        externalFilesDir = path.join(TEST_DATA_DIR_BASE, `ext-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        await fs.promises.mkdir(externalFilesDir, { recursive: true });
        
        db = new Indinis(testDataDir);
        console.log(`\n[INGESTION TEST START] Using data directory: ${testDataDir}`);
    });

    afterEach(async () => {
        if (db) {
            console.log(`[TEST END] Closing database for dir: ${testDataDir}`);
            await db.close();
            // db = null; // Help garbage collection
        }
            // This is especially important on Windows.
        console.log("[TEST END] Waiting a moment before cleanup...");
        await delay(500); // 500ms is usually a very safe margin.
        //await delay(500); 

        if (fs.existsSync(testDataDir)) {
            console.log(`[INGESTION TEST END] Cleaning up test directory: ${testDataDir}`);
            await rimraf(testDataDir, { maxRetries: 3, retryDelay: 500 }).catch(err => {
                console.warn(`Cleanup warning for ${testDataDir}: ${err.message}`);
            });
        }
        if (fs.existsSync(externalFilesDir)) {
            await rimraf(externalFilesDir, { maxRetries: 3, retryDelay: 500 }).catch(err => {
                console.warn(`Cleanup warning for ${externalFilesDir}: ${err.message}`);
            });
        }
    }, 30000);

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    it('should successfully ingest a valid SSTable file by moving it', async () => {
        const externalFilePath = path.join(externalFilesDir, 'bulk_data_move.sst');
        const testRecords = [
            { key: `${colPath}/user:100`, value: { name: 'Alice' } },
            { key: `${colPath}/user:200`, value: { name: 'Bob' } },
        ];
        
        await createTestSSTable(db, externalFilePath, testRecords);
        expect(fs.existsSync(externalFilePath)).toBe(true);

        await db.ingestFile(colPath, externalFilePath);
        
        expect(fs.existsSync(externalFilePath)).toBe(false);

        await db.transaction(async tx => {
            const val1 = await tx.get(testRecords[0].key);
            const val2 = await tx.get(testRecords[1].key);
            expect(JSON.parse(val1 as string)).toEqual(testRecords[0].value);
            expect(JSON.parse(val2 as string)).toEqual(testRecords[1].value);
        });
    });

    it('should successfully ingest a valid SSTable file by copying it', async () => {
        const externalFilePath = path.join(externalFilesDir, 'bulk_data_copy.sst');
        const testRecords = [
            { key: `${colPath}/item:A`, value: { data: 'A' } },
            { key: `${colPath}/item:B`, value: { data: 'B' } },
        ];
        
        await createTestSSTable(db, externalFilePath, testRecords);
        expect(fs.existsSync(externalFilePath)).toBe(true);

        await db.ingestFile(colPath, externalFilePath, { moveFile: false });

        expect(fs.existsSync(externalFilePath)).toBe(true);
        
        await db.transaction(async tx => {
            const valA = await tx.get(testRecords[0].key);
            expect(JSON.parse(valA as string)).toEqual(testRecords[0].value);
        });
    });

    it('should allow ingested data to coexist and be read with transactionally-written data', async () => {
        const externalFilePath = path.join(externalFilesDir, 'bulk_mixed.sst');
        const ingestedRecords = [
            { key: `${colPath}/ingested:1`, value: { source: 'ingest' } },
            { key: `${colPath}/ingested:3`, value: { source: 'ingest' } },
        ];
        await createTestSSTable(db, externalFilePath, ingestedRecords);
        await db.ingestFile(colPath, externalFilePath);

        await db.transaction(async tx => {
            await tx.set(`${colPath}/transactional:2`, JSON.stringify({ source: 'txn' }));
        });
        
        const allDocs = await db.store(colPath).take();
        expect(allDocs).toHaveLength(3);
        
        const sources = allDocs.map(doc => (doc as any).source).sort();
        expect(sources).toEqual(['ingest', 'ingest', 'txn']);
    });

    describe('Error Handling', () => {
        it('should throw an error if the external file does not exist', async () => {
            const nonExistentFile = path.join(externalFilesDir, 'i_do_not_exist.sst');
            await expect(db.ingestFile(colPath, nonExistentFile))
                .rejects.toThrow(/External file does not exist/);
        });

        it('should throw an error if the file is not a valid SSTable', async () => {
            const invalidFile = path.join(externalFilesDir, 'not_an_sstable.txt');
            fs.writeFileSync(invalidFile, 'This is just plain text.');
            
            await expect(db.ingestFile(colPath, invalidFile))
                .rejects.toThrow(/External file is not a valid SSTable or is corrupt/);
        });

        it('should throw an error if the user tries to build a file with out-of-order keys', async () => {
            const outOfOrderFile = path.join(externalFilesDir, 'bad_order.sst');
            const outOfOrderRecords = [
                { key: `${colPath}/user:Z`, value: { name: 'Zed' } },
                { key: `${colPath}/user:A`, value: { name: 'Amy' } },
            ];
            
            // The error now correctly comes from the builder, not the ingestion.
            await expect(createTestSSTable(db, outOfOrderFile, outOfOrderRecords))
                .rejects.toThrow(/Keys must be added in strictly increasing key order/);
        });
    });
});