// @tssrc/index.ts

/**
 * Indinis: Fluent NoSQL Library Wrapper
 */

import bindings = require('bindings');
import * as path from 'path';
import { StoreRef } from './fluent-api';
import { isValidCollectionPath } from './utils';
import {
    IndinisOptions,
    DefragmentationStatsJs,
    DatabaseStatsJs,
    CheckpointInfo,
    BTreeDebugScanEntry,
    TransactionCallback,
    ITransactionContext,
    INativeTransaction,
    IndexOptions,
    IndexInfo,
    CacheStatsJs,
    StorageValue,
    FilterCondition,
    KeyValueRecord,
    SortByCondition,
    IncrementSentinel,
    UpdateOperation,
    AtomicIncrementOperation,
    ColumnDefinition,
    ColumnSchemaDefinition,
    ColumnType,
    AggregationSpec,
    AggregationPlan,
    LsmVersionStats,
    RcuStatsJs,
    ThreadPoolStatsJs
} from './types';
import { EventEmitter } from 'events';
import { CloudProviderAdapter, SyncRuleset, OutboxRecord } from './types';
import { CloudStoreRef } from './cloud-fluent-api';
import { BatchWriter } from './batch-api';
export { fg, Infer } from './formguard';

// Load the native addon
const addon = bindings({
  bindings: 'indinis', // Ensure this matches binding.gyp target_name
  // addon_path: path.resolve(__dirname, '../build/Release'), // Optional: Specify path if needed
  try: [
    // Look for Release build
    ['module_root', 'build', 'Release', 'indinis.node'],
    // Look for Debug build
    ['module_root', 'build', 'Debug', 'indinis.node'],
    // Fallback if necessary
    // ['module_root', 'build', 'bindings.node']
  ]
});


/**
 * @internal
 * The default synchronization rules applied to any cloud store if not overridden.
 * Ensures a safe, offline-first default behavior.
 */
const defaultSyncRuleset: SyncRuleset<any> = {
    readStrategy: 'LocalFirst',
    writeStrategy: 'LocalFirstWithOutbox',
    onConflict: 'LastWriteWins',
    // validator, toCloud, and fromCloud are undefined by default.
};

/**
 * Indinis Database Class - Main entry point.
 */
export class Indinis extends EventEmitter { 
    private nativeEngine: any | null; // Holds the C++ IndinisWrapper instance
    #addonInstance: any;
    /**
     * Creates or opens an Indinis database instance.
     * @param dataDir The path to the directory where database files will be stored. Must be provided.
     */
    private cloudAdapter?: CloudProviderAdapter<any>;
    private outboxWorker?: NodeJS.Timeout;
    private isSyncing = false;

    constructor(dataDir: string, options?: IndinisOptions) { // options is now IndinisOptions
        super();
        if (!dataDir || typeof dataDir !== 'string') {
            throw new Error("Indinis constructor: dataDir (string) is required.");
        }
        if (options && typeof options !== 'object') { 
            throw new TypeError("Indinis constructor: options, if provided, must be an object.");
        }

        const nativeConstructorOptions: any = {};
        if (options?.checkpointIntervalSeconds !== undefined) {
            if (typeof options.checkpointIntervalSeconds !== 'number' || options.checkpointIntervalSeconds < 0) {
                throw new TypeError("Indinis constructor: options.checkpointIntervalSeconds must be a non-negative number.");
            }
            nativeConstructorOptions.checkpointIntervalSeconds = options.checkpointIntervalSeconds;
        }

        // *** PASS WAL OPTIONS TO NATIVE LAYER ***
        if (options?.walOptions) {
            if (typeof options.walOptions !== 'object') {
                throw new TypeError("Indinis constructor: options.walOptions must be an object.");
            }
            // Add validation for each field of walOptions if desired
            nativeConstructorOptions.walOptions = options.walOptions; 
        }

        if (options?.sstableDataBlockUncompressedSizeKB !== undefined) {
            if (typeof options.sstableDataBlockUncompressedSizeKB !== 'number' || options.sstableDataBlockUncompressedSizeKB <= 0) {
                throw new TypeError("Indinis constructor: options.sstableDataBlockUncompressedSizeKB must be a positive number.");
            }
            nativeConstructorOptions.sstableDataBlockUncompressedSizeKB = options.sstableDataBlockUncompressedSizeKB;
        }
        if (options?.sstableCompressionType !== undefined) {
            if (!["NONE", "ZSTD", "LZ4"].includes(options.sstableCompressionType)) {
                throw new TypeError("Indinis constructor: options.sstableCompressionType must be 'NONE', 'ZSTD', or 'LZ4'.");
            }
            nativeConstructorOptions.sstableCompressionType = options.sstableCompressionType;
        }
        if (options?.sstableCompressionLevel !== undefined) {
            if (typeof options.sstableCompressionLevel !== 'number') { // Level 0 is often default
                throw new TypeError("Indinis constructor: options.sstableCompressionLevel must be a number.");
            }
            nativeConstructorOptions.sstableCompressionLevel = options.sstableCompressionLevel;
        }

        if (options?.encryptionOptions) {
            if (typeof options.encryptionOptions !== 'object') {
                throw new TypeError("Indinis constructor: options.encryptionOptions must be an object.");
            }
            nativeConstructorOptions.encryptionOptions = {}; // Prepare to pass to C++

            if (options.encryptionOptions.password !== undefined) {
                if (typeof options.encryptionOptions.password !== 'string') {
                    throw new TypeError("Indinis constructor: options.encryptionOptions.password must be a string.");
                }
                nativeConstructorOptions.encryptionOptions.password = options.encryptionOptions.password;
            }

            if (options.encryptionOptions.scheme !== undefined) {
                const validSchemes = ["NONE", "AES256_CBC_PBKDF2", "AES256_GCM_PBKDF2"];
                if (!validSchemes.includes(options.encryptionOptions.scheme)) {
                    throw new TypeError(`Indinis constructor: options.encryptionOptions.scheme must be one of ${validSchemes.join(', ')}.`);
                }
                nativeConstructorOptions.encryptionOptions.scheme = options.encryptionOptions.scheme;
            }

            if (options.encryptionOptions.kdfIterations !== undefined) {
                if (typeof options.encryptionOptions.kdfIterations !== 'number' || !Number.isInteger(options.encryptionOptions.kdfIterations)) {
                    throw new TypeError("Indinis constructor: options.encryptionOptions.kdfIterations must be an integer.");
                }
                // C++ side will use default if <= 0
                nativeConstructorOptions.encryptionOptions.kdfIterations = options.encryptionOptions.kdfIterations;
            }
        }

        // ---  Cache Options Parsing ---
        if (options?.enableCache !== undefined) {
            if (typeof options.enableCache !== 'boolean') {
                throw new TypeError("Indinis constructor: options.enableCache must be a boolean.");
            }
            nativeConstructorOptions.enableCache = options.enableCache;
        } else {
            nativeConstructorOptions.enableCache = false; // Default to disabled
        }

        if (nativeConstructorOptions.enableCache && options?.cacheOptions) {
            if (typeof options.cacheOptions !== 'object') {
                throw new TypeError("Indinis constructor: options.cacheOptions must be an object.");
            }
            nativeConstructorOptions.cacheOptions = {};
            if (options.cacheOptions.maxSize !== undefined) {
                if (typeof options.cacheOptions.maxSize !== 'number' || options.cacheOptions.maxSize <= 0) {
                    throw new TypeError("cacheOptions.maxSize must be a positive number.");
                }
                nativeConstructorOptions.cacheOptions.maxSize = options.cacheOptions.maxSize;
            }
            if (options.cacheOptions.policy !== undefined) {
                if (!["LRU", "LFU"].includes(options.cacheOptions.policy)) {
                    throw new TypeError("cacheOptions.policy must be 'LRU' or 'LFU'.");
                }
                nativeConstructorOptions.cacheOptions.policy = options.cacheOptions.policy;
            }
            if (options.cacheOptions.defaultTTLMilliseconds !== undefined) {
                if (typeof options.cacheOptions.defaultTTLMilliseconds !== 'number' || options.cacheOptions.defaultTTLMilliseconds < 0) {
                    throw new TypeError("cacheOptions.defaultTTLMilliseconds must be a non-negative number.");
                }
                nativeConstructorOptions.cacheOptions.defaultTTLMilliseconds = options.cacheOptions.defaultTTLMilliseconds;
            }
            if (options.cacheOptions.enableStats !== undefined) {
                if (typeof options.cacheOptions.enableStats !== 'boolean') {
                    throw new TypeError("cacheOptions.enableStats must be a boolean.");
                }
                nativeConstructorOptions.cacheOptions.enableStats = options.cacheOptions.enableStats;
            }
        }
        // --- END  Cache Options Parsing ---
        if (options?.lsmOptions) {
            if (typeof options.lsmOptions !== 'object') {
                throw new TypeError("Indinis constructor: options.lsmOptions must be an object.");
            }
            nativeConstructorOptions.lsmOptions = {}; // Prepare object for C++

            if (options.lsmOptions.defaultMemTableType) {
                if (typeof options.lsmOptions.defaultMemTableType !== 'string') {
                    throw new TypeError("lsmOptions.defaultMemTableType must be a string.");
                }
                nativeConstructorOptions.lsmOptions.defaultMemTableType = options.lsmOptions.defaultMemTableType;
            }

            if (options.lsmOptions.rcuOptions) {
                if (typeof options.lsmOptions.rcuOptions !== 'object') {
                    throw new TypeError("lsmOptions.rcuOptions must be an object.");
                }
                // Pass the rcuOptions object through; the C++ layer will parse its contents.
                nativeConstructorOptions.lsmOptions.rcuOptions = options.lsmOptions.rcuOptions;
            }
        }

        try {
            this.nativeEngine = new addon.Indinis(path.resolve(dataDir), nativeConstructorOptions);
        } catch (e: any) {
            console.error("Failed to initialize Indinis native engine:", e);
            throw new Error(`Failed to initialize Indinis native engine: ${e.message}`);
        }
    }

    /**
     * @internal FOR TESTING ONLY. Verifies the integrity of the disk manager's freelist.
     * @returns A promise resolving to true if the freelist is healthy, false otherwise.
     */
    async debug_verifyFreeList(): Promise<boolean> {
        const engine = this.getNativeEngine();
        if (!engine) throw new Error("Indinis engine is not available for debug_verifyFreeList.");
        return engine.debug_verifyFreeList(); // Assumes this NAPI method exists
    }

    public batch(): BatchWriter {
        return new BatchWriter(this);
    }

    /**
     * @internal FOR TESTING ONLY. Retrieves statistics about the LSM-Tree's
     * internal versioning system for a specific store.
     * @param storePath The path of the store to inspect.
     * @returns A promise resolving to an object with versioning statistics.
     */
    async debug_getLsmVersionStats(storePath: string): Promise<LsmVersionStats> {
        const engine = this.getNativeEngine();
        if (!engine) {
            throw new Error("Indinis engine is not available for debug_getLsmVersionStats.");
        }
        return engine.debug_getLsmVersionStats(storePath);
    }

    /**
     * @internal FOR TESTING ONLY. Retrieves statistics about the active memtable
     * for a specific LSM store.
     */
    async debug_getLsmStoreStats(storePath: string): Promise<any> {
        const engine = this.getNativeEngine();
        if (!engine) throw new Error("Engine not available.");
        return engine.debug_getLsmStoreStats(storePath);
    }

     /**
     * @internal FOR TESTING ONLY. Retrieves statistics from the MemTableTuner for
     * a specific LSM store.
     */
    async debug_getMemTableTunerStats(storePath: string): Promise<any> {
        const engine = this.getNativeEngine();
        if (!engine) throw new Error("Engine not available.");
        return engine.debug_getMemTableTunerStats(storePath);
    }

    /**
     * @internal FOR TESTING ONLY. Retrieves statistics from the global Write Buffer Manager.
     */
    async debug_getWriteBufferManagerStats(): Promise<any> {
        const engine = this.getNativeEngine();
        if (!engine) throw new Error("Engine not available.");
        return engine.debug_getWriteBufferManagerStats();
    }
    
    /**
     * @internal FOR TESTING ONLY. Retrieves the total flush count for a specific LSM store.
     */
    async debug_getLsmFlushCount(storePath: string): Promise<number> {
        const engine = this.getNativeEngine();
        if (!engine) throw new Error("Engine not available.");
        return engine.debug_getLsmFlushCount(storePath);
    }
    /**
     * @internal FOR TESTING ONLY. Retrieves statistics from the RCU MemTable for
     * a specific LSM store, if it is using the RCU implementation.
     * @param storePath The path of the store to inspect.
     * @returns A promise resolving to an object with RCU statistics, or null if the store
     *          does not exist or is not using an RCU memtable.
     */
    async debug_getLsmRcuStats(storePath: string): Promise<RcuStatsJs | null> {
        const engine = this.getNativeEngine();
        if (!engine) {
            throw new Error("Indinis engine is not available for debug_getLsmRcuStats.");
        }

        const stats = await engine.debug_getLsmRcuStats(storePath);

        // The N-API layer returns a plain object. We need to convert numeric
        // string representations of uint64_t back into BigInts for type safety in JS.
        if (stats) {
            return {
                totalReads: BigInt(stats.totalReads),
                totalWrites: BigInt(stats.totalWrites),
                totalDeletes: BigInt(stats.totalDeletes),
                memoryReclamations: BigInt(stats.memoryReclamations),
                activeReaders: Number(stats.activeReaders),
                pendingReclamations: Number(stats.pendingReclamations),
                currentMemoryUsage: Number(stats.currentMemoryUsage),
                peakMemoryUsage: Number(stats.peakMemoryUsage),
            };
        }
        
        return null;
    }

    
    /**
     * Ingests a pre-built SSTable file into the specified store.
     * This is a high-performance method for bulk-loading data.
     * 
     * @param storePath The path of the store (e.g., 'users') to ingest the data into.
     * @param filePath The absolute path to the valid SSTable file.
     * @param options Configuration for the ingestion process.
     * @param options.moveFile If true (default), the source file is moved into the database directory (fast). 
     *                         If false, the file is copied, leaving the source file intact.
     * @returns A promise that resolves on successful ingestion.
     */
    async ingestFile(storePath: string, filePath: string, options: { moveFile?: boolean } = {}): Promise<void> {
        const engine = this.getNativeEngine();
        if (!engine) {
            throw new Error("Indinis engine is not initialized or has been closed.");
        }
        
        // Validate inputs
        if (!storePath || typeof storePath !== 'string') {
            throw new TypeError("storePath must be a non-empty string.");
        }
        if (!filePath || typeof filePath !== 'string') {
            throw new TypeError("filePath must be a non-empty string.");
        }
        
        const moveFile = options.moveFile ?? true;

        try {
            await engine.ingestFile_internal(storePath, filePath, moveFile);
        } catch (e: any) {
            // Re-throw with a more user-friendly message
            throw new Error(`Failed to ingest file '${filePath}' into store '${storePath}': ${e.message}`);
        }
    }
        /**
     * @internal FOR TESTING ONLY. Serializes a JavaScript value into the
     * C++ binary format for a ValueType, returned as a Buffer.
     * @param value The JavaScript value (string, number, boolean, Buffer).
     * @returns A Buffer containing the serialized C++ ValueType.
     */
    debug_serializeValue(value: any): Buffer {
        const engine = this.getNativeEngine();
        if (!engine) {
            throw new Error("Indinis engine is not available for serializing value.");
        }
        // This directly calls the N-API method we created.
        return engine.debug_serializeValue(value);
    }
    /**
     * @internal FOR TESTING ONLY. Creates a new SSTableBuilder instance.
     */
    debug_getSSTableBuilder(filepath: string, options: any): any {
        const engine = this.getNativeEngine();
        if (!engine) {
            throw new Error("Indinis engine is not available for creating a test SSTable builder.");
        }
        // This directly calls the N-API factory method we created.
        return engine.debug_getSSTableBuilder(filepath, options);
    }
    /**
     * @internal FOR TESTING ONLY. Analyzes disk fragmentation.
     * @returns A promise resolving to DefragmentationStatsJs object.
     */
    async debug_analyzeFragmentation(): Promise<DefragmentationStatsJs> { // Ensure DefragmentationStatsJs is exported or defined here
        const engine = this.getNativeEngine();
        if (!engine) throw new Error("Indinis engine is not available for debug_analyzeFragmentation.");
        const stats = await engine.debug_analyzeFragmentation();
        // N-API returns plain object, ensure numbers are numbers
        return {
            total_pages: Number(stats.total_pages),
            allocated_pages: Number(stats.allocated_pages),
            free_pages: Number(stats.free_pages),
            fragmentation_gaps: Number(stats.fragmentation_gaps),
            largest_gap: Number(stats.largest_gap),
            potential_pages_saved: Number(stats.potential_pages_saved),
            fragmentation_ratio: Number(stats.fragmentation_ratio),
        };
    }

    /**
     * @internal FOR TESTING ONLY. Pauses the RCU reclamation worker thread for the
     * specified store, allowing pending reclamations to accumulate for observation.
     * @param storePath The path of the RCU-enabled store.
     */
    async debug_pauseRcuReclamation(storePath: string): Promise<void> {
        const engine = this.getNativeEngine();
        if (!engine) {
            throw new Error("Indinis engine is not available for debug_pauseRcuReclamation.");
        }
        // The native method is synchronous, but we expose it as async for API consistency.
        await engine.debug_pauseRcuReclamation(storePath);
    }

    /**
     * @internal FOR TESTING ONLY. Resumes the RCU reclamation worker thread for the
     * specified store, allowing it to clean up pending nodes.
     * @param storePath The path of the RCU-enabled store.
     */
    async debug_resumeRcuReclamation(storePath: string): Promise<void> {
        const engine = this.getNativeEngine();
        if (!engine) {
            throw new Error("Indinis engine is not available for debug_resumeRcuReclamation.");
        }
        await engine.debug_resumeRcuReclamation(storePath);
    }
    /**
     * @internal FOR TESTING ONLY. Performs defragmentation.
     * @param mode 'CONSERVATIVE' or 'AGGRESSIVE'.
     * @returns A promise resolving to true if defragmentation was successful.
     */
    async debug_defragment(mode: 'CONSERVATIVE' | 'AGGRESSIVE'): Promise<boolean> {
        const engine = this.getNativeEngine();
        if (!engine) throw new Error("Indinis engine is not available for debug_defragment.");
        if (mode !== 'CONSERVATIVE' && mode !== 'AGGRESSIVE') {
            throw new TypeError("Invalid defragmentation mode. Must be 'CONSERVATIVE' or 'AGGRESSIVE'.");
        }
        return engine.debug_defragment(mode);
    }


    /**
     * Registers a columnar schema for a specific store path.
     * 
     * Registering a schema enables the columnar store for that path, which can significantly
     * accelerate analytical queries. This operation is idempotent; registering the same schema
     * version again will have no effect.
     *
     * @param schema The schema definition object.
     * @returns A promise that resolves to `true` if the schema was successfully registered.
     * @throws If the schema object is invalid or if there's a problem persisting it.
     */
    async registerStoreSchema(schema: ColumnSchemaDefinition): Promise<boolean> {
        const engine = this.getNativeEngine();
        if (!engine) {
            throw new Error("Indinis engine is not initialized or has been closed.");
        }

        // Input validation on the JS side before calling the native method.
        if (!schema || typeof schema !== 'object' || !schema.storePath || !Array.isArray(schema.columns)) {
            throw new TypeError("Invalid schema object provided. It must have 'storePath' and 'columns' properties.");
        }

        try {
            // Call the internal, N-API bound method.
            return await engine.registerStoreSchema_internal(schema);
        } catch (e: any) {
            console.error(`[Indinis] Native error during schema registration for store '${schema.storePath}': ${e.message}`);
            // Re-throw to make the user's promise reject.
            throw new Error(`Failed to register schema: ${e.message}`);
        }
    }

    /**
     * @internal FOR TESTING ONLY. Gets overall database statistics.
     * @returns A promise resolving to DatabaseStatsJs object.
     */
    async debug_getDatabaseStats(): Promise<DatabaseStatsJs> { // Ensure DatabaseStatsJs is exported or defined here
        const engine = this.getNativeEngine();
        if (!engine) throw new Error("Indinis engine is not available for debug_getDatabaseStats.");
        const stats = await engine.debug_getDatabaseStats();
        // N-API returns plain object, ensure numbers are numbers
        return {
            file_size_bytes: Number(stats.file_size_bytes),
            total_pages: Number(stats.total_pages),
            allocated_pages: Number(stats.allocated_pages),
            free_pages: Number(stats.free_pages),
            next_page_id: Number(stats.next_page_id),
            num_btrees: Number(stats.num_btrees),
            utilization_ratio: Number(stats.utilization_ratio),
        };
    }
    
    /**
     * @internal FOR TESTING/DEBUGGING ONLY. Retrieves statistics about the shared compaction thread pool.
     */
    async debug_getThreadPoolStats(): Promise<ThreadPoolStatsJs | null> {
        const engine = this.getNativeEngine();
        if (!engine) throw new Error("Engine not available.");
        return engine.debug_getThreadPoolStats_internal();
    }

    // --- New Checkpoint Methods ---
    async forceCheckpoint(): Promise<boolean> {
        const engine = this.getNativeEngine(); // CORRECTED: Use the getter
        if (!engine) throw new Error("Indinis engine is not available (closed or failed to init).");
        return engine.forceCheckpoint_internal();
    }

    /**
     * Changes the database's master password.
     * This operation re-encrypts the Data Encryption Key (DEK) with a new
     * Key Encryption Key (KEK) derived from the new password.
     * The actual data pages/blocks are NOT re-encrypted.
     * The database should ideally be quiescent (no active transactions) during this operation.
     *
     * @param oldPassword The current database password.
     * @param newPassword The new database password to set.
     * @param newKdfIterations Optional. The number of KDF iterations to use for deriving the
     *                         KEK from the newPassword. If 0 or undefined, a sensible default is used.
     *                         This new iteration count will be stored for future openings with newPassword.
     * @returns A promise resolving to true if the password was changed successfully, false otherwise.
     * @throws If the oldPassword is incorrect or an error occurs.
     */
    async changeDatabasePassword(oldPassword: string, newPassword: string, newKdfIterations?: number): Promise<boolean> {
        const engine = this.getNativeEngine();
        if (!engine) {
            throw new Error("Indinis engine is not available (closed or failed to init).");
        }
        if (typeof oldPassword !== 'string' || typeof newPassword !== 'string') {
            throw new TypeError("Old and new passwords must be strings.");
        }
        if (newPassword.length === 0) {
            throw new Error("New password cannot be empty.");
        }
        const iterations = (typeof newKdfIterations === 'number' && newKdfIterations > 0) ? newKdfIterations : 0;

        try {
            return await engine.changeDatabasePassword_internal(oldPassword, newPassword, iterations);
        } catch (e: any) {
            console.error(`[Indinis] Error calling changeDatabasePassword_internal: ${e.message}`, e);
            throw new Error(`Failed to change database password: ${e.message}`);
        }
    }

    async getCheckpointHistory(): Promise<CheckpointInfo[]> {
            const engine = this.getNativeEngine(); // CORRECTED: Use the getter
            if (!engine) throw new Error("Indinis engine is not available (closed or failed to init).");

            const rawHistory: any[] = await engine.getCheckpointHistory_internal();
            return rawHistory.map(h => ({
                checkpoint_id: Number(h.checkpoint_id),
                status: h.status as "NOT_STARTED" | "IN_PROGRESS" | "COMPLETED" | "FAILED",
                start_time: new Date(Number(h.start_time_ms)),
                end_time: new Date(Number(h.end_time_ms)),
                checkpoint_begin_wal_lsn: BigInt(h.checkpoint_begin_wal_lsn.toString()),
                checkpoint_end_wal_lsn: BigInt(h.checkpoint_end_wal_lsn.toString()),
                active_txns_at_begin: (h.active_txns_at_begin || []).map((id: any) => BigInt(id.toString())),
                error_msg: h.error_msg || "",
            }));
        }

   /**
     * @internal FOR TESTING ONLY. Scans raw composite keys in a BTree index.
     * Keys must be provided as binary strings.
     * @param indexName The name of the index BTree.
     * @param startKey The starting composite key (inclusive), as a binary string.
     * @param endKey The ending composite key (exclusive), as a binary string.
     * @returns A promise resolving to an array of raw composite keys as Buffers.
     */
    async debug_scanBTree(indexName: string, startKeyBuffer: Buffer, endKeyBuffer: Buffer): Promise<BTreeDebugScanEntry[]> {
        const engine = this.getNativeEngine();
        if (!engine) throw new Error("Indinis engine is not available for debug scan.");
        // --- THIS IS THE FIX ---
        // The C++ addon returns an array of objects. We cast the `any` return
        // to the correct, newly defined interface.
        const results = await engine.debug_scanBTree(indexName, startKeyBuffer, endKeyBuffer);
        return results as BTreeDebugScanEntry[];
    }

    /**
     * Gets a reference to a specific store path (collection or subcollection).
     * The path must have an odd number of segments.
     * @param storePath The path (e.g., 'users', 'users/user123/posts').
     * @returns A StoreRef instance for fluent operations on that path.
     * @throws If the storePath is invalid.
     */
    store<T extends { id?: string }>(storePath: string): StoreRef<T> {
        if (!storePath || typeof storePath !== 'string' || !isValidCollectionPath(storePath)) {
            throw new Error(`Invalid path for store(): "${storePath}". Store paths must have an odd number of segments.`);
        }
        return new StoreRef<T>(this, storePath);
    }

    /**
     * @internal Gets the underlying native engine instance. Avoid direct use.
     */
    getNativeEngine(): any | null {
        if (!this.nativeEngine) {
           // console.warn("Attempted to access native engine after close() or initialization failure.");
        }
        return this.nativeEngine;
    }

    /**
     * Closes the database connection, flushes data, stops background threads,
     * and releases native resources. It's recommended to call this before
     * your application exits gracefully.
     * Using the Indinis instance after calling close() will result in errors.
     * @returns A promise that resolves when the database has been closed.
     */
    async close(): Promise<void> {
        const engine = this.getNativeEngine();
        if (engine) {
            try {
                await engine.close_internal();
                this.nativeEngine = null; // Mark as closed on JS side
            } catch (e: any) {
                 console.error("[Indinis] Error during close_internal:", e);
                 // Still mark as closed even if native close had issues
                 this.nativeEngine = null;
                 throw e; // Re-throw? Or just log? Re-throwing might be better.
            }
        } else {
             // console.log("[Indinis] close() called, but engine already closed or not initialized.");
        }
    }

    /**
     * Executes database operations within an atomic transaction.
     *
     * @param callback An async function that receives the transaction context (ITransactionContext).
     * @returns A promise that resolves with the value returned by the callback function if the transaction commits successfully.
     * @throws Re-throws the error from the callback or from the engine if the transaction fails.
     */
    async transaction<T>(callback: TransactionCallback<T>): Promise<T> {
        const engine = this.getNativeEngine();
        if (!engine) {
            throw new Error("Indinis engine is not initialized or has been closed.");
        }

        let nativeTxn: INativeTransaction | null = null;
        try {
           nativeTxn = engine.beginTransaction_internal() as INativeTransaction;
           if (!nativeTxn) { throw new Error("Internal error: Failed to begin native transaction (returned null)."); }
        } catch (e: any) {
            console.error("[Indinis] Error beginning native transaction:", e);
            throw new Error(`Failed to begin transaction: ${e.message}`);
        }

        // Create the context object passed to the user's callback using async/await consistently.
        const txContext: ITransactionContext = {
            get: async (key: string) => {
                await Promise.resolve(); // Ensure async deferral
                if (!nativeTxn) throw new Error("Transaction context lost.");
                return nativeTxn.get(key);
            },
            
            set: async (key: string, value: StorageValue, options?: { overwrite?: boolean }) => {
                await Promise.resolve();
                if (!nativeTxn) throw new Error("Transaction context lost.");
                return nativeTxn.put(key, value, options);
            },

            getPrefix: async (prefix: string, limit?: number) => {
                await Promise.resolve();
                if (!nativeTxn) throw new Error("Transaction context lost.");
                return nativeTxn.getPrefix(prefix, limit);
            },

            remove: async (key: string) => {
                await Promise.resolve();
                if (!nativeTxn) throw new Error("Transaction context lost.");
                return nativeTxn.remove(key);
            },

            getId: async () => {
                await Promise.resolve();
                if (!nativeTxn) throw new Error("Transaction context lost.");
                return nativeTxn.getId();
            },

            query: async (
                storePath: string, 
                filters: FilterCondition[], 
                sortBy: SortByCondition | null, 
                aggPlan: AggregationPlan | null, // The missing parameter
                limit: number
            ): Promise<KeyValueRecord[]> => {
                await Promise.resolve();
                if (!nativeTxn) throw new Error("Transaction context lost.");
                // Pass all 5 arguments to the native layer
                return nativeTxn.query(storePath, filters, sortBy, aggPlan, limit); 
            },

            paginatedQuery: async (options) => {
                await Promise.resolve(); // Ensure async deferral
                if (!nativeTxn) throw new Error("Transaction context lost.");
                
                // The `paginatedQuery` method now exists on the native addon object.
                return nativeTxn.paginatedQuery(options);
            },
            
            update: async (key: string, operations: UpdateOperation) => {
                await Promise.resolve();
                if (!nativeTxn) throw new Error("Transaction context lost.");
                nativeTxn.update(key, operations); // Returns void
            }
        };

        // Execute user callback and handle commit/abort (this logic remains unchanged and correct)
        try {
            const result = await callback(txContext);
            const committed = nativeTxn.commit();
            if (!committed) {
                throw new Error("Transaction commit failed. Changes have been aborted.");
            }
            return result;
        } catch (error) {
            try {
                if (nativeTxn) {
                   nativeTxn.abort();
                }
            } catch (abortError: any) {
                console.error("[Indinis] Critical: Error during automatic transaction abort:", abortError);
            }
            throw error;
        }
    }




    // --- Indexing Methods ---

    /**
     * Creates a new secondary index on the specified collection path.
     * Index names must be unique across the entire database instance.
     * The index will apply only to documents directly within the specified `storePath`.
     *
     * @param storePath The path of the collection for which this index applies (e.g., 'users', 'users/user123/posts'). Must have an odd number of segments.
     * @param indexName A unique name for the index (e.g., 'usersByEmail', 'postsByTimestamp').
     * @param options An object specifying the field(s) and optionally the sort order(s).
     *   - Simple index: `{ field: 'fieldName', order?: 'asc'|'desc' }`
     *   - Compound index: `{ fields: ['field1', { name: 'field2', order: 'desc' }, ...] }`
     * @returns A promise resolving to true if the index was created successfully, false if an index with that name already exists.
     * @throws If the storePath is invalid, indexName is invalid, or options are invalid.
     */
    async createIndex(storePath: string, indexName: string, options: IndexOptions): Promise<'CREATED' | 'EXISTED'> {
        const engine = this.getNativeEngine();
        if (!engine) {
            throw new Error("Indinis engine is not available.");
        }

        // --- Input Validation ---
        if (!storePath || typeof storePath !== 'string') {
            throw new Error("storePath must be a non-empty string.");
        }
        if (!isValidCollectionPath(storePath)) {
            throw new Error(`Invalid store path for index creation: "${storePath}". Collection paths must have an odd number of segments.`);
        }
        if (!indexName || typeof indexName !== 'string') {
            throw new Error("Index name must be a non-empty string.");
        }
        if (!options || typeof options !== 'object') {
            throw new Error("Index options must be provided as an object.");
        }
         if (!('field' in options) && !('fields' in options)) {
            throw new Error("Index options object must contain either a 'field' (string) or 'fields' (array) property");
        }
        if ('field' in options && typeof options.field !== 'string') {
            throw new Error("Index option 'field' must be a string");
        }
         if ('fields' in options && (!Array.isArray(options.fields) || options.fields.length === 0)) {
            throw new Error("Index option 'fields' must be a non-empty array");
        }
        
        // --- Call Native Method ---
        try {
            // The N-API method now returns the status string directly.
            // The type assertion here tells TypeScript what to expect from the `any` return of the native addon.
            const status = await engine.createIndex_internal(storePath, indexName, options) as 'CREATED' | 'EXISTED';
            return status;
        } catch (e: any) {
             console.error(`[Indinis] Error calling createIndex_internal: ${e.message}`, e);
             throw new Error(`Failed to create index '${indexName}': ${e.message}`);
         }
    }

    /**
     * Lists index definitions.
     *
     * @param storePath Optional. If provided, lists only indexes defined *exactly* for that collection path. Must be a valid collection path (odd segments).
     * @returns A promise resolving to an array of IndexInfo objects. If storePath is provided, the array is filtered; otherwise, it contains all indexes in the database.
     * @throws If the provided storePath is invalid.
     */
    async listIndexes(storePath?: string): Promise<IndexInfo[]> {
        const engine = this.getNativeEngine();
        if (!engine) throw new Error("Indinis engine is not available.");

        if (storePath !== undefined) {
             if (typeof storePath !== 'string') throw new Error("storePath must be a string.");
             if (!isValidCollectionPath(storePath)) {
                throw new Error(`Invalid store path for listing indexes: "${storePath}". Collection paths must have an odd number of segments.`);
            }
        }

        try {
            // Get all indexes from the native layer
            const allIndexes: IndexInfo[] = await engine.listIndexes_internal();

            // Filter on the JS side if a storePath was provided
            if (storePath) {
                // Normalize paths slightly (remove trailing slash) for comparison
                const normalizedStorePath = storePath.endsWith('/') ? storePath.slice(0, -1) : storePath;
                return allIndexes.filter(indexDef => {
                    const normalizedIndexPath = indexDef.storePath.endsWith('/') ? indexDef.storePath.slice(0, -1) : indexDef.storePath;
                    return normalizedIndexPath === normalizedStorePath;
                });
            } else {
                return allIndexes; // Return all if no path specified
            }
        } catch (e: any) {
            console.error(`[Indinis] Error calling listIndexes_internal: ${e.message}`, e);
            throw new Error(`Failed to list indexes: ${e.message}`);
        }
    }

    /**
     * Deletes (drops) an existing secondary index by its unique name.
     *
     * @param indexName The globally unique name of the index to delete.
     * @returns A promise resolving to true if the index was found and deleted, false otherwise.
     * @throws If the indexName is invalid (empty string).
     */
    async deleteIndex(indexName: string): Promise<boolean> {
        const engine = this.getNativeEngine();
        if (!engine) throw new Error("Indinis engine is not available.");

        if (!indexName || typeof indexName !== 'string') {
            throw new Error("Index name must be a non-empty string.");
        }

         try {
            return await engine.deleteIndex_internal(indexName);
         } catch (e: any) {
             console.error(`[Indinis] Error calling deleteIndex_internal: ${e.message}`, e);
             throw new Error(`Failed to delete index '${indexName}': ${e.message}`);
         }
    }

    /**
     * Retrieves statistics about the internal data cache.
     * @returns A promise resolving to an object with cache statistics if stats are enabled, otherwise null.
     * Statistics include: hits, misses, evictions, expiredRemovals, hitRate.
     */
    async getCacheStats(): Promise<CacheStatsJs | null> { // Use the exported type
        const engine = this.getNativeEngine();
        if (!engine) throw new Error("Indinis engine is not available.");
        try {
            // The NAPI internal method should return an object matching CacheStatsJs or null
            const stats = await engine.getCacheStats_internal();
            return stats as CacheStatsJs | null; // Cast if necessary, ensure NAPI returns correct shape
        } catch (e: any) {
            throw new Error(`Failed to get cache stats: ${e.message}`);
        }
    }

    /**
     * Configures the cloud provider for this Indinis instance.
     * This must be called before using any cloud-enabled stores.
     * @param adapter An instance of a class that implements the CloudProviderAdapter interface.
     */
    public configureCloud(adapter: CloudProviderAdapter<any>): void {
        if (this.cloudAdapter) {
            console.warn("[Indinis] Cloud provider has already been configured. Overwriting the existing adapter.");
            if (this.outboxWorker) clearInterval(this.outboxWorker);
        }
        this.cloudAdapter = adapter;
        this.startOutboxWorker();
        this.emit('connection-state-changed', 'connected');
    }

    /**
     * Gets a reference to a cloud-enabled store path (collection or subcollection).
     * @param storePath The path (e.g., 'users').
     * @param ruleset Optional. A set of sync rules to override the defaults for this specific store.
     * @returns A CloudStoreRef instance for fluent, synchronized operations on that path.
     */
    public cloudStore<T extends { id?: string }>(
        storePath: string, 
        ruleset?: Partial<SyncRuleset<T>>
    ): CloudStoreRef<T> {
        if (!this.cloudAdapter) {
            throw new Error("Cloud provider has not been configured...");
        }
        // This merge now correctly provides defaults for any omitted properties.
        const finalRuleset = { ...defaultSyncRuleset, ...ruleset };
        return new CloudStoreRef<T>(this, this.cloudAdapter, storePath, finalRuleset as SyncRuleset<T>);
    }
    
    private startOutboxWorker(): void {
        if (this.outboxWorker) return; // Worker already running
        
        // <<< FIX IS HERE: Use a valid, single-segment path for the outbox store. >>>
        const outboxStore = this.store<OutboxRecord>('_outbox');
        
        const processOutbox = async () => {
            if (this.isSyncing || !this.cloudAdapter) return;
            this.isSyncing = true;
            
            try {
                const pendingItems = await outboxStore.take(25); // Process in batches
                if (pendingItems.length > 0) {
                    this.emit('sync-started');
                    console.log(`[Indinis Outbox] Found ${pendingItems.length} pending operations to sync.`);
                }

                for (const item of pendingItems) {
                    try {
                        const retryRuleset: SyncRuleset<any> = {
                            readStrategy: 'LocalFirst',
                            writeStrategy: 'LocalFirstWithOutbox',
                            onConflict: 'LastWriteWins'
                        };
                        
                        if (item.operation === 'set' && item.data) {
                            await this.cloudAdapter.set(item.key, JSON.parse(item.data), retryRuleset);
                        } else if (item.operation === 'remove') {
                            await this.cloudAdapter.remove(item.key);
                        }
                        // On success, remove the item from the outbox
                        await outboxStore.item(item.id!).remove();
                    } catch (e: any) {
                        // On persistent failure, update the error count
                        await outboxStore.item(item.id!).modify({
                            errorCount: (item.errorCount || 0) + 1,
                            lastError: e.message
                        });
                        this.emit('sync-error', { key: item.key, error: e });
                    }
                }
                
                if (pendingItems.length > 0 && (await outboxStore.take(1)).length === 0) {
                   this.emit('sync-completed');
                }
            } catch(e: any) {
                 console.error(`[Indinis Outbox] Error during outbox processing loop: ${e.message}`);
                 this.emit('sync-error', { error: e });
            } finally {
                this.isSyncing = false;
            }
        };

        this.outboxWorker = setInterval(processOutbox, 10000);
        this.outboxWorker.unref();
    }

} // End Indinis class

// Re-export all the types and fluent classes to maintain the public API
export * from './types';
export * from './fluent-api';

// Export the main class and relevant types
export default Indinis;

/**
 * Generates a sentinel value for atomically incrementing a numeric field
 * in a `modify()` operation.
 *
 * @param value The number to increment the field by (can be negative to decrement).
 * @returns A special object that represents the increment operation.
 */
export function increment(value: number): AtomicIncrementOperation {
    if (typeof value !== 'number' || !Number.isFinite(value)) {
        throw new TypeError("The value for increment() must be a finite number.");
    }
    // Now, the returned object is guaranteed to match the interface,
    // including the `unique symbol` type for its `$$indinis_op` property.
    return {
        $$indinis_op: IncrementSentinel,
        value: value
    };
}

/**
 * Creates an aggregation specification to calculate the sum of a numeric field.
 * @param field The name of the field to sum.
 * @returns An AggregationSpec object for use in the `.aggregate()` method.
 */
export function sum(field: string): AggregationSpec {
    if (!field || typeof field !== 'string') {
        throw new TypeError("The 'field' argument for sum() must be a non-empty string.");
    }
    return { op: 'SUM', field: field };
}

/**
 * Creates an aggregation specification to count the occurrences of a field.
 * Note: In the current backend, this counts non-null occurrences of the specified field.
 * @param field The name of the field to count.
 * @returns An AggregationSpec object for use in the `.aggregate()` method.
 */
export function count(field: string): AggregationSpec {
    if (!field || typeof field !== 'string') {
        throw new TypeError("The 'field' argument for count() must be a non-empty string.");
    }
    return { op: 'COUNT', field: field };
}

/**
 * Creates an aggregation specification to calculate the average of a numeric field.
 * @param field The name of the field to average.
 * @returns An AggregationSpec object for use in the `.aggregate()` method.
 */
export function avg(field: string): AggregationSpec {
    if (!field || typeof field !== 'string') {
        throw new TypeError("The 'field' argument for avg() must be a non-empty string.");
    }
    return { op: 'AVG', field: field };
}

/**
 * Creates an aggregation specification to find the minimum value of a field.
 * @param field The name of the field to find the minimum of.
 * @returns An AggregationSpec object for use in the `.aggregate()` method.
 */
export function min(field: string): AggregationSpec {
    if (!field || typeof field !== 'string') {
        throw new TypeError("The 'field' argument for min() must be a non-empty string.");
    }
    return { op: 'MIN', field: field };
}

/**
 * Creates an aggregation specification to find the maximum value of a field.
 * @param field The name of the field to find the maximum of.
 * @returns An AggregationSpec object for use in the `.aggregate()` method.
 */
export function max(field: string): AggregationSpec {
    if (!field || typeof field !== 'string') {
        throw new TypeError("The 'field' argument for max() must be a non-empty string.");
    }
    return { op: 'MAX', field: field };
}