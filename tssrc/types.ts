// @tssrc/types.ts
import { FormGuardSchema } from './formguard/types';

// --- Core Types ---
export interface ExtWALManagerConfigJs { // Suffix with Js to distinguish if C++ names are identical
    wal_directory?: string;
    wal_file_prefix?: string;
    segment_size?: number; // Represent uint64_t as number in JS
    // buffer_size?: number; // This was commented out as not directly used in C++ config for buffer limits
    background_flush_enabled?: boolean;
    flush_interval_ms?: number; // Represent uint32_t as number
    sync_on_commit_record?: boolean;
}

export interface INativeIndinis {
    // ...
    close_internal(): Promise<void>;
    ingestFile_internal(storePath: string, filePath: string, moveFile: boolean): Promise<boolean>;
}

export interface CacheOptionsJs {
    maxSize?: number;
    policy?: "LRU" | "LFU"; // Assuming Cache lib supports these as strings or needs mapping
    defaultTTLMilliseconds?: number;
    enableStats?: boolean;
    // shardCount is not used by the provided Cache implementation.
}

/**
 * @internal FOR TESTING/DEBUGGING ONLY.
 * Represents the raw key/value pair returned from a B-Tree scan.
 */
export interface BTreeDebugScanEntry {
    key: Buffer;
    value: Buffer;
}

/**
 * @internal FOR TESTING ONLY.
 * Statistics about the LSM-Tree's internal versioning system.
 */
export interface LsmVersionStats {
    currentVersionId: number;
    liveVersionsCount: number;
    sstablesPerLevel: number[];
}

/**
 * @internal FOR TESTING ONLY.
 * Statistics about a single partition within an active memtable.
 */
export interface LsmPartitionStats {
    partitionId: number;
    writeCount: number;
    readCount: number;
    currentSizeBytes: number;
}

/**
 * @internal FOR TESTING ONLY.
 * Statistics from the MemTableTuner for a specific LSM store.
 */
export interface MemTableTunerStats {
    currentTargetSizeBytes: number;
    smoothedWriteRateBps: number;
}

/**
 * @internal FOR TESTING ONLY.
 * Statistics from the global Write Buffer Manager.
 */
export interface WriteBufferManagerStats {
    currentMemoryUsageBytes: number;
    bufferSizeBytes: number;
    immutableMemtablesCount: number;
}
/**
 * @internal FOR TESTING ONLY.
 * Statistics about an active memtable for a specific LSM store.
 */
export interface LsmStoreStats {
    totalSizeBytes: number;
    recordCount: number;
    partitions: LsmPartitionStats[];
}

export interface CacheStatsJs { // Renamed to match usage, and EXPORTED
    hits: number;
    misses: number;
    evictions: number;
    expiredRemovals: number;
    hitRate: number;
}

export interface DefragmentationStatsJs {
    total_pages: number;
    allocated_pages: number;
    free_pages: number;
    fragmentation_gaps: number;
    largest_gap: number;
    potential_pages_saved: number;
    fragmentation_ratio: number;
}

export interface DatabaseStatsJs {
    file_size_bytes: number;
    total_pages: number;
    allocated_pages: number;
    free_pages: number;
    next_page_id: number; // This refers to the next page to allocate IF freelist empty
    num_btrees: number;
    utilization_ratio: number;
}


/**
 * Supported value types for storage.
 */
export type StorageValue = number | string | Buffer | boolean;

/** Type for the objects returned by getPrefix */
export interface KeyValueRecord {
    key: string;
    value: StorageValue;
}

export interface CheckpointInfo {
    checkpoint_id: number; // or bigint if IDs can exceed JS safe integer
    status: "NOT_STARTED" | "IN_PROGRESS" | "COMPLETED" | "FAILED";
    start_time: Date;
    end_time: Date;
    checkpoint_begin_wal_lsn: bigint; // LSNs are uint64_t
    checkpoint_end_wal_lsn: bigint;
    active_txns_at_begin: bigint[]; // TxnIDs are uint64_t
    error_msg: string;
}

export interface EncryptionOptionsJs {
    /**
     * Master password to derive the Database Encryption Key (DEK).
     * If not provided or empty, page-level encryption will be disabled.
     */
    password?: string;
    /**
     * Default: "AES256_GCM_PBKDF2" if password provided, "NONE" otherwise.
     */
    scheme?: "NONE" | "AES256_CBC_PBKDF2" | "AES256_GCM_PBKDF2";
    // Note: Salt is managed internally by C++ (generated or loaded from db.salt)
        /**
     * Number of iterations for PBKDF2.
     * Only applies if password and a PBKDF2 scheme are used.
     * Default will be used if not provided or <= 0.
     * WARNING: Changing this for an existing DB requires re-keying or providing the original iteration count.
     */
    kdfIterations?: number;
}

/**
 * Defines the comparison operators available for query filters.
 * These string values are passed to the N-API layer.
 */
export type FilterOperator =
  | '=='
  | '<'
  | '<='
  | '>'
  | '>='
  | 'array-contains'     
  | 'array-contains-any'; 

/**
 * Represents a single filter condition applied to a query.
 * e.g., { field: 'age', operator: '>=', value: 30 }
 */
export interface FilterCondition {
    field: string;
    operator: FilterOperator;
    value: StorageValue | StorageValue[];
}

/**
 * Interface for the transaction context passed to the user's callback.
 * This is the primary way users interact with data using the core API.
 */
export interface ITransactionContext {
  /**
   * Retrieves a value by its key within the current transaction.
   * @param key The key of the item to retrieve.
   * @returns A promise resolving to the stored value, or null if not found or deleted.
   */
  get(key: string): Promise<StorageValue | null>;

  /**
   * Creates or overwrites an item with the given key and value within the transaction.
   * @param key The key of the item.
   * @param value The value to store (number, string, or Buffer).
   * @param options Optional settings, like `overwrite`.
   * @returns A promise resolving to true if the operation was successfully added to the transaction's write set.
   */
  set(key: string, value: StorageValue, options?: { overwrite?: boolean }): Promise<boolean>;

  /**
   * Retrieves multiple records whose keys are direct children under the given prefix path.
   * @param prefix The key prefix to scan for.
   * @param limit Optional. The maximum number of items to return.
   * @returns A promise resolving to an array of objects `{ key: string; value: StorageValue }`.
   */
  getPrefix(prefix: string, limit?: number): Promise<KeyValueRecord[]>;

  /**
   * Marks an item for deletion within the transaction.
   * @param key The key of the item to remove.
   * @returns A promise resolving to true if the item exists and was marked for deletion, false otherwise.
   */
  remove(key: string): Promise<boolean>;

  /**
   * Gets the underlying transaction ID.
   * @returns A promise resolving to the transaction ID.
   */
  getId(): Promise<number>;

    /**
   * @internal
   * Performs an atomic update operation on a document.
   */
  update(key: string, operations: UpdateOperation): Promise<void>;

  /**
   * @internal
   * Executes a structured query against the database within the transaction.
   * This is called internally by the Query object and is not intended for direct use.
   * @param storePath The collection path to query.
   * @param filters An array of filter conditions.
   * @param limit The maximum number of results to return.
   * @returns A promise resolving to an array of raw KeyValueRecord objects.
   */
    query(
        storePath: string, 
        filters: FilterCondition[], 
        sortBy: SortByCondition | null, 
        aggPlan: AggregationPlan | null, // The new parameter
        limit: number
    ): Promise<KeyValueRecord[]>;

    paginatedQuery(options: {
        storePath: string;
        filters: FilterCondition[];
        orderBy: OrderByClause[];
        limit: number;
        startAfter?: Cursor;
        startAt?: Cursor;
        endBefore?: Cursor;
        endAt?: Cursor;
    }): Promise<PaginatedQueryResult<KeyValueRecord>>;
}

/**
 * Callback function type for database transactions.
 * If the callback resolves, the transaction is committed.
 * If the callback rejects (throws an error), the transaction is automatically aborted.
 * @param tx The transaction context object to perform operations.
 * @returns A promise resolving with the result of the transaction logic when the transaction commits.
 */
export type TransactionCallback<T = void> = (tx: ITransactionContext) => Promise<T>;

// Internal representation of cache options for NAPI binding
export interface CacheOptionsJsInternal {
    maxSize?: number;
    policy?: "LRU" | "LFU";
    defaultTTLMilliseconds?: number;
    enableStats?: boolean;
    // shardCount is not part of the provided C++ cache lib config directly
}

// Internal interface representing the native transaction object wrapper provided by the addon
export interface INativeTransaction {
  // --- Existing Methods ---
  put(key: string, value: StorageValue, options?: { overwrite?: boolean }): boolean;
  get(key: string): StorageValue | null;
  getPrefix(prefix: string, limit?: number): KeyValueRecord[];
  remove(key: string): boolean;
  commit(): boolean;
  abort(): void;
  getId(): number;
/**
   * @internal
   * The native binding for performing atomic updates.
   */
  update(key: string, operations: UpdateOperation): void;
  
  // --- FIX: Add the required 'query' method signature ---
  /**
   * @internal
   * The native binding for executing a structured query.
   * @param storePath The collection path to query.
   * @param filters An array of filter condition objects.
   * @param limit The maximum number of results to return.
   * @returns An array of raw KeyValueRecord objects matching the query.
   */
    query(
        storePath: string,
        filters: FilterCondition[],
        sortBy: SortByCondition | null,
        aggPlan: AggregationPlan | null, // The new parameter
        limit: number
    ): Promise<KeyValueRecord[]>;
    paginatedQuery(options: any): Promise<PaginatedQueryResult<KeyValueRecord>>;
}

// --- Indexing Types ---

/** Defines sort order for an index field. */
export type IndexSortOrder = 'asc' | 'desc';

/** Structure for defining a single field within an index. */
export interface IndexFieldOptions {
    name: string;
    order?: IndexSortOrder;
}

/**
 * Options for creating an index.
 * 
 * @property unique - If true, enforces that the value for the indexed field(s) must be unique across all documents in the collection. Defaults to false.
 * @property field - For a simple index, the name of the field to index.
 * @property fields - For a compound index, an array of field names or field option objects.
 */
export type IndexOptions = {
    unique?: boolean; 
    multikey?: boolean;
} & ({ field: string; order?: IndexSortOrder } | { fields: (string | IndexFieldOptions)[] });

/** Information about a single field within a retrieved index definition. */
export interface IndexFieldInfo {
    name: string;
    order: IndexSortOrder;
}

/** Information about a defined index retrieved via `listIndexes`. */
export interface IndexInfo {
    name: string;
    storePath: string;
    fields: IndexFieldInfo[];
}



/**
 * Defines the desired sort order for a query.
 */
export interface SortByCondition {
    field: string;
    direction: 'asc' | 'desc';
}

// Forward-declare FilterBuilder to break dependency cycle
// This tells TypeScript "a class with this name and shape exists somewhere".
export interface IFilterBuilder<T> {
    equals(value: StorageValue): IQuery<T>;
    greaterThan(value: StorageValue): IQuery<T>;
    greaterThanOrEqual(value: StorageValue): IQuery<T>;
    lessThan(value: StorageValue): IQuery<T>;
    lessThanOrEqual(value: StorageValue): IQuery<T>;
    arrayContains(value: StorageValue): IQuery<T>;
    arrayContainsAny(values: StorageValue[]): IQuery<T>;
}

/**
 * Defines the supported aggregation operations.
 * These string values must be recognized by the C++ engine.
 */
export type AggregationOperator = 'SUM' | 'COUNT' | 'AVG' | 'MIN' | 'MAX';

/**
 * Defines a single aggregation to be performed.
 * e.g., { op: 'SUM', field: 'units_sold' }
 */
export interface AggregationSpec {
    op: AggregationOperator;
    field: string; // The field to aggregate
}

/**
 * The map of aggregations passed to the .aggregate() method.
 * The key is the desired name for the result field.
 * e.g., { total_sales: { op: 'SUM', field: 'price' } }
 */
export type AggregationMap = {
    [resultFieldName: string]: AggregationSpec;
};

/**
 * @internal
 * The full plan sent to the native layer.
 */
export interface AggregationPlan {
    groupBy: string[];
    aggregations: AggregationMap;
}


/**
 * Defines the public interface for a query object.
 * This allows for type-safe query composition without circular dependencies.
 */
export interface IQuery<T> {
    /**
     * Adds a filter condition to the query.
     * @param field The document field to filter on.
     * @returns A FilterBuilder to continue building the query.
     */
    filter(field: keyof T & string): IFilterBuilder<T>;

    /**
     * Applies a reusable query part to the current query.
     * @param part A function that takes a query and returns a modified query.
     */
    use(part: QueryPart<T>): IQuery<T>;

    /**
     * Specifies a sort order for the query results. Primarily used as a hint for
     * index selection in non-paginated queries.
     * @param field The document field to sort by.
     * @param direction The sort direction ('asc' or 'desc').
     */
    sortBy(field: keyof T & string, direction?: 'asc' | 'desc'): IQuery<T>;
    
    // --- NEW: Add all pagination method signatures to the interface ---

    /**
     * Adds a sort order clause to the query. Can be called multiple times for compound sorting.
     * The order of calls defines the sort priority.
     * @param field The document field to sort by.
     * @param direction The sort direction ('asc' or 'desc').
     */
    orderBy(field: keyof T & string, direction?: 'asc' | 'desc'): IQuery<T>;

    /**
     * Sets the maximum number of documents to return in a page.
     * @param count The number of documents.
     */
    limit(count: number): IQuery<T>;

    /**
     * Creates a new query that starts after the provided cursor.
     * @param cursorValues The document snapshot or an array of values matching the orderBy clauses.
     */
    startAfter(...cursorValues: Cursor | [T]): IQuery<T>;
    
    /**
     * Creates a new query that starts at the provided cursor (inclusive).
     * @param cursorValues The document snapshot or an array of values matching the orderBy clauses.
     */
    startAt(...cursorValues: Cursor | [T]): IQuery<T>;

    /**
     * Creates a new query that ends before the provided cursor (exclusive).
     * @param cursorValues The document snapshot or an array of values matching the orderBy clauses.
     */
    endBefore(...cursorValues: Cursor | [T]): IQuery<T>;

    /**
     * Creates a new query that ends at the provided cursor (inclusive).
     * @param cursorValues The document snapshot or an array of values matching the orderBy clauses.
     */
    endAt(...cursorValues: Cursor | [T]): IQuery<T>;
    
    // --- END NEW ---


    /**
     * Specifies fields to group results by, turning this into an aggregation query.
     */
    groupBy(...fields: (keyof T & string)[]): IQuery<T>;

    /**
     * Specifies aggregation operations to perform.
     */
    aggregate(aggregations: AggregationMap): IQuery<any>;

    /**
     * Executes the query and returns the first matching document.
     * Cannot be used with aggregation queries.
     */
    one(): Promise<T | null>;

    /**
     * Executes the query and returns an array of matching documents or aggregation results.
     * For non-paginated queries.
     */
    take(limit?: number): Promise<any[]>;
    
    /**
     * Executes a paginated query.
     * Requires .orderBy() and .limit() to have been called.
     * @returns A promise resolving to a PaginatedQueryResult object.
     */
    get(): Promise<PaginatedQueryResult<T>>;
}


/**
 * Represents a reusable function that can be applied to a query.
 * It takes a query object conforming to IQuery<T> and returns one.
 * 
 * @template T The document type of the query.
 * @param query The query object to modify.
 * @returns The modified query object for chaining.
 */
export type QueryPart<T> = (query: IQuery<T>) => IQuery<T>;



/**
 * Represents a sentinel value for an atomic increment operation.
 * @internal
 */
export const IncrementSentinel = Symbol.for('indinis.increment');

/**
 * Represents an atomic update command.
 * @internal
 */
export interface AtomicIncrementOperation {
  $$indinis_op: typeof IncrementSentinel;
  value: number;
}

/**
 * A map of field names to the atomic update operation to perform on them.
 */
export type UpdateOperation = {
  [field: string]: AtomicIncrementOperation;
};

/**
 * Defines the data type for a column in a columnar schema.
 * These string values must match the C++ ColumnType enum.
 */
export enum ColumnType {
    STRING = "STRING",
    INT64 = "INT64",
    DOUBLE = "DOUBLE",
    BOOLEAN = "BOOLEAN",
    // Add other types as they are supported
}

/**
 * Defines the structure for a single column within a schema.
 */
export interface ColumnDefinition {
    name: string;
    column_id: number;
    type: ColumnType;
    nullable?: boolean;
}

export interface UniversalCompactionOptionsJs {
    sizeRatioPercent?: number;
    minMergeWidth?: number;
    maxMergeWidth?: number;
    l0CompactionTrigger?: number;
    l0SlowdownTrigger?: number;
    l0StopTrigger?: number;
    maxSpaceAmplification?: number;
}

/**
 * Represents the complete schema definition for a specific store path.
 * This is the object you pass to `db.registerStoreSchema()`.
 */
export interface ColumnSchemaDefinition {
    storePath: string;
    schemaVersion?: number;
    columns: ColumnDefinition[];
}

export type MemTableType = 'SkipList' | 'Vector' | 'Hash' | 'PrefixHash' | 'CompressedSkipList'| 'RCU';

export interface LsmOptions {
    /**
     * Specifies the default in-memory data structure for new stores.
     * - 'SkipList': (Default) Balanced performance for general-purpose workloads.
     * - 'Vector': Optimized for read-heavy workloads or sequential writes. Slower writes.
     * - 'Hash': Fastest point lookups (get/set). Does NOT support prefix scans.
     * - 'PrefixHash': Fast point lookups and efficient prefix scans. Higher memory usage.
     * - 'CompressedSkipList': A SkipList that compresses values to save memory, at the cost of CPU.
     * - 'RCU': Highest read concurrency via a lock-free design. Ideal for extreme read-heavy workloads.
     */
    defaultMemTableType?: MemTableType;

    /**
     * @internal FOR TESTING ONLY.
     * Specific configuration for the RCU MemTable.
     */
    rcuOptions?: RcuMemTableOptions;

    //  Add the compaction strategy option >>>
    /**
     * Selects the compaction algorithm for the LSM-Tree. Different strategies
     * offer trade-offs between read, write, and space amplification.
     * - 'LEVELED': (Default) Optimizes for read performance and predictable space usage. Ideal for read-heavy or mixed workloads.
     * - 'UNIVERSAL': Optimizes for write performance by reducing write amplification. Can use more space. Ideal for write-heavy workloads.
     * - 'FIFO': Discards entire old files based on a time-to-live (TTL). Does not merge data. Ideal for caches or ephemeral time-series data.
     */
    compactionStrategy?: 'LEVELED' | 'UNIVERSAL' | 'FIFO' | 'HYBRID'; 


    /** Configuration options for the 'LEVELED' compaction strategy. */
    leveledCompactionOptions?: LeveledCompactionOptionsJs;

    /** Configuration options for the 'UNIVERSAL' compaction strategy. */
    universalCompactionOptions?: UniversalCompactionOptionsJs;

    fifoCompactionOptions?: {
        ttlSeconds?: number;
        maxFilesToDeleteInOneGo?: number;
    };

    hybridCompactionOptions?: HybridCompactionOptionsJs;

}
export interface HybridCompactionOptionsJs {
    /** The configuration for the Leveled strategy, used for lower levels. */
    leveledOptions?: LeveledCompactionOptionsJs;
    /** The configuration for the Universal strategy, used for deeper levels. */
    universalOptions?: UniversalCompactionOptionsJs;
    /** The level at which the strategy switches from Leveled to Universal. 
     *  E.g., a value of 3 means L0, L1, and L2 use Leveled, while L3 and deeper use Universal.
     *  Defaults to 3.
     */
    leveledToUniversalThreshold?: number;
}

/**
 * @internal FOR TESTING ONLY.
 * Configuration options specific to the RCU MemTable.
 */
export interface RcuMemTableOptions {
    gracePeriodMilliseconds?: number;
    memoryThresholdBytes?: number;
}

export interface LeveledCompactionOptionsJs {
    l0CompactionTrigger?: number;
    l0SlowdownTrigger?: number;
    l0StopTrigger?: number;
    sstableTargetSizeBytes?: number; // Expose as bytes for clarity
    levelSizeMultiplier?: number;
    maxCompactionBytes?: number;
    maxFilesPerCompaction?: number;
    enableParallelCompaction?: boolean; // Though not used by current strategy, good for future
}

/**
 * @internal FOR TESTING ONLY.
 * Statistics from the RCU MemTable.
 */
export interface RcuStatsJs {
    totalReads: bigint;
    totalWrites: bigint;
    totalDeletes: bigint;
    memoryReclamations: bigint;
    activeReaders: number;
    pendingReclamations: number;
    currentMemoryUsage: number;
    peakMemoryUsage: number;
}

export interface ThreadPoolStatsJs {
  name: string;
  minThreads: number;
  maxThreads: number;
  currentThreadCount: number;
  activeThreadCount: number;
  totalPendingTasks: number;
}

export interface IndinisOptions {
    checkpointIntervalSeconds?: number;
     walOptions?: ExtWALManagerConfigJs;
         enableBTreeCompression?: boolean;
    btreeCompressionType?: "NONE" | "ZSTD"; // Map to CompressionType enum
    enableSSTableCompression?: boolean;

     sstableDataBlockUncompressedSizeKB?: number; // Uncompressed size of data blocks in KB
    sstableCompressionType?: "NONE" | "ZSTD" | "LZ4"; // Type of compression for SSTable blocks
    sstableCompressionLevel?: number;            // Compression level (e.g., for ZSTD)
    encryptionOptions?: EncryptionOptionsJs;
    // ---  Cache Options ---
    enableCache?: boolean;
    cacheOptions?: CacheOptionsJsInternal;
    // --- END  Cache Options ---
    lsmTreeMemtableMaxSizeBytes?: number; 
    lsmOptions?: LsmOptions;
    /** The minimum number of threads to keep in the compaction pool. Defaults to 2. */
    minCompactionThreads?: number;
    /** The maximum number of threads the compaction pool can scale up to. Defaults to hardware concurrency. */
    maxCompactionThreads?: number;
}
// <<< NEW: Cloud Provider and Synchronization Interfaces >>>

/**
 * A function that can be called to unsubscribe from a real-time listener.
 * This is typically returned by the cloud adapter's onSnapshot method.
 */
export type UnsubscribeFunction = () => void;

/**
 * Defines the contract for a cloud provider adapter. External packages (like @indinis/adapter-firestore)
 * must implement this interface to be compatible with the Indinis Cloud Federation Layer.
 */
export interface CloudProviderAdapter<T extends { id?: string }> {
    /** Fetches a single document from the cloud backend. */
    get(key: string): Promise<T | null>;
    
    /** 
     * Sets or overwrites a document in the cloud. This method is responsible
     * for implementing the conflict resolution strategy defined in the ruleset.
     */
    set(key: string, data: any, ruleset: SyncRuleset<T>): Promise<void>;
    
    /** Deletes a document from the cloud backend. */
    remove(key: string): Promise<void>;

    /**
     * Establishes a real-time listener for a document from the cloud.
     * @param key The key of the document to listen to.
     * @param onUpdate A callback that fires with the new data whenever it changes in the cloud.
     * @returns An UnsubscribeFunction to stop the listener.
     */
    onSnapshot(key: string, onUpdate: (data: any | null) => void): UnsubscribeFunction;
}

/**
 * An interface for providing custom, user-defined data validation and sanitization logic.
 */
export interface Validator<T> {
    /**
     * Validates and potentially sanitizes the input data.
     * @param data The data object to be written.
     * @returns The validated (and possibly sanitized) data object.
     * @throws An error if validation fails.
     */
    validate(data: Partial<T>): T;
}

/**
 * A declarative ruleset that governs how a CloudStoreRef synchronizes
 * with a cloud backend.
 */
export interface SyncRuleset<T extends { id?: string }> {
    /**
     * Determines where to fetch data from.
     * - 'LocalFirst': (Default) Tries local DB first. If not found, fetches from cloud and caches locally.
     * - 'CloudFirst': Always fetches from the cloud to ensure latest data, then updates local cache.
     * - 'LocalOnly': Never contacts the cloud for reads.
     * - 'CloudOnly': Never reads from the local DB; always fetches from the cloud.
     */
    // <<< FIX IS HERE: Add the missing 'CloudOnly' option >>>
    readStrategy: 'LocalFirst' | 'CloudFirst' | 'LocalOnly' | 'CloudOnly';

    /**
     * Governs the behavior of write operations.
     * - 'LocalFirstWithOutbox': (Default) Commits locally first for speed and queues cloud sync.
     * - 'TwoPhaseCommit': Writes to the cloud first. Fails the operation if the cloud is unreachable.
     */
    writeStrategy: 'LocalFirstWithOutbox' | 'TwoPhaseCommit';
    
    onConflict: 'LastWriteWins' | 'CloudWins' | ((local: T, cloud: T) => T | Promise<T>);
    validator?: FormGuardSchema<T> | Validator<T>;
    toCloud?: (data: T) => any;
    fromCloud?: (cloudData: any) => T;
}

/**
 * @internal
 * Represents a failed operation stored in the local outbox for later retry.
 */
export interface OutboxRecord {
    id?: string;
    key: string;
    operation: 'set' | 'remove';
    data?: string; // JSON string of the data for a 'set' operation
    timestamp: number;
    errorCount: number;
    lastError?: string;
}

// --- NEW: Pagination and Sorting Types ---

/**
 * Represents the cursor for pagination. It's an array of values corresponding
 * to the fields in the orderBy clauses.
 */
export type Cursor = any[];

/**
 * Defines a single ordering clause for a query.
 */
export interface OrderByClause {
    field: string;
    direction: 'asc' | 'desc';
}

/**
 * The structured result for a paginated query, including documents and cursors.
 * @template T The type of the documents in the result set.
 */
export interface PaginatedQueryResult<T> {
    /** The documents for the current page. */
    docs: T[];
    /** A boolean indicating if there is a next page of results. */
    hasNextPage: boolean;
    /** A boolean indicating if there is a previous page of results. */
    hasPrevPage: boolean;
    /** The cursor pointing to the start of the current page. */
    startCursor?: Cursor;
    /** The cursor pointing to the end of the current page, used to fetch the next page. */
    endCursor?: Cursor;
}

/**
 * @internal
 * Defines the types of operations that can be included in a batch.
 * These string values are passed to the C++ layer.
 */
export enum BatchOperationType {
    MAKE = 'make',
    MODIFY = 'modify',
    REMOVE = 'remove',
}

/**
 * @internal
 * Represents a single operation queued within a BatchWriter before being sent to C++.
 */
export interface BatchOperation {
    op: BatchOperationType;
    key: string;
    value?: any; // Data for make/modify. For modify, this is a partial object.
    overwrite?: boolean; // Flag for make
}



