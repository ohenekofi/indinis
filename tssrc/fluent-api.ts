/**
 * @file tssrc/fluent-api.ts
 * @description Implementation of the Indinis fluent API classes: StoreRef, ItemRef, Query, and FilterBuilder.
 */

import { Indinis } from './index';
import {
    SortByCondition, KeyValueRecord, StorageValue, FilterCondition,
    FilterOperator, IndexInfo, QueryPart, IFilterBuilder, IQuery,
    AtomicIncrementOperation, IncrementSentinel, UpdateOperation,
    AggregationPlan, AggregationMap, OrderByClause, Cursor, PaginatedQueryResult
} from './types';
import { constructKey, countPathSegments, generateId, isValidCollectionPath, isValidDocumentPath } from './utils';
// --- Fluent API Classes ---

class MakeOperation<T extends { id?: string }> {
    private executionPromise: Promise<{ id: string, document: T }> | null = null;

    constructor(
        private readonly db: Indinis,
        private readonly storePath: string,
        private readonly data: Omit<T, 'id'>
    ) {}

    /**
     * Executes the database operation if it hasn't been run yet, and memoizes the result.
     * @returns A promise that resolves to an object containing the new ID and the full document.
     */
    private execute(): Promise<{ id: string, document: T }> {
        if (!this.executionPromise) {
            this.executionPromise = (async () => {
                const newId = generateId();
                const finalDoc = { ...this.data, id: newId } as T;

                await this.db.transaction(async tx => {
                    // Using internal set for directness. This is equivalent to item(newId).make(...)
                    await tx.set(`${this.storePath}/${newId}`, JSON.stringify(finalDoc));
                });
                
                return { id: newId, document: finalDoc };
            })();
        }
        return this.executionPromise;
    }

    /**
     * Makes the object "thenable" for `await`.
     * By default, awaiting this operation resolves to the document ID.
     */
    then<TResult1 = string, TResult2 = never>(
        onfulfilled?: ((value: string) => TResult1 | PromiseLike<TResult1>) | null,
        onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null
    ): Promise<TResult1 | TResult2> {
        return this.execute().then(result => result.id).then(onfulfilled, onrejected);
    }

    /**
     * Executes the operation and returns a promise that resolves with the full document,
     * including its newly generated ID.
     * @returns A promise resolving to the complete document.
     */
    public withSnapshot(): Promise<T> {
        return this.execute().then(result => result.document);
    }
}
/**
 * Represents a reference to a specific document/item.
 * The full path (storePath + itemId) must have an even number of segments.
 */
export class ItemRef<T extends { id?: string }> {
    public readonly fullPath: string;

    constructor(
        private readonly dbInstance: Indinis,
        public readonly storePath: string, // Path to the parent store (odd segments)
        public readonly itemId: string    // ID of the item within the store
    ) {
        if (!itemId) { throw new Error("Item ID cannot be empty."); }
        this.fullPath = constructKey(storePath, itemId);
         if (!isValidDocumentPath(this.fullPath)) {
            throw new Error(`Invalid path for ItemRef: "${this.fullPath}". Document paths must have an even number of segments (e.g., collection/docId). Found ${countPathSegments(this.fullPath)} segments.`);
        }
    }

    /**
     * Creates or overwrites a document with the provided data at this specific path.
     * @param data The data for the document. The 'id' field, if present, will be overwritten with this item's ID.
     * @param overwrite If false (default), the operation will fail if a document already exists at this path. If true, it will overwrite the existing document completely.
     * @returns A promise that resolves when the operation is complete.
     * @throws If overwrite is false and a document already exists.
     */
    async make(data: Omit<T, 'id'>, overwrite: boolean = false): Promise<void> {
        // Automatically add/overwrite the ID to the data object to ensure consistency.
        const dataToStore = { ...data, id: this.itemId };
        
        await this.dbInstance.transaction(async (tx) => {
            const internalTx = tx as any; // Cast to access internal methods
            const success = await internalTx.set(this.fullPath, JSON.stringify(dataToStore), { overwrite });

            if (!success) {
                // This condition is met if overwrite is false and the document exists.
                throw new Error(`Failed to create document at "${this.fullPath}": A document with this ID already exists.`);
            }
        });
    }

     /**
      * Sets the document's content, overwriting it completely if it exists,
      * or creating it if it doesn't. Alias for `make`.
      * Ensures the ID in the data matches the ItemRef ID.
      */
     async set(data: T): Promise<void> {
         if (data.id && data.id !== this.itemId) {
             console.warn(`[Indinis] Warning: Data ID '${data.id}' in set() differs from ItemRef ID '${this.itemId}'. Using ItemRef ID.`);
         }
         const dataToStore = { ...data, id: this.itemId };
         await this.dbInstance.transaction(async tx => {
             await tx.set(this.fullPath, JSON.stringify(dataToStore));
         });
     }

    /**
     * Fetches the document data.
     * @returns A promise resolving to the document data (object T), or null if the document does not exist.
     */
    async one(): Promise<T | null> {
        return this.dbInstance.transaction(async tx => {
            const raw = await tx.get(this.fullPath);
            if (raw === null) return null;
            try {
                // Assume Buffer is always JSON string
                const jsonString = (typeof raw === 'string') ? raw : (raw as Buffer).toString('utf-8');
                return JSON.parse(jsonString) as T;
            } catch (e: any) {
                console.error(`[Indinis] Failed to parse JSON for key "${this.fullPath}": ${e.message}`);
                // Decide whether to return null or re-throw
                return null; // Or throw new Error(`Invalid data format for item ${this.itemId}`);
            }
        });
    }

    /**
     * Deletes the document. If the document doesn't exist, the operation succeeds silently.
     */
    async remove(): Promise<void> {
        await this.dbInstance.transaction(async tx => {
            await tx.remove(this.fullPath); // C++ remove returns bool, but we don't expose it here
        });
    }

    /**
     * Checks if the document exists.
     * @returns A promise resolving to true if the document exists, false otherwise.
     */
    async exists(): Promise<boolean> {
        return this.dbInstance.transaction(async tx => {
            const raw = await tx.get(this.fullPath);
            return raw !== null;
        });
    }

    /**
     * Updates specific fields of an existing document.
     * This operation can perform two types of updates:
     * 1. **Merge Update**: Merges the provided data with the existing document data.
     *    This is a non-atomic "read-merge-write" operation.
     * 2. **Atomic Update**: If the `increment()` sentinel is used, performs an atomic,
     *    server-side counter update, preventing race conditions.
     * 
     * If the document does not exist, the transaction will fail and this method will throw an error.
     * @param data An object containing the fields to update, which can include `increment()` sentinels.
     * @throws If the document does not exist or if an atomic operation fails.
     */
    async modify(data: Partial<{ [K in keyof T]: T[K] | AtomicIncrementOperation }>): Promise<void> {
        await this.dbInstance.transaction(async tx => {
            const simpleMergeData: Partial<T> = {};
            const atomicUpdateOps: UpdateOperation = {};

            // Separate the simple merge fields from the atomic operation commands.
            for (const key in data) {
                const value = data[key as keyof typeof data];
                if (typeof value === 'object' && value !== null && (value as any).$$indinis_op === IncrementSentinel) {
                    atomicUpdateOps[key] = value as AtomicIncrementOperation;
                } else {
                    (simpleMergeData as any)[key] = value;
                }
            }
            
            // Execute Atomic Updates First (by populating the update_set)
            if (Object.keys(atomicUpdateOps).length > 0) {
                await (tx as any).update(this.fullPath, atomicUpdateOps);
            }

            // Execute Simple Merge Update (if any fields remain)
            if (Object.keys(simpleMergeData).length > 0) {
                const currentRaw = await tx.get(this.fullPath);
                if (!currentRaw) {
                    // If we only had simple updates, this is a clear error.
                    // If we had atomic updates too, they might have been queued before this failure.
                    // The C++ commit will handle the final check.
                    throw new Error(`Document "${this.fullPath}" not found for modification.`);
                }
                let currentData: T;
                try {
                    const jsonString = (typeof currentRaw === 'string') ? currentRaw : (currentRaw as Buffer).toString('utf-8');
                    currentData = JSON.parse(jsonString) as T;
                } catch (e: any) {
                    throw new Error(`Failed to parse existing data for modification (ID: ${this.itemId}): ${e.message}`);
                }

                const updatedData = { ...currentData, ...simpleMergeData, id: this.itemId };

                // --- THIS IS THE FIX ---
                // The 'modify' operation is inherently an update, so 'overwrite' should be true.
                await tx.set(this.fullPath, JSON.stringify(updatedData), { overwrite: true });
            }
        });
    }

     /**
      * Creates a document if it doesn't exist, or updates specific fields
      * if it does exist (merging the provided data).
      * @param data An object containing the fields to set or merge. The `id` field is ignored and taken from the ItemRef.
      */
     async upsert(data: Partial<T>): Promise<void> {
        await this.dbInstance.transaction(async tx => {
            const currentRaw = await tx.get(this.fullPath);
            let finalData: T;
            if (currentRaw) { // Document exists, merge
                 let currentData: T;
                 try {
                     const jsonString = (typeof currentRaw === 'string') ? currentRaw : (currentRaw as Buffer).toString('utf-8');
                    currentData = JSON.parse(jsonString) as T;
                 } catch (e: any) {
                    console.error(`[Indinis] Failed to parse existing JSON for upsert key "${this.fullPath}": ${e.message}`);
                    throw new Error(`Failed to parse existing data for upsert (ID: ${this.itemId})`);
                 }
                finalData = { ...currentData, ...data, id: this.itemId }; // Merge, ensure ID
            } else { // Document doesn't exist, create
                // Ensure ID is present from ItemRef
                finalData = { ...data, id: this.itemId } as T;
            }
            await tx.set(this.fullPath, JSON.stringify(finalData));
        });
     }

} // End ItemRef

/**
 * Represents a query against a specific store path.
 * It allows chaining multiple filters and is executed by a terminal method like .one() or .take().
 */
export class Query<T extends { id?: string }> implements IQuery<T> {
    private readonly filters: FilterCondition[] = [];
    private sortByClause: SortByCondition | null = null;
    private aggregationPlan: AggregationPlan | null = null;

    private orderByClauses: OrderByClause[] = [];
    private limitValue: number | null = null;
    private startAfterCursor: Cursor | null = null;
    private startAtCursor: Cursor | null = null;
    private endBeforeCursor: Cursor | null = null;
    private endAtCursor: Cursor | null = null;
    /**
     * @internal
     * Users should not construct this directly. Use `db.store('...').filter(...)` or `db.store('...').query()` to start a query.
     */
    constructor(
        private readonly db: Indinis,
        private readonly storePath: string
    ) {}

    /**
     * @internal
     * Called by the FilterBuilder to add a new condition to this query's state.
     */
    _addFilterCondition(condition: FilterCondition): void {
        this.filters.push(condition);
    }

    /**
     * Applies a reusable query part to the current query.
     */
    use(part: QueryPart<T>): IQuery<T> {
        return part(this);
    }

    /**
     * Adds a filter condition to the query.
     */
    filter(field: keyof T & string): IFilterBuilder<T> {
        return new FilterBuilder<T>(this, field);
    }

    /**
     * Specifies the sort order for the query results.
     */
    sortBy(field: keyof T & string, direction: 'asc' | 'desc' = 'asc'): IQuery<T> {
        if (this.aggregationPlan) {
            throw new Error("Cannot use .sortBy() after .groupBy() or .aggregate(). Sorting should be on the final aggregated result if needed.");
        }
        this.sortByClause = { field, direction };
        return this;
    }

    /**
     * Specifies fields to group results by, turning this into an aggregation query.
     */
    groupBy(...fields: (keyof T & string)[]): IQuery<T> {
        if (!this.aggregationPlan) {
            this.aggregationPlan = { groupBy: [], aggregations: {} };
        }
        this.aggregationPlan.groupBy = fields;
        return this;
    }

    /**
     * Specifies aggregation operations to perform. This is a terminal query builder.
     */
    aggregate(aggregations: AggregationMap): IQuery<any> {
        if (!this.aggregationPlan) {
            this.aggregationPlan = { groupBy: [], aggregations: {} };
        }
        if (Object.keys(aggregations).length === 0) {
            throw new Error("The .aggregate() method requires at least one aggregation specification.");
        }
        this.aggregationPlan.aggregations = aggregations;
        return this as IQuery<any>; // Cast to a query of unknown result type
    }


    // --- NEW: Pagination Methods ---

    /**
     * Adds a sort order clause to the query. Can be called multiple times for compound sorting.
     * The order of calls matters.
     * @param field The document field to sort by.
     * @param direction The sort direction ('asc' or 'desc').
     */
    orderBy(field: keyof T & string, direction: 'asc' | 'desc' = 'asc'): IQuery<T> {
        this.orderByClauses.push({ field, direction });
        return this;
    }

    /**
     * Sets the maximum number of documents to return in a page.
     * @param count The number of documents.
     */
    limit(count: number): IQuery<T> {
        if (typeof count !== 'number' || !Number.isInteger(count) || count < 1) {
            throw new TypeError("limit() requires a positive integer.");
        }
        this.limitValue = count;
        return this;
    }
    
    /**
     * Creates a new query that starts after the provided cursor.
     * The cursor can be a document snapshot from a previous query or an array of values.
     * @param cursorValues The document snapshot or an array of values matching the orderBy clauses.
     */
    startAfter(...cursorValues: Cursor | [T]): IQuery<T> {
        this.startAtCursor = null; // Cursors are mutually exclusive
        this.startAfterCursor = this.extractCursorValues(cursorValues);
        return this;
    }
    
    // ... (Implement startAt, endBefore, endAt similarly) ...
    startAt(...cursorValues: Cursor | [T]): IQuery<T> {
        this.startAfterCursor = null;
        this.startAtCursor = this.extractCursorValues(cursorValues);
        return this;
    }

    endBefore(...cursorValues: Cursor | [T]): IQuery<T> {
        this.endAtCursor = null;
        this.endBeforeCursor = this.extractCursorValues(cursorValues);
        return this;
    }

    endAt(...cursorValues: Cursor | [T]): IQuery<T> {
        this.endBeforeCursor = null;
        this.endAtCursor = this.extractCursorValues(cursorValues);
        return this;
    }

    /**
     * @internal Extracts cursor values from either a raw array or a document object.
     */
    private extractCursorValues(cursorInput: Cursor | [T]): Cursor {
        if (this.orderByClauses.length === 0) {
            throw new Error("Cannot use a cursor without at least one .orderBy() clause.");
        }
        if (cursorInput.length === 1 && typeof cursorInput[0] === 'object' && cursorInput[0] !== null) {
            const doc = cursorInput[0] as T;
            return this.orderByClauses.map(clause => {
                const value = (doc as any)[clause.field];
                if (value === undefined) {
                    throw new Error(`Cursor document is missing value for orderBy field: '${clause.field}'`);
                }
                return value;
            });
        }
        return cursorInput as Cursor;
    }
    
    // --- END NEW Pagination Methods ---


    /**
     * Executes the query and returns the first matching document.
     */
    async one(): Promise<T | null> {
        if (this.aggregationPlan) {
            throw new Error(".one() cannot be used with an aggregation query. Use .take() to get aggregation results.");
        }
        const results = await this.take(1);
        return results.length > 0 ? results[0] : null;
    }

    /**
     * Executes the query and returns an array of matching documents or aggregation results.
     */
    async take(limit: number = 0): Promise<any[]> {
        return this.db.transaction(async (tx) => {
            const internalTx = tx as any; // Cast to access the internal N-API method

            // The `query` method on the native transaction object is the bridge to C++.
            // It now accepts the optional aggregationPlan as its fourth argument.
            const records: KeyValueRecord[] = await internalTx.query(
                this.storePath, 
                this.filters,
                this.sortByClause, 
                this.aggregationPlan, // Pass the plan (or null)
                limit
            );

            // If it was an aggregation query, the results are already shaped correctly by C++.
            // If not, they are standard documents. We parse the value field.
            if (this.aggregationPlan && Object.keys(this.aggregationPlan.aggregations).length > 0) {
                return records.map(record => JSON.parse(record.value as string));
            } else {
                return records.map(record => {
                    try {
                        const jsonString = (typeof record.value === 'string')
                            ? record.value
                            : (record.value as Buffer).toString('utf-8');
                        return JSON.parse(jsonString);
                    } catch (e) {
                        console.error(`[Indinis Query] Failed to parse JSON for key ${record.key}`, e);
                        return null;
                    }
                }).filter(item => item !== null);
            }
        });
    }


    async get(): Promise<PaginatedQueryResult<T>> {
        if (!this.limitValue) {
            throw new Error("Paginated queries must have a .limit() value set.");
        }
        if (this.orderByClauses.length === 0) {
            throw new Error("Paginated queries must have at least one .orderBy() clause.");
        }

        return this.db.transaction(async (tx) => {
            const internalTx = tx as any;

            // This is the new, extended call to the N-API layer.
            const result = await internalTx.paginatedQuery({
                storePath: this.storePath,
                filters: this.filters,
                orderBy: this.orderByClauses,
                limit: this.limitValue,
                startAfter: this.startAfterCursor,
                startAt: this.startAtCursor,
                endBefore: this.endBeforeCursor,
                endAt: this.endAtCursor,
            });

            // The C++ layer returns a structure that needs to be parsed into the final TS type.
            return {
                ...result,
                docs: result.docs.map((record: KeyValueRecord) => {
                    try {
                        const jsonString = (typeof record.value === 'string')
                            ? record.value
                            : (record.value as Buffer).toString('utf-8');
                        return JSON.parse(jsonString);
                    } catch (e) {
                        console.error(`[Indinis Query] Failed to parse JSON for key ${record.key}`, e);
                        return null;
                    }
                }).filter((item: T | null) => item !== null)
            };
        });
    }


}

/**
 * @internal
 * A helper class returned by `query.filter()` to provide a fluent interface for comparison operators.
 */
export class FilterBuilder<T extends { id?: string }> implements IFilterBuilder<T> {
    constructor(
        private readonly query: Query<T>,
        private readonly field: keyof T & string
    ) {}

    private addFilter(operator: FilterOperator, value: StorageValue | StorageValue[]): IQuery<T> {
        // This internal method constructs the filter condition and adds it to the parent Query object.
        // It then returns the Query object to allow for further chaining.
        this.query._addFilterCondition({ field: this.field, operator, value });
        return this.query;
    }

    equals(value: StorageValue): IQuery<T> { return this.addFilter('==', value); }
    greaterThan(value: StorageValue): IQuery<T> { return this.addFilter('>', value); }
    greaterThanOrEqual(value: StorageValue): IQuery<T> { return this.addFilter('>=', value); }
    lessThan(value: StorageValue): IQuery<T> { return this.addFilter('<', value); }
    lessThanOrEqual(value: StorageValue): IQuery<T> { return this.addFilter('<=', value); }
    
    arrayContains(value: StorageValue): IQuery<T> {
        if (Array.isArray(value)) {
            throw new TypeError("The value for 'arrayContains' must be a single element, not an array. Use 'arrayContainsAny' for multiple values.");
        }
        return this.addFilter('array-contains', value);
    }

    arrayContainsAny(values: StorageValue[]): IQuery<T> {
        if (!Array.isArray(values) || values.length === 0) {
            throw new TypeError("The argument for 'arrayContainsAny' must be a non-empty array of values.");
        }
        return this.addFilter('array-contains-any', values);
    }
}



/**
 * Represents a reference to a specific store (collection or subcollection).
 * The path must have an odd number of segments (e.g., 'users', 'users/id1/posts').
 */
export class StoreRef<T extends { id?: string }> {
    constructor(
        private readonly dbInstance: Indinis,
        public readonly storePath: string
    ) {
         if (!isValidCollectionPath(this.storePath)) {
            throw new Error(`Invalid path for StoreRef: "${this.storePath}". Collection paths must have an odd number of segments. Found ${countPathSegments(this.storePath)} segments.`);
         }
    }

    /** Gets a reference to a specific document within this store by its ID. */
    item(itemId: string): ItemRef<T> {
        if (!itemId || typeof itemId !== 'string') {
            throw new Error("Item ID must be a non-empty string.");
        }
        return new ItemRef<T>(this.dbInstance, this.storePath, itemId);
    }

    /**
     * Creates a new document within this store with an automatically generated unique ID.
     * @param data The data for the new document. The 'id' field will be ignored if present.
     * @returns A promise resolving to the generated ID of the new document.
     */
    make(data: Omit<T, 'id'>): Promise<string> & { withSnapshot(): Promise<T> } {
        return new MakeOperation<T>(this.dbInstance, this.storePath, data) as any;
    }

    /**
     * Fetches all documents that are direct children of this store's path.
     * This is a shortcut for a query with no filters. Use with caution on large stores.
     * @param limit Optional limit on the number of documents to retrieve.
     * @returns A promise resolving to an array of document data objects.
     */
    async take(limit: number = 0): Promise<T[]> {
        // This method no longer calls `getPrefix` directly.
        // It's now a consistent entry point to the query system.
        const query = new Query<T>(this.dbInstance, this.storePath);
        return query.take(limit);
    }



    /**
     * Creates a query against this store, starting with a filter condition.
     * @param field The document field to filter on.
     * @returns A FilterBuilder to continue building the query.
     */
    filter(field: keyof T & string): IFilterBuilder<T> {
        const query = new Query<T>(this.dbInstance, this.storePath);
        return query.filter(field);
    }

        // --- NEW METHOD ---
    /**
     * Creates a new query builder instance for this store.
     * This is the entry point for building compositional queries with `.use()`.
     * @returns A new Query object.
     */
    query(): IQuery<T> {
        return new Query<T>(this.dbInstance, this.storePath);
    }

    /**
     * Lists all indexes defined specifically for this store path.
     * @returns A promise resolving to an array of IndexInfo objects.
     */
    async listIndexes(): Promise<IndexInfo[]> {
        // This fluently calls the main db.listIndexes method, passing its own path.
        return this.dbInstance.listIndexes(this.storePath);
    }

} // End StoreRef