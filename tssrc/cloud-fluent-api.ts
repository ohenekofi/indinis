/**
 * @file tssrc/cloud-fluent-api.ts
 * @description Implements the fluent API wrappers (CloudStoreRef, CloudItemRef) for interacting
 *              with cloud-synchronized data stores. This is the primary interface for developers
 *              using the Cloud Federation Layer.
 */

import { Indinis } from './index';
import { FormGuardSchema } from './formguard/types';
import { CloudProviderAdapter, SyncRuleset, UnsubscribeFunction, Validator, OutboxRecord, UpdateOperation, AtomicIncrementOperation, IncrementSentinel } from './types';
import { StoreRef, ItemRef } from './fluent-api';
import { generateId } from './utils';

// Helper to check if a validator is a FormGuard schema using duck typing.
// This is a robust way to distinguish between our built-in and custom validators.
function isFormGuardSchema<T>(validator: any): validator is FormGuardSchema<T> {
    return validator && typeof validator._isFormGuardSchema === 'boolean' && typeof validator.parse === 'function';
}

/**
 * A reference to a cloud-synced store (collection), mirroring the API of a local-only StoreRef.
 * It acts as a factory for creating references to cloud-synced items and queries.
 */
export class CloudStoreRef<T extends { id?: string }> extends StoreRef<T> {
    constructor(
        private readonly db: Indinis,
        private readonly adapter: CloudProviderAdapter<T>,
        public readonly storePath: string,
        private readonly ruleset: SyncRuleset<T>
    ) {
        super(db, storePath);
    }

    /** 
     * Gets a reference to a specific cloud-synced document within this store by its ID.
     * @param itemId The ID of the document.
     * @returns A `CloudItemRef` for performing operations on that specific document.
     */
    item(itemId: string): CloudItemRef<T> {
        return new CloudItemRef<T>(this.db, this.adapter, this.storePath, itemId, this.ruleset);
    }

    /**
     * Fetches all documents that are direct children of this cloud store's path.
     * This operation respects the configured `readStrategy`. For collection scans,
     * `'CloudFirst'` behaves like `'LocalFirst'` to ensure responsiveness.
     * @param limit Optional limit on the number of documents to retrieve.
     * @returns A promise resolving to an array of document data objects.
     */
    async take(limit: number = 0): Promise<T[]> {
        switch (this.ruleset.readStrategy) {
            case 'CloudOnly':
                // This would require a `query` method on the adapter. For now, it's not supported.
                throw new Error("The 'CloudOnly' read strategy is not supported for collection scans (.take()). Use it with .item(id).one().");
            case 'CloudFirst': // Fallthrough: Treat CloudFirst like LocalFirst for collection scans
            case 'LocalFirst':
            case 'LocalOnly':
            default:
                // All practical read paths for `take()` delegate to the fast local store.
                // The local store is kept up-to-date by real-time listeners and other sync operations.
                return super.take(limit); // Calls the original StoreRef.take()
        }
    }

    /**
     * Establishes a real-time listener for the entire collection.
     * @throws Not yet implemented. Collection-level snapshots are a complex future enhancement.
     */
    onSnapshot(callback: (data: T[]) => void): UnsubscribeFunction {
        throw new Error("Real-time collection snapshots are not yet implemented. Use .item(id).onSnapshot() for individual documents.");
    }
}

/**
 * A reference to a specific cloud-synced document, providing methods for CRUD,
 * validation, and real-time updates.
 */
export class CloudItemRef<T extends { id?: string }> extends ItemRef<T> {
    private localItemRef: ItemRef<T>;

    constructor(
        private readonly db: Indinis,
        private readonly adapter: CloudProviderAdapter<T>,
        storePath: string,
        itemId: string,
        private readonly ruleset: SyncRuleset<T>
    ) {
        super(db, storePath, itemId);
        this.localItemRef = new ItemRef<T>(db, storePath, itemId);
    }

    /**
     * Validates and transforms data according to the ruleset.
     * @internal
     */
    private async validateAndTransform(data: any): Promise<[T, any]> {
        let validatedData: T = data as T;
        
        if (this.ruleset.validator) {
            try {
                const validator = this.ruleset.validator;
                if (isFormGuardSchema(validator)) {
                    const result = validator.parse(data); // `parse` already accepts `any`
                    if (!result.success) throw new Error(result.errors.map(e => `${e.path.join('.')}: ${e.message}`).join('; '));
                    validatedData = result.data;
                } else if (typeof (validator as any).validate === 'function') {
                    validatedData = (validator as Validator<T>).validate(data);
                }
            } catch (error: any) {
                throw new Error(`Data validation failed for item '${this.itemId}': ${error.message}`);
            }
        }
        
        const cloudData = this.ruleset.toCloud ? this.ruleset.toCloud(validatedData) : validatedData;
        return [validatedData, cloudData];
    }

    /**
     * Enqueues a failed operation into the local outbox for later retry.
     * @internal
     */
    private async enqueueInOutbox(operation: 'set' | 'remove', data?: any, error?: Error): Promise<void> {
        const outboxStore = this.db.store<OutboxRecord>('_outbox');
        const newId = generateId(); // <<< FIX: Generate ID here
        const record: OutboxRecord = {
            id: newId,
            key: this.fullPath,
            operation: operation,
            data: data ? JSON.stringify(data) : undefined,
            timestamp: Date.now(),
            errorCount: 1,
            lastError: error?.message,
        };
        try {
            // Use an ItemRef with the new ID to perform the write.
            await outboxStore.item(newId).make(record, true);
        } catch (err: any) {
            console.error(`CRITICAL: Failed to write to outbox for key ${this.fullPath}. Data may be lost. Error: ${err.message}`);
            throw err;
        }
    }

    /**
     * Creates or overwrites a document, orchestrating validation and synchronization.
     */
    async make(data: Omit<T, 'id'>, overwrite: boolean = false, options?: { writeStrategy?: SyncRuleset<T>['writeStrategy'] }): Promise<void> {
        const [validatedData, cloudData] = await this.validateAndTransform({ ...data, id: this.itemId });
        const strategy = options?.writeStrategy ?? this.ruleset.writeStrategy;
        
        switch (strategy) {
            case 'LocalFirstWithOutbox':
                // 1. Await the local commit. This is the part the user waits for.
                // If this throws, the entire method will reject, which is correct.
                await this.localItemRef.make(validatedData, overwrite);
                
                // 2. Start the cloud sync as a separate, non-awaited async task.
                // We wrap it in an IIFE to handle its lifecycle independently.
                (async () => {
                    try {
                        await this.adapter.set(this.fullPath, cloudData, this.ruleset);
                    } catch (err: any) {
                        // If the cloud sync fails, enqueue it. We don't want this
                        // error to propagate to the user, so we catch it here.
                        try {
                            await this.enqueueInOutbox('set', cloudData, err);
                        } catch (outboxErr) {
                            // This is a critical failure (can't even write to the outbox).
                            // It's already logged by enqueueInOutbox.
                        }
                    }
                })(); // The () immediately invokes the function.
                break;

            case 'TwoPhaseCommit':
                await this.adapter.set(this.fullPath, cloudData, this.ruleset);
                try {
                    await this.localItemRef.make(validatedData, overwrite);
                } catch (localErr: any) {
                    // Fire-and-forget outbox write for inconsistency logging.
                    this.enqueueInOutbox('set', cloudData, localErr);
                    throw localErr;
                }
                break;
        }
    }
    
    /**
     * Fetches a document according to the configured read strategy.
     */
    async one(): Promise<T | null> {
        switch (this.ruleset.readStrategy) {
            case 'CloudOnly':
            case 'CloudFirst': {
                const cloudData = await this.adapter.get(this.fullPath);
                if (cloudData) {
                    const finalData = this.ruleset.fromCloud ? this.ruleset.fromCloud(cloudData) : (cloudData as T);
                    this.localItemRef.make(finalData, true).catch(err => console.error(`[Indinis] Failed to update local cache for ${this.fullPath}: ${err.message}`));
                    return finalData;
                }
                return null;
            }
            case 'LocalOnly':
                return this.localItemRef.one();
            case 'LocalFirst':
            default: {
                const localData = await this.localItemRef.one();
                if (localData) return localData;
                const cloudData = await this.adapter.get(this.fullPath);
                if (cloudData) {
                    const finalData = this.ruleset.fromCloud ? this.ruleset.fromCloud(cloudData) : (cloudData as T);
                    this.localItemRef.make(finalData, true).catch(err => console.error(`[Indinis] Failed to populate local cache for ${this.fullPath}: ${err.message}`));
                    return finalData;
                }
                return null;
            }
        }
    }
    
    /**
     * Deletes a document from both the local store and the cloud, according to the write strategy.
     */
    async remove(options?: { writeStrategy?: SyncRuleset<T>['writeStrategy'] }): Promise<void> {
        const strategy = options?.writeStrategy ?? this.ruleset.writeStrategy;

        switch (strategy) {
            case 'LocalFirstWithOutbox':
                await this.localItemRef.remove();
                (async () => {
                    try {
                        await this.adapter.remove(this.fullPath);
                    } catch (err: any) {
                        await this.enqueueInOutbox('remove', undefined, err);
                    }
                })();
                break;
            case 'TwoPhaseCommit':
                await this.adapter.remove(this.fullPath);
                try {
                    await this.localItemRef.remove();
                } catch (localErr: any) {
                    this.enqueueInOutbox('remove', undefined, localErr);
                    throw localErr;
                }
                break;
        }
    }

    /**
     * Updates specific fields of an existing document.
     */
    async modify(data: Partial<{ [K in keyof T]: T[K] | AtomicIncrementOperation }>, options?: { writeStrategy?: SyncRuleset<T>['writeStrategy'] }): Promise<void> {
        const strategy = options?.writeStrategy ?? this.ruleset.writeStrategy;
        
        // The `modify` operation is inherently a read-modify-write cycle.
        // For cloud sync, the safest approach is to get the latest local state, apply changes,
        // validate the final result, and then treat it as a full `set` operation.
        const localCurrent = await this.localItemRef.one();
        if (!localCurrent) {
            throw new Error(`Document "${this.fullPath}" not found for modification.`);
        }

        const simpleMergeData: Partial<T> = {};
        const atomicOps: UpdateOperation = {};

        for (const key in data) {
            const value = data[key as keyof typeof data];
            if (typeof value === 'object' && value !== null && (value as any).$$indinis_op === IncrementSentinel) {
                atomicOps[key] = value as AtomicIncrementOperation;
            } else {
                (simpleMergeData as any)[key] = value;
            }
        }
        
        // This is a complex operation that needs to be atomic locally.
        // We will perform the local part within a single transaction.
        await this.db.transaction(async tx => {
            const txInternal = tx as any;
            let finalDataForCloud: T;

            // If there are simple merges, we must perform a read-modify-write.
            if (Object.keys(simpleMergeData).length > 0) {
                const currentRaw = await tx.get(this.fullPath);
                if (!currentRaw) throw new Error(`Document "${this.fullPath}" disappeared during modify transaction.`);
                const currentData = JSON.parse(currentRaw as string);
                const updatedData = { ...currentData, ...simpleMergeData, id: this.itemId };
                const [validatedData] = await this.validateAndTransform(updatedData);
                finalDataForCloud = validatedData;
                await tx.set(this.fullPath, JSON.stringify(validatedData), { overwrite: true });
            }

            // Apply atomic operations after simple merges.
            if (Object.keys(atomicOps).length > 0) {
                await txInternal.update(this.fullPath, atomicOps);
                // If we also did a simple merge, the cloud data is already set.
                // If ONLY atomic ops, we need to fetch the result for the cloud sync.
                if (Object.keys(simpleMergeData).length === 0) {
                    const finalRaw = await tx.get(this.fullPath);
                    finalDataForCloud = JSON.parse(finalRaw as string);
                }
            }
        });
        
        // After the local transaction commits, sync the final state to the cloud.
        const finalStateAfterTx = await this.localItemRef.one();
        if (finalStateAfterTx) {
             const [, cloudData] = await this.validateAndTransform(finalStateAfterTx);
             this.adapter.set(this.fullPath, cloudData, this.ruleset).catch(err => {
                 this.enqueueInOutbox('set', cloudData, err);
             });
        }
    }
    
    /**
     * Creates a document if it doesn't exist, or merges new fields into it if it does.
     */
    async upsert(data: Partial<T>, options?: { writeStrategy?: SyncRuleset<T>['writeStrategy'] }): Promise<void> {
        const localCurrent = await this.localItemRef.one();
        const dataToMake = localCurrent ? { ...localCurrent, ...data } : data;
        await this.make(dataToMake as Omit<T, 'id'>, true, options);
    }

    /**
     * Establishes a real-time listener on this document from the cloud.
     */
    onSnapshot(callback: (data: T | null) => void): UnsubscribeFunction {
        return this.adapter.onSnapshot(this.fullPath, (cloudData) => {
            const finalData = cloudData ? (this.ruleset.fromCloud ? this.ruleset.fromCloud(cloudData) : (cloudData as T)) : null;
            const updateLocalPromise = finalData
                ? this.localItemRef.make(finalData, true)
                : this.localItemRef.remove();
            updateLocalPromise.then(() => callback(finalData)).catch(err => console.error(`[Indinis] Failed to apply snapshot update for ${this.fullPath} locally: ${err.message}`));
        });
    }
}