// tssrc/batch-api.ts

import { Indinis } from './index';
import { ItemRef, StoreRef } from './fluent-api';
import { AtomicIncrementOperation, BatchOperation, BatchOperationType } from './types';

/**
 * A temporary reference to a document used only for building a batch operation.
 * It exposes the core write methods, but instead of executing them, it queues
 * them in the parent BatchWriter.
 * @internal
 */
export class BatchItemRef<T extends { id?: string }> {
    constructor(
        private readonly batchWriter: BatchWriter,
        private readonly fullPath: string
    ) {}

    /**
     * Queues a 'create' or 'overwrite' operation for this document in the batch.
     * @param data The data for the document.
     * @param overwrite If false (default), the transaction will fail if the document exists. If true, it overwrites.
     * @returns The parent BatchWriter instance for further chaining.
     */
    make(data: Omit<T, 'id'>, overwrite: boolean = false): BatchWriter {
        (this.batchWriter as any)._addOperation({
            op: BatchOperationType.MAKE,
            key: this.fullPath,
            value: data,
            overwrite: overwrite,
        });
        return this.batchWriter;
    }

    /**
     * Queues a 'modify' (merge and/or atomic) operation for this document in the batch.
     * @param data An object containing fields to update, including atomic increments.
     * @returns The parent BatchWriter instance for further chaining.
     */
    modify(data: Partial<{ [K in keyof T]: T[K] | AtomicIncrementOperation }>): BatchWriter {
        (this.batchWriter as any)._addOperation({
            op: BatchOperationType.MODIFY,
            key: this.fullPath,
            value: data,
        });
        return this.batchWriter;
    }
    
    /**
     * Queues a 'delete' operation for this document in the batch.
     * @returns The parent BatchWriter instance for further chaining.
     */
    remove(): BatchWriter {
        (this.batchWriter as any)._addOperation({
            op: BatchOperationType.REMOVE,
            key: this.fullPath,
        });
        return this.batchWriter;
    }
}

/**
 * A temporary reference to a store path used only for building a batch.
 * @internal
 */
export class BatchStoreRef<T extends { id?: string }> {
    constructor(
        private readonly batchWriter: BatchWriter,
        public readonly storePath: string,
    ) {}

    /**
     * Gets a reference to a document within this store to add a batch operation.
     * @param itemId The ID of the document.
     * @returns A BatchItemRef instance for specifying the write operation.
     */
    item(itemId: string): BatchItemRef<T> {
        const fullPath = `${this.storePath}/${itemId}`;
        return new BatchItemRef<T>(this.batchWriter, fullPath);
    }
}

/**
 * The main batch writer class. It collects operations and sends them to the
 * native C++ engine for atomic execution upon calling .commit().
 */
export class BatchWriter {
    private operations: BatchOperation[] = [];

    /** @internal */
    constructor(private readonly db: Indinis) {}

    /**
     * Gets a reference to a store path to begin chaining batch operations.
     * @param storePath The path to the collection (e.g., 'users').
     * @returns A BatchStoreRef instance.
     */
    public store<T extends { id?: string }>(storePath: string): BatchStoreRef<T> {
        return new BatchStoreRef<T>(this, storePath);
    }
    
    /** @internal - Used by BatchItemRef to add an operation. */
    public _addOperation(op: BatchOperation): void {
        this.operations.push(op);
    }

    /**
     * Commits all queued operations to the database atomically.
     * @returns A promise that resolves when the commit is successful.
     */
    public async commit(): Promise<void> {
        if (this.operations.length === 0) {
            // No-op, resolve immediately.
            return Promise.resolve();
        }
        const engine = (this.db as any).getNativeEngine();
        if (!engine) {
            throw new Error("Indinis engine is not initialized or has been closed.");
        }
        // This will call the new N-API binding.
        await engine.commitBatch_internal(this.operations);
    }
}