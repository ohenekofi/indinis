// Base types and utilities
export type DocumentId = string;
export type CollectionPath = string;
export type FieldPath = string;
export type Timestamp = Date | number;

// Generic document structure
export interface BaseDocument {
  id?: DocumentId;
  createdAt?: Timestamp;
  updatedAt?: Timestamp;
  [key: string]: any;
}

// Query operators for filtering
export enum QueryOperator {
  EQUAL = '==',
  NOT_EQUAL = '!=',
  LESS_THAN = '<',
  LESS_THAN_OR_EQUAL = '<=',
  GREATER_THAN = '>',
  GREATER_THAN_OR_EQUAL = '>=',
  ARRAY_CONTAINS = 'array-contains',
  ARRAY_CONTAINS_ANY = 'array-contains-any',
  IN = 'in',
  NOT_IN = 'not-in',
  LIKE = 'like',
  STARTS_WITH = 'startsWith',
  ENDS_WITH = 'endsWith'
}

// Query filter condition
export interface QueryFilter {
  field: FieldPath;
  operator: QueryOperator;
  value: any;
}

// Sorting options
export interface OrderBy {
  field: FieldPath;
  direction: 'asc' | 'desc';
}

// Pagination cursor
export interface PaginationCursor {
  id: DocumentId;
  values: Record<string, any>;
}

// Query options
export interface QueryOptions {
  filters?: QueryFilter[];
  orderBy?: OrderBy[];
  limit?: number;
  offset?: number;
  startAfter?: PaginationCursor;
  startAt?: PaginationCursor;
  endBefore?: PaginationCursor;
  endAt?: PaginationCursor;
}

// Query result with pagination metadata
export interface QueryResult<T extends BaseDocument = BaseDocument> {
  documents: T[];
  hasMore: boolean;
  nextCursor?: PaginationCursor;
  prevCursor?: PaginationCursor;
  totalCount?: number;
}

// Batch operation types
export enum BatchOperationType {
  CREATE = 'create',
  UPDATE = 'update',
  DELETE = 'delete',
  UPSERT = 'upsert'
}

export interface BatchOperation<T extends BaseDocument = BaseDocument> {
  type: BatchOperationType;
  collection: CollectionPath;
  id?: DocumentId;
  data?: Partial<T>;
  merge?: boolean;
}

export interface BatchResult {
  success: boolean;
  operations: {
    operation: BatchOperation;
    success: boolean;
    id?: DocumentId;
    error?: Error;
  }[];
}

// Transaction context
export interface TransactionContext {
  get<T extends BaseDocument>(collection: CollectionPath, id: DocumentId): Promise<T | null>;
  query<T extends BaseDocument>(collection: CollectionPath, options?: QueryOptions): Promise<QueryResult<T>>;
  create<T extends BaseDocument>(collection: CollectionPath, data: Omit<T, 'id'>, id?: DocumentId): void;
  update<T extends BaseDocument>(collection: CollectionPath, id: DocumentId, data: Partial<T>, merge?: boolean): void;
  delete(collection: CollectionPath, id: DocumentId): void;
}

// Real-time subscription options
export interface SubscriptionOptions extends QueryOptions {
  includeMetadataChanges?: boolean;
}

// Real-time change types
export enum ChangeType {
  ADDED = 'added',
  MODIFIED = 'modified',
  REMOVED = 'removed'
}

export interface DocumentChange<T extends BaseDocument = BaseDocument> {
  type: ChangeType;
  document: T;
  oldDocument?: T;
  newIndex: number;
  oldIndex?: number;
}

export interface SubscriptionResult<T extends BaseDocument = BaseDocument> {
  documents: T[];
  changes: DocumentChange<T>[];
  fromCache: boolean;
  hasPendingWrites: boolean;
}

// Index management
export interface IndexField {
  field: FieldPath;
  direction: 'asc' | 'desc';
}

export interface IndexDefinition {
  collection: CollectionPath;
  fields: IndexField[];
  unique?: boolean;
  sparse?: boolean;
}

// Aggregation operations
export enum AggregationType {
  COUNT = 'count',
  SUM = 'sum',
  AVG = 'avg',
  MIN = 'min',
  MAX = 'max'
}

export interface AggregationOperation {
  type: AggregationType;
  field?: FieldPath;
}

export interface AggregationOptions extends QueryOptions {
  operations: AggregationOperation[];
}

export interface AggregationResult {
  [key: string]: number;
}

// Backup and restore
export interface BackupOptions {
  collections?: CollectionPath[];
  includeIndexes?: boolean;
  compression?: boolean;
}

export interface BackupMetadata {
  id: string;
  timestamp: Timestamp;
  collections: CollectionPath[];
  size: number;
  checksum: string;
}

// Storage statistics
export interface CollectionStats {
  collection: CollectionPath;
  documentCount: number;
  totalSize: number;
  avgDocumentSize: number;
  indexCount: number;
}

export interface StorageStats {
  totalCollections: number;
  totalDocuments: number;
  totalSize: number;
  collections: CollectionStats[];
}

// Security rules (provider-specific implementation)
export interface SecurityRule {
  collection: CollectionPath;
  operations: ('read' | 'write' | 'create' | 'update' | 'delete')[];
  condition: string;
}

// Main cloud storage interface
export interface ICloudStorage {
  // Connection management
  connect(config: Record<string, any>): Promise<void>;
  disconnect(): Promise<void>;
  isConnected(): boolean;

  // Basic CRUD operations
  create<T extends BaseDocument>(
    collection: CollectionPath,
    data: Omit<T, 'id'>,
    id?: DocumentId
  ): Promise<T>;

  get<T extends BaseDocument>(
    collection: CollectionPath,
    id: DocumentId
  ): Promise<T | null>;

  update<T extends BaseDocument>(
    collection: CollectionPath,
    id: DocumentId,
    data: Partial<T>,
    merge?: boolean
  ): Promise<T>;

  delete(collection: CollectionPath, id: DocumentId): Promise<void>;

  upsert<T extends BaseDocument>(
    collection: CollectionPath,
    data: T,
    id?: DocumentId
  ): Promise<T>;

  // Query operations
  query<T extends BaseDocument>(
    collection: CollectionPath,
    options?: QueryOptions
  ): Promise<QueryResult<T>>;

  // Batch operations
  batch(operations: BatchOperation[]): Promise<BatchResult>;

  // Transaction support
  runTransaction<T>(
    callback: (context: TransactionContext) => Promise<T>
  ): Promise<T>;

  // Real-time subscriptions
  subscribe<T extends BaseDocument>(
    collection: CollectionPath,
    callback: (result: SubscriptionResult<T>) => void,
    options?: SubscriptionOptions
  ): () => void; // Returns unsubscribe function

  subscribeToDocument<T extends BaseDocument>(
    collection: CollectionPath,
    id: DocumentId,
    callback: (document: T | null, change?: DocumentChange<T>) => void
  ): () => void;

  // Collection management
  listCollections(): Promise<CollectionPath[]>;
  createCollection(collection: CollectionPath): Promise<void>;
  deleteCollection(collection: CollectionPath): Promise<void>;
  collectionExists(collection: CollectionPath): Promise<boolean>;

  // Index management
  createIndex(index: IndexDefinition): Promise<void>;
  dropIndex(collection: CollectionPath, indexName: string): Promise<void>;
  listIndexes(collection: CollectionPath): Promise<IndexDefinition[]>;

  // Aggregation operations
  aggregate(
    collection: CollectionPath,
    options: AggregationOptions
  ): Promise<AggregationResult>;

  // Full-text search (if supported)
  search<T extends BaseDocument>(
    collection: CollectionPath,
    searchTerm: string,
    options?: QueryOptions & { searchFields?: FieldPath[] }
  ): Promise<QueryResult<T>>;

  // Backup and restore
  backup(options?: BackupOptions): Promise<BackupMetadata>;
  restore(backupId: string): Promise<void>;
  listBackups(): Promise<BackupMetadata[]>;
  deleteBackup(backupId: string): Promise<void>;

  // Statistics and monitoring
  getStorageStats(): Promise<StorageStats>;
  getCollectionStats(collection: CollectionPath): Promise<CollectionStats>;

  // Security and permissions
  setSecurityRules(rules: SecurityRule[]): Promise<void>;
  getSecurityRules(): Promise<SecurityRule[]>;

  // Utility methods
  validateDocument<T extends BaseDocument>(
    collection: CollectionPath,
    data: T
  ): Promise<boolean>;

  // Health check
  health(): Promise<{
    status: 'healthy' | 'degraded' | 'unhealthy';
    latency: number;
    details?: Record<string, any>;
  }>;
}

// Abstract base class for implementations
export abstract class CloudStorageAdapter implements ICloudStorage {
  protected connected = false;
  protected config: Record<string, any> = {};

  abstract connect(config: Record<string, any>): Promise<void>;
  abstract disconnect(): Promise<void>;
  
  isConnected(): boolean {
    return this.connected;
  }

  // All other methods must be implemented by concrete classes
  abstract create<T extends BaseDocument>(
    collection: CollectionPath,
    data: Omit<T, 'id'>,
    id?: DocumentId
  ): Promise<T>;

  abstract get<T extends BaseDocument>(
    collection: CollectionPath,
    id: DocumentId
  ): Promise<T | null>;

  abstract update<T extends BaseDocument>(
    collection: CollectionPath,
    id: DocumentId,
    data: Partial<T>,
    merge?: boolean
  ): Promise<T>;

  abstract delete(collection: CollectionPath, id: DocumentId): Promise<void>;

  abstract upsert<T extends BaseDocument>(
    collection: CollectionPath,
    data: T,
    id?: DocumentId
  ): Promise<T>;

  abstract query<T extends BaseDocument>(
    collection: CollectionPath,
    options?: QueryOptions
  ): Promise<QueryResult<T>>;

  abstract batch(operations: BatchOperation[]): Promise<BatchResult>;

  abstract runTransaction<T>(
    callback: (context: TransactionContext) => Promise<T>
  ): Promise<T>;

  abstract subscribe<T extends BaseDocument>(
    collection: CollectionPath,
    callback: (result: SubscriptionResult<T>) => void,
    options?: SubscriptionOptions
  ): () => void;

  abstract subscribeToDocument<T extends BaseDocument>(
    collection: CollectionPath,
    id: DocumentId,
    callback: (document: T | null, change?: DocumentChange<T>) => void
  ): () => void;

  abstract listCollections(): Promise<CollectionPath[]>;
  abstract createCollection(collection: CollectionPath): Promise<void>;
  abstract deleteCollection(collection: CollectionPath): Promise<void>;
  abstract collectionExists(collection: CollectionPath): Promise<boolean>;

  abstract createIndex(index: IndexDefinition): Promise<void>;
  abstract dropIndex(collection: CollectionPath, indexName: string): Promise<void>;
  abstract listIndexes(collection: CollectionPath): Promise<IndexDefinition[]>;

  abstract aggregate(
    collection: CollectionPath,
    options: AggregationOptions
  ): Promise<AggregationResult>;

  abstract search<T extends BaseDocument>(
    collection: CollectionPath,
    searchTerm: string,
    options?: QueryOptions & { searchFields?: FieldPath[] }
  ): Promise<QueryResult<T>>;

  abstract backup(options?: BackupOptions): Promise<BackupMetadata>;
  abstract restore(backupId: string): Promise<void>;
  abstract listBackups(): Promise<BackupMetadata[]>;
  abstract deleteBackup(backupId: string): Promise<void>;

  abstract getStorageStats(): Promise<StorageStats>;
  abstract getCollectionStats(collection: CollectionPath): Promise<CollectionStats>;

  abstract setSecurityRules(rules: SecurityRule[]): Promise<void>;
  abstract getSecurityRules(): Promise<SecurityRule[]>;

  abstract validateDocument<T extends BaseDocument>(
    collection: CollectionPath,
    data: T
  ): Promise<boolean>;

  abstract health(): Promise<{
    status: 'healthy' | 'degraded' | 'unhealthy';
    latency: number;
    details?: Record<string, any>;
  }>;
}

// Error classes
export class CloudStorageError extends Error {
  constructor(
    message: string,
    public code: string,
    public details?: Record<string, any>
  ) {
    super(message);
    this.name = 'CloudStorageError';
  }
}

export class DocumentNotFoundError extends CloudStorageError {
  constructor(collection: CollectionPath, id: DocumentId) {
    super(
      `Document with id '${id}' not found in collection '${collection}'`,
      'DOCUMENT_NOT_FOUND',
      { collection, id }
    );
    this.name = 'DocumentNotFoundError';
  }
}

export class CollectionNotFoundError extends CloudStorageError {
  constructor(collection: CollectionPath) {
    super(
      `Collection '${collection}' not found`,
      'COLLECTION_NOT_FOUND',
      { collection }
    );
    this.name = 'CollectionNotFoundError';
  }
}

export class ValidationError extends CloudStorageError {
  constructor(message: string, field?: string) {
    super(message, 'VALIDATION_ERROR', { field });
    this.name = 'ValidationError';
  }
}

export class PermissionError extends CloudStorageError {
  constructor(operation: string, resource: string) {
    super(
      `Permission denied for operation '${operation}' on resource '${resource}'`,
      'PERMISSION_DENIED',
      { operation, resource }
    );
    this.name = 'PermissionError';
  }
}

// Factory for creating storage adapters
export type StorageProvider = 'firestore' | 'appwrite' | 'dynamodb' | 'cosmosdb' | 'mongodb';

export interface StorageFactory {
  create(provider: StorageProvider, config: Record<string, any>): ICloudStorage;
}

// Usage example interface for type safety
export interface User extends BaseDocument {
  name: string;
  email: string;
  age: number;
  tags: string[];
}

export interface Product extends BaseDocument {
  title: string;
  price: number;
  category: string;
  inStock: boolean;
}