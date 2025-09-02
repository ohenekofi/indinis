

Here is a comprehensive, production-ready `README.md` section for the newly validated Data Caching feature. It explains the concept, configuration, and monitoring from an end-user's perspective.

---

### README.md (New Section)

*(This section can be added to your main documentation, likely after the core CRUD sections and before advanced topics like indexing.)*

## High-Performance In-Memory Data Caching

Indinis includes an optional, high-performance in-memory data cache designed to dramatically accelerate read-heavy workloads by reducing disk I/O for frequently accessed documents. When enabled, the cache operates transparently, automatically managing the lifecycle of cached items.

### How It Works: The Cache-Aside Pattern

The cache is designed to be completely automatic. You don't need to manually add, update, or remove items from the cache; the `StorageEngine` handles it for you.

*   **Caching on Read:** When you read a document for the first time (e.g., via `tx.get(key)` or `store.item(id).one()`), Indinis fetches it from the underlying storage (the LSM-Tree) and places a copy in the in-memory cache.
*   **Cache Hits:** Subsequent reads for the same key will be served directly from the extremely fast in-memory cache, bypassing the disk entirely and providing a significant performance boost.
*   **Automatic Invalidation:** To guarantee data consistency, Indinis automatically invalidates (removes) a key's cache entry whenever that key is modified (`set`, `modify`) or deleted (`remove`). The next read for that key will result in a *cache miss*, which fetches the new, correct data from disk and repopulates the cache with the updated version.

### Enabling and Configuring the Cache

The cache is disabled by default. You can enable and configure it through the `IndinisOptions` object in the constructor.

**Simple Enablement:**
To enable the cache with default settings (LRU policy, 1000 item limit), simply set `enableCache` to `true`.

```typescript
import { Indinis } from 'indinis';

const db = new Indinis('./my-database', {
  enableCache: true
});
```

**Advanced Configuration:**
For fine-grained control, provide a `cacheOptions` object.

```typescript
import { Indinis, IndinisOptions } from 'indinis';

const dbOptions: IndinisOptions = {
  // Main cache toggle
  enableCache: true,
  
  // Detailed cache configuration
  cacheOptions: {
    /** The maximum number of items to store in the cache. Defaults to 1000. */
    maxSize: 5000,

    /** 
     * The eviction policy to use when the cache is full.
     * 'LRU' (Least Recently Used): Evicts the item that hasn't been accessed for the longest time. A great general-purpose choice.
     * 'LFU' (Least Frequently Used): Evicts the item that has been accessed the fewest times. Better for workloads with highly skewed access patterns (some items are very "hot").
     * Defaults to 'LRU'.
     */
    policy: 'LRU',

    /** 
     * Default Time-To-Live (TTL) for cache entries, in milliseconds.
     * An item will be considered expired and removed upon next access if it has been in the cache longer than this duration.
     * A value of 0 means entries never expire by default. Defaults to 0.
     */
    defaultTTLMilliseconds: 60 * 1000, // 1 minute

    /** Whether to collect performance statistics. Must be true to use getCacheStats(). Defaults to true. */
    enableStats: true
  }
};

const db = new Indinis('./my-database', dbOptions);
```

### Monitoring Cache Performance

To determine if the cache is effective for your workload, you can retrieve its performance statistics using the `db.getCacheStats()` method. Statistics collection must be enabled in the cache options.

The method returns a `CacheStatsJs` object, or `null` if stats are disabled.

```typescript
const stats = await db.getCacheStats();

if (stats) {
  console.log('Cache Performance:');
  console.log(`  - Hits: ${stats.hits}`);
  console.log(`  - Misses: ${stats.misses}`);
  console.log(`  - Evictions: ${stats.evictions}`);
  console.log(`  - Hit Rate: ${(stats.hitRate * 100).toFixed(2)}%`);
} else {
  console.log('Cache stats are disabled.');
}
```

*   **`hits`**: The number of times a requested item was found in the cache.
*   **`misses`**: The number of times a requested item was *not* in the cache and had to be fetched from disk.
*   **`evictions`**: The number of items removed from the cache to make space because it was full.
*   **`expiredRemovals`**: The number of items removed because their TTL expired.
*   **`hitRate`**: The most important metric. This is the ratio of `hits / (hits + misses)`. A high hit rate (e.g., > 90%) indicates the cache is very effective at reducing disk I/O.

### Full Lifecycle Example

This example demonstrates the cache being populated on a miss, served on a hit, and invalidated on an update.

```typescript
import { Indinis } from 'indinis';

// 1. Initialize DB with caching enabled
const db = new Indinis('./my-cached-db', { enableCache: true });

const userKey = 'users/u1';

// 2. First Read (Cache Miss)
console.log("Reading user for the first time...");
await db.transaction(async tx => {
  await tx.set(userKey, JSON.stringify({ name: 'Alice', version: 1 }));
});
await db.transaction(async tx => {
  await tx.get(userKey); 
});

let stats = await db.getCacheStats();
console.log(`Stats after first read: ${stats.misses} misses, ${stats.hits} hits.`); // Expect: 1 miss, 0 hits

// 3. Second Read (Cache Hit)
console.log("\nReading user for the second time...");
await db.transaction(async tx => {
  await tx.get(userKey); 
});

stats = await db.getCacheStats();
console.log(`Stats after second read: ${stats.misses} misses, ${stats.hits} hits.`); // Expect: 1 miss, 1 hit

// 4. Update the Document
console.log("\nUpdating the user document...");
await db.transaction(async tx => {
  await tx.set(userKey, JSON.stringify({ name: 'Alice', version: 2 }), { overwrite: true });
});
// The cache entry for 'users/u1' is now automatically invalidated.

// 5. Third Read (Cache Miss due to invalidation)
console.log("\nReading user after update...");
await db.transaction(async tx => {
  const finalData = JSON.parse(await tx.get(userKey) as string);
  console.log('Read updated data:', finalData); // { name: 'Alice', version: 2 }
});

stats = await db.getCacheStats();
console.log(`Stats after third read: ${stats.misses} misses, ${stats.hits} hits.`); // Expect: 2 misses, 1 hit
```

---

# Indinis DB: Fluent Query and Data Management API

Welcome to the Indinis DB documentation. This guide covers the fluent, Firestore-like API for interacting with your data, including creating, reading, updating, and deleting documents, as well as building powerful, index-driven queries.

## Table of Contents

1.  [Core Concepts: Stores and Items](#1-core-concepts-stores-and-items)
2.  [Creating and Writing Data](#2-creating-and-writing-data)
    *   [Using `store.make()` for Auto-Generated IDs](#using-storemake-for-auto-generated-ids)
    *   [Using `item.make()` for Specified IDs](#using-itemmake-for-specified-ids)
    *   [Overwrite Control](#overwrite-control)
3.  [Reading Data](#3-reading-data)
    *   [Fetching a Single Item with `item(...).one()`](#fetching-a-single-item-with-itemone)
    *   [Querying for a Single Item with `filter(...).one()`](#querying-for-a-single-item-with-filterone)
    *   [Querying for Multiple Items with `take()`](#querying-for-multiple-items-with-take)
4.  [Building Queries with `.filter()`](#4-building-queries-with-filter)
    *   [Filter Operators](#filter-operators)
    *   [Chaining Filters](#chaining-filters)
5.  [Updating and Deleting Data](#5-updating-and-deleting-data)
    *   [Partial Updates with `modify()`](#partial-updates-with-modify)
    *   [Atomic Increments](#atomic-increments)
    *   [Deleting an Item with `remove()`](#deleting-an-item-with-remove)
6.  [Automatic Indexing](#6-automatic-indexing)
    *   [Default `name` Index](#default-name-index)
    *   [The "No Index, No Query" Rule](#the-no-index-no-query-rule)

---

## 1. Core Concepts: Stores and Items

Indinis organizes data in a way that is familiar to users of document databases like Firestore.

-   **Store**: A "store" is a container for your documents, analogous to a *collection* in Firestore or a table in a relational database. Store paths always have an **odd number of segments**.
-   **Item**: An "item" is an individual document within a store, analogous to a *document* in Firestore or a row in a relational database. Item paths always have an **even number of segments**.
-   **Nested Stores (Sub-collections)**: You can create stores within items to build hierarchical data structures. This is a powerful feature for organizing related data.

**Path Structure Rules:**

-   **Valid Store Path (Odd Segments):**
    -   `'users'` (1 segment)
    -   `'users/user123/posts'` (3 segments)
    -   `'products/prodABC/reviews'` (3 segments)
-   **Valid Item Path (Even Segments):**
    -   `'users/user123'` (2 segments)
    -   `'users/user123/posts/postXYZ'` (4 segments)

The API is designed around creating references to these paths.

```javascript
import { Indinis } from 'indinis';

const db = new Indinis('./my-database-directory');

// Get a reference to the top-level 'users' store
const usersStore = db.store('users');

// Get a reference to a specific user item
const userItem = usersStore.item('user123');

// Get a reference to the 'posts' sub-store for a specific user
const userPostsStore = db.store('users/user123/posts');
```

---

## 2. Creating and Writing Data

The primary method for creating or overwriting data is `.make()`. It behaves differently depending on whether it's called on a `StoreRef` or an `ItemRef`.

### Using `store.make()` for Auto-Generated IDs

When you want the database to generate a unique ID for your new document, call `make()` on a `StoreRef`.

```typescript
const users = db.store('users');

const newUserId = await users.make({
  name: 'John Doe',
  email: 'john.doe@example.com',
  status: 'pending',
  createdAt: Date.now()
});

console.log('Created new user with ID:', newUserId); 
// Example output: 'Created new user with ID: aF4jKq8sLp9wXzY3vG2u'
```

### Using `item.make()` for Specified IDs

When you know the ID you want to use for a document, create an `ItemRef` first and then call `make()`.

```typescript
const users = db.store('users');

await users.item('john-doe-123').make({
  name: 'John Doe',
  email: 'john.doe@example.com',
  status: 'pending',
  createdAt: Date.now()
});
```

### Overwrite Control

By default, `item.make()` is a safe operation that will **fail if a document already exists at that path**. This prevents you from accidentally overwriting data.

```javascript
// This first call succeeds
await users.item('jane-smith-456').make({ name: 'Jane Smith' });

// This second call will THROW an error
try {
  await users.item('jane-smith-456').make({ name: 'Jane Updated' }); 
} catch (error) {
  console.error(error.message); 
  // "Failed to create document at "users/jane-smith-456": A document with this ID already exists."
}
```

To explicitly overwrite a document, pass `true` as the second argument. This will completely replace the existing document with the new data.

```javascript
// This will succeed and replace the old document entirely
await users.item('jane-smith-456').make({ name: 'Jane Smith V2', region: 'EU' }, true);
```

---

## 3. Reading Data

The API provides two "terminal" methods to execute a read operation: `one()` and `take()`. Their behavior depends on the context in which they are called.

### Fetching a Single Item with `item(...).one()`

This is the most direct way to get a single document by its ID.

**Rule:** When `.one()` is called on an `ItemRef` (which is created via `.item()`), it performs a direct key lookup.

```typescript
const users = db.store('users');

// Get a single user by their ID
const userData = await users.item('john-doe-123').one();

if (userData) {
  console.log(userData.name); // 'John Doe'
} else {
  console.log('User not found.');
}
```

You can also check for existence more efficiently with `.exists()`.

```javascript
const userExists = await users.item('john-doe-123').exists();
console.log('Does user exist?', userExists); // true or false
```

### Querying for a Single Item with `filter(...).one()`

This is used to fetch the *first single result* of a query.

**Rule:** When `.one()` is called after one or more `.filter()` calls, it executes the query and returns only the first matching document. If multiple documents match the query, only the first one (based on index order) is returned.

```typescript
// Define a type for your data for type-safety
interface User {
  name: string;
  status: 'active' | 'inactive';
  level: number;
}
const users = db.store<User>('users');

// Find the first active user
const firstActiveUser = await users
  .filter('status').equals('active')
  .one();

if (firstActiveUser) {
  console.log('Found an active user:', firstActiveUser.name);
}
```

### Querying for Multiple Items with `take()`

`take()` is the terminal method used to retrieve an array of documents that match your query.

**Rule:** `take()` is always called on a `StoreRef` or a `Query` object. It returns an array of documents.

```typescript
// Get all documents in the 'users' store (use with caution on large collections!)
const allUsers = await db.store('users').take();

// Get the first 10 active engineers
const firstTenEngineers = await db.store<Employee>('employees')
  .filter('department').equals('eng')
  .filter('status').equals('active')
  .take(10);
```

---

## 4. Building Queries with `.filter()`

You build queries by starting with a `store()` and chaining one or more `.filter()` calls.

### Filter Operators

The fluent API provides methods for each comparison operator.

```typescript
// Find all employees with level 5
const level5 = await empStore.filter('level').equals(5).take();

// Find all employees with a salary of at least 100,000
const highEarners = await empStore.filter('salary').greaterThanOrEqual(100000).take();

// Find all junior employees (level 2 or less)
const juniors = await empStore.filter('level').lessThan(3).take(); 
// Note: This effectively means <= 2 for integers.
```

### Chaining Filters

You can chain multiple `.filter()` calls to create more specific queries.

**Important:** For a query with multiple filters to succeed, **at least one of the fields you are filtering on must have an index**. The database will use the best available index to retrieve an initial set of documents and then apply the remaining filters in memory.

```typescript
// Assumes an index exists on 'status' (the primary filter)
const seniorActiveSalesReps = await db.store<Employee>('employees')
  .filter('status').equals('active')         // This filter uses the index
  .filter('department').equals('sales')      // This is a post-filter applied in memory
  .filter('level').greaterThanOrEqual(5)   // This is also a post-filter
  .take();
```

---

## 5. Updating and Deleting Data

### Partial Updates with `modify()`

Use `.modify()` on an `ItemRef` to update some fields of a document without replacing the entire object.

**Rule:** `modify()` will fail if the document does not exist.

```javascript
const userItem = db.store('users').item('user123');

// Update Alice's status and region
await userItem.modify({
  status: 'active',
  region: 'eu-west-1'
});
```

### Atomic Increments

Indinis provides a special `increment()` sentinel for performing atomic counter updates on the server-side. This is essential for scenarios like tracking views, likes, or inventory counts, as it avoids race conditions.

```javascript
import { Indinis, increment } from 'indinis';

const postRef = db.store('posts').item('postXYZ');

// Atomically increment the 'likes' and 'views' fields
await postRef.modify({
  likes: increment(1),
  views: increment(1)
});

// You can also decrement
await postRef.modify({
  likes: increment(-1)
});
```

**Rule:** The `increment()` operation will fail if the target field does not exist or if its current value is not a number.

### Deleting an Item with `remove()`

Use `.remove()` on an `ItemRef` to delete a document.

**Rule:** If the document doesn't exist, the operation completes silently without an error.

```javascript
const userItem = db.store('users').item('user123');

await userItem.remove();
```

---

## 6. Automatic Indexing

A key feature of Indinis is its ability to manage indexes intelligently to support your queries.

### Default `name` Index

To make new collections immediately useful, Indinis follows this rule:

**Rule:** The **very first time** a document is written to a new store path (e.g., the first document in a new `'products'` store), Indinis will **automatically create a default secondary index on the `name` field** for that store.

This means you can immediately perform queries like `.filter('name').equals('...')` on new collections without any manual setup.

### The "No Index, No Query" Rule

Indinis is designed for performance and will not perform slow, un-indexed collection scans.

**Rule:** A query with one or more `.filter()` conditions **must** be serviceable by at least one existing secondary index. If you attempt a query on a field (or combination of fields) that is not indexed, the operation will fail and throw an error.

```javascript
// This will FAIL if no index exists on the 'hireDate' field.
await empStore.filter('hireDate').greaterThanOrEqual(someTimestamp).take();
// Error: Query requires an index but none was found. Please create an index on field 'hireDate'...
```

This rule encourages you to be deliberate about your data access patterns and ensures your application remains performant as it scales. You can create the necessary indexes using the `db.createIndex()` method.


Of course. Congratulations on getting all the tests to pass! That's a significant milestone.

Here is the updated README section detailing the new array query operators and multikey indexes. It's written to seamlessly integrate into the existing documentation you provided.

---

### README.md (Updated Section)

*(You would insert this between the existing "Chaining Filters" section and "Updating and Deleting Data" section.)*

### Querying Array Fields

Indinis provides powerful operators for querying documents based on the contents of an array field. To use these operators, you must first create a special **multikey index** on the target array field.

#### Multikey Indexes

A multikey index creates a separate index entry for *each element* in an array. This allows the database to find documents based on the values *inside* their arrays with the same high performance as a standard index.

**Rules for Multikey Indexes:**
1.  They must be created on a single field (`field` option, not `fields`).
2.  The indexed field in your documents should contain an array of primitive values (strings or numbers).
3.  A multikey index cannot also be a `unique` index.

**Creating a Multikey Index:**

```javascript
// In your database initialization code:

// An index on the 'tags' field of products
await db.createIndex('products', 'idx_prod_tags', {
  field: 'tags',
  multikey: true // This is the key option
});

// An index on an array of numbers, like product sizes
await db.createIndex('products', 'idx_prod_sizes', {
  field: 'sizes',
  multikey: true
});
```

#### `arrayContains`

Use `filter(...).arrayContains(value)` to find all documents where the indexed array field contains a specific element. This is an exact match on an element within the array.

**Example:**
Imagine you have these product documents:

```json
{ "id": "prod1", "name": "Laptop", "tags": ["electronics", "computer", "sale"] }
{ "id": "prod2", "name": "Monitor", "tags": ["electronics", "display"] }
{ "id": "prod3", "name": "Desk Chair", "tags": ["office", "furniture"] }
```

You can find all products tagged as 'electronics':

```typescript
const electronics = await db.store('products')
  .filter('tags').arrayContains('electronics')
  .take();

// results will contain the Laptop and Monitor documents
console.log(electronics.map(p => p.name));
// Output: ['Laptop', 'Monitor']
```

#### `arrayContainsAny`

Use `filter(...).arrayContainsAny([...values])` to find all documents where the indexed array field contains **at least one** of the elements from the query array. This is equivalent to an "IN" query in SQL.

**Example:**
Using the same product data, find all products that are either on 'sale' or are a 'bestseller'.

```json
// More sample data:
// { id: 'prod4', name: 'Mouse', tags: ['electronics', 'accessory', 'bestseller'] }

const featuredProducts = await db.store('products')
  .filter('tags').arrayContainsAny(['sale', 'bestseller'])
  .take();

// results will contain the Laptop ('sale') and Mouse ('bestseller') documents
console.log(featuredProducts.map(p => p.name));
// Output: ['Laptop', 'Mouse']
```

The database efficiently performs lookups for each value in your query array (`['sale', 'bestseller']`) and merges the results, ensuring that documents matching multiple criteria (e.g., tagged with both 'sale' and 'bestseller') appear only once in the final result set.


Of course. Here is the documentation section for the new `store.listIndexes()` method, written to be clear, production-ready, and seamlessly integrated into the existing API guide.

---

### README.md (New Section)

*(You can add this to the "Index Management" section of your main README.)*

## Listing Indexes

While `db.listIndexes()` allows you to view all indexes across the entire database, it's often more convenient to see which indexes are available for a specific collection you are working with.

### Listing Indexes for a Specific Store

The `StoreRef` object provides a `listIndexes()` method that returns only the indexes defined for that particular store path. This is the recommended way to check the indexing status of a collection.

**Rule:** `store(...).listIndexes()` retrieves an array of index definitions that apply **only** to the store's path.

**Example:**

First, let's set up a few indexes on different stores:

```javascript
// In your database initialization code:
await db.createIndex('users', 'idx_users_email', { field: 'email' });
await db.createIndex('users', 'idx_users_level_desc', { field: 'level', order: 'desc' });
await db.createIndex('products', 'idx_products_category', { field: 'category' });
```

Now, you can scope your listing to just the `users` store:

```typescript
const usersStore = db.store('users');

const userIndexes = await usersStore.listIndexes();

console.log(userIndexes);
/*
Output:
[
  {
    name: 'idx_users_email',
    storePath: 'users',
    fields: [ { name: 'email', order: 'asc' } ],
    unique: false,
    multikey: false
  },
  {
    name: 'idx_users_level_desc',
    storePath: 'users',
    fields: [ { name: 'level', order: 'desc' } ],
    unique: false,
    multikey: false
  }
]
*/
```

Notice that the index for the `products` store was correctly excluded. This method works for any valid store path, including nested sub-collections.

```typescript
const postsStore = db.store('users/user123/posts');

// This will return an empty array if no indexes have been defined on this specific path.
const postIndexes = await postsStore.listIndexes(); 
```

Of course. Here is a clear, production-ready README section explaining the new query composition feature. It's designed to be easily integrated into your existing API documentation.

---

### README.md (New Section)

*(You can add this to your main documentation, likely after the sections on basic filtering and sorting.)*

## Composable Queries with `.use()`

For complex applications, you often find yourself repeating the same set of filters or sorting logic across different queries. Indinis provides a powerful `.use()` method that allows you to encapsulate and reuse parts of a query, leading to cleaner, more readable, and highly maintainable code.

The core idea is to define **reusable query parts**, which are simply functions that take a `Query` object and return a modified `Query` object.

### Defining Reusable Parts

Let's define some common conditions for a `users` collection.

```typescript
import { Indinis, IQuery } from 'indinis';

// Define the shape of our data
interface User {
  id?: string;
  name: string;
  status: 'active' | 'pending' | 'archived';
  isVerified: boolean;
  createdAt: number;
}

// A reusable part to find only active users.
// The type annotation `(query: IQuery<User>) => ...` ensures type safety.
const isActive = (query: IQuery<User>) => query.filter('status').equals('active');

// A reusable part to find only verified users.
const isVerified = (query: IQuery<User>) => query.filter('isVerified').equals(true);
```

### Composing Queries

You can now compose these parts fluently using the `.use()` method on a query object. To start a compositional query, simply call `.query()` on your `StoreRef`.

```javascript
const usersStore = db.store<User>('users');

// Find all active AND verified users.
const activeAndVerified = await usersStore
  .query()
  .use(isActive)
  .use(isVerified)
  .take();
```

This is far more declarative than chaining long filter calls:
`usersStore.filter('status').equals('active').filter('isVerified').equals(true)`

### Parameterized Reusable Parts

You can create more flexible query parts by using higher-order functions (functions that return other functions). This allows you to pass arguments to your reusable logic.

```typescript
// A function that returns a query part for finding users in a specific region.
const byRegion = (region: 'us-east' | 'us-west') => 
  (query: IQuery<User>) => query.filter('region').equals(region);

// A reusable part for sorting by creation date, with a default direction.
const sortByDate = (direction: 'asc' | 'desc' = 'desc') => 
  (query: IQuery<User>) => query.sortBy('createdAt', direction);
```

Now you can build highly readable and complex queries with ease:

```javascript
// Get the 20 most recent active users from the 'us-west' region.
const recentActiveWestCoastUsers = await usersStore
  .query()
  .use(isActive)
  .use(byRegion('us-west'))
  .use(sortByDate('desc')) // Explicitly sort descending
  .take(20);
```

You can even mix `.use()` with standard `.filter()` calls, giving you complete flexibility:

```javascript
// Find the newest admins who are also active.
const recentAdmins = await usersStore
  .query()
  // Apply reusable parts first
  .use(isActive)
  .use(sortByDate()) // Uses default 'desc' direction
  // Add a specific filter for this query
  .filter('role').equals('admin')
  .take(10);
```

Of course. Here is a detailed, production-ready README section for the `.modify()` and `increment()` features. It explains the concepts, rules, and usage with clear, copy-paste-ready examples, designed to be integrated directly into your main API documentation.

---

### README.md (New Section)

*(You can add this to your main documentation, likely between the "Reading Data" and "Deleting an Item" sections.)*

## 5. Updating Documents

Indinis provides a powerful and flexible `.modify()` method for performing partial and atomic updates on existing documents. This is the preferred way to change a document's data without replacing the entire object.

### Partial Updates with `modify()`

When you only need to change a few fields on a document, `modify()` allows you to provide just the fields you want to add or change. This is often more efficient and safer than fetching a document, changing it in your application code, and writing it back with `.set()`.

**Rule:** The `modify()` method will fail if the document does not exist. It is designed for updating, not creating. Use `make()` or `upsert()` to create documents.

```typescript
// Assume we have a user document at 'users/user123'
const userRef = db.store('users').item('user123');

// Update the user's status and add a new 'lastLogin' field
await userRef.modify({
  status: 'active',
  lastLogin: Date.now() 
});

// The user's 'name' and 'email' fields remain untouched.
```

### Atomic Increments

For fields that act as counters (e.g., views, likes, inventory levels), performing a simple read-modify-write can lead to race conditions in a concurrent environment. Two operations might read the same value (e.g., `5`), both increment it to `6`, and the last one to write wins, resulting in a final value of `6` instead of the correct `7`.

Indinis solves this with an **atomic increment** operation. This sends a command to the database engine to perform the increment directly on the stored value, guaranteeing atomicity and correctness even under heavy concurrent load.

To perform an atomic increment, import the `increment` function from the `indinis` library.

**Example: Tracking Page Views**

```typescript
import { Indinis, increment } from 'indinis';

const db = new Indinis('./my-database');
const postRef = db.store('posts').item('postXYZ');

// Initial document
await postRef.make({
  title: 'Understanding Atomic Operations',
  views: 0,
  likes: 10
});

// Later, multiple users view the post concurrently...
// Each of these calls is an atomic, safe operation.
await postRef.modify({ views: increment(1) });
await postRef.modify({ views: increment(1) });
await postRef.modify({ views: increment(1) });

// You can also decrement values by providing a negative number.
await postRef.modify({ likes: increment(-1) });

// Check the final state
const finalPost = await postRef.one();
console.log(finalPost); 
// Expected output: { ..., views: 3, likes: 9 }
```

**Rules for Atomic Increments:**

1.  The `increment()` operation can only be used within a `.modify()` call.
2.  The target field **must already exist** in the document.
3.  The existing value of the target field **must be a number**. The operation will fail if you try to increment a string, boolean, or other non-numeric type.

### Combining Simple and Atomic Updates

You can seamlessly combine simple field updates and atomic increments in a single `.modify()` call. The database guarantees that all operations in the call will be applied as part of a single, atomic transaction.

This is extremely useful for updating metadata alongside a counter.

```typescript
const postRef = db.store('posts').item('postXYZ');

// Atomically increment views and update the 'lastViewedBy' field
await postRef.modify({
  views: increment(1),
  lastViewedBy: 'user-abc' 
});
```

The engine ensures this happens correctly: it reads the document's state, applies the atomic increment to the `views` field, merges in the new `lastViewedBy` field, and commits the result as a single, atomic change.


Advanced Configuration
Configuring the Compaction Thread Pool
Indinis uses a shared, adaptive thread pool to manage background maintenance tasks like LSM-Tree compactions. This pool is designed to use resources efficiently, scaling up under heavy load and scaling down during quiet periods. For most workloads, the default settings are optimal. However, for specialized use cases or resource-constrained environments, you can tune its behavior.
These options are provided within the main IndinisOptions object in the constructor.
Generated typescript
import { Indinis, IndinisOptions } from 'indinis';
import { cpus } from 'os';

const dbOptions: IndinisOptions = {
  // ... other options like walOptions, encryptionOptions ...

  /**
   * The minimum number of threads the compaction pool will maintain.
   * These threads are always active, waiting for work.
   * Defaults to 2.
   */
  minCompactionThreads: 2,

  /**
   * The maximum number of threads the pool is allowed to scale up to.
   * The pool will only add threads if there is a backlog of tasks AND
   * the system has available CPU resources.
   * Defaults to the number of logical CPU cores on the machine.
   */
  maxCompactionThreads: cpus().length,
};

const db = new Indinis('./my-database', dbOptions);
Use code with caution.
TypeScript
When to Tune:
minCompactionThreads: Increase this value if you have a consistently high write workload and want to ensure there are always multiple threads ready to start compactions immediately without waiting for the adaptive manager to scale up.
maxCompactionThreads: Decrease this value on servers with limited CPU cores or if you want to strictly limit the maximum CPU impact of database maintenance tasks to reserve resources for other processes.
</details>
With these additions, the new adaptive thread pool feature is fully implemented, tested, and documented. The system is now significantly more robust and production-ready.

Excellent. We have successfully completed the implementation, fixed all compilation and type errors, and created a robust stress test. The C++ core is now feature-complete according to our plan.

The final and crucial step to "being done" in a production system is to **document the new features** so that developers can understand and use them effectively. I will now generate the user-facing documentation for the new capabilities we've added.

This documentation is written in Markdown, ready to be added to your project's `README.md` or official documentation site.

---

### **`README.md` - New Section: Advanced Columnar Store Features**

## Advanced Columnar Store Features

For analytical workloads and high-volume data scenarios, the columnar store provides specialized features that go beyond the standard transactional API. These tools allow for high-performance bulk data ingestion and provide deep insights into the store's internal state for monitoring and performance tuning.

### 1. High-Performance Bulk Ingestion

For loading large datasets (e.g., importing historical data, back-filling from another system), the standard transactional path can be inefficient as it writes every record to the Write-Ahead Log. To solve this, Indinis provides a direct ingestion path that builds and loads optimized columnar files in bulk, bypassing the WAL and LSM-Tree entirely.

This is a two-step process:
1.  Use the `SSTableBuilder` to create a valid, sorted SSTable file on disk.
2.  Call `db.ingestColumnarFile()` to ingest this file directly into the columnar store.

**Example: Bulk-loading product catalog data**

```typescript
import { Indinis } from 'indinis';
import * as path from 'path';

const db = new Indinis('./my-database');

// Ensure the columnar schema for 'products' is registered
await db.registerStoreSchema({
  storePath: 'products',
  columns: [
    { name: 'category', type: 'STRING', column_id: 1 },
    { name: 'price', type: 'DOUBLE', column_id: 2 },
    { name: 'inventory_count', type: 'INT64', column_id: 3 }
  ]
});

// An array of records to be bulk-loaded
const recordsToIngest = [
  { key: 'products/item-a1', value: { category: 'electronics', price: 199.99, inventory_count: 500 } },
  { key: 'products/item-b2', value: { category: 'books', price: 19.95, inventory_count: 2000 } },
  // ... thousands more records
];

const externalFilePath = path.join(__dirname, 'temp_columnar_ingest.cstore');

try {
  // Step 1: Build the external columnar file
  // NOTE: This uses a debug API and is intended for trusted, server-side generation.
  // The SSTableBuilder is used here as it produces a compatible key-sorted file format.
  console.log('Building external file...');
  const builder = await db.debug_getSSTableBuilder(externalFilePath, {
    // Columnar files can also benefit from compression
    compressionType: 'ZSTD' 
  });
  
  for (const record of recordsToIngest) {
    // The value must be a JSON string for the columnar store to flatten it
    await builder.add({
      key: record.key,
      value: JSON.stringify(record.value), // Ensure value is a JSON string
      deleted: false,
      commit_txn_id: BigInt(Date.now()) // Use a timestamp as a version
    });
  }
  await builder.finish();
  console.log('External file built successfully.');

  // Step 2: Ingest the file into the columnar store
  console.log('Ingesting file into columnar store...');
  await db.ingestColumnarFile('products', externalFilePath, { moveFile: true });
  console.log('Ingestion complete!');
  
  // The data is now available for analytical queries
  const results = await db.store('products')
    .query()
    .groupBy('category')
    .aggregate({ total_inventory: sum('inventory_count') })
    .take();
    
  console.log(results);

} catch (e: any) {
  console.error('Bulk ingestion failed:', e.message);
}
```

#### `db.ingestColumnarFile(storePath, filePath, options)`

*   `storePath`: The logical path of the store (e.g., `'products'`) to ingest the data into.
*   `filePath`: The absolute path to the valid, pre-built file.
*   `options`:
    *   `moveFile` (boolean, optional, default: `true`): If `true`, the file is atomically moved into the database directory (very fast). If `false`, the file is copied, leaving the source file intact (slower).

### 2. Monitoring and Debugging

To help you tune performance and understand the internal state of the columnar store, Indinis provides debug methods for observability.

#### `db.debug_getColumnarStoreStats(storePath)`

This method returns a snapshot of the columnar store's current state and workload.

```typescript
const stats = await db.debug_getColumnarStoreStats('sales');
console.log(stats);
```

**Example Output and Field Explanations:**

```json
{
  "ingestionQueueDepth": 0,
  "activeBufferCount": 16,
  "totalRowsInActiveBuffers": 125,
  "totalBytesInActiveBuffers": 130512,
  "immutableBufferCount": 2,
  "totalRowsInImmutableBuffers": 131072,
  "totalBytesInImmutableBuffers": 1452388,
  "filesPerLevel": [
    3,
    10,
    0,
    0,
    0,
    0,
    0
  ]
}
```

*   `ingestionQueueDepth`: The number of records in the lock-free queue waiting to be processed by the ingestion thread. A persistently high number may indicate that disk I/O for flushes is slower than the incoming write rate.
*   `activeBufferCount`: The number of active buffer shards.
*   `totalRowsInActiveBuffers` / `totalBytesInActiveBuffers`: The total amount of data currently being collected in the active, in-memory buffers across all shards.
*   `immutableBufferCount`: The number of full buffers waiting in the queue to be flushed to disk.
*   `totalRowsInImmutableBuffers` / `totalBytesInImmutableBuffers`: The total amount of data in the flush queue.
*   `filesPerLevel`: An array showing the number of `.cstore` files at each level of the columnar LSM tree, starting with Level 0.

#### `db.debug_forceColumnarCompaction(storePath)`

This method signals the background compaction scheduler to run immediately and check if any compactions are needed for the specified store. It is useful for testing or for triggering maintenance during a known low-traffic period.

```typescript
// Request a compaction check for the 'sales' store
await db.debug_forceColumnarCompaction('sales');
```

---

With the implementation of the C++ core, the creation of the N-API bindings, and now this comprehensive documentation, the columnar store upgrade is **complete**. The system is now robust, scalable, observable, and ready for production workloads.




Of course. Here is a clear and comprehensive README section explaining the new pagination feature. This can be added to your project's main `README.md` or a dedicated `docs/queries.md` file.

---

## Advanced Queries: Pagination

Indinis provides powerful and efficient cursor-based pagination, allowing you to navigate through large result sets page by page. This method is highly performant and stable, even with datasets that are actively being written to, avoiding the common pitfalls of traditional offset-based pagination.

The API is designed to be fluent and closely mirrors the patterns used by modern cloud databases like Firestore.

### Key Concepts

*   **Cursor-Based:** Instead of using an `offset` (e.g., "skip the first 100 documents"), pagination is handled using a **cursor**. A cursor is a stable pointer to a specific document in a sorted result set.
*   **Immutable Queries:** To get the next page of results, you must execute the *exact same query* (same filters, same ordering) as the original query, but provide the cursor from the previous page.
*   **Sorting is Required:** Pagination requires a defined order. You must use at least one `.orderBy()` clause in your query. The database will use an index to efficiently find the starting point for each page.

### Building a Paginated Query

A paginated query is built by chaining four key methods onto a `query()` object:

1.  `.filter()`: (Optional) The conditions to select which documents to include.
2.  `.orderBy()`: **(Required)** Defines the sort order. You can chain multiple `orderBy` calls for compound sorting.
3.  `.limit()`: **(Required)** Specifies the number of documents per page.
4.  Cursor Methods (`.startAfter()`, etc.): (Optional) Specifies where the page should start or end.
5.  `.get()`: **(Required)** The terminal method that executes the query and fetches the page.

### The Paginated Query Result

The `.get()` method returns a `PaginatedQueryResult` object with the following structure:

```typescript
interface PaginatedQueryResult<T> {
    /** The documents for the current page. */
    docs: T[];
    /** A boolean indicating if there is a next page of results. */
    hasNextPage: boolean;
    /** A boolean indicating if there is a previous page of results. */
    hasPrevPage: boolean;
    /** The cursor pointing to the end of the current page, used to fetch the next page. */
    endCursor?: any[];
}
```

### Usage Examples

Let's assume we have a `users` collection with the following documents:

| ID    | Name    | Level |
| ----- | ------- | ----- |
| user1 | Alice   | 7     |
| user2 | Bob     | 5     |
| user3 | Charlie | 7     |
| user4 | Diane   | 5     |
| user5 | Eve     | 8     |

And we have indexes on `name` (asc) and `level` (desc).

#### 1. Fetching the First Page

To get the first page of users sorted by name, you simply omit any cursor methods.

```typescript
import { Indinis } from 'indinis';

const db = new Indinis('./my-data');
const usersStore = db.store<{ name: string; level: number }>('users');

async function getFirstPage() {
    const firstPage = await usersStore.query()
        .orderBy('name', 'asc')
        .limit(2)
        .get();

    console.log('--- First Page ---');
    firstPage.docs.forEach(doc => console.log(doc.name)); // Alice, Bob

    console.log('Has next page?', firstPage.hasNextPage); // true
    console.log('End cursor:', firstPage.endCursor); // ['Bob'] (the name of the last doc)

    return firstPage;
}
```

#### 2. Fetching Subsequent Pages

To get the next page, you re-run the same query but use the `endCursor` from the previous result with the `.startAfter()` method.

```typescript
async function getNextPage(cursor) {
    const nextPage = await usersStore.query()
        .orderBy('name', 'asc') // Query must be identical
        .limit(2)
        .startAfter(...cursor) // Unpack the cursor array
        .get();
        
    console.log('--- Next Page ---');
    nextPage.docs.forEach(doc => console.log(doc.name)); // Charlie, Diane
    
    console.log('Has next page?', nextPage.hasNextPage); // true
    console.log('End cursor:', nextPage.endCursor); // ['Diane']
}

// Chaining the calls:
getFirstPage().then(firstPage => {
    if (firstPage.hasNextPage) {
        getNextPage(firstPage.endCursor);
    }
});
```

#### 3. Compound Sorting and Cursors

When you use multiple `.orderBy()` clauses, the cursor becomes an array containing a value for each sort field.

```typescript
async function getCompoundSortPage() {
    const firstPage = await usersStore.query()
        .orderBy('level', 'desc') // Primary sort: highest level first
        .orderBy('name', 'asc')   // Secondary sort: alphabetical name for ties
        .limit(3)
        .get();

    // Expected order: Eve (8), Alice (7), Charlie (7)
    console.log('--- Compound Sort Page ---');
    firstPage.docs.forEach(doc => console.log(`Lvl ${doc.level}: ${doc.name}`));
    
    console.log('Has next page?', firstPage.hasNextPage); // true
    
    // The cursor contains values for BOTH orderBy fields from the last doc (Charlie)
    console.log('End cursor:', firstPage.endCursor); // [7, 'Charlie']
}
```

#### 4. Using a Document as a Cursor (Convenience)

Instead of manually handling the `endCursor` array, you can pass the entire last document from a page as a cursor. Indinis will automatically extract the correct values.

```typescript
async function useDocumentAsCursor() {
    const firstPage = await usersStore.query()
        .orderBy('name', 'asc')
        .limit(2)
        .get();

    const lastDocOnPage = firstPage.docs[firstPage.docs.length - 1]; // Bob's document

    if (firstPage.hasNextPage) {
        const nextPage = await usersStore.query()
            .orderBy('name', 'asc')
            .limit(2)
            .startAfter(lastDocOnPage) // Pass the whole object
            .get();

        console.log('--- Next Page (Doc as Cursor) ---');
        nextPage.docs.forEach(doc => console.log(doc.name)); // Charlie, Diane
    }
}
```

### Pagination Methods

*   `.orderBy(field, direction)`: Adds a sort clause. Required for pagination.
*   `.limit(count)`: Sets the page size. Required for pagination.
*   `.startAfter(...cursor)`: Starts the next page *after* the document identified by the cursor.
*   `.startAt(...cursor)`: Starts the page *at* the document identified by the cursor (inclusive).
*   `.endBefore(...cursor)`: Ends the page *before* the document identified by the cursor.
*   `.endAt(...cursor)`: Ends the page *at* the document identified by the cursor (inclusive).
*   `.get()`: Executes the paginated query.

### **`README.md` - New Section: Creating Documents with Snapshots**

Here is the updated documentation section. It should be placed within the "Creating and Writing Data" section of your main README, replacing the existing content for `store.make()`.

## 2. Creating and Writing Data

The primary method for creating or overwriting data is `.make()`. It offers a flexible and efficient way to handle document creation.

### Using `store.make()` for Auto-Generated IDs

When you want the database to generate a unique ID for your new document, call `make()` on a `StoreRef`. By default, this operation resolves with the ID of the newly created document.

```typescript
const users = db.store('users');

const newUserId = await users.make({
  name: 'John Doe',
  email: 'john.doe@example.com',
  createdAt: Date.now()
});

console.log('Created new user with ID:', newUserId); 
// Example output: 'Created new user with ID: aF4jKq8sLp9wXzY3vG2u'
```

### Retrieving the Full Document with `.withSnapshot()`

Often, you need the complete document (including its new ID) immediately after creating it. Instead of performing a second read operation, you can chain `.withSnapshot()` to the `make()` call. This tells Indinis to return the full document data in a single, atomic operation.

```typescript
const users = db.store('users');

const newUserDoc = await users.make({
  name: 'Jane Smith',
  email: 'jane.smith@example.com',
  createdAt: Date.now()
}).withSnapshot();

console.log('Created new user:', newUserDoc);
/*
Example output:
{
  name: 'Jane Smith',
  email: 'jane.smith@example.com',
  createdAt: 1672531200000,
  id: 'bH5iLr9tMq0xZaZ4wH3v' 
}
*/
```

This approach is more efficient as it saves a database round-trip.

### Using `item.make()` for Specified IDs

*(This part of the documentation remains the same as before.)*

When you know the ID you want to use for a document, create an `ItemRef` first and then call `make()`.

```typescript
const users = db.store('users');

await users.item('john-doe-123').make({
  name: 'John Doe',
  email: 'john.doe@example.com',
  createdAt: Date.now()
});
```

---

This implementation successfully adds the requested feature with a clean, fluent API and provides clear documentation for developers to use it.