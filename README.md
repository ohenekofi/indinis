# Indinis DB

[![npm version](https://img.shields.io/npm/v/indinis.svg)](https://www.npmjs.com/package/indinis)
[![build status](https://img.shields.io/github/actions/workflow/status/your-repo/indinis/ci.yml?branch=main)](https://github.com/your-repo/indinis/actions)
[![license](https://img.shields.io/npm/l/indinis.svg)](https://opensource.org/licenses/MIT)

**Indinis** is a high-performance, embedded NoSQL database for Node.js and TypeScript, featuring a fluent, Firestore-like API. It's designed to be fast, easy to use, and powerful enough for complex applications.

### Key Features

*   **Fluent, Type-Safe API**: A modern, chainable API that feels familiar and boosts productivity.
*   **High-Performance Caching**: Optional in-memory cache to dramatically accelerate read-heavy workloads.
*   **Powerful Indexing**: Automatic indexing for common fields and robust support for secondary and multikey indexes.
*   **Advanced Queries**: Perform rich queries with filtering, sorting, pagination, and array operators (`arrayContains`, `arrayContainsAny`).
*   **Atomic Operations**: Safe, concurrent-friendly atomic increments for counters.
*   **Composable Queries**: Build clean, reusable, and maintainable query logic.
*   **Zero Dependencies**: A lightweight, self-contained package.

## Table of Contents

1.  [Installation](#installation)
2.  [Quick Start](#quick-start)
3.  [Core Concepts: Stores and Items](#core-concepts-stores-and-items)
4.  [Core API Guide](#core-api-guide)
    *   [Creating Data](#creating-data)
    *   [Reading Data](#reading-data)
    *   [Updating Data](#updating-data)
    *   [Deleting Data](#deleting-data)
5.  [Querying Data](#querying-data)
    *   [Filtering with `.filter()`](#filtering-with-filter)
    *   [Querying Arrays](#querying-arrays)
    *   [Sorting with `.sortBy()`](#sorting-with-sortby)
    *   [Pagination](#pagination)
6.  [Advanced Topics](#advanced-topics)
    *   [In-Memory Data Caching](#in-memory-data-caching)
    *   [Indexing](#indexing)
    *   [Composable Queries with `.use()`](#composable-queries-with-use)
7.  [Configuration](#configuration)
8.  [License](#license)

## Installation

```bash
npm install indinis
```

## Quick Start

Here's a simple example to get you up and running in minutes.

```typescript
import { Indinis } from 'indinis';

// Define a type for your data for type-safety
interface User {
  id?: string;
  name: string;
  email: string;
  createdAt: number;
}

async function main() {
  // 1. Initialize the database
  const db = new Indinis('./my-first-database');
  const usersStore = db.store<User>('users');

  // 2. Create a new user with an auto-generated ID
  console.log('Creating a new user...');
  const newUser = await usersStore.make({
    name: 'Alice',
    email: 'alice@example.com',
    createdAt: Date.now()
  }).withSnapshot(); // .withSnapshot() returns the full document

  console.log('User created:', newUser);

  // 3. Read the user back by their ID
  console.log('\nFetching user by ID...');
  const fetchedUser = await usersStore.item(newUser.id!).one();
  console.log('Found:', fetchedUser);

  // 4. Query for the user by their email
  console.log('\nQuerying for user by email...');
  const userFromQuery = await usersStore
    .filter('email').equals('alice@example.com')
    .one();
  console.log('Found via query:', userFromQuery);
}

main().catch(console.error);
```

## Core Concepts: Stores and Items

Indinis organizes data in a hierarchical structure familiar to users of document databases.

-   **Store**: A container for documents, analogous to a *collection* in Firestore or a table. Store paths must have an **odd number of segments** (e.g., `'users'`, `'users/user123/posts'`).
-   **Item**: An individual document within a store. Item paths must have an **even number of segments** (e.g., `'users/user123'`).

```typescript
import { Indinis } from 'indinis';

const db = new Indinis('./my-database');

// A reference to the top-level 'users' store
const usersStore = db.store('users');

// A reference to a specific user item
const userItem = usersStore.item('user123');

// A reference to a nested 'posts' store for a user
const userPostsStore = db.store('users/user123/posts');
```

## Core API Guide

### Creating Data

Use `.make()` to create or overwrite documents.

#### Auto-Generated IDs

Call `.make()` on a `StoreRef` to create a document with a unique, auto-generated ID.

```typescript
const users = db.store('users');

// By default, make() resolves with the new document's ID
const newUserId = await users.make({
  name: 'John Doe',
  email: 'john.doe@example.com'
});
console.log('Created user with ID:', newUserId);

// Chain .withSnapshot() to get the full document back immediately
const newUserDoc = await users.make({
  name: 'Jane Smith',
  email: 'jane.smith@example.com'
}).withSnapshot();

console.log('Created new user:', newUserDoc);
// { name: 'Jane Smith', ..., id: 'bH5iLr9tMq0xZaZ4wH3v' }
```

#### Specified IDs

Call `.make()` on an `ItemRef` to create a document with an ID you provide. By default, this fails if the document already exists to prevent accidental overwrites.

```typescript
const userItem = db.store('users').item('john-doe-123');
await userItem.make({ name: 'John Doe', email: 'john@example.com' });

// To overwrite an existing document, pass `true` as the second argument.
await userItem.make({ name: 'John Doe V2', status: 'active' }, true);
```

### Reading Data

#### Fetch a Single Item by ID

The most direct way to get a document is by its ID using `item(...).one()`.

```typescript
const userData = await db.store('users').item('john-doe-123').one();

if (userData) {
  console.log(userData.name); // 'John Doe V2'
}
```

You can also efficiently check for existence with `.exists()`.

```typescript
const userExists = await db.store('users').item('john-doe-123').exists(); // true or false
```

### Updating Data

#### Partial Updates with `modify()`

Use `.modify()` on an `ItemRef` to update specific fields of a document without replacing the entire object. This method will fail if the document does not exist.

```typescript
const userItem = db.store('users').item('john-doe-123');

// Update the user's status and add a new 'lastLogin' field
await userItem.modify({
  status: 'active',
  lastLogin: Date.now()
});
```

#### Atomic Increments

For counters (likes, views, etc.), use the atomic `increment()` operator to prevent race conditions. Import the `increment` function from the `indinis` library.

**Rules:** The target field must exist and its value must be a number.

```typescript
import { Indinis, increment } from 'indinis';

const postRef = db.store('posts').item('postXYZ');

// Atomically increment 'views' and update 'lastViewedBy' in one operation
await postRef.modify({
  views: increment(1),
  lastViewedBy: 'user-abc'
});

// You can also decrement
await postRef.modify({ likes: increment(-1) });
```

### Deleting Data

Use `.remove()` on an `ItemRef` to delete a document. The operation completes silently if the document doesn't exist.

```typescript
await db.store('users').item('user123').remove();
```

## Querying Data

Build queries by starting with a `store()` or `query()` and chaining methods.

### Filtering with `.filter()`

Use `.filter()` with a chainable operator to define your query conditions.

| Operator             | Description                         |
| -------------------- | ----------------------------------- |
| `.equals(val)`       | Field is equal to `val`             |
| `.greaterThan(val)`  | Field is greater than `val`         |
| `.greaterThanOrEqual(val)` | Field is >= `val`             |
| `.lessThan(val)`     | Field is less than `val`            |
| `.lessThanOrEqual(val)` | Field is <= `val`                |

You can chain multiple `.filter()` calls. For a query to succeed, **at least one of the filtered fields must be indexed**.

```typescript
const activeEngineers = await db.store<Employee>('employees')
  .filter('department').equals('eng')         // Uses an index
  .filter('status').equals('active')          // Post-filtered in memory
  .filter('level').greaterThanOrEqual(5)      // Post-filtered in memory
  .take(10); // Retrieves up to 10 matching documents
```

### Querying Arrays

Indinis supports powerful queries on array fields, provided a **multikey index** exists on the field.

#### Creating a Multikey Index

```typescript
// Create an index on the 'tags' array field for the 'products' store
await db.createIndex('products', 'idx_prod_tags', {
  field: 'tags',
  multikey: true
});
```

#### `arrayContains`

Find documents where the array field contains a specific element.

```typescript
// Find all products tagged as 'electronics'
const electronics = await db.store('products')
  .filter('tags').arrayContains('electronics')
  .take();
```

#### `arrayContainsAny`

Find documents where the array field contains at least one of the elements from the provided list.

```typescript
// Find products that are on 'sale' OR are a 'bestseller'
const featured = await db.store('products')
  .filter('tags').arrayContainsAny(['sale', 'bestseller'])
  .take();
```

### Sorting with `.sortBy()`

Use `.sortBy()` to order your query results. Sorting requires an index on the field you are sorting by.

```typescript
const recentUsers = await db.store('users')
  .sortBy('createdAt', 'desc') // Sort by creation date, newest first
  .take(20);
```

### Pagination

Indinis provides efficient, cursor-based pagination. Pagination queries **must** include at least one `.sortBy()` clause.

The `.get()` method executes the query and returns a `PaginatedQueryResult` object containing the documents, cursors, and page information.

#### Example: Paginating through Users

```typescript
const usersStore = db.store('users');
const PAGE_SIZE = 10;

// --- Fetch the First Page ---
const firstPage = await usersStore.query()
  .sortBy('name', 'asc')
  .limit(PAGE_SIZE)
  .get();

console.log('First page users:', firstPage.docs.map(u => u.name));
console.log('Has next page?', firstPage.hasNextPage);

// --- Fetch the Next Page ---
if (firstPage.hasNextPage) {
  const nextPage = await usersStore.query()
    .sortBy('name', 'asc')    // The query must be identical
    .limit(PAGE_SIZE)
    .startAfter(...firstPage.endCursor!) // Use the cursor from the previous page
    .get();

  console.log('Next page users:', nextPage.docs.map(u => u.name));
}
```

## Advanced Topics

### In-Memory Data Caching

Indinis includes an optional in-memory cache to dramatically accelerate read-heavy workloads. The cache operates transparently using a cache-aside pattern: items are cached on first read, and automatically invalidated on any write (`set`, `modify`, `remove`) to guarantee consistency.

#### Enabling and Configuring the Cache

Enable the cache via `IndinisOptions`.

```typescript
import { Indinis, IndinisOptions } from 'indinis';

const dbOptions: IndinisOptions = {
  enableCache: true, // Simple enablement with defaults
  
  // Advanced configuration
  cacheOptions: {
    maxSize: 5000,                  // Max items in cache (default: 1000)
    policy: 'LRU',                  // Eviction policy: 'LRU' or 'LFU'
    defaultTTLMilliseconds: 60000,  // Expire entries after 1 minute (default: 0, no TTL)
    enableStats: true               // Set true to monitor performance
  }
};

const db = new Indinis('./my-cached-db', dbOptions);
```

#### Monitoring Cache Performance

Use `db.getCacheStats()` to check the cache's effectiveness. The most important metric is the `hitRate`.

```typescript
const stats = await db.getCacheStats();
if (stats) {
  console.log(`Cache Hit Rate: ${(stats.hitRate * 100).toFixed(2)}%`);
}
```

### Indexing

Indinis is designed for performance and will not perform slow, un-indexed collection scans.

**Rule: No Index, No Query.** A query with one or more `.filter()` conditions **must** be serviceable by at least one existing index. If no suitable index is found, the operation will throw an error.

#### Automatic `name` Index

For convenience, the **first time** a document is written to a new store, Indinis automatically creates a default secondary index on the `name` field. This allows you to start querying immediately on new collections.

#### Listing Indexes

You can see which indexes are available for a specific collection.

```typescript
const userIndexes = await db.store('users').listIndexes();
console.log(userIndexes);
```

### Composable Queries with `.use()`

For complex applications, you can encapsulate and reuse query logic using `.use()`. Define reusable parts as functions that take and return a `Query` object.

```typescript
import { IQuery } from 'indinis';

// A reusable part to find only active users
const isActive = (query: IQuery<User>) => query.filter('status').equals('active');

// A parameterized part to find users by region
const byRegion = (region: string) => 
  (query: IQuery<User>) => query.filter('region').equals(region);

// Compose the parts to build a clean, readable query
const recentActiveWestCoastUsers = await db.store<User>('users')
  .query() // Start a composable query
  .use(isActive)
  .use(byRegion('us-west'))
  .sortBy('createdAt', 'desc')
  .take(20);
```

## Configuration

You can customize the database behavior by passing an `IndinisOptions` object to the constructor.

```typescript
import { Indinis, IndinisOptions } from 'indinis';

const dbOptions: IndinisOptions = {
  enableCache: true,
  cacheOptions: {
    maxSize: 10000
  },
  // ... other options
};

const db = new Indinis('./my-database', dbOptions);
```

## License

[MIT](LICENSE)