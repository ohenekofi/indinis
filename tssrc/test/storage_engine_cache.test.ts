// @tssrc/test/storage_engine_cache.test.ts

import { Indinis, IndinisOptions, StorageValue, CacheStatsJs } from '../index'; // Import CacheStatsJs
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';

const JEST_HOOK_TIMEOUT = 90000; // 90 seconds for setup/teardown hooks
const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-storage-engine-cache-v1');

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const s = (obj: any): string => JSON.stringify(obj);

interface CacheTestDoc {
    id: string;
    data: string;
    version: number;
}

// This interface represents the JS structure for cache options passed to Indinis constructor
interface JsCacheOptionsForTest {
    max_size?: number;
    policy_str?: "LRU" | "LFU"; // Policy as string
    default_ttl_ms?: number;    // TTL as milliseconds
    enable_stats?: boolean;
}

describe('StorageEngine Data Cache Functionality', () => {
    let db: Indinis | null = null;
    let testDataDir: string; // Will be set in beforeEach for each test
    const colPath = 'cached_docs';

    // Function to initialize DB with specific cache options
    const initializeDbWithCache = async (
        dirSuffix: string,
        enableCache: boolean,
        jsCacheConfig?: JsCacheOptionsForTest // Use the JS-friendly options type
    ): Promise<Indinis> => {
        const currentTestDataDir = path.join(TEST_DATA_DIR_BASE, `test-${dirSuffix}-${Date.now()}-${Math.random().toString(16).slice(2)}`);
        await fs.promises.mkdir(currentTestDataDir, { recursive: true });
        testDataDir = currentTestDataDir; // Update global for cleanup
        console.log(`[CACHE TEST INIT - ${dirSuffix}] Using data directory: ${testDataDir}`);

        const indinisOpts: IndinisOptions = {
            checkpointIntervalSeconds: 10,
            enableCache: enableCache,
            cacheOptions: enableCache && jsCacheConfig ? { // Map to CacheOptionsJsInternal (defined in index.ts)
                maxSize: jsCacheConfig.max_size,
                policy: jsCacheConfig.policy_str,
                defaultTTLMilliseconds: jsCacheConfig.default_ttl_ms,
                enableStats: jsCacheConfig.enable_stats,
            } : undefined,
            walOptions: { // Basic WAL config
                wal_directory: path.join(testDataDir, 'cache_test_wal_for_' + dirSuffix),
                segment_size: 1024 * 1024, // 1MB WAL segments
            }
        };
        return new Indinis(testDataDir, indinisOpts);
    };

    beforeAll(async () => {
        await fs.promises.mkdir(TEST_DATA_DIR_BASE, { recursive: true });
    }, JEST_HOOK_TIMEOUT);

    afterEach(async () => {
        console.log(`[CACHE TEST END - ${path.basename(testDataDir)}] Closing database...`);
        if (db) {
            try { await db.close(); } catch (e) { console.error("Error closing DB in afterEach:", e); }
            db = null;
        }
        await delay(300); // Increased delay
        if (testDataDir && fs.existsSync(testDataDir)) {
            console.log(`[CACHE TEST END - ${path.basename(testDataDir)}] Cleaning up test directory:`, testDataDir);
            await rimraf(testDataDir, { maxRetries: 3, retryDelay: 700 }).catch(err => // Increased delay
                console.error(`Cleanup error for ${testDataDir}:`, err)
            );
        }
    }, JEST_HOOK_TIMEOUT);

    afterAll(async () => {
        console.log("[CACHE SUITE END] Cleaning up base directory...");
        if (fs.existsSync(TEST_DATA_DIR_BASE)) {
            await rimraf(TEST_DATA_DIR_BASE).catch(err => console.error(`Base cleanup error:`, err));
        }
    }, JEST_HOOK_TIMEOUT);

    jest.setTimeout(60000); // Default timeout per test case

    it('should serve data from cache after first read (cache enabled, LRU)', async () => {
        db = await initializeDbWithCache('hit-lru', true, {
            max_size: 100,
            policy_str: "LRU",
            enable_stats: true
        });

        const key1 = `${colPath}/item1_lru`;
        const doc1: CacheTestDoc = { id: "item1_lru", data: "Initial data for LRU item1", version: 1 };

        await db.transaction(async tx => { await tx.set(key1, s(doc1)); });

        console.log("Test: cache-hit-lru - First read for key1 (expect cache miss, populate)");
        await db.transaction(async tx => {
            const val = await tx.get(key1);
            expect(val).not.toBeNull();
            expect(JSON.parse(val as string)).toEqual(doc1);
        });

        let stats = await db.getCacheStats();
        expect(stats).not.toBeNull();
        const initialHits = stats?.hits || 0;
        const initialMisses = stats?.misses || 0;
        expect(initialMisses).toBeGreaterThanOrEqual(1);

        console.log("Test: cache-hit-lru - Second read for key1 (expect cache hit)");
        await db.transaction(async tx => {
            const val = await tx.get(key1);
            expect(val).not.toBeNull();
            expect(JSON.parse(val as string)).toEqual(doc1);
        });

        stats = await db.getCacheStats();
        expect(stats).not.toBeNull();
        if (stats) {
            console.log("Test: cache-hit-lru - Stats after second read:", stats);
            expect(stats.hits).toBeGreaterThan(initialHits);
        }
    });

    it('should not use cache if disabled', async () => {
        db = await initializeDbWithCache('disabled', false);

        const key1 = `${colPath}/item_nocache`;
        const doc1: CacheTestDoc = { id: "item_nocache", data: "Data no cache", version: 1 };

        await db.transaction(async tx => { await tx.set(key1, s(doc1)); });
        await db.transaction(async tx => { await tx.get(key1); });
        
        const stats = await db.getCacheStats();
        expect(stats).toBeNull();
    });

    it('should invalidate cache entry on update', async () => {
        db = await initializeDbWithCache('invalidate-update', true, { max_size: 10, enable_stats: true });
        const key1 = `${colPath}/item_upd_inv`;
        const docV1: CacheTestDoc = { id: "item_upd_inv", data: "Version 1 data inv", version: 1 };
        const docV2: CacheTestDoc = { id: "item_upd_inv", data: "Version 2 data inv - updated", version: 2 };

        await db.transaction(async tx => { await tx.set(key1, s(docV1)); });
        await db.transaction(async tx => { await tx.get(key1); }); // Populate cache

        let stats = await db.getCacheStats();
        const missesBeforeUpdate = stats?.misses || 0;

        console.log("Test: cache-invalidate-update - Updating key1 to Version 2");
        await db.transaction(async tx => { 
            await tx.set(key1, s(docV2), { overwrite: true }); 
        });

        console.log("Test: cache-invalidate-update - Reading key1 after update (expect miss for V2)");
        await db.transaction(async tx => {
            const val = await tx.get(key1);
            expect(val).not.toBeNull();
            expect(JSON.parse(val as string)).toEqual(docV2);
        });

        stats = await db.getCacheStats();
        expect(stats).not.toBeNull();
        if (stats) {
            expect(stats.misses).toBeGreaterThan(missesBeforeUpdate);
        }
    });

    it('should invalidate cache entry on remove', async () => {
        db = await initializeDbWithCache('invalidate-remove', true, { max_size: 10, enable_stats: true });
        const key1 = `${colPath}/item_rem_inv`;
        const doc1: CacheTestDoc = { id: "item_rem_inv", data: "To be removed inv", version: 1 };

        await db.transaction(async tx => { await tx.set(key1, s(doc1)); });
        await db.transaction(async tx => { await tx.get(key1); }); // Populate

        const missesBeforeRemove = (await db.getCacheStats())?.misses || 0;

        console.log("Test: cache-invalidate-remove - Removing key1");
        await db.transaction(async tx => { await tx.remove(key1); });

        console.log("Test: cache-invalidate-remove - Reading key1 after remove (expect null & miss)");
        await db.transaction(async tx => {
            const val = await tx.get(key1);
            expect(val).toBeNull();
        });

        const stats = await db.getCacheStats();
        expect(stats).not.toBeNull();
        if (stats) {
            expect(stats.misses).toBeGreaterThan(missesBeforeRemove);
        }
    });

    it('should respect TTL for cache entries', async () => {
        const shortTTL_ms = 150; // Slightly longer to be safer with JS timing
        db = await initializeDbWithCache('ttl', true, {
            max_size: 10,
            default_ttl_ms: shortTTL_ms,
            enable_stats: true
        });

        const keyTTL = `${colPath}/item_ttl_expire`;
        const docTTL: CacheTestDoc = { id: "item_ttl_expire", data: "TTL test data for expiry", version: 1 };

        await db.transaction(async tx => { await tx.set(keyTTL, s(docTTL)); });
        console.log("Test: cache-ttl - Reading keyTTL (populates cache with TTL)");
        await db.transaction(async tx => {
            const val = await tx.get(keyTTL);
            expect(JSON.parse(val as string).data).toEqual(docTTL.data);
        });

        let stats = await db.getCacheStats();
        const initialMisses = stats?.misses || 0;
        const initialExpired = stats?.expiredRemovals || 0;

        console.log(`Test: cache-ttl - Waiting for TTL (${shortTTL_ms * 1.8} ms) to expire...`);
        await delay(shortTTL_ms * 1.8); // Wait longer than TTL

        console.log("Test: cache-ttl - Reading keyTTL after TTL expiry (expect miss & expired increment)");
        await db.transaction(async tx => {
            const val = await tx.get(keyTTL); // This should be a miss, then fetch from LSM
            expect(val).not.toBeNull();
            expect(JSON.parse(val as string).data).toEqual(docTTL.data);
        });

        stats = await db.getCacheStats();
        expect(stats).not.toBeNull();
        if (stats) {
            console.log("Test: cache-ttl - Stats after TTL expiry and re-read:", stats);
            expect(stats.misses).toBeGreaterThan(initialMisses);
            // The cache::Cache::get() method, if it finds an expired item,
            // removes it and records an expired_removal THEN records a miss.
            expect(stats.expiredRemovals).toBeGreaterThan(initialExpired);
        }
    });

    it('LRU cache eviction should work when max_size is reached', async () => {
        const cacheSize = 3;
        db = await initializeDbWithCache('evict-lru', true, {
            max_size: cacheSize,
            policy_str: "LRU",
            enable_stats: true
        });

        const keys: string[] = [];
        const docsMap = new Map<string, CacheTestDoc>();

        console.log(`Test: cache-evict-lru - Writing ${cacheSize + 2} items to exceed cache size ${cacheSize}...`);
        for (let i = 0; i < cacheSize + 2; i++) {
            const key = `${colPath}/lru_evict_item_${i}`;
            const doc: CacheTestDoc = { id: `lru_evict_item_${i}`, data: `LRU Data ${i}`, version: 1 };
            keys.push(key);
            docsMap.set(key, doc);
            await db.transaction(async tx => { await tx.set(key, s(doc)); });
            await db.transaction(async tx => { await tx.get(key); }); // Read to make it MRU
        }

        const statsAfterWrites = await db.getCacheStats();
        console.log("Test: cache-evict-lru - Stats after initial writes/reads:", statsAfterWrites);
        // keys[0] and keys[1] should have been evicted.
        // In cache (MRU to LRU): keys[4], keys[3], keys[2]

        console.log("Test: cache-evict-lru - Verifying eviction...");
        let missesForEvicted = 0;
        let hitsForKept = 0;

        // Attempt to read keys[0] - should be a miss (evicted)
        await db.transaction(async tx => { await tx.get(keys[0]); });
        let currentStats = await db.getCacheStats();
        if (currentStats && statsAfterWrites) missesForEvicted += (currentStats.misses - statsAfterWrites.misses);

        // Attempt to read keys[1] - should be a miss (evicted)
        await db.transaction(async tx => { await tx.get(keys[1]); });
        currentStats = await db.getCacheStats();
        if (currentStats && statsAfterWrites) missesForEvicted += (currentStats.misses - statsAfterWrites.misses - (missesForEvicted > 0 ? 1:0) );


        // Attempt to read keys[2], keys[3], keys[4] - these were the last ones, should be hits
        for (let i = cacheSize -1; i < keys.length; i++) { // keys[2], keys[3], keys[4] if cacheSize=3
            await db.transaction(async tx => { await tx.get(keys[i]); });
        }
        const statsAfterVerification = await db.getCacheStats();

        console.log("Test: cache-evict-lru - Stats after verification reads:", statsAfterVerification);
        if (statsAfterWrites && statsAfterVerification) {
            // Expect at least 2 misses for keys[0] and keys[1]
            expect(statsAfterVerification.misses).toBeGreaterThanOrEqual(statsAfterWrites.misses + 2);
            // Expect evictions to have occurred
            expect(statsAfterVerification.evictions).toBeGreaterThanOrEqual(2);
        }

        // Final check on data
        await db.transaction(async tx => {
            for (const key of keys) {
                const val = await tx.get(key);
                expect(val).not.toBeNull();
                expect(JSON.parse(val as string).data).toEqual(docsMap.get(key)?.data);
            }
        });
    });
});

// TypeScript type declaration for the C++ cache namespace (if not already in a .d.ts file)
// This is for the test file's internal type-checking when using cacheOptionsForCpp.
// It doesn't affect the JS interface of Indinis itself.
declare global {
    namespace cache {
        enum EvictionPolicy { LRU = 0, LFU = 1, FIFO = 2, TTL = 3 }
        interface CacheConfig { // Mirrors the C++ struct for test helper clarity
            max_size?: number;
            policy?: EvictionPolicy;
            default_ttl?: { count: () => number }; // Mock for std::chrono::milliseconds
            enable_stats?: boolean;
            shard_count?: number; // Though not used by provided simple cache
        }
    }
}