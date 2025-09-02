//tssrc/test/cloud_federated_mock.test.ts
import { Indinis, CloudProviderAdapter, SyncRuleset, fg, Infer, OutboxRecord, UnsubscribeFunction } from '../index';
import * as fs from 'fs';
import * as path from 'path';
import { rimraf } from 'rimraf';
import { EventEmitter } from 'events';

const TEST_DATA_DIR_BASE = path.resolve(__dirname, '..', '..', '.test-data', 'indinis-cloud-federation-mock');

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// --- Test Data and Schema ---
const UserSchema = fg.object({
  id: fg.string(),
  name: fg.string().min(2, "Name is too short.").sanitize({ trim: true }),
  email: fg.string().email().sanitize({ lowercase: true }),
  version: fg.number().int().min(1),
});
type User = Infer<typeof UserSchema>;

// --- Mock Cloud Provider Adapter ---
// This class simulates a cloud backend, allowing us to control its behavior
// for testing (e.g., simulate failures, check if methods were called).
class MockCloudAdapter extends EventEmitter implements CloudProviderAdapter<User> {
    public cloudData = new Map<string, User>();
    public methodCalls = {
        get: 0,
        set: 0,
        remove: 0,
        onSnapshot: 0,
    };
    public shouldFail = false; // Toggle to simulate network errors
    private snapshotListeners = new Map<string, (data: User | null) => void>();

    async get(key: string): Promise<User | null> {
        this.methodCalls.get++;
        if (this.shouldFail) throw new Error("Mock Cloud Error: GET failed");
        return this.cloudData.get(key) || null;
    }

    async set(key: string, data: User, ruleset: SyncRuleset<User>): Promise<void> {
        this.methodCalls.set++;
        if (this.shouldFail) throw new Error("Mock Cloud Error: SET failed");
        if (ruleset.onConflict === 'CloudWins' && this.cloudData.has(key)) {
            const existing = this.cloudData.get(key)!;
            if (existing.version > (data.version - 1)) {
                throw new Error("Conflict detected: Cloud version is newer.");
            }
        }
        this.cloudData.set(key, data);
        if (this.snapshotListeners.has(key)) {
            this.snapshotListeners.get(key)!(data);
        }
    }

    async remove(key: string): Promise<void> {
        this.methodCalls.remove++;
        if (this.shouldFail) throw new Error("Mock Cloud Error: REMOVE failed");
        this.cloudData.delete(key);
        if (this.snapshotListeners.has(key)) {
            this.snapshotListeners.get(key)!(null);
        }
    }
    
    onSnapshot(key: string, onUpdate: (data: User | null) => void): UnsubscribeFunction {
        this.methodCalls.onSnapshot++;
        this.snapshotListeners.set(key, onUpdate);
        setTimeout(() => onUpdate(this.cloudData.get(key) || null), 10);
        return () => {
            this.snapshotListeners.delete(key);
        };
    }

    async getConnectionState(): Promise<'connected' | 'disconnected'> {
        return this.shouldFail ? 'disconnected' : 'connected';
    }

    reset() {
        this.cloudData.clear();
        this.methodCalls = { get: 0, set: 0, remove: 0, onSnapshot: 0 };
        this.shouldFail = false;
    }
}

describe('Indinis Cloud Federation Layer with Mock Adapter', () => {
    let db: Indinis;
    let testDataDir: string;
    let mockAdapter: MockCloudAdapter;

    const usersPath = 'users';
    const userId = 'user123'; // The ID of the item
    const userKey = `${usersPath}/${userId}`; // The full path for verification

    jest.setTimeout(30000);

    beforeEach(async () => {
        const randomSuffix = `${Date.now()}`;
        testDataDir = path.join(TEST_DATA_DIR_BASE, `test-${randomSuffix}`);
        await fs.promises.mkdir(testDataDir, { recursive: true });
        db = new Indinis(testDataDir);
        mockAdapter = new MockCloudAdapter();
        db.configureCloud(mockAdapter);
        console.log(`\n[MOCK ADAPTER TEST START] Using data directory: ${testDataDir}`);
    });

    afterEach(async () => {
        if (db) await db.close();
        if (fs.existsSync(testDataDir)) await rimraf(testDataDir);
    });

    afterAll(async () => {
        if (fs.existsSync(TEST_DATA_DIR_BASE)) await rimraf(TEST_DATA_DIR_BASE);
    });

    it('should run validator before any other operation', async () => {
        const rules: Partial<SyncRuleset<User>> = { validator: UserSchema };
        const users = db.cloudStore<User>(usersPath, rules);
        const invalidUserId = 'invalidUser';
        const invalidUserData = { name: 'A', email: 'not-an-email', version: 0 };
        
        await expect(
            users.item(invalidUserId).make(invalidUserData)
        ).rejects.toThrow(/Data validation failed.*Name is too short.*Invalid email/);
        
        expect(mockAdapter.methodCalls.set).toBe(0);
        const localData = await db.store(usersPath).item(invalidUserId).one();
        expect(localData).toBeNull();
    });

    it('should follow "LocalFirstWithOutbox" write strategy', async () => {
        const users = db.cloudStore<User>(usersPath, { validator: UserSchema });
        const user2Id = 'user456';
        const user2Data: Omit<User, 'id'> = { name: 'Bob', email: 'bob@example.com', version: 1 };
        
        mockAdapter.shouldFail = true;
        
        // This call will now resolve quickly after the local commit.
        await users.item(user2Id).make(user2Data);

        // Verify local write succeeded
        const localUser2 = await db.store<User>(usersPath).item(user2Id).one();
        expect(localUser2?.name).toBe('Bob');
        await delay(500);
        // --- Poll the outbox until the item appears or we time out ---
        let outboxItems: OutboxRecord[] = [];
        const startTime = Date.now();
        const timeout = 5000;

        console.log("Polling for outbox record...");
        while (Date.now() - startTime < timeout) {
            outboxItems = await db.store<OutboxRecord>('_outbox').take();
            if (outboxItems.length > 0) {
                console.log("Found outbox record!");
                break;
            }
            await delay(200); // Poll every 200ms
        }
        
        // <<< FIX IS HERE: Use `throw new Error` instead of `fail`. >>>
        if (outboxItems.length === 0) {
            throw new Error("Outbox record was not created within the 5-second timeout.");
        }

        expect(outboxItems.length).toBe(1);
        expect(outboxItems[0].key).toBe(`${usersPath}/${user2Id}`);
        expect(outboxItems[0].operation).toBe('set');
        expect(outboxItems[0].errorCount).toBe(1);
        expect(outboxItems[0].lastError).toContain("Mock Cloud Error: SET failed");
    });

    it('should follow "LocalFirst" read strategy', async () => {
        const rules: Partial<SyncRuleset<User>> = { readStrategy: 'LocalFirst' };
        const users = db.cloudStore<User>(usersPath, rules);
        const localOnlyData: User = { id: userId, name: 'Local Alice', email: 'local@a.com', version: 1 };
        
        await db.store<User>(usersPath).item(userId).make(localOnlyData);
        const result = await users.item(userId).one();
        
        expect(result?.name).toBe('Local Alice');
        expect(mockAdapter.methodCalls.get).toBe(0);
    });

    it('should follow "CloudFirst" read strategy', async () => {
        const rules: Partial<SyncRuleset<User>> = { readStrategy: 'CloudFirst' };
        const users = db.cloudStore<User>(usersPath, rules);
        const cloudOnlyData: User = { id: userId, name: 'Cloud Alice', email: 'cloud@a.com', version: 2 };
        
        mockAdapter.cloudData.set(userKey, cloudOnlyData);
        const result = await users.item(userId).one();
        
        expect(result?.name).toBe('Cloud Alice');
        expect(mockAdapter.methodCalls.get).toBe(1);

        await delay(100);
        const localData = await db.store<User>(usersPath).item(userId).one();
        expect(localData?.name).toBe('Cloud Alice');
    });

    it('should handle onSnapshot real-time updates', (done) => {
        const rules: Partial<SyncRuleset<User>> = { validator: UserSchema };
        const userItem = db.cloudStore<User>(usersPath, rules).item(userId);
        
        const updates: (User | null)[] = [];
        let unsubscribe: UnsubscribeFunction | null = null;
        
        const runTest = async () => {
            try {
                unsubscribe = userItem.onSnapshot(data => {
                    updates.push(data);
                    if (updates.length === 3) {
                        expect(updates[0]).toBeNull();
                        expect(updates[1]?.version).toBe(1);
                        expect(updates[2]?.version).toBe(2);
                        unsubscribe!();
                        done();
                    }
                });

                await delay(100);
                const docV1: User = { id: userId, name: 'Realtime User', email: 'rt@example.com', version: 1 };
                const fullRuleset: SyncRuleset<User> = { readStrategy: 'LocalFirst', writeStrategy: 'LocalFirstWithOutbox', onConflict: 'LastWriteWins' };
                await mockAdapter.set(userKey, docV1, fullRuleset);

                await delay(100);
                const docV2: User = { ...docV1, version: 2 };
                await mockAdapter.set(userKey, docV2, fullRuleset);
            } catch (error) {
                if (unsubscribe) unsubscribe();
                done(error);
            }
        };

        runTest();
    });

});