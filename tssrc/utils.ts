// @tssrc/utils.ts

// --- Helper Functions ---

/** Generates a Firestore-style random ID (20 alphanumeric chars). */
export const generateId = (): string => {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let id = '';
    for (let i = 0; i < 20; i++) {
        id += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return id;
};

/** Constructs the full storage key. */
export function constructKey(storePath: string, itemId: string): string {
     // Implementation unchanged...
    if (!storePath) throw new Error("Store path cannot be empty.");
    if (!itemId) throw new Error("Item ID cannot be empty.");
    const cleanedPath = storePath.endsWith('/') ? storePath.slice(0, -1) : storePath;
    const cleanedId = itemId.startsWith('/') ? itemId.slice(1) : itemId; // Avoid double slash if ID starts with /
    return `${cleanedPath}/${cleanedId}`;
}

/** Counts non-empty segments in a path separated by '/'. */
export function countPathSegments(pathStr: string): number {
     if (!pathStr) return 0;
     return pathStr.split('/').filter(s => s.length > 0).length;
 }

/** Validates a path for a collection/subcollection (must have ODD number of segments). */
export function isValidCollectionPath(storePath: string): boolean {
     return countPathSegments(storePath) % 2 !== 0;
 }

/** Validates a path for a document/item (must have EVEN number of segments). */
export function isValidDocumentPath(fullKey: string): boolean {
     return countPathSegments(fullKey) % 2 === 0;
 }