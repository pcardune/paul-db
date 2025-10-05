import { Struct } from "../binary/Struct.ts"
import { PageId } from "./BufferPool.ts"

export class IndexedDBWrapper {
  constructor(private db: IDBDatabase) {}

  setKeyVal<T>(key: string, value: T): Promise<void> {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction("keyval", "readwrite")
      const store = tx.objectStore("keyval")
      store.put(value, key)
      tx.oncomplete = () => {
        resolve()
      }
      tx.onerror = () => {
        reject(tx.error)
      }
    })
  }

  getKeyVal<T>(key: string): Promise<T | undefined> {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction("keyval")
      const store = tx.objectStore("keyval")
      const req = store.get(key)
      req.onsuccess = () => {
        resolve(req.result)
      }
      req.onerror = () => {
        reject(req.error)
      }
    })
  }

  deletePages(pageIds: PageId[]): Promise<void> {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction("pages", "readwrite")
      const store = tx.objectStore("pages")
      for (const pageId of pageIds) {
        store.delete(Struct.bigUint64.toArrayBuffer(pageId))
      }
      tx.oncomplete = () => {
        resolve()
      }
      tx.onerror = () => {
        reject(tx.error)
      }
    })
  }

  getPage(pageId: PageId): Promise<Uint8Array | undefined> {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction("pages")
      const store = tx.objectStore("pages")
      const req = store.get(Struct.bigUint64.toArrayBuffer(pageId))
      req.onsuccess = () => {
        resolve(req.result)
      }
      req.onerror = () => {
        reject(req.error)
      }
    })
  }

  setPages(pages: { pageId: PageId; data: Uint8Array }[]): Promise<void> {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction("pages", "readwrite")
      const store = tx.objectStore("pages")
      for (const { pageId, data } of pages) {
        store.put(data, Struct.bigUint64.toArrayBuffer(pageId))
      }
      tx.oncomplete = () => {
        resolve()
      }
      tx.onerror = () => {
        reject(tx.error)
      }
    })
  }

  static open(
    name: string,
    indexedDB: IDBFactory = globalThis.indexedDB,
  ): Promise<IndexedDBWrapper> {
    if (!indexedDB) {
      throw new Error("indexedDB not available")
    }
    return new Promise((resolve, reject) => {
      const openRequest = indexedDB.open(name, 1)
      openRequest.onsuccess = () => {
        const db = openRequest.result
        resolve(new IndexedDBWrapper(db))
      }
      openRequest.onerror = () => {
        reject(openRequest.error)
      }
      openRequest.onupgradeneeded = () => {
        const db = openRequest.result
        db.createObjectStore("pages")
        db.createObjectStore("keyval")
      }
    })
  }
}
