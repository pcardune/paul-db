import type { Promisable, UnknownRecord } from "type-fest"
import { AsyncIterableWrapper } from "../async.ts"
import { ITableStorage } from "./TableStorage.ts"

export class IndexedDBTableStorage<RowData extends UnknownRecord>
  implements ITableStorage<number, RowData> {
  constructor(private db: IDBDatabase, private tableName: string) {}

  private _activeReadTransaction: IDBTransaction | null = null
  private _txReadPromise: Promise<void> | null = null
  private get readTransaction() {
    if (this._activeReadTransaction == null) {
      this._txReadPromise = new Promise((resolve, reject) => {
        this._activeReadTransaction = this.db.transaction(
          this.tableName,
          "readwrite",
        )
        this._activeReadTransaction!.oncomplete = () => {
          this._activeReadTransaction = null
          resolve()
        }
        this._activeReadTransaction!.onerror = () => {
          this._activeReadTransaction = null
          reject(
            this._activeReadTransaction!.error,
          )
        }
      })
    }
    return this._activeReadTransaction!
  }

  private _activeWriteTransaction: IDBTransaction | null = null
  private _txWritePromise: Promise<void> | null = null
  private get writeTransaction() {
    if (this._activeWriteTransaction == null) {
      this._txWritePromise = new Promise((resolve, reject) => {
        this._activeWriteTransaction = this.db.transaction(
          this.tableName,
          "readwrite",
        )
        this._activeWriteTransaction!.oncomplete = () => {
          this._activeWriteTransaction = null
          resolve()
        }
        this._activeWriteTransaction!.onerror = () => {
          this._activeWriteTransaction = null
          reject(
            this._activeWriteTransaction!.error,
          )
        }
      })
    }
    return this._activeWriteTransaction!
  }

  async waitForTransactions(): Promise<void> {
    await Promise.all([
      this._txReadPromise ?? Promise.resolve(),
      this._txWritePromise ?? Promise.resolve(),
    ])
  }

  /**
   * Creates an object store in the given database.
   */
  static createObjectStore(db: IDBDatabase, tableName: string) {
    db.createObjectStore(tableName, { autoIncrement: true })
  }

  get(id: number): Promisable<RowData | undefined> {
    return new Promise((resolve, reject) => {
      const req = this.readTransaction.objectStore(
        this.tableName,
      ).get(id)
      req.onsuccess = () => resolve(req.result)
      req.onerror = () => reject(req.error)
    })
  }

  set(id: number, data: RowData): Promisable<number> {
    return new Promise((resolve, reject) => {
      const req = this.writeTransaction.objectStore(
        this.tableName,
      ).put(data, id)
      req.onsuccess = () => resolve(id)
      req.onerror = () => reject(req.error)
    })
  }

  insert(data: RowData): Promisable<number> {
    return new Promise((resolve, reject) => {
      const req = this.writeTransaction.objectStore(
        this.tableName,
      ).add(data)
      req.onsuccess = () => resolve(req.result as number)
      req.onerror = () => reject(req.error)
    })
  }

  remove(id: number): Promisable<void> {
    return new Promise((resolve, reject) => {
      const req = this.writeTransaction.objectStore(
        this.tableName,
      ).delete(id)
      req.onsuccess = () => resolve()
      req.onerror = () => reject(req.error)
    })
  }

  commit(): Promisable<void> {
    this.writeTransaction.commit()
    const txPromise = this._txWritePromise!
    this._txWritePromise = null
    return txPromise
  }

  iterate(): AsyncIterableWrapper<[number, RowData]> {
    const store = this.readTransaction.objectStore(
      this.tableName,
    )
    return new AsyncIterableWrapper(async function* () {
      const req = store.openCursor()
      while (true) {
        const cursor = await new Promise<IDBCursorWithValue | null>(
          (resolve, reject) => {
            req.onsuccess = () => {
              resolve(req.result)
            }
            req.onerror = () => reject(req.error)
          },
        )
        if (cursor == null) break
        yield [cursor.key, cursor.value] as [number, RowData]
        cursor.continue()
      }
    })
  }

  drop(): void {
    return this.db.deleteObjectStore(this.tableName)
  }
}
