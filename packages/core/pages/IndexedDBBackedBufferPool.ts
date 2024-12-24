import type { Promisable } from "type-fest"
import { IBufferPool, PageId, WriteablePage } from "./BufferPool.ts"
import { ReadonlyDataView } from "../binary/dataview.ts"
import { IndexedDBWrapper } from "./IndexedDbWrapper.ts"

export class IndexedDBBackedBufferPool implements IBufferPool {
  private __lastWrittenFreePageId: PageId = -1n

  private constructor(
    private db: IndexedDBWrapper,
    private _freePageId: PageId,
    readonly pageSize: number = 4096,
  ) {}

  static async create(
    indexedDB: IndexedDBWrapper,
    pageSize: number = 4096,
  ): Promise<IndexedDBBackedBufferPool> {
    if (!indexedDB) {
      throw new Error("indexedDB not available")
    }

    const nextPageId = await indexedDB.getKeyVal<bigint | null>("nextPageId")
    if (nextPageId) {
      return new IndexedDBBackedBufferPool(
        indexedDB,
        nextPageId,
        pageSize,
      )
    } else {
      await indexedDB.setKeyVal("nextPageId", 1n)
      return new IndexedDBBackedBufferPool(indexedDB, 1n, pageSize)
    }
  }

  private pageCache: Map<PageId, Uint8Array> = new Map()

  allocatePage(): PageId {
    const pageId = this._freePageId++
    this.pageCache.set(pageId, new Uint8Array(this.pageSize))
    this.markDirty(pageId)
    return pageId
  }

  freePage(pageId: PageId): Promise<void> {
    return this.freePages([pageId])
  }

  freePages(pageIds: PageId[]): Promise<void> {
    return this.db.deletePages(pageIds)
  }

  async getPageView(pageId: PageId): Promise<ReadonlyDataView> {
    return new ReadonlyDataView((await this._getPageBuffer(pageId)).buffer)
  }

  private async _getPageBuffer(pageId: PageId): Promise<Uint8Array> {
    const page = this.pageCache.get(pageId)
    if (page) {
      return page
    }
    const data = await this.db.getPage(pageId)
    if (!data) {
      throw new Error(`Page ${pageId} not found`)
    }
    const buffer = data
    this.pageCache.set(pageId, buffer)
    return buffer
  }

  async writeToPage<R>(
    pageId: PageId,
    writer: (view: WriteablePage) => Promisable<R>,
  ): Promise<R> {
    const page = await this._getPageBuffer(pageId)
    const result = writer(new WriteablePage(page.buffer))
    this.markDirty(pageId)
    return result
  }

  private dirtyPages: Set<PageId> = new Set()

  markDirty(pageId: PageId): void {
    this.dirtyPages.add(pageId)
  }

  async commit(): Promise<void> {
    if (this._freePageId !== this.__lastWrittenFreePageId) {
      await this.db.setKeyVal("nextPageId", this._freePageId)
      this.__lastWrittenFreePageId = this._freePageId
    }
    await this.db.setPages([...this.dirtyPages].map((pageId) => {
      const data = this.pageCache.get(pageId)
      if (!data) {
        throw new Error(`Page ${pageId} not found`)
      }
      return ({
        pageId,
        data,
      })
    }))
    this.dirtyPages.clear()
  }
}
