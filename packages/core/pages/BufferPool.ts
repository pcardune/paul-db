import { ReadonlyDataView, WriteableDataView } from "../binary/dataview.ts"
import { Struct } from "../binary/Struct.ts"
import { EOFError, readBytesAt, writeBytesAt } from "../io.ts"
import { debugLogger } from "../logging.ts"
import { Promisable } from "npm:type-fest"

const debugLog = debugLogger(false)

export type PageId = bigint

class WriteablePage extends WriteableDataView {}

export interface IBufferPool {
  get pageSize(): number
  allocatePage(): Promise<PageId>
  freePage(pageId: PageId): Promisable<void>
  freePages(pageIds: PageId[]): Promisable<void>

  /**
   * Get a DataView for the given page ID.
   */
  getPageView(pageId: PageId): Promise<ReadonlyDataView>

  // getWriteablePage(pageId: PageId): Promise<WriteablePage>
  writeToPage<R>(
    pageId: PageId,
    writer: (view: WriteablePage) => R | Promise<R>,
  ): R | Promise<R>

  markDirty(pageId: PageId): void
  commit(): Promise<void>
}

export class InMemoryBufferPool implements IBufferPool {
  private pages: Map<PageId, Uint8Array> = new Map()
  private freeList: PageId[] = []

  constructor(readonly pageSize: number) {}

  allocatePage(): Promise<PageId> {
    if (this.freeList.length > 0) {
      return Promise.resolve(this.freeList.pop()!)
    }
    const pageId = BigInt(this.pages.size)
    this.pages.set(pageId, new Uint8Array(this.pageSize))
    return Promise.resolve(pageId)
  }

  freePages(pageIds: PageId[]): void {
    for (const pageId of pageIds) {
      this.freePage(pageId)
    }
  }
  freePage(pageId: PageId): void {
    this.freeList.push(pageId)
  }

  private getPage(pageId: PageId): Promise<Uint8Array> {
    const page = this.pages.get(pageId)
    if (!page) {
      throw new Error(`Page ${pageId} not found`)
    }
    return Promise.resolve(page)
  }

  async getPageView(pageId: PageId): Promise<ReadonlyDataView> {
    const page = await this.getPage(pageId)
    return new ReadonlyDataView(page.buffer)
  }

  async writeToPage<R>(
    pageId: PageId,
    writer: (view: WriteablePage) => Promise<R> | R,
  ): Promise<R> {
    const page = await this.getPage(pageId)
    const result = writer(new WriteablePage(page.buffer))
    this.markDirty(pageId)
    return result
  }

  markDirty(_pageId: PageId): void {
    // not needed for in-memory implementation
  }

  commit(): Promise<void> {
    // not needed for in-memory implementation
    return Promise.resolve()
  }
}

export class FileBackedBufferPool implements IBufferPool {
  private _pageCache: Map<PageId, Uint8Array> = new Map()
  private dirtyPages: Set<PageId> = new Set()
  private __lastWrittenFreePageId: PageId = -1n

  static headerStruct = Struct.bigUint64

  private constructor(
    private file: Deno.FsFile,
    readonly pageSize: number,
    private _freePageId: PageId,
    private fileOffset: bigint,
  ) {}

  get freePageId(): PageId {
    return this._freePageId
  }

  static async create(
    file: Deno.FsFile,
    pageSize: number,
    offset: bigint | number = 0n,
  ): Promise<FileBackedBufferPool> {
    const stat = await file.stat()
    let freePageId: PageId = BigInt(offset)
    if (stat.size > freePageId + 8n) {
      // when the file is not empty, we need to look at the first
      // 8 bytes to see where the free list starts
      const freeListData = await readBytesAt(
        file,
        offset,
        FileBackedBufferPool.headerStruct.size,
      )
      freePageId = FileBackedBufferPool.headerStruct.readAt(
        new ReadonlyDataView(freeListData.buffer),
        0,
      )
      freePageId = new DataView(freeListData.buffer).getBigUint64(0)
    }
    return new FileBackedBufferPool(file, pageSize, freePageId, BigInt(offset))
  }

  [Symbol.dispose](): void {
    this.file.close()
  }

  close(): void {
    this.file.close()
  }

  async allocatePage(): Promise<PageId> {
    if (this._freePageId !== this.fileOffset) {
      // there is a free page we can reuse
      const pageId = this._freePageId
      try {
        await this.writeToPage(pageId, (view) => {
          this._freePageId = view.getBigUint64(0)
          view.fill(0)
        })
      } catch (e) {
        if (e instanceof EOFError) {
          // we hit the end of the file, so let's just make some more space
          // by appending a new page
          this._freePageId = pageId + BigInt(this.pageSize)
          this._pageCache.set(pageId, new Uint8Array(this.pageSize))
          this.markDirty(pageId)
        } else {
          throw e
        }
      }
      return pageId
    }

    // no free pages, so we need to allocate a new one
    const pageId = this.fileOffset + 8n +
      BigInt(this._pageCache.size * this.pageSize)
    this._freePageId = pageId + BigInt(this.pageSize)
    this._pageCache.set(pageId, new Uint8Array(this.pageSize))
    this.markDirty(pageId)
    return pageId
  }

  async freePages(pageIds: Iterable<PageId>): Promise<void> {
    for (const pageId of pageIds) {
      await this.writeToPage(pageId, (view) => {
        view.setBigUint64(0, this._freePageId)
      })
      this._freePageId = pageId
    }
  }
  async freePage(pageId: PageId): Promise<void> {
    await this.freePages([pageId])
  }

  private async _getPageBuffer(pageId: PageId): Promise<Uint8Array> {
    const page = this._pageCache.get(pageId)
    if (page) {
      return Promise.resolve(page)
    }
    const data = await readBytesAt(this.file, pageId, this.pageSize)
    this._pageCache.set(pageId, data)
    return data
  }

  async getPageView(pageId: PageId): Promise<ReadonlyDataView> {
    debugLog(() => `BufferPool.getPageView(${pageId})`)
    const page = await this._getPageBuffer(pageId)
    return new ReadonlyDataView(page.buffer)
  }

  async writeToPage<R>(
    pageId: PageId,
    writer: (view: WriteablePage) => R | Promise<R>,
  ): Promise<R> {
    const page = await this._getPageBuffer(pageId)
    const result = writer(new WriteablePage(page.buffer))
    this.markDirty(pageId)
    return result
  }

  markDirty(pageId: PageId): void {
    debugLog(() => `BufferPool.markDirty(${pageId})`)
    this.dirtyPages.add(pageId)
  }

  get isDirty(): boolean {
    return this.dirtyPages.size > 0 ||
      this._freePageId !== this.__lastWrittenFreePageId
  }

  async commit(): Promise<void> {
    if (this._freePageId !== this.__lastWrittenFreePageId) {
      await writeBytesAt(
        this.file,
        this.fileOffset,
        FileBackedBufferPool.headerStruct.toUint8Array(this._freePageId),
      )
      this.__lastWrittenFreePageId = this._freePageId
    }
    debugLog(() =>
      `BufferPool.commit() Committing ${this.dirtyPages.size} pages: ${
        Array.from(this.dirtyPages).join(", ")
      }`
    )
    for (const pageId of this.dirtyPages) {
      const data = this._pageCache.get(pageId)
      if (!data) {
        throw new Error(`Dirty Page ${pageId} not found in page cache`)
      }
      await writeBytesAt(this.file, pageId, data)
    }
    this.dirtyPages.clear()
    this._pageCache.clear()
  }
}
