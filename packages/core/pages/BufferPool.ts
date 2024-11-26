import { readBytesAt } from "../io.ts"

export type PageId = bigint

export interface IBufferPool {
  get pageSize(): number
  allocatePage(): Promise<PageId>
  freePage(pageId: PageId): void
  getPage(pageId: PageId): Promise<Uint8Array>

  /**
   * Get a DataView for the given page ID.
   */
  getPageView(pageId: PageId): Promise<DataView>
  markDirty(pageId: PageId): void
  commit(): Promise<void>
}

abstract class BaseBufferPool implements IBufferPool {
  abstract get pageSize(): number
  abstract allocatePage(): Promise<PageId>
  abstract freePage(pageId: PageId): void
  abstract getPage(pageId: PageId): Promise<Uint8Array>
  getPageView(pageId: PageId): Promise<DataView> {
    return this.getPage(pageId).then((page) => new DataView(page.buffer))
  }
  abstract markDirty(pageId: PageId): void
  abstract commit(): Promise<void>
}

export class InMemoryBufferPool extends BaseBufferPool implements IBufferPool {
  private pages: Map<PageId, Uint8Array> = new Map()
  private freeList: PageId[] = []

  constructor(readonly pageSize: number) {
    super()
  }

  allocatePage(): Promise<PageId> {
    if (this.freeList.length > 0) {
      return Promise.resolve(this.freeList.pop()!)
    }
    const pageId = BigInt(this.pages.size)
    this.pages.set(pageId, new Uint8Array(this.pageSize))
    return Promise.resolve(pageId)
  }

  freePage(pageId: PageId): void {
    this.freeList.push(pageId)
  }

  getPage(pageId: PageId): Promise<Uint8Array> {
    const page = this.pages.get(pageId)
    if (!page) {
      throw new Error(`Page ${pageId} not found`)
    }
    return Promise.resolve(page)
  }

  markDirty(_pageId: PageId): void {
    // not needed for in-memory implementation
  }

  commit(): Promise<void> {
    // not needed for in-memory implementation
    return Promise.resolve()
  }
}

export class FileBackedBufferPool extends BaseBufferPool
  implements IBufferPool {
  private pages: Map<PageId, Uint8Array> = new Map()
  private dirtyPages: Set<PageId> = new Set()
  private __lastWrittenFreePageId: PageId = 0n

  private constructor(
    private file: Deno.FsFile,
    readonly pageSize: number,
    private freePageId: PageId,
    private fileOffset: bigint,
  ) {
    super()
    // this.__lastWrittenFreePageId = freePageId
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
      const freeListData = await readBytesAt(file, offset, 8)
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
    if (this.freePageId !== this.fileOffset) {
      const pageId = this.freePageId
      const page = await this.getPage(pageId)
      this.freePageId = new DataView(page.buffer).getBigUint64(0)
      page.fill(0)
      this.markDirty(pageId)
      return pageId
    }

    const pageId = this.fileOffset + 8n +
      BigInt(this.pages.size * this.pageSize)
    this.pages.set(pageId, new Uint8Array(this.pageSize))
    return pageId
  }

  freePage(pageId: PageId): void {
    const page = this.pages.get(pageId)
    if (page === undefined) {
      throw new Error(`Page ${pageId} not found`)
    }
    new DataView(page.buffer).setBigUint64(0, this.freePageId)
    this.dirtyPages.add(pageId)
    this.freePageId = pageId
  }

  async getPage(pageId: PageId): Promise<Uint8Array> {
    const page = this.pages.get(pageId)
    if (page) {
      return Promise.resolve(page)
    }
    const data = await readBytesAt(this.file, pageId, this.pageSize)
    this.pages.set(pageId, data)
    return data
  }

  markDirty(pageId: PageId): void {
    this.dirtyPages.add(pageId)
  }

  get isDirty(): boolean {
    return this.dirtyPages.size > 0 ||
      this.freePageId !== this.__lastWrittenFreePageId
  }

  async commit(): Promise<void> {
    if (this.freePageId !== this.__lastWrittenFreePageId) {
      const freeListData = new Uint8Array(8)
      new DataView(freeListData.buffer).setBigUint64(0, this.freePageId)
      await this.file.seek(this.fileOffset, Deno.SeekMode.Start)
      const bytesWritten = await this.file.write(freeListData)
      if (bytesWritten !== 8) {
        throw new Error(`Unexpected number of bytes written: ${bytesWritten}`)
      }
      this.__lastWrittenFreePageId = this.freePageId
    }
    for (const pageId of this.dirtyPages) {
      const data = this.pages.get(pageId)
      if (!data) {
        throw new Error(`Page ${pageId} not found`)
      }
      await this.file.seek(pageId, Deno.SeekMode.Start)
      const bytesWritten = await this.file.write(data)
      if (bytesWritten !== this.pageSize) {
        throw new Error(`Unexpected number of bytes written: ${bytesWritten}`)
      }
    }
    this.dirtyPages.clear()
  }
}
