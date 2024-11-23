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
  ) {
    super()
    this.__lastWrittenFreePageId = freePageId
  }

  static async create(
    file: Deno.FsFile,
    pageSize: number,
  ): Promise<FileBackedBufferPool> {
    const stat = await file.stat()
    let freePageId: PageId = 0n
    if (stat.size > 0) {
      // when the file is not empty, we need to look at the first
      // 8 bytes to see where the free list starts
      const freeListData = new Uint8Array(8)
      const bytesRead = await readBytes(file, freeListData)
      if (bytesRead === null) {
        throw new Error("Failed to read free list")
      }
      if (bytesRead !== freeListData.length) {
        throw new Error(
          `Unexpected number of bytes read (${bytesRead}) wanted ${pageSize}`,
        )
      }
      freePageId = new DataView(freeListData.buffer).getBigUint64(0)
    }
    return new FileBackedBufferPool(file, pageSize, freePageId)
  }

  [Symbol.dispose](): void {
    this.file.close()
  }

  close(): void {
    this.file.close()
  }

  async allocatePage(): Promise<PageId> {
    if (this.freePageId !== 0n) {
      const pageId = this.freePageId
      const page = await this.getPage(pageId)
      this.freePageId = new DataView(page.buffer).getBigUint64(0)
      page.fill(0)
      this.markDirty(pageId)
      return pageId
    }

    const pageId = 8n + BigInt(this.pages.size * this.pageSize)
    // this.freePageId = pageId + BigInt(this.pageSize)
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

    await this.file.seek(pageId, Deno.SeekMode.Start)

    const data = new Uint8Array(this.pageSize)
    const bytesRead = await readBytes(this.file, data)
    if (bytesRead === null) {
      throw new Error(`Failed to read page ${pageId}`)
    }
    if (bytesRead !== this.pageSize) {
      throw new Error(`Unexpected number of bytes read: ${bytesRead}`)
    }
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
      await this.file.seek(0, Deno.SeekMode.Start)
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

async function readBytes(file: Deno.FsFile, into: Uint8Array) {
  let bytesRead = 0
  while (bytesRead < into.length) {
    const n = await file.read(into.subarray(bytesRead))
    if (n === null) {
      throw new Error("Failed to read")
    }
    bytesRead += n
  }
  return bytesRead
}
