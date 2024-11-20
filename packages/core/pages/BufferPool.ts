type PageId = bigint

interface IPageBufferPool {
  allocatePage(): Promise<PageId>
  freePage(pageId: PageId): void
  getPage(pageId: PageId): Promise<Uint8Array>
  markDirty(pageId: PageId): void
  commit(): Promise<void>
}

export class InMemoryBufferPool implements IPageBufferPool {
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

export class FileBackedBufferPool implements IPageBufferPool {
  private pages: Map<PageId, Uint8Array> = new Map()
  private dirtyPages: Set<PageId> = new Set()
  private __lastWrittenFreePageId: PageId = 0n

  private constructor(
    private file: Deno.FsFile,
    readonly pageSize: number,
    private freePageId: PageId,
  ) {
    this.__lastWrittenFreePageId = freePageId
  }

  static async create(
    filename: string,
    pageSize: number,
  ): Promise<FileBackedBufferPool> {
    const file = await Deno.open(filename, {
      read: true,
      write: true,
      create: true,
    })
    const stat = await file.stat()
    let freePageId: PageId = 0n
    if (stat.size > 0) {
      // read existing free list offset
      const freeListData = new Uint8Array(8)
      const bytesRead = await file.read(freeListData)
      if (bytesRead === null) {
        throw new Error("Failed to read free list")
      }
      if (bytesRead !== pageSize) {
        throw new Error("Unexpected number of bytes read")
      }
      freePageId = new DataView(freeListData.buffer).getBigUint64(0)
    }
    return new FileBackedBufferPool(file, pageSize, freePageId)
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
    const bytesRead = await this.file.read(data)
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
    console.log("dirtyPages.size", this.dirtyPages.size)
    console.log("freePageId", this.freePageId)
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
