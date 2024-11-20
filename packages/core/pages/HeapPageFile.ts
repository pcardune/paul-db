import { FixedWidthArray, Struct } from "../binary/FixedWidthArray.ts"
import { IBufferPool, PageId } from "./BufferPool.ts"

type PageEntry = Readonly<{ pageId: PageId; freeSpace: number }>

const headerPageEntryStruct: Struct<PageEntry> = {
  size: 12,
  write: (value, view) => {
    view.setBigUint64(0, value.pageId)
    view.setUint32(8, value.freeSpace)
  },
  read: (view) => ({
    pageId: view.getBigUint64(0),
    freeSpace: view.getUint32(8),
  }),
}

type HeaderPage = Readonly<{
  nextPageId: bigint | null
  entries: FixedWidthArray<PageEntry>
}>

const headerPageStruct = (pageSize: number) => ({
  size: pageSize,
  write: (value: Pick<HeaderPage, "nextPageId">, view) => {
    view.setBigUint64(0, value.nextPageId ?? 0n)
  },
  read: (view) => {
    const nextPageId = view.getBigUint64(0)
    return ({
      nextPageId: nextPageId === 0n ? null : nextPageId,
      entries: new FixedWidthArray(
        new DataView(view.buffer, 8, view.byteLength - 8),
        headerPageEntryStruct,
      ),
    })
  },
} satisfies Struct<HeaderPage>)

class HeaderPageRef {
  constructor(
    private bufferPool: IBufferPool,
    readonly pageId: PageId,
  ) {
  }

  async get(): Promise<HeaderPage> {
    const data = await this.bufferPool.getPage(this.pageId)
    return headerPageStruct(this.bufferPool.pageSize).read(
      new DataView(data.buffer),
    )
  }

  async pushNew(): Promise<HeaderPageRef> {
    const newHeaderPageId = await this.bufferPool.allocatePage()
    const headerPageData = await this.bufferPool.getPage(newHeaderPageId)
    const dataView = new DataView(headerPageData.buffer)
    headerPageStruct(this.bufferPool.pageSize).write(
      { nextPageId: this.pageId },
      dataView,
    )
    return new HeaderPageRef(this.bufferPool, newHeaderPageId)
  }

  async getNext(): Promise<HeaderPageRef | null> {
    const headerPage = await this.get()
    if (headerPage.nextPageId === null) {
      return null
    }
    return new HeaderPageRef(this.bufferPool, headerPage.nextPageId)
  }
}

export class HeapPageFile {
  private _headerPageRef: HeaderPageRef
  constructor(private bufferPool: IBufferPool, pageId: PageId) {
    this._headerPageRef = new HeaderPageRef(bufferPool, pageId)
  }

  get pageId(): PageId {
    return this._headerPageRef.pageId
  }

  get headerPageRef(): HeaderPageRef {
    return this._headerPageRef
  }

  async allocateSpace(
    bytes: number,
  ): Promise<{ pageId: PageId; freeSpace: number }> {
    let headerPage = await this.headerPageRef.get()
    for (const [i, entry] of headerPage.entries.enumerate()) {
      if (entry.freeSpace >= bytes) {
        headerPage.entries.set(i, {
          pageId: entry.pageId,
          freeSpace: entry.freeSpace - bytes,
        })
        return headerPage.entries.get(i)
      }
    }

    if (headerPage.entries.length >= headerPage.entries.maxLength) {
      // no room for a new entry in this header page, so we'll allocate a
      // new header page
      this._headerPageRef = await this.headerPageRef.pushNew()
      headerPage = await this._headerPageRef.get()
    }

    const newPageId = await this.bufferPool.allocatePage()
    headerPage.entries.push({
      pageId: newPageId,
      freeSpace: this.bufferPool.pageSize - bytes,
    })
    return headerPage.entries.get(headerPage.entries.length - 1)
  }

  static async create(bufferPool: IBufferPool): Promise<HeapPageFile> {
    const pageId = await bufferPool.allocatePage()
    return new HeapPageFile(bufferPool, pageId)
  }
}
