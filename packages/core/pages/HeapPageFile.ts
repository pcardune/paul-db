import { FixedWidthArray } from "../binary/FixedWidthArray.ts"
import { FixedWidthStruct, Struct } from "../binary/Struct.ts"
import { IBufferPool, PageId } from "./BufferPool.ts"

type PageEntry = Readonly<{ pageId: PageId; freeSpace: number }>

const headerPageEntryStruct = Struct.record<PageEntry>({
  pageId: [0, Struct.bigUint64],
  freeSpace: [8, Struct.uint32],
})

type HeaderPage = Readonly<{
  nextPageId: bigint | null
  entries: FixedWidthArray<PageEntry>
}>

const headerPageStruct = (pageSize: number) =>
  new FixedWidthStruct<HeaderPage>({
    toJSON: (_value) => {
      throw new Error("Not Implemented")
    },
    fromJSON: (_json) => {
      throw new Error("Not Implemented")
    },
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
  })

/**
 * This class models the linked list of header pages.
 */
class HeaderPageRef {
  constructor(
    private bufferPool: IBufferPool,
    readonly pageId: PageId,
  ) {
  }

  /**
   * Get the header page at this reference's page ID.
   */
  async get(): Promise<HeaderPage> {
    const view = await this.bufferPool.getPageView(this.pageId)
    return headerPageStruct(this.bufferPool.pageSize).readAt(
      view,
      0,
    )
  }

  /**
   * Push a new empty header page onto the linked list. It will be the new
   * head of the list.
   */
  async pushNew(): Promise<HeaderPageRef> {
    const newHeaderPageId = await this.bufferPool.allocatePage()
    const dataView = await this.bufferPool.getPageView(newHeaderPageId)
    headerPageStruct(this.bufferPool.pageSize).writeAt(
      {
        nextPageId: this.pageId,
        entries: FixedWidthArray.empty({
          type: headerPageEntryStruct,
          length: 0,
        }),
      },
      dataView,
      0,
    )
    return new HeaderPageRef(this.bufferPool, newHeaderPageId)
  }

  /**
   * Traverse to the next header page in the linked list.
   */
  async getNext(): Promise<HeaderPageRef | null> {
    const headerPage = await this.get()
    if (headerPage.nextPageId === null) {
      return null
    }
    return new HeaderPageRef(this.bufferPool, headerPage.nextPageId)
  }
}

/**
 * A page space allocator allocates space in a page.
 *
 * Different page formats may have different ways of allocating space. For
 * example, a page may have a header that contains a list of free slots, or
 * it may have a bitmap that indicates which slots are free. Or this info
 * could be in the footer of the page, or stored somewhere else entirely.
 *
 * This interface abstracts over these differences.
 */
export type PageSpaceAllocator<AllocInfo extends { freeSpace: number }> = {
  allocateSpaceInPage: (
    pageView: DataView,
    numBytes: number,
  ) => AllocInfo | Promise<AllocInfo>
}

export type { HeaderPageRef }

/**
 * A heap page file is a file that stores variable-length records in a
 * collection of pages. There are multiple ways to implement this, but
 * this class uses a "page directory".
 *
 * The file has a header page that contains a list of pages that have free
 * space. Each entry in the list contains the page ID and the amount of free
 * space in the page. The header page also contains a pointer to the next
 * header page, so that the list of pages can be spread across multiple
 * header pages.
 *
 * See https://cs186berkeley.net/notes/note3/#page-directory-implementation
 * for an overview.
 */
export class HeapPageFile<AllocInfo extends { freeSpace: number }> {
  private _headerPageRef: HeaderPageRef

  /**
   * Reference to the heap page file whose header page starts at the given
   * page ID.
   *
   * @param bufferPool
   * @param pageId
   * @param allocator
   */
  constructor(
    private bufferPool: IBufferPool,
    pageId: PageId,
    private allocator: PageSpaceAllocator<AllocInfo>,
  ) {
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
  ): Promise<{ pageId: PageId; allocInfo: AllocInfo }> {
    let headerPage = await this.headerPageRef.get()
    for (const [i, entry] of headerPage.entries.enumerate()) {
      if (entry.freeSpace >= bytes) {
        const allocInfo = await this.allocator.allocateSpaceInPage(
          await this.bufferPool.getPageView(entry.pageId),
          bytes,
        )
        this.bufferPool.markDirty(entry.pageId)
        headerPage.entries.set(i, {
          pageId: entry.pageId,
          freeSpace: allocInfo.freeSpace,
        })
        return { pageId: headerPage.entries.get(i).pageId, allocInfo }
      }
    }

    if (headerPage.entries.length >= headerPage.entries.maxLength) {
      // no room for a new entry in this header page, so we'll allocate a
      // new header page
      this._headerPageRef = await this.headerPageRef.pushNew()
      headerPage = await this._headerPageRef.get()
    }

    const newPageId = await this.bufferPool.allocatePage()
    const allocInfo = await this.allocator.allocateSpaceInPage(
      await this.bufferPool.getPageView(newPageId),
      bytes,
    )
    this.bufferPool.markDirty(newPageId)
    headerPage.entries.push({
      pageId: newPageId,
      freeSpace: allocInfo.freeSpace,
    })
    return {
      pageId: headerPage.entries.get(headerPage.entries.length - 1).pageId,
      allocInfo,
    }
  }
}
