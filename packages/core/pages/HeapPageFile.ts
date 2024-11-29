import { WriteableDataView } from "../binary/dataview.ts"
import { Struct } from "../binary/Struct.ts"
import { IBufferPool, PageId } from "./BufferPool.ts"

type PageEntry = Readonly<{ pageId: PageId; freeSpace: number }>

const headerPageEntryStruct = Struct.record<PageEntry>({
  pageId: [0, Struct.bigUint64],
  freeSpace: [8, Struct.uint32],
})

type HeaderPage = Readonly<{
  nextPageId: bigint
  entries: PageEntry[]
}>

const headerPageStruct = Struct.record<HeaderPage>({
  nextPageId: [0, Struct.bigUint64],
  entries: [1, headerPageEntryStruct.array()],
})

/**
 * This class models the linked list of header pages.
 */
class HeaderPageRef {
  constructor(
    private bufferPool: IBufferPool,
    readonly pageId: PageId,
  ) {}

  static OutOfSpaceError = class extends Error {}

  /**
   * Get the header page at this reference's page ID.
   */
  async get(): Promise<HeaderPage> {
    const view = await this.bufferPool.getPageView(this.pageId)
    return headerPageStruct.readAt(
      view,
      0,
    )
  }

  async set(headerPage: HeaderPage) {
    const view = await this.bufferPool.getWriteablePage(this.pageId)
    if (headerPageStruct.sizeof(headerPage) > view.byteLength) {
      throw new HeaderPageRef.OutOfSpaceError("Header page is out of space")
    }
    headerPageStruct.writeAt(headerPage, view, 0)
    this.bufferPool.markDirty(this.pageId)
    return await this.get()
  }

  /**
   * Traverse to the next header page in the linked list.
   */
  async getNext(): Promise<HeaderPageRef | null> {
    const headerPage = await this.get()
    if (headerPage.nextPageId === 0n) {
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
    pageView: WriteableDataView,
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

  /**
   * Push a new empty header page onto the linked list. It will be the new
   * head of the list.
   */
  private async pushNewHeaderPageRef(): Promise<HeaderPageRef> {
    const newHeaderPageId = await this.bufferPool.allocatePage()
    const dataView = await this.bufferPool.getWriteablePage(newHeaderPageId)
    headerPageStruct.writeAt(
      {
        nextPageId: this.headerPageRef.pageId,
        entries: [],
      },
      dataView,
      0,
    )
    this._headerPageRef = new HeaderPageRef(this.bufferPool, newHeaderPageId)
    return this._headerPageRef
  }

  async allocateSpace(
    bytes: number,
  ): Promise<{ pageId: PageId; allocInfo: AllocInfo }> {
    let headerPage = await this.headerPageRef.get()
    for (const [i, entry] of headerPage.entries.entries()) {
      if (entry.freeSpace >= bytes) {
        const allocInfo = await this.allocator.allocateSpaceInPage(
          await this.bufferPool.getWriteablePage(entry.pageId),
          bytes,
        )
        this.bufferPool.markDirty(entry.pageId)
        headerPage = await this.headerPageRef.set({
          ...headerPage,
          entries: headerPage.entries.map((e, j) =>
            j === i
              ? {
                ...e,
                freeSpace: allocInfo.freeSpace,
              }
              : e
          ),
        })
        return { pageId: headerPage.entries[i].pageId, allocInfo }
      }
    }

    const newPageId = await this.bufferPool.allocatePage()
    const allocInfo = await this.allocator.allocateSpaceInPage(
      await this.bufferPool.getWriteablePage(newPageId),
      bytes,
    )
    this.bufferPool.markDirty(newPageId)

    const newEntry = {
      pageId: newPageId,
      freeSpace: allocInfo.freeSpace,
    }
    try {
      headerPage = await this.headerPageRef.set({
        ...headerPage,
        entries: [...headerPage.entries, newEntry],
      })
      return {
        pageId: headerPage.entries.at(-1)!.pageId,
        allocInfo,
      }
    } catch (e) {
      if (!(e instanceof HeaderPageRef.OutOfSpaceError)) {
        throw e
      }
    }
    // no room for a new entry in this header page, so we'll allocate a
    // new header page
    const newPageRef = await this.pushNewHeaderPageRef()
    headerPage = await newPageRef.get()
    headerPage = await newPageRef.set({
      ...headerPage,
      entries: [newEntry],
    })
    return {
      pageId: headerPage.entries.at(-1)!.pageId,
      allocInfo,
    }
  }
}
