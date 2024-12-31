import { Struct } from "../binary/Struct.ts"
import { IBufferPool, PageId } from "./BufferPool.ts"
import { Droppable, IDroppable } from "../droppable.ts"

/**
 * A linked list of pages that store data. Useful for storing data that
 * doesn't fit in a single page.
 */
export class LinkedPageList implements IDroppable {
  private droppable: Droppable
  constructor(private bufferPool: IBufferPool, readonly headPageId: PageId) {
    if (headPageId <= 0n) {
      throw new Error("Invalid head page ID. Must be greater than 0.")
    }
    this.droppable = new Droppable(async () => {
      let currentPageId = this.headPageId
      while (currentPageId !== 0n) {
        const view = await this.bufferPool.getPageView(currentPageId)
        const header = LinkedPageList.headerStruct.readAt(view, 0)
        await this.bufferPool.freePage(currentPageId)
        currentPageId = header.nextPageId
      }
    })
  }

  static readonly headerStruct = Struct.record({
    nextPageId: [0, Struct.bigUint64],
    byteLength: [1, Struct.uint32],
  })

  /**
   * Drop all pages in the linked list, including the head page.
   */
  async drop() {
    await this.droppable.drop()
  }

  async writeData(data: Uint8Array): Promise<void> {
    this.droppable.assertNotDropped("Cannot read from a dropped LinkedPageList")
    let currentStartOffset = 0

    let currentPageId = this.headPageId

    while (currentPageId !== 0n) {
      await this.bufferPool.writeToPage(currentPageId, async (view) => {
        const byteLength = Math.min(
          view.byteLength - LinkedPageList.headerStruct.size,
          data.length - currentStartOffset,
        )
        const endOffset = currentStartOffset + byteLength
        view.setUint8Array(
          LinkedPageList.headerStruct.size,
          data.slice(currentStartOffset, endOffset),
        )
        currentStartOffset = endOffset
        const header = LinkedPageList.headerStruct.readAt(view, 0)
        if (header.nextPageId === 0n && endOffset < data.length) {
          // we still have more to write.
          header.nextPageId = await this.bufferPool.allocatePage()
        } else if (header.nextPageId !== 0n && endOffset >= data.length) {
          // we have written all the data, but there are still pages to free.
          let currentPageIdToFree = header.nextPageId
          while (currentPageIdToFree !== 0n) {
            const view = await this.bufferPool.getPageView(currentPageIdToFree)
            const header = LinkedPageList.headerStruct.readAt(view, 0)
            await this.bufferPool.freePage(currentPageIdToFree)
            currentPageIdToFree = header.nextPageId
          }
          header.nextPageId = 0n
        }
        header.byteLength = byteLength
        LinkedPageList.headerStruct.writeAt(header, view, 0)
        currentPageId = header.nextPageId
      })
    }
  }

  async readData(): Promise<Uint8Array> {
    this.droppable.assertNotDropped("Cannot read from a dropped LinkedPageList")
    let currentPageId = this.headPageId
    const data: Uint8Array[] = []

    while (currentPageId !== 0n) {
      const view = await this.bufferPool.getPageView(currentPageId)
      const header = LinkedPageList.headerStruct.readAt(view, 0)
      data.push(
        view.slice(LinkedPageList.headerStruct.size, header.byteLength)
          .toUint8Array(),
      )
      currentPageId = header.nextPageId
    }

    const totalLength = data.reduce((acc, chunk) => acc + chunk.byteLength, 0)
    const result = new Uint8Array(totalLength)
    let offset = 0
    for (const chunk of data) {
      result.set(chunk, offset)
      offset += chunk.byteLength
    }
    return result
  }
}
