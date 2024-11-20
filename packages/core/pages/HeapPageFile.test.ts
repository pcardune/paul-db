import { expect } from "jsr:@std/expect"
import { afterEach, beforeEach, describe, it } from "jsr:@std/testing/bdd"
import { FileBackedBufferPool } from "./BufferPool.ts"
import { HeapPageFile } from "./HeapPageFile.ts"

describe("HeapPageFile", () => {
  const filePath = "/tmp/heap-page-file-test"
  const pageSize = 100
  let bufferPool: FileBackedBufferPool
  let heapPageFile: HeapPageFile
  beforeEach(async () => {
    Deno.openSync(filePath, {
      create: true,
      write: true,
      truncate: true,
    }).close()
    bufferPool = await FileBackedBufferPool.create(filePath, pageSize)
    heapPageFile = await HeapPageFile.create(bufferPool)
  })
  afterEach(() => {
    bufferPool.close()
    Deno.removeSync(filePath)
  })

  describe("Allocating space", () => {
    it("allocates space to the same page until it fills up", async () => {
      expect(await heapPageFile.headerPageRef.get()).toHaveProperty(
        "nextPageId",
        null,
      )

      const alloc0 = await heapPageFile.allocateSpace(10)
      expect(alloc0.freeSpace).toBe(pageSize - 10)
      expect(alloc0.pageId).toBe(108n)
      const alloc1 = await heapPageFile.allocateSpace(34)
      expect(alloc1.freeSpace).toBe(pageSize - 10 - 34)
      expect(alloc1.pageId).toBe(alloc0.pageId)

      // eventually we'll fill up the page and a new one will be allocated
      // for us
      while (true) {
        const alloc = await heapPageFile.allocateSpace(10)
        if (alloc.pageId !== alloc0.pageId) {
          expect(alloc.freeSpace).toBe(pageSize - 10)
          break
        }
      }
    })

    it("links headers pages together, allocating new ones as needed", async () => {
      const initialHeaderPageId = heapPageFile.headerPageRef.pageId
      expect(initialHeaderPageId).toBe(8n)
      expect(heapPageFile.headerPageRef.getNext()).resolves.toBe(null)
      for (let i = 0; i < 100; i++) {
        await heapPageFile.allocateSpace(50)
      }
      const pageList = [heapPageFile.headerPageRef.pageId]
      let nextPage = await heapPageFile.headerPageRef.getNext()
      while (nextPage !== null) {
        pageList.push(nextPage.pageId)
        nextPage = await nextPage.getNext()
      }
      expect(pageList).toEqual([
        5608n,
        4808n,
        4008n,
        3208n,
        2408n,
        1608n,
        808n,
        8n,
      ])
    })
  })
})
