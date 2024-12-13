import { expect } from "@std/expect"
import { afterEach, beforeEach, describe, it } from "@std/testing/bdd"
import { FileBackedBufferPool } from "./BufferPool.ts"
import { HeapPageFile } from "./HeapPageFile.ts"
import { generateTestFilePath, spyOnBufferPool } from "../testing.ts"

describe("HeapPageFile", () => {
  const tempFile = generateTestFilePath("HeapPageFile.data")
  const pageSize = 100
  let bufferPool: FileBackedBufferPool
  let heapPageFile: HeapPageFile<{ freeSpace: number }>
  let bufferPoolSpy: ReturnType<typeof spyOnBufferPool>

  beforeEach(async () => {
    bufferPool = await FileBackedBufferPool.create(
      await Deno.open(tempFile.filePath, {
        read: true,
        write: true,
        create: true,
        truncate: true,
      }),
      pageSize,
    )
    bufferPoolSpy = spyOnBufferPool(bufferPool)

    const heapPageId = await bufferPool.allocatePage()
    heapPageFile = new HeapPageFile(
      bufferPool,
      heapPageId,
      {
        allocateSpaceInPage: (pageView, numBytes) => {
          const existingFreeSpace = pageView.getUint32(0)
          const newFreeSpace = existingFreeSpace === 0
            ? pageSize - numBytes
            : existingFreeSpace - numBytes
          pageView.setUint32(0, newFreeSpace)
          return { freeSpace: newFreeSpace }
        },
      },
    )
  })
  afterEach(() => {
    bufferPool.close()
    tempFile[Symbol.dispose]()
  })

  it("Initially allocates one page of space", () => {
    expect(bufferPoolSpy.allocatePage.calls).toHaveLength(1)
  })

  describe("Allocating space", () => {
    it("allocates space to the same page until it fills up", async () => {
      expect(await heapPageFile.headerPageRef.get()).toHaveProperty(
        "nextPageId",
        null,
      )

      const alloc0 = await heapPageFile.allocateSpace(10)
      expect(alloc0.allocInfo.freeSpace).toBe(pageSize - 10)
      expect(alloc0.pageId).toBe(108n)
      const alloc1 = await heapPageFile.allocateSpace(34)
      expect(alloc1.allocInfo.freeSpace).toBe(pageSize - 10 - 34)
      expect(alloc1.pageId).toBe(alloc0.pageId)

      expect(bufferPoolSpy.allocatePage.calls).toHaveLength(2)

      await heapPageFile.allocateSpace(10)
      expect(bufferPoolSpy.allocatePage.calls).toHaveLength(2)

      // eventually we'll fill up the page and a new one will be allocated
      // for us
      while (true) {
        const alloc = await heapPageFile.allocateSpace(10)
        if (alloc.pageId !== alloc0.pageId) {
          expect(alloc.allocInfo.freeSpace).toBe(pageSize - 10)
          break
        }
      }
      // look, it allocated a new one
      expect(bufferPoolSpy.allocatePage.calls).toHaveLength(3)
    })

    it("links headers pages together, allocating new ones as needed", async () => {
      const initialHeaderPageId = heapPageFile.headerPageRef.pageId
      expect(initialHeaderPageId).toBe(8n)
      expect(await heapPageFile.headerPageRef.getNext()).toBe(null)
      for (let i = 0; i < 100; i++) {
        await heapPageFile.allocateSpace(50)
      }

      const pageList = await heapPageFile.headerPageRefsIter().map(
        (headerPageRef) => headerPageRef.pageId,
      ).toArray()
      expect(pageList).toEqual([
        5708n,
        4908n,
        4108n,
        3308n,
        2508n,
        1708n,
        908n,
        8n,
      ])
    })

    it("Frees space when dropped", async () => {
      expect(bufferPoolSpy.freePages.calls).toHaveLength(0)
      // let's allocate a bundle of pages
      for (let i = 0; i < 20; i++) {
        await heapPageFile.allocateSpace(50)
      }
      expect(bufferPoolSpy.allocatePage.calls).toHaveLength(12)
      const sortPageIds = (a: bigint, b: bigint) => Number(a - b)
      const allocatedPageIds = await Promise.all(
        bufferPoolSpy.allocatePage.calls.flatMap((c) =>
          c.returned == null ? [] : [c.returned]
        ),
      )
      await heapPageFile.drop()
      const freedPageIds = bufferPoolSpy.freePages.calls.flatMap(
        (c) => c.args[0],
      ).sort(sortPageIds)

      expect(freedPageIds).toEqual(allocatedPageIds.sort(sortPageIds))
    })
  })
})
