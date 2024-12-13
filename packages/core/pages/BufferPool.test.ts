import { afterEach, beforeEach, describe, it } from "@std/testing/bdd"
import { FileBackedBufferPool } from "./BufferPool.ts"
import { expect } from "@std/expect"
import { generateTestFilePath } from "../testing.ts"

describe("FileBackedBufferPool", () => {
  let bufferPool: FileBackedBufferPool
  using tempFile = generateTestFilePath("FileBackedBufferPool.data")
  const pageSize = 4096
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
  })
  afterEach(() => {
    bufferPool.close()
    Deno.removeSync(tempFile.filePath)
  })

  describe("Allocation", () => {
    let pages: bigint[] = []
    beforeEach(async () => {
      pages = [
        await bufferPool.allocatePage(),
        await bufferPool.allocatePage(),
      ]
    })

    it("doesn't write to disk after allocating a page", () => {
      expect(pages).toEqual([8n, BigInt(pageSize) + 8n])
      expect(Deno.statSync(tempFile.filePath).size).toBe(0)
    })

    it("writes to disk on commit", async () => {
      await bufferPool.commit()
      expect(Deno.statSync(tempFile.filePath).size).toBe(8200)
    })

    it.skip("writes to disk after marking a page dirty", async () => {
      bufferPool.markDirty(pages[0])
      await bufferPool.commit()
      expect(Deno.statSync(tempFile.filePath).size).toBe(4096 + 8)
    })

    it("writes the correct data to disk", async () => {
      await bufferPool.writeToPage(
        pages[1],
        (view) => view.setUint32(0, 0xdeadbeef),
      )
      bufferPool.markDirty(pages[1])
      expect(Deno.statSync(tempFile.filePath).size).toBe(0)

      // now we commit and the data is written
      await bufferPool.commit()
      expect(Deno.statSync(tempFile.filePath).size).toBe(8192 + 8)
      const data = await readBytesFromFile(
        tempFile.filePath,
        Number(pages[1]),
        8,
      )
      expect(data.getUint32(0)).toBe(0xdeadbeef)
    })

    describe("Reading data", () => {
      let newBufferPool: FileBackedBufferPool

      let copyPath: ReturnType<typeof generateTestFilePath>

      beforeEach(async () => {
        copyPath = generateTestFilePath("Btree.data")

        // copy filePath to a new file
        await bufferPool.writeToPage(
          pages[1],
          (view) => view.setUint32(0, 0xdeadbeef),
        )
        bufferPool.markDirty(pages[1])
        await bufferPool.commit()
        Deno.copyFileSync(tempFile.filePath, copyPath.filePath)
        newBufferPool = await FileBackedBufferPool.create(
          await Deno.open(copyPath.filePath, {
            read: true,
            write: true,
            create: true,
          }),
          pageSize,
        )
      })
      afterEach(() => {
        newBufferPool.close()
        copyPath[Symbol.dispose]()
      })

      it("reads the correct data from disk", async () => {
        const view = await newBufferPool.getPageView(pages[1])
        expect(view.getUint32(0)).toBe(0xdeadbeef)
      })
    })

    describe("Freed pages", () => {
      beforeEach(async () => {
        await bufferPool.commit()
      })
      it("Cause the buffer pool to be dirty", async () => {
        expect(bufferPool.isDirty).toBe(false)
        await bufferPool.freePage(pages[0])
        expect(bufferPool.isDirty).toBe(true)
      })

      it("are tracked in the file after commit", async () => {
        await bufferPool.freePage(pages[0])
        await bufferPool.commit()
        expect(readBytesFromFileSync(tempFile.filePath, 0, 8).getBigUint64(0))
          .toBe(
            pages[0],
          )
      })

      it("multiple freed pages are linked together", async () => {
        pages.push(await bufferPool.allocatePage())
        pages.push(await bufferPool.allocatePage())
        await bufferPool.freePage(pages[1])
        await bufferPool.freePage(pages[3])
        await bufferPool.commit()
        const firstFreePage = readBytesFromFileSync(tempFile.filePath, 0, 8)
          .getBigUint64(0)
        expect(firstFreePage).toBe(pages[3])
        const secondFreePage = readBytesFromFileSync(
          tempFile.filePath,
          firstFreePage,
          8,
        )
          .getBigUint64(0)
        expect(secondFreePage).toBe(pages[1])
        const thirdFreePage = readBytesFromFileSync(
          tempFile.filePath,
          secondFreePage,
          8,
        )
          .getBigUint64(0)
        // the last free page should point to the end of the file
        expect(thirdFreePage).toBe(pages[3] + BigInt(bufferPool.pageSize))
      })

      it("freed pages will be reused", async () => {
        await bufferPool.freePage(pages[0])
        const newPageId = await bufferPool.allocatePage()
        expect(newPageId).toBe(pages[0])
      })

      it.skip("when a free page gets reused before the next commit, it does not affect the dirty state", async () => {
        expect(bufferPool.isDirty).toBe(false)
        await bufferPool.freePage(pages[0])
        expect(bufferPool.isDirty).toBe(true)
        bufferPool.allocatePage()
        expect(bufferPool.isDirty).toBe(false)
      })
    })
  })
})

async function readBytesFromFile(
  filePath: string,
  offset: number | bigint,
  numBytes: number,
): Promise<DataView> {
  const file = await Deno.open(filePath)
  await file.seek(offset, Deno.SeekMode.Start)
  const data = new Uint8Array(numBytes)
  await file.read(data)
  file.close()
  return new DataView(data.buffer)
}

function readBytesFromFileSync(
  filePath: string,
  offset: number | bigint,
  numBytes: number,
): DataView {
  const file = Deno.openSync(filePath)
  file.seekSync(offset, Deno.SeekMode.Start)
  const data = new Uint8Array(numBytes)
  file.readSync(data)
  file.close()
  return new DataView(data.buffer)
}
