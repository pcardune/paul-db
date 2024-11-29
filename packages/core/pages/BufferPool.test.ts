import { afterEach, beforeEach, describe, it } from "jsr:@std/testing/bdd"
import { FileBackedBufferPool } from "./BufferPool.ts"
import { expect } from "jsr:@std/expect"

describe("FileBackedBufferPool", () => {
  let bufferPool: FileBackedBufferPool
  const filePath = "/tmp/bufferpool"
  const pageSize = 4096
  beforeEach(async () => {
    Deno.openSync(filePath, {
      create: true,
      write: true,
      truncate: true,
    }).close()
    bufferPool = await FileBackedBufferPool.create(
      await Deno.open(filePath, {
        read: true,
        write: true,
        create: true,
      }),
      pageSize,
    )
  })
  afterEach(() => {
    bufferPool.close()
    Deno.removeSync(filePath)
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
      expect(Deno.statSync(filePath).size).toBe(0)
    })

    it("doesn't write to disk if the pages are never marked dirty", async () => {
      await bufferPool.commit()
      expect(Deno.statSync(filePath).size).toBe(0)
    })

    it("writes to disk after marking a page dirty", async () => {
      bufferPool.markDirty(pages[0])
      await bufferPool.commit()
      expect(Deno.statSync(filePath).size).toBe(4096 + 8)
    })

    it("writes the correct data to disk", async () => {
      const view = await bufferPool.getWriteablePage(pages[1])
      view.setUint32(0, 0xdeadbeef)
      bufferPool.markDirty(pages[1])
      expect(Deno.statSync(filePath).size).toBe(0)

      // now we commit and the data is written
      await bufferPool.commit()
      expect(Deno.statSync(filePath).size).toBe(8192 + 8)
      const data = await readBytesFromFile(filePath, Number(pages[1]), 8)
      expect(data.getUint32(0)).toBe(0xdeadbeef)
    })

    describe("Reading data", () => {
      let newBufferPool: FileBackedBufferPool
      beforeEach(async () => {
        // copy filePath to a new file
        const view = await bufferPool.getWriteablePage(pages[1])
        view.setUint32(0, 0xdeadbeef)
        bufferPool.markDirty(pages[1])
        await bufferPool.commit()
        Deno.copyFileSync(filePath, filePath + ".copy")
        newBufferPool = await FileBackedBufferPool.create(
          await Deno.open(filePath + ".copy", {
            read: true,
            write: true,
            create: true,
          }),
          pageSize,
        )
      })
      afterEach(() => {
        newBufferPool.close()
      })

      it("reads the correct data from disk", async () => {
        const view = await newBufferPool.getPageView(pages[1])
        expect(view.getUint32(0)).toBe(0xdeadbeef)
      })
    })

    describe("Freed pages", () => {
      it("Cause the buffer pool to be dirty", () => {
        expect(bufferPool.isDirty).toBe(false)
        bufferPool.freePage(pages[0])
        expect(bufferPool.isDirty).toBe(true)
      })

      it("are tracked in the file after commit", async () => {
        bufferPool.freePage(pages[0])
        await bufferPool.commit()
        expect(readBytesFromFileSync(filePath, 0, 8).getBigUint64(0)).toBe(
          pages[0],
        )
      })

      it("multiple freed pages are linked together", async () => {
        pages.push(await bufferPool.allocatePage())
        pages.push(await bufferPool.allocatePage())
        bufferPool.freePage(pages[1])
        bufferPool.freePage(pages[3])
        await bufferPool.commit()
        const firstFreePage = readBytesFromFileSync(filePath, 0, 8)
          .getBigUint64(0)
        expect(firstFreePage).toBe(pages[3])
        const secondFreePage = readBytesFromFileSync(filePath, firstFreePage, 8)
          .getBigUint64(0)
        expect(secondFreePage).toBe(pages[1])
        const thirdFreePage = readBytesFromFileSync(filePath, secondFreePage, 8)
          .getBigUint64(0)
        expect(thirdFreePage).toBe(0n)
      })

      it("freed pages will be reused", async () => {
        bufferPool.freePage(pages[0])
        const newPageId = await bufferPool.allocatePage()
        expect(newPageId).toBe(pages[0])
      })

      it.skip("when a free page gets reused before the next commit, it does not affect the dirty state", () => {
        expect(bufferPool.isDirty).toBe(false)
        bufferPool.freePage(pages[0])
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
