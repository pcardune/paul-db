import { expect } from "jsr:@std/expect/expect"
import { Struct } from "../binary/Struct.ts"
import { FileBackedBufferPool } from "../pages/BufferPool.ts"
import { generateTestFilePath, spyOnBufferPool } from "../testing.ts"
import { FileBackedBTree } from "./FileBackedBTree.ts"

async function makeBufferPool() {
  const tempFile = generateTestFilePath("Btree.data")
  const file = await Deno.open(tempFile.filePath, {
    read: true,
    write: true,
    create: true,
    truncate: true,
  })
  const bufferPool = await FileBackedBufferPool.create(file, 4096)

  return {
    bufferPool,
    bufferPoolSpy: spyOnBufferPool(bufferPool),
    [Symbol.dispose]: () => {
      file[Symbol.dispose]()
      tempFile[Symbol.dispose]()
    },
  }
}
const compare = (a: number, b: number) => a - b

async function makeFileBackedBTree() {
  const bp = await makeBufferPool()
  const btreePageId = await bp.bufferPool.allocatePage()
  const fbbt = await FileBackedBTree.create<number, string>(
    bp.bufferPool,
    btreePageId,
    Struct.uint32,
    Struct.unicodeStringStruct,
    { order: 2, compare },
  )
  return {
    fbbt,
    ...bp,
  }
}

Deno.test("FileBackedBTree.create()", async () => {
  using r = await makeFileBackedBTree()
  const { fbbt, bufferPool } = r

  expect(fbbt.pageId).toEqual(8n)
  expect(fbbt.bufferPool).toBe(bufferPool)
  expect(fbbt.config.order).toBe(2)
  expect(fbbt.config.compare).toBe(compare)
  expect(fbbt.config.isEqual).toBeDefined()
})

Deno.test("FileBackedBufferPool.pageIdsIter()", async () => {
  using r = await makeFileBackedBTree()
  const { fbbt } = r
  expect(await fbbt.pageIdsIter().toArray()).toEqual([
    8200n,
    4104n,
    8n,
  ])

  await fbbt.btree.insertMany(
    Array.from({ length: 500 }, (_, i) => [i, i.toString()]),
  )
  expect(await fbbt.pageIdsIter().toArray()).toEqual([
    8200n,
    12296n,
    16392n,
    20488n,
    24584n,
    28680n,
    32776n,
    36872n,
    40968n,
    4104n,
    8n,
  ])
})

Deno.test("FileBackedBTree.drop()", async () => {
  using r = await makeFileBackedBTree()
  const { fbbt } = r
  await fbbt.btree.insertMany(
    Array.from({ length: 500 }, (_, i) => [i, i.toString()]),
  )
  await fbbt.drop()
  expect(r.bufferPoolSpy.getFreedPages()).toEqual(
    await r.bufferPoolSpy.getAllocatedPages(),
  )
})
