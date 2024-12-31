import { expect } from "@std/expect"
import { IBufferPool, InMemoryBufferPool } from "./BufferPool.ts"
import { LinkedPageList } from "./LinkedPageList.ts"
import { spyOnBufferPool } from "../testing.ts"

Deno.test("LinkedPageList", async (test) => {
  const data = "a".repeat(350)

  const bufferPool: IBufferPool = new InMemoryBufferPool(100)
  await bufferPool.allocatePage()
  const spy = spyOnBufferPool(bufferPool)
  const headPageId = await bufferPool.allocatePage()
  await test.step(".writeData()", async () => {
    const linkedPageList = new LinkedPageList(
      bufferPool,
      headPageId,
    )
    await linkedPageList.writeData(new TextEncoder().encode(data))

    expect(linkedPageList.headPageId).toEqual(1n)
    const header = LinkedPageList.headerStruct.readAt(
      await bufferPool.getPageView(1n),
      0,
    )
    expect(header).toEqual({
      nextPageId: 2n,
      byteLength: 88,
    })
    expect(
      (await bufferPool.getPageView(1n)).getUint8(
        LinkedPageList.headerStruct.size,
      ),
    )
      .toEqual("a".charCodeAt(0))
    expect(await spy.getAllocatedPages()).toEqual([1n, 2n, 3n, 4n])
  })

  await test.step(".readData()", async () => {
    const linkedPageList = new LinkedPageList(
      bufferPool,
      headPageId,
    )

    expect(new TextDecoder().decode(await linkedPageList.readData())).toEqual(
      data,
    )
  })

  await test.step(".drop()", async () => {
    const linkedPageList = new LinkedPageList(
      bufferPool,
      headPageId,
    )
    await linkedPageList.drop()
    expect(spy.getFreedPages()).toEqual([1n, 2n, 3n, 4n])
    await expect(linkedPageList.readData()).rejects.toThrow(
      "Cannot read from a dropped LinkedPageList",
    )
  })
})

Deno.test("LinkedPageList overwriting data", async (test) => {
  const bufferPool: IBufferPool = new InMemoryBufferPool(100)
  await bufferPool.allocatePage()
  const spy = spyOnBufferPool(bufferPool)
  const headPageId = await bufferPool.allocatePage()
  const linkedPageList = new LinkedPageList(
    bufferPool,
    headPageId,
  )

  await linkedPageList.writeData(new TextEncoder().encode("a".repeat(350)))
  expect(await spy.getAllocatedPages()).toEqual([1n, 2n, 3n, 4n])

  await test.step("Overwriting with more data allocates more pages", async () => {
    await linkedPageList.writeData(new TextEncoder().encode("b".repeat(550)))
    expect(await spy.getAllocatedPages()).toEqual([1n, 2n, 3n, 4n, 5n, 6n, 7n])
    expect(spy.getFreedPages()).toEqual([])
    expect(await linkedPageList.readData()).toEqual(
      new TextEncoder().encode("b".repeat(550)),
    )
  })

  await test.step("Overwriting with less data frees pages", async () => {
    await linkedPageList.writeData(new TextEncoder().encode("c".repeat(150)))
    expect(await spy.getAllocatedPages()).toEqual([1n, 2n, 3n, 4n, 5n, 6n, 7n])
    expect(spy.getFreedPages()).toEqual([3n, 4n, 5n, 6n, 7n])
    expect(await linkedPageList.readData()).toEqual(
      new TextEncoder().encode("c".repeat(150)),
    )
  })
})
