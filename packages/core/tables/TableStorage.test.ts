import { expect } from "@std/expect"
import { Struct } from "../binary/Struct.ts"
import { InMemoryBufferPool } from "../pages/BufferPool.ts"
import { HeapFileTableStorage } from "./TableStorage.ts"
import { spyOnBufferPool } from "../testing.ts"

function makeHeapFileStorage() {
  const bufferpool = new InMemoryBufferPool(4092)
  const pageId = bufferpool.allocatePage()

  return new HeapFileTableStorage(
    bufferpool,
    pageId,
    Struct.unicodeStringStruct,
    0,
  )
}

Deno.test("HeapFileTableStorage.insert()", async () => {
  const storage = makeHeapFileStorage()

  const helloId = await storage.insert("hello")
  expect(helloId).toEqual({ pageId: 1n, slotIndex: 0 })
  const worldId = await storage.insert("world")
  expect(worldId).toEqual({ pageId: 1n, slotIndex: 1 })

  expect(await storage.get(helloId)).toEqual("hello")
  expect(await storage.get(worldId)).toEqual("world")

  expect(
    await storage.iterate().toArray(),
  ).toEqual([
    [helloId, "hello"],
    [worldId, "world"],
  ])
})

Deno.test("HeapFileTableStorage.set()", async (t) => {
  const storage = makeHeapFileStorage()
  const helloId = await storage.insert("hello")
  const worldId = await storage.insert("world")

  await t.step("set() returns the same ID if the value fits", async () => {
    const newHelloId = await storage.set(helloId, "HELLO")
    expect(newHelloId).toEqual(helloId)
    expect(await storage.get(newHelloId)).toEqual("HELLO")
  })

  await t.step(
    "set() returns the same ID if the value doesn't fit",
    async () => {
      const newHelloId = await storage.set(
        helloId,
        "HELLO THAT TAKES UP MORE SPACE",
      )
      expect(newHelloId).toEqual(helloId)
      expect(await storage.get(helloId)).toEqual(
        "HELLO THAT TAKES UP MORE SPACE",
      )
    },
  )

  await t.step("The old ID is still valid", async () => {
    expect(await storage.get(helloId)).toEqual("HELLO THAT TAKES UP MORE SPACE")
  })

  await t.step(
    "iterate() returns the updated values in the order they are stored",
    async () => {
      expect(
        await storage.iterate().toArray(),
      ).toEqual([
        [helloId, "HELLO THAT TAKES UP MORE SPACE"],
        [worldId, "world"],
      ])
    },
  )

  await t.step("Still works after setting yet another value", async () => {
    const newHelloId = await storage.set(
      helloId,
      "HELLO THAT TAKES UP even MORE SPACE",
    )
    expect(newHelloId).toEqual(helloId)
    expect(await storage.get(helloId)).toEqual(
      "HELLO THAT TAKES UP even MORE SPACE",
    )
    expect(
      await storage.iterate().toArray(),
    ).toEqual([
      [helloId, "HELLO THAT TAKES UP even MORE SPACE"],
      [worldId, "world"],
    ])
  })
})

Deno.test("HeapFileTableStorage.remove() on a shallow item", async () => {
  const storage = makeHeapFileStorage()
  const helloId = await storage.insert("hello")
  const worldId = await storage.insert("world")

  await storage.remove(helloId)
  expect(await storage.get(helloId)).toBeUndefined()
  expect(await storage.get(worldId)).toEqual("world")
  expect(await storage.iterate().toArray()).toEqual([[worldId, "world"]])
})

Deno.test("HeapFileTableStorage.remove() on a deep item", async () => {
  const storage = makeHeapFileStorage()
  const helloId = await storage.insert("hello")
  const worldId = await storage.insert("world")

  const newHelloId = await storage.set(
    helloId,
    "HELLO THAT TAKES UP MORE SPACE",
  )

  await storage.remove(helloId)
  expect(await storage.get(newHelloId)).toBeUndefined()
  expect(await storage.get(helloId)).toBeUndefined()
  expect(await storage.get(worldId)).toEqual("world")
  expect(await storage.iterate().toArray()).toEqual([
    [worldId, "world"],
  ])
})

Deno.test("Storing something larger than a page", async (test) => {
  const storage = makeHeapFileStorage()
  const spy = spyOnBufferPool(storage.bufferPool)
  const id = await storage.insert("a".repeat(4092 * 2))
  expect(await spy.getAllocatedPages()).toEqual([1n, 2n, 3n, 4n])
  expect(await storage.get(id)).toEqual("a".repeat(4092 * 2))

  await test.step("Overwriting it with something even larger", async () => {
    spy.reset()
    const newId = await storage.set(id, "b".repeat(4092 * 3))
    // we reuse the same pages
    expect(await spy.getAllocatedPages()).toEqual([5n])
    expect(await storage.get(newId)).toEqual("b".repeat(4092 * 3))
    expect(newId).toEqual(id)
  })

  await test.step("Overwriting it with something smaller", async () => {
    spy.reset()
    const newId = await storage.set(id, "c".repeat(40))
    expect(spy.getFreedPages()).toEqual([1n, 2n, 3n, 5n])
    expect(await spy.getAllocatedPages()).toEqual([])
    expect(await storage.get(newId)).toEqual("c".repeat(40))
  })
})

Deno.test("Setting a value that doesn't fit and is larger than a page", async () => {
  const storage = makeHeapFileStorage()
  const spy = spyOnBufferPool(storage.bufferPool)

  // this should fit.
  const id = await storage.insert("a".repeat(100))
  // this won't fit the old location, so it will be forwarded
  const newId = await storage.set(id, "b".repeat(200))
  expect(newId).toEqual(id)
  expect(await spy.getAllocatedPages()).toEqual([1n])
  expect(spy.getFreedPages()).toEqual([])

  // this won't fit the old location, and it's larger than a page
  spy.reset()
  const oversizedId = await storage.set(id, "c".repeat(4092 * 2))
  expect(oversizedId).toEqual(id)
  expect(await storage.get(newId)).toEqual("c".repeat(4092 * 2))
  expect(await spy.getAllocatedPages()).toEqual([2n, 3n, 4n])
  expect(spy.getFreedPages()).toEqual([])

  // now we go back to something that will fit the original location
  spy.reset()
  const smallerId = await storage.set(id, "d".repeat(50))
  expect(smallerId).toEqual(id)
  expect(await storage.get(smallerId)).toEqual("d".repeat(50))
  expect(await spy.getAllocatedPages()).toEqual([])
  expect(spy.getFreedPages()).toEqual([2n, 3n, 4n])
})
