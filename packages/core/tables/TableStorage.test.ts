import { expect } from "jsr:@std/expect"
import { Struct } from "../binary/Struct.ts"
import { InMemoryBufferPool } from "../pages/BufferPool.ts"
import { HeapFileTableStorage } from "./TableStorage.ts"

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
