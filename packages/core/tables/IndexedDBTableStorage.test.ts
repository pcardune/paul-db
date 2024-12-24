import { expect } from "@std/expect"
import { IndexedDBTableStorage } from "./IndexedDBTableStorage.ts"

async function makeIndexedDBStorage() {
  const { default: indexedDB } = await import("npm:fake-indexeddb")

  const dbName = "testDB"
  const tableName = "testTable"
  return new Promise<
    {
      storage: IndexedDBTableStorage<{ data: string }>
      [Symbol.asyncDispose]: () => Promise<void>
    }
  >(
    (resolve, reject) => {
      const openReq = indexedDB.open(dbName, 1)
      openReq.onupgradeneeded = () => {
        IndexedDBTableStorage.createObjectStore(openReq.result, tableName)
      }
      openReq.onsuccess = () => {
        const db = openReq.result
        const storage = new IndexedDBTableStorage<{ data: string }>(
          db,
          tableName,
        )
        resolve({
          storage,
          [Symbol.asyncDispose]: async () => {
            await storage.waitForTransactions()
            openReq.result.close()
            const req = indexedDB.deleteDatabase(dbName)
            await new Promise((resolve, reject) => {
              req.onsuccess = resolve
              req.onerror = () => reject(req.error)
            })
          },
        })
      }
      openReq.onerror = () => reject(openReq.error)
    },
  )
}

Deno.test(".insert()", async () => {
  await using s = await makeIndexedDBStorage()
  const { storage } = s

  const helloId = await storage.insert({ data: "hello" })
  expect(helloId).toEqual(1)
  const worldId = await storage.insert({ data: "world" })
  expect(worldId).toEqual(2)

  expect(await storage.get(helloId)).toEqual({ data: "hello" })
  expect(await storage.get(worldId)).toEqual({ data: "world" })

  expect(
    await storage.iterate().toArray(),
  ).toEqual([
    [helloId, { data: "hello" }],
    [worldId, { data: "world" }],
  ])
})

Deno.test(".set()", async (t) => {
  await using s = await makeIndexedDBStorage()
  const { storage } = s
  const helloId = await storage.insert({ data: "hello" })
  const worldId = await storage.insert({ data: "world" })

  await t.step("set() returns the same ID if the value fits", async () => {
    const newHelloId = await storage.set(helloId, { data: "HELLO" })
    expect(newHelloId).toEqual(helloId)
    expect(await storage.get(newHelloId)).toEqual({ data: "HELLO" })
  })

  await t.step(
    "set() returns the same ID if the value doesn't fit",
    async () => {
      const newHelloId = await storage.set(
        helloId,
        { data: "HELLO THAT TAKES UP MORE SPACE" },
      )
      expect(newHelloId).toEqual(helloId)
      expect(await storage.get(helloId)).toEqual(
        { data: "HELLO THAT TAKES UP MORE SPACE" },
      )
    },
  )

  await t.step("The old ID is still valid", async () => {
    expect(await storage.get(helloId)).toEqual({
      data: "HELLO THAT TAKES UP MORE SPACE",
    })
  })

  await t.step(
    "iterate() returns the updated values in the order they are stored",
    async () => {
      expect(
        await storage.iterate().toArray(),
      ).toEqual([
        [helloId, { data: "HELLO THAT TAKES UP MORE SPACE" }],
        [worldId, { data: "world" }],
      ])
    },
  )

  await t.step("Still works after setting yet another value", async () => {
    const newHelloId = await storage.set(
      helloId,
      { data: "HELLO THAT TAKES UP even MORE SPACE" },
    )
    expect(newHelloId).toEqual(helloId)
    expect(await storage.get(helloId)).toEqual(
      { data: "HELLO THAT TAKES UP even MORE SPACE" },
    )
    expect(
      await storage.iterate().toArray(),
    ).toEqual([
      [helloId, { data: "HELLO THAT TAKES UP even MORE SPACE" }],
      [worldId, { data: "world" }],
    ])
  })
})

Deno.test(".remove()", async () => {
  await using s = await makeIndexedDBStorage()
  const { storage } = s
  const helloId = await storage.insert({ data: "hello" })
  const worldId = await storage.insert({ data: "world" })

  await storage.remove(helloId)
  expect(await storage.get(helloId)).toBeUndefined()
  expect(await storage.get(worldId)).toEqual({ data: "world" })
  expect(await storage.iterate().toArray()).toEqual([[worldId, {
    data: "world",
  }]])
})
