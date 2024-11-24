import { assert, assertEquals, assertNotStrictEquals } from "@std/assert"
import { uint32Struct, unicodeStringStruct } from "../binary/Struct.ts"
import { FileBackedBufferPool } from "../pages/BufferPool.ts"
import { HeapPageFile } from "../pages/HeapPageFile.ts"
import { VariableLengthRecordPage } from "../pages/VariableLengthRecordPage.ts"
import { FileBackedNodeList } from "./FileBackedNodeList.ts"
import { assertStrictEquals } from "@std/assert/strict-equals"

async function makeNodeList() {
  const file = await Deno.open("/tmp/test.db", {
    read: true,
    write: true,
    create: true,
    truncate: true,
  })
  const bufferPool = await FileBackedBufferPool.create(file, 4096)
  const heapPageFile = await HeapPageFile.create(
    bufferPool,
    VariableLengthRecordPage.allocator,
  )
  const keySerializer = uint32Struct
  const nodelist = new FileBackedNodeList<number, string>(
    bufferPool,
    heapPageFile,
    keySerializer,
    unicodeStringStruct,
  )
  return { nodelist, [Symbol.dispose]: () => file.close() }
}

Deno.test("FileBackedNodeList.createLeafNode()", async (t) => {
  await t.step("creates an empty node", async (t) => {
    using n = await makeNodeList()
    const node = await n.nodelist.createLeafNode({
      keyvals: [{ key: 1, vals: ["hello"] }, { key: 2, vals: ["world"] }],
      nextLeafNodeId: null,
    })

    assertEquals(node.keyvals, [{ key: 1, vals: ["hello"] }, {
      key: 2,
      vals: ["world"],
    }], "uses the given keyvals")
    assertStrictEquals(
      node.nextLeafNodeId,
      null,
      "Uses the given nextLeafNodeId",
    )
    assertEquals(
      node.nodeId,
      { pageId: 4104n, slotIndex: 0 },
      "Creates a new node id",
    )

    await t.step("reads the node back", async () => {
      const sameNode = await n.nodelist.get(node.nodeId)
      assertStrictEquals(
        sameNode,
        node,
        "Expected the exact node instance to be retrieved from cache",
      )
    })

    await n.nodelist.commit()

    await t.step(
      "reads the node from disk after it's been committed",
      async () => {
        const sameNode = await n.nodelist.get(node.nodeId)
        assertNotStrictEquals(
          sameNode,
          node,
          "Expected a new instance to be retrieved",
        )
        assertEquals(
          sameNode.serialize(),
          node.serialize(),
          "Expected the same data to be retrieved",
        )
      },
    )

    await t.step("lets you replace the node with a new one", async (t) => {
      const newNode = await n.nodelist.createLeafNode({
        keyvals: [{ key: 3, vals: ["a new value"] }],
        nextLeafNodeId: null,
      }, node.nodeId)
      assertEquals(newNode.keyvals, [{ key: 3, vals: ["a new value"] }])
      assertEquals(newNode.nodeId, node.nodeId)

      await n.nodelist.commit()
      await t.step("reads the new node back after commit", async () => {
        const sameNode = await n.nodelist.get(newNode.nodeId)
        assertNotStrictEquals(
          sameNode,
          newNode,
          "Expected a new instance to be retrieved",
        )
        assertEquals(
          sameNode.serialize(),
          newNode.serialize(),
          "Expected the same data to be retrieved",
        )
      })
    })
  })
})

Deno.test("FileBackedNodeList.createInternalNode()", async (t) => {
  await t.step("creates an internal node", async () => {
    using n = await makeNodeList()
    const childNode = await n.nodelist.createLeafNode({
      keyvals: [],
      nextLeafNodeId: null,
    })
    const node = await n.nodelist.createInternalNode({
      keys: [],
      childrenNodeIds: [childNode.nodeId],
    })
    assert(node.keys.length === 0, "Uses the given keys")
    assert(node.childrenNodeIds.length === 1, "Uses the given childrenNodeIds")
    assertEquals(
      node.nodeId,
      { pageId: 4104n, slotIndex: 1 },
      "Creates a new node in the next slot of the page",
    )
  })
})
