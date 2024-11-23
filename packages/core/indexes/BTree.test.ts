import { expect } from "jsr:@std/expect"
import { beforeEach, describe, it } from "jsr:@std/testing/bdd"
import { BTree } from "../indexes/BTree.ts"
import { randomIntegerBetween, randomSeeded } from "@std/random"
import { BTreeNode, InternalBTreeNode, LeafBTreeNode } from "./BTreeNode.ts"

// TODO: consider using expect.extend to make custom matchers for this
// see https://jsr.io/@std/expect/doc/~/expect.extend
async function assertWellFormedBtree<K, V, NodeId>(btree: BTree<K, V, NodeId>) {
  /**
   * The number d is the order of a B+ tree. Each node (with the exception of
   * the root node) must have d ≤ x ≤ 2d entries assuming no deletes happen
   * (it’s possible for leaf nodes to end up with < d entries if you
   * delete data). The entries within each node must be sorted.
   */

  const rootNode = await btree.getRootNode()
  expect(rootNode, "Expect btree to have a root node").toBeDefined()
  expect(rootNode.type, "root node should always be an internal node")
    .toBe("internal")
  if (rootNode.type !== "internal") {
    throw new Error("Unreachable")
  }
  await assertWellFormedNode(btree, rootNode)

  async function collectLeafNodesWithDepth(
    node: BTreeNode<K, V, NodeId>,
    depth: number,
  ): Promise<{ node: LeafBTreeNode<K, V, NodeId>; depth: number }[]> {
    if (node.type === "leaf") {
      return [{ node, depth }]
    }
    const children = await btree.childrenForNode(node)
    const descendents = await Promise.all(children.map((childNode) => {
      return collectLeafNodesWithDepth(childNode, depth + 1)
    }))
    return descendents.flat()
  }
  const depths = new Set(
    (await collectLeafNodesWithDepth(rootNode, 0)).map(({ depth }) => depth),
  )
  expect(depths.size, "All leaf nodes should be at the same depth").toBe(1)
}

async function assertWellFormedNode<K, V, NodeId>(
  btree: BTree<K, V, NodeId>,
  node: BTreeNode<K, V, NodeId>,
) {
  expect(
    (await btree.getNodeWithId(node.nodeId)).serialize(),
    "Node should have the correct id",
  )
    .toEqual(node.serialize())
  if (node.type === "internal") {
    await assertWellFormedInternalNode(btree, node)
  } else {
    assertWellFormedLeafNode(btree, node)
  }
}

function keysForNode<K>(node: BTreeNode<K, unknown, unknown>): readonly K[] {
  if (node.type === "leaf") {
    return node.keyvals.map((keyval) => keyval.key)
  }
  return node.keys
}

function assertWellFormedLeafNode<K, V, NodeId>(
  btree: BTree<K, V, NodeId>,
  node: LeafBTreeNode<K, V, NodeId>,
) {
  const keys = keysForNode(node)
  const sortedKeys = Array.from(keys).sort(btree.compare)
  expect(keys, "Keys should be sorted").toEqual(sortedKeys)
  expect(keys.length, "Leaf nodes should have at most 2 * _order_ keys")
    .toBeLessThanOrEqual(2 * btree.order)
}

function ord<A>(
  a: A,
  cmp: "<" | ">" | "=" | "<=" | ">=",
  b: A,
  comparator: (a: A, b: A) => number,
): boolean {
  switch (cmp) {
    case "<":
      return comparator(a, b) < 0
    case ">":
      return comparator(a, b) > 0
    case "=":
      return comparator(a, b) === 0
    case "<=":
      return comparator(a, b) <= 0
    case ">=":
      return comparator(a, b) >= 0
  }
}

async function assertWellFormedInternalNode<K, V, NodeId>(
  btree: BTree<K, V, NodeId>,
  node: InternalBTreeNode<K, NodeId>,
) {
  const sortedKeys = Array.from(node.keys).sort(btree.compare)
  expect(node.keys, "Keys should be sorted").toEqual(sortedKeys)

  /**
   * In between each entry of an inner node, there is a pointer to a child node.
   * Since there are at most 2d entries in a node, inner nodes may have at most
   * 2d+1 child pointers. This is also called the tree’s fanout.
   */
  expect(
    node.childrenNodeIds.length,
    "Internal nodes should have at most 2 * _order_ + 1 children",
  ).toBeLessThanOrEqual(2 * btree.order + 1)

  expect(
    node.childrenNodeIds.length,
    "Internal nodes should have one more child than keys",
  ).toEqual(node.keys.length + 1)

  /**
   * The keys in the children to the left of an entry must be less than the
   * entry while the keys in the children to the right must be greater than or
   * equal to the entry.
   */
  for (let i = 0; i < node.keys.length; i++) {
    const key = node.keys[i]
    const leftChild = await btree.getNodeWithId(node.childrenNodeIds[i])
    const rightChild = await btree.getNodeWithId(node.childrenNodeIds[i + 1])
    const leftKeys = Array.from(keysForNode(leftChild)).sort(btree.compare)
    const rightKeys = Array.from(keysForNode(rightChild)).sort(btree.compare)
    if (leftKeys.length > 0) {
      expect(
        ord(leftKeys[leftKeys.length - 1], "<", key, btree.compare),
        `Keys in left child should be less than the key at index ${i}`,
      ).toBe(true)
    }
    if (rightKeys.length > 0) {
      expect(
        ord(rightKeys[0], ">=", key, btree.compare),
        `Keys in right child should be greater than or equal to the key at index ${i}`,
      ).toBe(true)
    }
  }

  if (node.nodeId != (await btree.getRootNode()).nodeId) {
    expect(
      node.keys.length,
      "Internal nodes should have at least _order_ entries",
    ).toBeGreaterThanOrEqual(btree.order)
    expect(
      node.keys.length,
      "Internal nodes should have at most 2 * _order_ entries",
    ).toBeLessThanOrEqual(2 * btree.order)
  }

  for (const childNode of await btree.childrenForNode(node)) {
    await assertWellFormedNode(btree, childNode)
  }
}

it("Starts out empty", async () => {
  const btree = await BTree.inMemory<number, { name: string }>({
    compare: (a, b) => a - b,
  })
  await assertWellFormedBtree(btree)
  await btree.insert(1, { name: "Paul" })
  await assertWellFormedBtree(btree)
})

it("Does basic operations", async () => {
  const btree = await BTree.inMemory<number, { name: string }>({
    compare: (a, b) => a - b,
  })
  expect(await btree.has(1)).toBe(false)
  expect(await btree.get(1)).toEqual([])

  await btree.insert(1, { name: "Paul" })
  expect(await btree.has(1)).toBe(true)
  expect(await btree.get(1)).toEqual([{ name: "Paul" }])

  await btree.insert(1, { name: "Meghan" })
  expect(await btree.get(1)).toEqual([{ name: "Paul" }, { name: "Meghan" }])

  await btree.insert(2, { name: "Mr. Blue" })
  expect(await btree.get(2)).toEqual([{ name: "Mr. Blue" }])

  await assertWellFormedBtree(btree)

  await btree.removeAll(1)
  expect(await btree.get(1)).toEqual([])
})

it("Inserting multiple values with the same key will return multiple values", async () => {
  const btree = await BTree.inMemory<number, string>({
    compare: (a, b) => a - b,
  })
  await btree.insert(1, "Paul")
  await btree.insert(1, "Meghan")
  expect(await btree.get(1)).toEqual(["Paul", "Meghan"])
  await assertWellFormedBtree(btree)
})

describe("Inserting nodes", () => {
  const order = 1
  let btree: Awaited<ReturnType<typeof BTree.inMemory<number, string>>>
  beforeEach(async () => {
    btree = await BTree.inMemory<number, string>({
      order,
      compare: (a, b) => a - b,
    })
  })
  it("starts with only two nodes in the tree", async () => {
    expect(btree.nodes.size).toBe(2)
    expect((await btree.getRootNode()).type).toBe("internal")
    const children = await btree.childrenForNode(await btree.getRootNode())
    expect(children).toHaveLength(1)
    expect(children[0].type).toBe("leaf")
    await assertWellFormedBtree(btree)
  })
  describe("After inserting up to order*2 entries", () => {
    beforeEach(async () => {
      await btree.insert(0, `Person 0`)
      await btree.insert(1, `Person 1`)
    })
    it("No new nodes will be added", async () => {
      await assertWellFormedBtree(btree)
      expect(btree.nodes.size).toBe(2)
    })
    it("All nodes will have the correct values", async () => {
      for (let i = 0; i < order * 2; i++) {
        expect(await btree.get(i)).toEqual([`Person ${i}`])
      }
    })

    describe("After inserting one more entry", () => {
      beforeEach(async () => {
        await btree.insert(2, `Person 2`)
      })
      it("A new node will be added", async () => {
        expect(btree.nodes.size).toBe(3)
        expect(await btree.childrenForNode(await btree.getRootNode()))
          .toHaveLength(2)
        await assertWellFormedBtree(btree)
      })

      it("All entries will have the correct values", async () => {
        for (let i = 0; i <= 2; i++) {
          expect(await btree.get(i)).toEqual([`Person ${i}`])
        }
      })

      describe("After inserting enough entries to split things more", () => {
        let originalRootNodeId: number
        beforeEach(async () => {
          originalRootNodeId = (await btree.getRootNode()).nodeId
          await btree.insert(3, `Person 3`)
          await btree.insert(4, `Person 4`)
        })
        it("A new node will be added", async () => {
          await assertWellFormedBtree(btree)
        })
        it("There will be a new root node", async () => {
          expect((await btree.getRootNode()).nodeId).not.toBe(
            originalRootNodeId,
          )
        })
        it("All entries will have the correct values", async () => {
          for (let i = 0; i <= 4; i++) {
            expect(await btree.get(i)).toEqual([`Person ${i}`])
          }
        })

        describe("After inserting another 2 entries", () => {
          beforeEach(async () => {
            await btree.insert(5, `Person 5`)
            await btree.insert(6, `Person 6`)
          })
          it("it will still be well formed", async () => {
            await assertWellFormedBtree(btree)
          })
        })
      })
    })
  })
})

describe("Removing items", () => {
  let btree: Awaited<ReturnType<typeof BTree.inMemory<number, string>>>
  beforeEach(async () => {
    btree = await BTree.inMemory<number, string>({
      order: 3,
      compare: (a, b) => a - b,
    })
    for (let i = 0; i < 10; i++) {
      await btree.insert(i, `Person ${i}`)
    }
    await btree.insert(5, "foo")
    await btree.insert(5, "bar")
    await btree.insert(5, "baz")
  })
  it("Can remove all items", async () => {
    expect(await btree.get(5)).toEqual(["Person 5", "foo", "bar", "baz"])
    await btree.removeAll(5)
    expect(await btree.get(5)).toEqual([])
    await assertWellFormedBtree(btree)
  })
  it("Can remove an individual entry for a particular key", async () => {
    expect(await btree.get(5)).toEqual(["Person 5", "foo", "bar", "baz"])
    await btree.remove(5, "bar")
    expect(await btree.get(5)).toEqual(["Person 5", "foo", "baz"])
    await assertWellFormedBtree(btree)
  })
})

describe("Range requests", () => {
  let btree: Awaited<ReturnType<typeof BTree.inMemory<number, string>>>
  beforeEach(async () => {
    btree = await BTree.inMemory<number, string>({
      order: 3,
      compare: (a, b) => a - b,
    })
    for (let i = 0; i < 10; i++) {
      await btree.insert(i, `Person ${i}`)
    }
  })
  it("Can get all values in a range", async () => {
    expect(await btree.getRange({ gte: 3, lte: 7 })).toEqual([
      { key: 3, vals: ["Person 3"] },
      { key: 4, vals: ["Person 4"] },
      { key: 5, vals: ["Person 5"] },
      { key: 6, vals: ["Person 6"] },
      { key: 7, vals: ["Person 7"] },
    ])
  })
  it("exclusive lower bound", async () => {
    expect(await btree.getRange({ gt: 3, lte: 7 })).toEqual([
      { key: 4, vals: ["Person 4"] },
      { key: 5, vals: ["Person 5"] },
      { key: 6, vals: ["Person 6"] },
      { key: 7, vals: ["Person 7"] },
    ])
  })
  it("exclusive upper bound", async () => {
    expect(await btree.getRange({ gt: 3, lt: 7 })).toEqual([
      { key: 4, vals: ["Person 4"] },
      { key: 5, vals: ["Person 5"] },
      { key: 6, vals: ["Person 6"] },
    ])
  })
  it("no lower bound", async () => {
    expect(await btree.getRange({ lte: 3 })).toEqual([
      { key: 0, vals: ["Person 0"] },
      { key: 1, vals: ["Person 1"] },
      { key: 2, vals: ["Person 2"] },
      { key: 3, vals: ["Person 3"] },
    ])
  })
  it("no upper bound", async () => {
    expect(await btree.getRange({ gte: 7 })).toEqual([
      { key: 7, vals: ["Person 7"] },
      { key: 8, vals: ["Person 8"] },
      { key: 9, vals: ["Person 9"] },
    ])
  })
  it("no bounds", async () => {
    expect(await btree.getRange({})).toEqual([
      { key: 0, vals: ["Person 0"] },
      { key: 1, vals: ["Person 1"] },
      { key: 2, vals: ["Person 2"] },
      { key: 3, vals: ["Person 3"] },
      { key: 4, vals: ["Person 4"] },
      { key: 5, vals: ["Person 5"] },
      { key: 6, vals: ["Person 6"] },
      { key: 7, vals: ["Person 7"] },
      { key: 8, vals: ["Person 8"] },
      { key: 9, vals: ["Person 9"] },
    ])
  })
})

describe("Bulk tests", () => {
  it("lots of nodes inserted in ascending order", async () => {
    const btree = await BTree.inMemory<number, string>({
      order: 3,
      compare: (a, b) => a - b,
    })

    for (let i = 0; i < 40; i++) {
      await btree.insert(i, `Person ${i}`)
      for (let j = 0; j <= i; j++) {
        expect({ i, j, values: await btree.get(j) }).toEqual({
          i,
          j,
          values: [`Person ${j}`],
        })
      }
      await assertWellFormedBtree(btree)
    }
  })

  it("lots of nodes inserted in descending order", async () => {
    const btree = await BTree.inMemory<number, string>({
      compare: (a, b) => a - b,
      order: 3,
    })

    const inserted: number[] = []
    for (let i = 40; i >= 0; i--) {
      inserted.push(i)
      await btree.insert(i, `Person ${i}`)
      for (const j of inserted) {
        expect({ i, j, values: await btree.get(j) }).toEqual({
          i,
          j,
          values: [`Person ${j}`],
        })
      }
      await assertWellFormedBtree(btree)
    }
  })

  it("lots of nodes inserted in random order", async () => {
    const btree = await BTree.inMemory<number, string>({
      compare: (a, b) => a - b,
      order: 3,
    })

    const prng = randomSeeded(0n)

    const inserted: number[] = []
    for (let i = 0; i < 40; i++) {
      let index: number
      do {
        index = randomIntegerBetween(1, 10000, { prng })
      } while (inserted.includes(index))
      inserted.push(index)
      await btree.insert(index, `Person ${index}`)
      for (const j of inserted) {
        expect({ i, j, values: await btree.get(j) }).toEqual({
          i,
          j,
          values: [`Person ${j}`],
        })
      }
      await assertWellFormedBtree(btree)
    }
  })
})
