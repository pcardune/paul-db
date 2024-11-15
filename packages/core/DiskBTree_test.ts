import { expect } from "jsr:@std/expect"
import { beforeEach, describe, it } from "jsr:@std/testing/bdd"
import { BTree, BTreeNode } from "./DiskBTree.ts"
import { randomIntegerBetween, randomSeeded } from "@std/random"
import { InternalBTreeNode } from "./DiskBTree.ts"
import { LeafBTreeNode } from "./DiskBTree.ts"

// TODO: consider using expect.extend to make custom matchers for this
// see https://jsr.io/@std/expect/doc/~/expect.extend
function assertWellFormedBtree<K, V, NodeId>(btree: BTree<K, V, NodeId>) {
  /**
   * The number d is the order of a B+ tree. Each node (with the exception of
   * the root node) must have d ≤ x ≤ 2d entries assuming no deletes happen
   * (it’s possible for leaf nodes to end up with < d entries if you
   * delete data). The entries within each node must be sorted.
   */

  const rootNode = btree.rootNode
  expect(rootNode, "Expect btree to have a root node").toBeDefined()
  expect(rootNode.type, "root node should always be an internal node")
    .toBe("internal")
  if (rootNode.type !== "internal") {
    throw new Error("Unreachable")
  }
  assertWellFormedNode(btree, rootNode)

  function collectLeafNodesWithDepth(
    node: BTreeNode<K, V, NodeId>,
    depth: number,
  ): { node: LeafBTreeNode<K, V, NodeId>; depth: number }[] {
    if (node.type === "leaf") {
      return [{ node, depth }]
    }
    return btree.childrenForNode(node).flatMap((childNode) => {
      return collectLeafNodesWithDepth(childNode, depth + 1)
    })
  }
  const depths = new Set(
    collectLeafNodesWithDepth(rootNode, 0).map(({ depth }) => depth),
  )
  expect(depths.size, "All leaf nodes should be at the same depth").toBe(1)
}

function assertWellFormedNode<K, V, NodeId>(
  btree: BTree<K, V, NodeId>,
  node: BTreeNode<K, V, NodeId>,
) {
  expect(btree.getNodeWithId(node.nodeId), "Node should have the correct id")
    .toBe(node)
  if (node.type === "internal") {
    assertWellFormedInternalNode(btree, node)
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

function assertWellFormedInternalNode<K, V, NodeId>(
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
    const leftChild = btree.getNodeWithId(node.childrenNodeIds[i])
    const rightChild = btree.getNodeWithId(node.childrenNodeIds[i + 1])
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

  if (node != btree.rootNode) {
    expect(
      node.keys.length,
      "Internal nodes should have at least _order_ entries",
    ).toBeGreaterThanOrEqual(btree.order)
    expect(
      node.keys.length,
      "Internal nodes should have at most 2 * _order_ entries",
    ).toBeLessThanOrEqual(2 * btree.order)
  }

  for (const childNode of btree.childrenForNode(node)) {
    assertWellFormedNode(btree, childNode)
  }
}

it("Starts out empty", () => {
  const btree = BTree.inMemory<number, { name: string }>({
    compare: (a, b) => a - b,
  })
  assertWellFormedBtree(btree)
  btree.insert(1, { name: "Paul" })
  assertWellFormedBtree(btree)
})

it("Does basic operations", () => {
  const btree = BTree.inMemory<number, { name: string }>({
    compare: (a, b) => a - b,
  })
  expect(btree.has(1)).toBe(false)
  expect(btree.get(1)).toEqual([])

  btree.insert(1, { name: "Paul" })
  expect(btree.has(1)).toBe(true)
  expect(btree.get(1)).toEqual([{ name: "Paul" }])

  btree.insert(1, { name: "Meghan" })
  expect(btree.get(1)).toEqual([{ name: "Paul" }, { name: "Meghan" }])

  btree.insert(2, { name: "Mr. Blue" })
  expect(btree.get(2)).toEqual([{ name: "Mr. Blue" }])

  assertWellFormedBtree(btree)

  btree.removeAll(1)
  expect(btree.get(1)).toEqual([])
})

it("Inserting multiple values with the same key will return multiple values", () => {
  const btree = BTree.inMemory<number, string>({ compare: (a, b) => a - b })
  btree.insert(1, "Paul")
  btree.insert(1, "Meghan")
  expect(btree.get(1)).toEqual(["Paul", "Meghan"])
  assertWellFormedBtree(btree)
})

describe("Inserting nodes", () => {
  const order = 1
  let btree: ReturnType<typeof BTree.inMemory<number, string>>
  beforeEach(() => {
    btree = BTree.inMemory<number, string>({ order, compare: (a, b) => a - b })
  })
  it("starts with only two nodes in the tree", () => {
    expect(btree.nodes.size).toBe(2)
    expect(btree.rootNode.type).toBe("internal")
    const children = btree.childrenForNode(btree.rootNode)
    expect(children).toHaveLength(1)
    expect(children[0].type).toBe("leaf")
    assertWellFormedBtree(btree)
  })
  describe("After inserting up to order*2 entries", () => {
    beforeEach(() => {
      btree.insert(0, `Person 0`)
      btree.insert(1, `Person 1`)
    })
    it("No new nodes will be added", () => {
      assertWellFormedBtree(btree)
      expect(btree.nodes.size).toBe(2)
    })
    it("All nodes will have the correct values", () => {
      for (let i = 0; i < order * 2; i++) {
        expect(btree.get(i)).toEqual([`Person ${i}`])
      }
    })

    describe("After inserting one more entry", () => {
      beforeEach(() => {
        btree.insert(2, `Person 2`)
      })
      it("A new node will be added", () => {
        assertWellFormedBtree(btree)
        expect(btree.nodes.size).toBe(3)
        expect(btree.childrenForNode(btree.rootNode)).toHaveLength(2)
      })

      it("All entries will have the correct values", () => {
        for (let i = 0; i <= 2; i++) {
          expect(btree.get(i)).toEqual([`Person ${i}`])
        }
      })

      describe("After inserting enough entries to split things more", () => {
        let originalRootNodeId: number
        beforeEach(() => {
          originalRootNodeId = btree.rootNode.nodeId
          btree.insert(3, `Person 3`)
          btree.insert(4, `Person 4`)
        })
        it("A new node will be added", () => {
          assertWellFormedBtree(btree)
        })
        it("There will be a new root node", () => {
          expect(btree.rootNode.nodeId).not.toBe(originalRootNodeId)
        })
        it("All entries will have the correct values", () => {
          for (let i = 0; i <= 4; i++) {
            expect(btree.get(i)).toEqual([`Person ${i}`])
          }
        })

        describe("After inserting another 2 entries", () => {
          beforeEach(() => {
            btree.insert(5, `Person 5`)
            btree.insert(6, `Person 6`)
          })
          it("it will still be well formed", () => {
            assertWellFormedBtree(btree)
          })
        })
      })
    })
  })
})

describe("Removing items", () => {
  let btree: ReturnType<typeof BTree.inMemory<number, string>>
  beforeEach(() => {
    btree = BTree.inMemory<number, string>({
      order: 3,
      compare: (a, b) => a - b,
    })
    for (let i = 0; i < 10; i++) {
      btree.insert(i, `Person ${i}`)
    }
    btree.insert(5, "foo")
    btree.insert(5, "bar")
    btree.insert(5, "baz")
  })
  it("Can remove all items", () => {
    expect(btree.get(5)).toEqual(["Person 5", "foo", "bar", "baz"])
    btree.removeAll(5)
    expect(btree.get(5)).toEqual([])
    assertWellFormedBtree(btree)
  })
  it("Can remove an individual entry for a particular key", () => {
    expect(btree.get(5)).toEqual(["Person 5", "foo", "bar", "baz"])
    btree.remove(5, "bar")
    expect(btree.get(5)).toEqual(["Person 5", "foo", "baz"])
    assertWellFormedBtree(btree)
  })
})

describe("Range requests", () => {
  let btree: ReturnType<typeof BTree.inMemory<number, string>>
  beforeEach(() => {
    btree = BTree.inMemory<number, string>({
      order: 3,
      compare: (a, b) => a - b,
    })
    for (let i = 0; i < 10; i++) {
      btree.insert(i, `Person ${i}`)
    }
  })
  it("Can get all values in a range", () => {
    expect(btree.getRange({ gte: 3, lte: 7 })).toEqual([
      { key: 3, vals: ["Person 3"] },
      { key: 4, vals: ["Person 4"] },
      { key: 5, vals: ["Person 5"] },
      { key: 6, vals: ["Person 6"] },
      { key: 7, vals: ["Person 7"] },
    ])
  })
  it("exclusive lower bound", () => {
    expect(btree.getRange({ gt: 3, lte: 7 })).toEqual([
      { key: 4, vals: ["Person 4"] },
      { key: 5, vals: ["Person 5"] },
      { key: 6, vals: ["Person 6"] },
      { key: 7, vals: ["Person 7"] },
    ])
  })
  it("exclusive upper bound", () => {
    expect(btree.getRange({ gt: 3, lt: 7 })).toEqual([
      { key: 4, vals: ["Person 4"] },
      { key: 5, vals: ["Person 5"] },
      { key: 6, vals: ["Person 6"] },
    ])
  })
  it("no lower bound", () => {
    expect(btree.getRange({ lte: 3 })).toEqual([
      { key: 0, vals: ["Person 0"] },
      { key: 1, vals: ["Person 1"] },
      { key: 2, vals: ["Person 2"] },
      { key: 3, vals: ["Person 3"] },
    ])
  })
  it("no upper bound", () => {
    expect(btree.getRange({ gte: 7 })).toEqual([
      { key: 7, vals: ["Person 7"] },
      { key: 8, vals: ["Person 8"] },
      { key: 9, vals: ["Person 9"] },
    ])
  })
  it("no bounds", () => {
    expect(btree.getRange({})).toEqual([
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
  it("lots of nodes inserted in ascending order", () => {
    const btree = BTree.inMemory<number, string>({
      order: 3,
      compare: (a, b) => a - b,
    })

    for (let i = 0; i < 40; i++) {
      btree.insert(i, `Person ${i}`)
      for (let j = 0; j <= i; j++) {
        expect({ i, j, values: btree.get(j) }).toEqual({
          i,
          j,
          values: [`Person ${j}`],
        })
      }
      assertWellFormedBtree(btree)
    }
  })

  it("lots of nodes inserted in descending order", () => {
    const btree = BTree.inMemory<number, string>({
      compare: (a, b) => a - b,
      order: 3,
    })

    const inserted: number[] = []
    for (let i = 40; i >= 0; i--) {
      inserted.push(i)
      btree.insert(i, `Person ${i}`)
      for (const j of inserted) {
        expect({ i, j, values: btree.get(j) }).toEqual({
          i,
          j,
          values: [`Person ${j}`],
        })
      }
      assertWellFormedBtree(btree)
    }
  })

  it("lots of nodes inserted in random order", () => {
    const btree = BTree.inMemory<number, string>({
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
      btree.insert(index, `Person ${index}`)
      for (const j of inserted) {
        expect({ i, j, values: btree.get(j) }).toEqual({
          i,
          j,
          values: [`Person ${j}`],
        })
      }
      assertWellFormedBtree(btree)
    }
  })
})
