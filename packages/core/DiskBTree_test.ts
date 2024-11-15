import { expect } from "jsr:@std/expect"
import { beforeAll, beforeEach, describe, it } from "jsr:@std/testing/bdd"
import { BTree, BTreeNode, keysForNode } from "./DiskBTree.ts"
import { randomIntegerBetween, randomSeeded } from "@std/random"
import { InternalBTreeNode } from "./DiskBTree.ts"
import { LeafBTreeNode } from "./DiskBTree.ts"

// TODO: consider using expect.extend to make custom matchers for this
// see https://jsr.io/@std/expect/doc/~/expect.extend
function assertWellFormedBtree<K, V>(btree: BTree<K, V>) {
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
  expect(rootNode.parentNodeId, "root node should have no parent node")
    .toBeUndefined()
  if (rootNode.type !== "internal") {
    throw new Error("Unreachable")
  }
  assertWellFormedNode(btree, rootNode)

  function collectLeafNodesWithDepth(
    node: BTreeNode<K, V>,
    depth: number,
  ): { node: LeafBTreeNode<K, V>; depth: number }[] {
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

function assertWellFormedNode<K, V>(btree: BTree<K, V>, node: BTreeNode<K, V>) {
  expect(btree.getNodeWithId(node.nodeId), "Node should have the correct id")
    .toBe(node)
  if (node.type === "internal") {
    assertWellFormedInternalNode(btree, node)
  } else {
    assertWellFormedLeafNode(btree, node)
  }
}

function assertWellFormedLeafNode<K, V>(
  btree: BTree<K, V>,
  node: LeafBTreeNode<K, V>,
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

function assertWellFormedInternalNode<K, V>(
  btree: BTree<K, V>,
  node: InternalBTreeNode<K>,
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

  if (node.parentNodeId == null) {
    expect(node, "A node without a parent node should be the root node").toBe(
      btree.rootNode,
    )
  } else {
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
    expect(childNode.parentNodeId, "Child nodes should have a parent node")
      .toBe(
        node.nodeId,
      )
    assertWellFormedNode(btree, childNode)
  }
}

it("Starts out empty", () => {
  const btree = new BTree<number, { name: string }>((a, b) => a - b)
  assertWellFormedBtree(btree)
  btree.insert(1, { name: "Paul" })
  assertWellFormedBtree(btree)
})

it("Does basic operations", () => {
  const btree = new BTree<number, { name: string }>((a, b) => a - b)
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
  const btree = new BTree<number, string>((a, b) => a - b)
  btree.insert(1, "Paul")
  btree.insert(1, "Meghan")
  expect(btree.get(1)).toEqual(["Paul", "Meghan"])
  assertWellFormedBtree(btree)
})

describe("Inserting nodes", () => {
  const order = 1
  let btree: BTree<number, string>
  beforeEach(() => {
    btree = new BTree<number, string>((a, b) => a - b, { order })
  })
  it("starts with only two nodes in the tree", () => {
    expect(btree.nodes).toHaveLength(2)
    expect(btree.rootNode.type).toBe("internal")
    const children = btree.childrenForNode(btree.rootNode)
    expect(children).toHaveLength(1)
    expect(children[0].type).toBe("leaf")
    assertWellFormedBtree(btree)
  })
  describe("After inserting up to order*2 entries", () => {
    beforeEach(() => {
      for (let i = 0; i < order * 2; i++) {
        btree.insert(i, `Person ${i}`)
      }
    })
    it("No new nodes will be added", () => {
      assertWellFormedBtree(btree)
      expect(btree.nodes.length).toBe(2)
    })
    it("All nodes will have the correct values", () => {
      for (let i = 0; i < order * 2; i++) {
        expect(btree.get(i)).toEqual([`Person ${i}`])
      }
    })

    describe("After inserting one more entry", () => {
      beforeEach(() => {
        console.dir(btree.dump(), { depth: 100 })
        btree.insert(order * 2, `Person ${order * 2}`)
        console.dir(btree.dump(), { depth: 100 })
      })
      it("A new node will be added", () => {
        assertWellFormedBtree(btree)
        expect(btree.nodes.length).toBe(3)
        expect(btree.childrenForNode(btree.rootNode)).toHaveLength(2)
      })
      it("All entries will have the correct values", () => {
        for (let i = 0; i < order * 2 + 1; i++) {
          expect(btree.get(i)).toEqual([`Person ${i}`])
        }
      })

      describe("After inserting enough entries to split things more", () => {
        beforeEach(() => {
          for (let i = order * 2 + 1; i < order * 4; i++) {
            btree.insert(i, `Person ${i}`)
          }
        })
      })
    })
  })
})

describe.skip("Bulk tests", () => {
  it("lots of nodes inserted in ascending order", () => {
    const btree = new BTree<number, string>((a, b) => a - b, { order: 2 })

    for (let i = 0; i < 40; i++) {
      btree.insert(i, `Person ${i}`)
      for (let j = 0; j <= i; j++) {
        expect({ i, j, values: btree.get(j) }).toEqual({
          i,
          j,
          values: [`Person ${j}`],
        })
      }
    }
    assertWellFormedBtree(btree)
  })

  it("lots of nodes inserted in descending order", () => {
    const btree = new BTree<number, string>((a, b) => a - b, {
      order: 2,
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
    }
  })

  it.skip("lots of nodes inserted in random order", () => {
    const btree = new BTree<number, string>((a, b) => a - b, {
      order: 2,
      // shouldLog: true,
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
        if (btree.get(j).length === 0) {
          console.dir(btree.dump(), { depth: 100 })
        }
        expect({ i, j, values: btree.get(j) }).toEqual({
          i,
          j,
          values: [`Person ${j}`],
        })
      }
    }
  })
})
