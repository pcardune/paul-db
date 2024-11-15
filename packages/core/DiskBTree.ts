import { binarySearch } from "./binarySearch.ts"

export type InternalBTreeNode<K> = {
  type: "internal"
  nodeId: number
  parentNodeId?: number
  keys: K[]
  childrenNodeIds: number[]
}

export type LeafBTreeNode<K, V> = {
  nodeId: number
  parentNodeId: number
  type: "leaf"
  keyvals: { key: K; vals: V[] }[]
  nextLeafNodeId: number
}

export type BTreeNode<K, V> = InternalBTreeNode<K> | LeafBTreeNode<K, V>

type DumpedNode<K, V> =
  | { type: "leaf"; nodeId: number; keyvals: [K, V[]][] }
  | {
    type: "internal"
    nodeId: number
    keys: K[]
    children: DumpedNode<K, V>[]
  }

function indexOfInSortedArray<T, V>(
  arr: T[],
  value: V,
  compare: (a: V, b: T) => number,
): number | null {
  let i
  for (i = 0; i < arr.length && compare(value, arr[i]) >= 0; i++) {
    if (compare(value, arr[i]) === 0) {
      return i
    }
  }
  return null
}

export function findIndexInSortedArray<T>(
  arr: T[],
  value: T,
  compare: (a: T, b: T) => number,
): number {
  let i
  for (i = 0; i < arr.length && compare(value, arr[i]) >= 0; i++) {
    // keep counting
  }
  return i
}

/**
 * A BTree implementation that stores key-value pairs.
 *
 * See https://cs186berkeley.net/notes/note4/ for more information
 * about how a BTree works.
 */
export class BTree<K, V> {
  public nodes: BTreeNode<K, V>[]
  private maxKeys: number
  private rootNodeId: number
  private shouldLog: boolean
  public readonly order: number

  get rootNode(): BTreeNode<K, V> {
    return this.nodes[this.rootNodeId]
  }

  getNodeWithId(nodeId: number): BTreeNode<K, V> {
    return this.nodes[nodeId]
  }

  constructor(
    public readonly compare: (a: K, b: K) => number,
    { shouldLog = false, order = 2 } = {},
  ) {
    this.shouldLog = shouldLog
    this.order = order
    this.maxKeys = order * 2
    this.rootNodeId = 0
    this.nodes = [
      {
        nodeId: 0,
        type: "internal",
        keys: [],
        childrenNodeIds: [1],
      },
      {
        nodeId: 1,
        parentNodeId: 0,
        type: "leaf",
        keyvals: [],
        nextLeafNodeId: -1,
      },
    ]
  }

  childrenForNode(node: BTreeNode<K, V>): BTreeNode<K, V>[] {
    if (node.type === "leaf") {
      return []
    }
    return node.childrenNodeIds.map((id) => this.nodes[id])
  }

  dumpNode(nodeId: number): DumpedNode<K, V> {
    const node = this.nodes[nodeId]
    if (node.type === "leaf") {
      return {
        type: "leaf",
        nodeId: node.nodeId,
        keyvals: node.keyvals.map((key) => [key.key, key.vals]),
      }
    }
    return {
      type: "internal",
      nodeId: node.nodeId,
      keys: node.keys,
      children: node.childrenNodeIds.map((id) => {
        return this.dumpNode(id)
      }),
    }
  }

  dump() {
    return this.dumpNode(this.rootNodeId)
  }

  _get(
    nodeId: number,
    key: K,
    depth = 1,
  ): {
    nodeId: number
    node: LeafBTreeNode<K, V>
    key: K
    keyval: { key: K; vals: V[] } | null
    keyIndex: number
  } {
    this.shouldLog && console.log(">".repeat(depth), "_get(", nodeId, key, ")")
    const node = this.nodes[nodeId]
    if (node.type === "leaf") {
      const keyIndex = indexOfInSortedArray(
        node.keyvals,
        key,
        (a, b) => this.compare(a, b.key),
      )
      if (keyIndex == null) {
        return {
          nodeId,
          node,
          key,
          keyIndex: -1,
          keyval: null,
        }
      }
      const values = node.keyvals[keyIndex].vals
      this.shouldLog && console.log(
        ">".repeat(depth),
        "Found",
        values,
        "in",
        this.dumpNode(nodeId),
      )
      return {
        nodeId,
        node,
        key,
        keyIndex,
        keyval: keyIndex < node.keyvals.length ? node.keyvals[keyIndex] : null,
      }
    }
    if (node.keys.length === 0) {
      this.shouldLog && console.log(">".repeat(depth), "No Keys")
      return this._get(node.childrenNodeIds[0], key, depth + 1)
    }
    const i = findIndexInSortedArray(node.keys, key, this.compare)
    return this._get(node.childrenNodeIds[i], key, depth + 1)
  }

  insert(key: K, value: V) {
    this.shouldLog &&
      console.log("\n===== insert(", key, JSON.stringify(value), ")")
    const found = this._get(this.rootNodeId, key)
    if (found.keyval != null) {
      found.keyval.vals.push(value)
      return
    }
    const { node } = found

    node.keyvals.push({ key, vals: [value] })
    node.keyvals.sort((a, b) => this.compare(a.key, b.key))
    if (node.keyvals.length <= this.maxKeys) {
      return
    }
    this.splitNode(found.nodeId)
  }

  private insertIntoParent(
    parent: InternalBTreeNode<K>,
    node: BTreeNode<K, V>,
    key: K,
    depth: number,
  ) {
    const index = binarySearch(
      parent.keys,
      key,
      this.compare,
    )
    parent.keys.splice(index, 0, key)
    parent.childrenNodeIds.splice(index + 1, 0, node.nodeId)
    if (parent.keys.length <= this.maxKeys) {
      return
    }
    this.splitNode(parent.nodeId, depth + 1)
  }

  private splitLeafNode(node: LeafBTreeNode<K, V>, depth: number) {
    const L2: LeafBTreeNode<K, V> = {
      nodeId: this.nodes.length,
      parentNodeId: node.parentNodeId,
      type: "leaf",
      keyvals: node.keyvals.slice(this.order),
      nextLeafNodeId: node.nextLeafNodeId,
    }
    node.nextLeafNodeId = L2.nodeId
    this.nodes.push(L2)
    const L1: LeafBTreeNode<K, V> = {
      nodeId: node.nodeId,
      parentNodeId: node.parentNodeId,
      type: "leaf",
      keyvals: node.keyvals.slice(0, this.order),
      nextLeafNodeId: L2.nodeId,
    }
    this.nodes[L1.nodeId] = L1
    this.insertIntoParent(
      this.nodes[node.parentNodeId] as InternalBTreeNode<K>,
      L2,
      L2.keyvals[0].key,
      depth,
    )
  }

  private splitInternalNode(node: InternalBTreeNode<K>, depth: number) {
    let parentNode: InternalBTreeNode<K>
    if (node.parentNodeId == null) {
      // make a new parent node
      parentNode = {
        nodeId: this.nodes.length,
        type: "internal",
        keys: [],
        childrenNodeIds: [node.nodeId],
      }
      this.nodes.push(parentNode)
      this.rootNodeId = parentNode.nodeId
      node.parentNodeId = parentNode.nodeId
    } else {
      parentNode = this.nodes[node.parentNodeId] as InternalBTreeNode<K>
    }

    const keyToMove = node.keys[this.order]
    const L2: InternalBTreeNode<K> = {
      nodeId: this.nodes.length,
      type: "internal",
      keys: node.keys.slice(this.order + 1),
      childrenNodeIds: node.childrenNodeIds.slice(this.order + 1),
      parentNodeId: parentNode.nodeId,
    }
    this.nodes.push(L2)
    for (const childId of L2.childrenNodeIds) {
      this.nodes[childId].parentNodeId = L2.nodeId
    }
    const L1: InternalBTreeNode<K> = {
      nodeId: node.nodeId,
      type: "internal",
      keys: node.keys.slice(0, this.order),
      childrenNodeIds: node.childrenNodeIds.slice(0, this.order + 1),
      parentNodeId: parentNode.nodeId,
    }
    this.nodes[L1.nodeId] = L1
    const i = binarySearch(
      parentNode.keys,
      keyToMove,
      this.compare,
    )
    parentNode.keys.splice(i, 0, keyToMove)
    parentNode.childrenNodeIds.splice(i + 1, 0, L2.nodeId)
    if (parentNode.keys.length > this.maxKeys) {
      this.splitNode(parentNode.nodeId, depth + 1)
    }
  }

  private splitNode(nodeId: number, depth = 1) {
    this.shouldLog && console.log(">".repeat(depth), "Splitting node", nodeId)
    this.shouldLog && console.dir(this.dump(), { depth: 100 })
    const node = this.nodes[nodeId]
    if (node.type === "leaf") {
      this.splitLeafNode(node, depth)
    } else {
      this.splitInternalNode(node, depth)
    }
  }

  removeAll(key: K) {
    const found = this._get(this.rootNodeId, key)
    if (found.keyval === undefined) {
      return
    }
    const { node, keyIndex } = found
    node.keyvals.splice(keyIndex, 1)
  }

  has(key: K): boolean {
    const found = this._get(this.rootNodeId, key)
    return found.keyval !== null && found.keyval.vals.length > 0
  }

  get(key: K): V[] {
    this.shouldLog && console.log("get(", key, ")")
    return this._get(this.rootNodeId, key).keyval?.vals ?? []
  }
}
