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

export function keysForNode<K>(node: BTreeNode<K, unknown>): readonly K[] {
  if (node.type === "leaf") {
    return node.keyvals.map((keyval) => keyval.key)
  }
  return node.keys
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
      let keyIndex
      for (keyIndex = 0; keyIndex < node.keyvals.length; keyIndex++) {
        const cmp = this.compare(key, node.keyvals[keyIndex].key)
        if (cmp === 0) {
          break
        }
      }
      const values: V[] | undefined = keyIndex < node.keyvals.length
        ? node.keyvals[keyIndex].vals
        : undefined
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
    let i
    for (
      i = 0;
      i < node.keys.length && this.compare(key, node.keys[i]) >= 0;
      i++
    ) {
      // keep counting
    }
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
    node: BTreeNode<K, V>,
    key: K,
    depth: number,
  ) {
    if (node.parentNodeId == null) {
      throw new Error("not implemented")
      // this is the root node! so we need to create a new root node
      // const newRoot: InternalBTreeNode<K> = {
      //   nodeId: this.nodes.length,
      //   type: "internal",
      //   keys: [key],
      //   childrenNodeIds: [nodeId, newChildNodeId],
      // }
      // this.rootNodeId = newRoot.nodeId
      // this.nodes.push(newRoot)
      // node.parentNodeId = newRoot.nodeId
      // this.nodes[newChildNodeId].parentNodeId = newRoot.nodeId
      // return
    }
    const parent = this.nodes[node.parentNodeId]
    if (parent.type === "leaf") {
      throw new Error("Parent should not be a leaf")
    }
    let index: number
    for (index = 0; index < parent.keys.length; index++) {
      if (this.compare(key, parent.keys[index]) < 0) {
        break
      }
    }
    parent.keys.splice(index, 0, key)
    parent.childrenNodeIds.splice(index + 1, 0, node.nodeId)
    if (parent.keys.length <= this.maxKeys) {
      return
    }
    this.splitNode(parent.nodeId, depth + 1)
  }

  private splitNode(nodeId: number, depth = 1) {
    this.shouldLog && console.log(">".repeat(depth), "Splitting node", nodeId)
    this.shouldLog && console.dir(this.dump(), { depth: 100 })
    const node = this.nodes[nodeId]
    if (node.type === "leaf") {
      const L1: LeafBTreeNode<K, V> = {
        nodeId: this.nodes.length,
        parentNodeId: node.parentNodeId,
        type: "leaf",
        keyvals: node.keyvals.slice(this.order),
        nextLeafNodeId: node.nextLeafNodeId,
      }
      node.nextLeafNodeId = L1.nodeId
      this.nodes.push(L1)
      const L2: LeafBTreeNode<K, V> = {
        nodeId: node.nodeId,
        parentNodeId: node.parentNodeId,
        type: "leaf",
        keyvals: node.keyvals.slice(0, this.order),
        nextLeafNodeId: L1.nodeId,
      }
      this.nodes[L2.nodeId] = L2
      this.insertIntoParent(
        L1,
        L1.keyvals[0].key,
        depth,
      )
    } else {
      const mid = Math.floor(node.keys.length / 2)
      const newKeys = node.keys.splice(mid)
      const newChildrenNodeIds = node.childrenNodeIds.splice(mid + 1)
      const newInternalNode: InternalBTreeNode<K> = {
        nodeId: this.nodes.length,
        type: "internal",
        keys: newKeys.slice(1),
        childrenNodeIds: newChildrenNodeIds,
        parentNodeId: node.parentNodeId,
      }
      this.nodes.push(newInternalNode)
      this.insertIntoParent(newInternalNode, newKeys[0], depth)
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
