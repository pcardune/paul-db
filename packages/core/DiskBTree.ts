type InternalBTreeNote<K> = {
  type: "internal"
  nodeId: number
  parentNodeId?: number
  keys: K[]
  childrenNodeIds: number[]
}

type LeafBTreeNode<K, V> = {
  nodeId: number
  parentNodeId: number
  type: "leaf"
  keyvals: { key: K; vals: V[] }[]
  nextLeafNodeId: number
}

type DumpedNode<K, V> =
  | { type: "leaf"; nodeId: number; keyvals: [K, V[]][] }
  | {
    type: "internal"
    nodeId: number
    keys: K[]
    children: DumpedNode<K, V>[]
  }

export class BTree<K, V> {
  private nodes: Array<InternalBTreeNote<K> | LeafBTreeNode<K, V>>
  private maxKeys: number
  private rootNodeId: number

  constructor(private compare: (a: K, b: K) => number, { maxKeys = 4 } = {}) {
    this.maxKeys = maxKeys
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
    keyIndex: number
    values?: V[]
  } {
    console.log(">".repeat(depth), "_get(", nodeId, key, ")")
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
      console.log(
        ">".repeat(depth),
        "Found",
        values,
        "in",
        this.dumpNode(nodeId),
      )
      return { nodeId, node, key, values, keyIndex }
    }
    if (node.keys.length === 0) {
      console.log(">".repeat(depth), "No Keys")
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
    console.log("\n===== insert(", key, JSON.stringify(value), ")")
    const found = this._get(this.rootNodeId, key)
    if (found.values !== undefined) {
      found.values.push(value)
      return
    }
    const { node, keyIndex } = found
    node.keyvals.splice(keyIndex, 0, { key, vals: [value] })
    if (node.keyvals.length <= this.maxKeys) {
      return
    }
    this.splitNode(found.nodeId)
  }

  private insertIntoParent(
    nodeId: number,
    key: K,
    newChildNodeId: number,
    depth: number,
  ) {
    const node = this.nodes[nodeId]
    if (node.parentNodeId == null) {
      // this is the root node! so we need to create a new root node
      const newRoot: InternalBTreeNote<K> = {
        nodeId: this.nodes.length,
        type: "internal",
        keys: [key],
        childrenNodeIds: [nodeId, newChildNodeId],
      }
      this.rootNodeId = newRoot.nodeId
      this.nodes.push(newRoot)
      node.parentNodeId = newRoot.nodeId
      this.nodes[newChildNodeId].parentNodeId = newRoot.nodeId
      return
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
    parent.childrenNodeIds.splice(index + 1, 0, newChildNodeId)
    if (parent.keys.length <= this.maxKeys) {
      return
    }
    this.splitNode(parent.nodeId, depth + 1)
  }

  private splitNode(nodeId: number, depth = 1) {
    console.log(">".repeat(depth), "Splitting node", nodeId)
    console.dir(this.dump(), { depth: 100 })
    const node = this.nodes[nodeId]
    if (node.type === "leaf") {
      const mid = Math.floor(node.keyvals.length / 2)
      const newKeys = node.keyvals.splice(mid)
      const newLeafNode: LeafBTreeNode<K, V> = {
        nodeId: this.nodes.length,
        parentNodeId: node.parentNodeId,
        type: "leaf",
        keyvals: newKeys,
        nextLeafNodeId: node.nextLeafNodeId,
      }
      node.nextLeafNodeId = newLeafNode.nodeId
      this.nodes.push(newLeafNode)
      this.insertIntoParent(
        node.parentNodeId,
        newKeys[0].key,
        newLeafNode.nodeId,
        depth,
      )
    } else {
      const mid = Math.floor(node.keys.length / 2)
      const newKeys = node.keys.splice(mid)
      const newChildrenNodeIds = node.childrenNodeIds.splice(mid + 1)
      const newInternalNode: InternalBTreeNote<K> = {
        nodeId: this.nodes.length,
        type: "internal",
        keys: newKeys.slice(1),
        childrenNodeIds: newChildrenNodeIds,
        parentNodeId: node.parentNodeId,
      }
      this.nodes.push(newInternalNode)
      this.insertIntoParent(nodeId, newKeys[0], newInternalNode.nodeId, depth)
    }
  }

  removeAll(key: K) {
    const found = this._get(this.rootNodeId, key)
    if (found.values === undefined) {
      return
    }
    const { node, keyIndex } = found
    node.keyvals.splice(keyIndex, 1)
  }

  has(key: K): boolean {
    const { values } = this._get(this.rootNodeId, key)
    return values !== undefined && values.length > 0
  }

  get(key: K): V[] {
    console.log("get(", key, ")")
    return this._get(this.rootNodeId, key).values ?? []
  }
}
