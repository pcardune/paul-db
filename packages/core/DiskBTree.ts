import { binarySearch } from "./binarySearch.ts"

export type InternalBTreeNode<K, NodeId> = {
  type: "internal"
  nodeId: NodeId
  keys: K[]
  childrenNodeIds: NodeId[]
}

export type LeafBTreeNode<K, V, NodeId> = {
  nodeId: NodeId
  type: "leaf"
  keyvals: { key: K; vals: V[] }[]
  nextLeafNodeId: NodeId | null
}

export type BTreeNode<K, V, NodeId> =
  | InternalBTreeNode<K, NodeId>
  | LeafBTreeNode<K, V, NodeId>

type DumpedNode<K, V, NodeId> =
  | { type: "leaf"; nodeId: NodeId; keyvals: [K, V[]][] }
  | {
    type: "internal"
    nodeId: NodeId
    keys: K[]
    children: DumpedNode<K, V, NodeId>[]
  }

class LinkedList<V> {
  constructor(
    public head: V,
    public tail: LinkedList<V> | null = null,
  ) {}
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

interface INodeList<K, V, NodeId> {
  getNextNodeId(): NodeId
  size: number
  get(nodeId: NodeId): BTreeNode<K, V, NodeId>
  set(nodeId: NodeId, node: BTreeNode<K, V, NodeId>): void
}

class NodeList<K, V> implements INodeList<K, V, number> {
  constructor(private _nodes: BTreeNode<K, V, number>[]) {}

  get size() {
    return this._nodes.length
  }

  get nodes(): readonly BTreeNode<K, V, number>[] {
    return this._nodes
  }

  getNextNodeId(): number {
    return this._nodes.length
  }

  get(nodeId: number): BTreeNode<K, V, number> {
    return this._nodes[nodeId]
  }

  set(nodeId: number, node: BTreeNode<K, V, number>) {
    this._nodes[nodeId] = node
  }

  push(...node: BTreeNode<K, V, number>[]) {
    this._nodes.push(...node)
  }
}

/**
 * A BTree implementation that stores key-value pairs.
 *
 * See https://cs186berkeley.net/notes/note4/ for more information
 * about how a BTree works.
 */
export class BTree<
  K,
  V,
  NodeId,
  NodeListT extends INodeList<K, V, NodeId> = INodeList<K, V, NodeId>,
> {
  public nodes: NodeListT
  private rootNodeId: NodeId
  public readonly order: number

  get rootNode(): InternalBTreeNode<K, NodeId> {
    return this.nodes.get(this.rootNodeId) as InternalBTreeNode<K, NodeId>
  }

  static inMemory<K, V>(compare: (a: K, b: K) => number, { order = 2 } = {}) {
    return new BTree<K, V, number, NodeList<K, V>>(compare, {
      order,
      nodes: new NodeList<K, V>([]),
    })
  }

  getNodeWithId(nodeId: NodeId): BTreeNode<K, V, NodeId> {
    return this.nodes.get(nodeId)
  }

  constructor(
    public readonly compare: (a: K, b: K) => number,
    { order = 2, nodes }: {
      order?: number
      nodes: NodeListT
    },
  ) {
    this.nodes = nodes
    this.order = order
    this.rootNodeId = this.nodes.getNextNodeId()
    this.nodes.set(
      this.rootNodeId,
      {
        nodeId: this.rootNodeId,
        type: "internal",
        keys: [],
        childrenNodeIds: [],
      },
    )
    const childNodeId = this.nodes.getNextNodeId()
    this.nodes.set(childNodeId, {
      nodeId: childNodeId,
      type: "leaf",
      keyvals: [],
      nextLeafNodeId: null,
    })
    this.rootNode.childrenNodeIds.push(childNodeId)
  }

  childrenForNode(node: BTreeNode<K, V, NodeId>): BTreeNode<K, V, NodeId>[] {
    if (node.type === "leaf") {
      return []
    }
    return node.childrenNodeIds.map((id) => this.nodes.get(id))
  }

  dumpNode(nodeId: NodeId): DumpedNode<K, V, NodeId> {
    const node = this.nodes.get(nodeId)
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
    nodeId: NodeId,
    key: K,
    parents: LinkedList<NodeId> | null = null,
    depth = 1,
  ): {
    nodeId: NodeId
    node: LeafBTreeNode<K, V, NodeId>
    parents: LinkedList<NodeId>
    key: K
    keyval: { key: K; vals: V[] } | null
    keyIndex: number
  } {
    const node = this.nodes.get(nodeId)
    if (node.type === "leaf") {
      if (parents == null) {
        throw new Error("all leaf nodes should have a parent")
      }
      const keyIndex = indexOfInSortedArray(
        node.keyvals,
        key,
        (a, b) => this.compare(a, b.key),
      )
      if (keyIndex == null) {
        return {
          nodeId,
          node,
          parents,
          key,
          keyIndex: -1,
          keyval: null,
        }
      }
      return {
        nodeId,
        node,
        parents,
        key,
        keyIndex,
        keyval: keyIndex < node.keyvals.length ? node.keyvals[keyIndex] : null,
      }
    }
    if (node.keys.length === 0) {
      return this._get(
        node.childrenNodeIds[0],
        key,
        new LinkedList(node.nodeId, parents),
        depth + 1,
      )
    }
    const i = findIndexInSortedArray(node.keys, key, this.compare)
    return this._get(
      node.childrenNodeIds[i],
      key,
      new LinkedList(node.nodeId, parents),
      depth + 1,
    )
  }

  insert(key: K, value: V) {
    const found = this._get(this.rootNodeId, key)
    if (found.keyval != null) {
      found.keyval.vals.push(value)
      return
    }
    const { node } = found

    node.keyvals.push({ key, vals: [value] })
    node.keyvals.sort((a, b) => this.compare(a.key, b.key))
    if (node.keyvals.length <= this.order * 2) {
      return
    }
    this.splitNode(found.nodeId, found.parents)
  }

  private insertIntoParent(
    parent: InternalBTreeNode<K, NodeId>,
    grandParents: LinkedList<NodeId>,
    node: BTreeNode<K, V, NodeId>,
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
    if (parent.keys.length <= this.order * 2) {
      return
    }
    this.splitNode(parent.nodeId, grandParents, depth + 1)
  }

  private splitLeafNode(
    node: LeafBTreeNode<K, V, NodeId>,
    parents: LinkedList<NodeId>,
    depth: number,
  ) {
    const L2: LeafBTreeNode<K, V, NodeId> = {
      nodeId: this.nodes.getNextNodeId(),
      type: "leaf",
      keyvals: node.keyvals.slice(this.order),
      nextLeafNodeId: node.nextLeafNodeId,
    }
    node.nextLeafNodeId = L2.nodeId
    this.nodes.set(L2.nodeId, L2)
    const L1: LeafBTreeNode<K, V, NodeId> = {
      nodeId: node.nodeId,
      type: "leaf",
      keyvals: node.keyvals.slice(0, this.order),
      nextLeafNodeId: L2.nodeId,
    }
    this.nodes.set(L1.nodeId, L1)
    this.insertIntoParent(
      this.nodes.get(parents.head as NodeId) as InternalBTreeNode<K, NodeId>,
      parents.tail as LinkedList<NodeId>,
      L2,
      L2.keyvals[0].key,
      depth,
    )
  }

  private splitInternalNode(
    node: InternalBTreeNode<K, NodeId>,
    parents: LinkedList<NodeId> | null,
    depth: number,
  ) {
    let parentNode: InternalBTreeNode<K, NodeId>
    if (parents == null) {
      // make a new parent node
      parentNode = {
        nodeId: this.nodes.getNextNodeId(),
        type: "internal",
        keys: [],
        childrenNodeIds: [node.nodeId],
      }
      this.nodes.set(parentNode.nodeId, parentNode)
      this.rootNodeId = parentNode.nodeId
      parents = new LinkedList(parentNode.nodeId)
    } else {
      parentNode = this.nodes.get(parents.head) as InternalBTreeNode<
        K,
        NodeId
      >
    }

    const keyToMove = node.keys[this.order]
    const L2: InternalBTreeNode<K, NodeId> = {
      nodeId: this.nodes.getNextNodeId(),
      type: "internal",
      keys: node.keys.slice(this.order + 1),
      childrenNodeIds: node.childrenNodeIds.slice(this.order + 1),
    }
    this.nodes.set(L2.nodeId, L2)
    const L1: InternalBTreeNode<K, NodeId> = {
      nodeId: node.nodeId,
      type: "internal",
      keys: node.keys.slice(0, this.order),
      childrenNodeIds: node.childrenNodeIds.slice(0, this.order + 1),
    }
    this.nodes.set(L1.nodeId, L1)
    const i = binarySearch(
      parentNode.keys,
      keyToMove,
      this.compare,
    )
    parentNode.keys.splice(i, 0, keyToMove)
    parentNode.childrenNodeIds.splice(i + 1, 0, L2.nodeId)
    if (parentNode.keys.length > this.order * 2) {
      this.splitNode(parentNode.nodeId, parents.tail, depth + 1)
    }
  }

  private splitNode(
    nodeId: NodeId,
    parents: LinkedList<NodeId> | null,
    depth = 1,
  ) {
    const node = this.nodes.get(nodeId)
    if (node.type === "leaf") {
      if (parents == null) {
        throw new Error("all leaf nodes should have a parent")
      }
      this.splitLeafNode(node, parents, depth)
    } else {
      this.splitInternalNode(node, parents, depth)
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
    return this._get(this.rootNodeId, key).keyval?.vals ?? []
  }
}
