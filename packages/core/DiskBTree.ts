import { binarySearch } from "./binarySearch.ts"

class INode<NodeId> {
  constructor(
    protected readonly markDirty: () => void,
    readonly nodeId: NodeId,
  ) {}
}

export class InternalBTreeNode<K, NodeId> extends INode<NodeId> {
  readonly type: "internal"
  private _keys: K[]
  private _childrenNodeIds: NodeId[]

  get keys(): readonly K[] {
    return this._keys
  }

  get childrenNodeIds(): readonly NodeId[] {
    return this._childrenNodeIds
  }

  constructor(
    markDirty: () => void,
    nodeId: NodeId,
    { keys, childrenNodeIds }: {
      keys: K[]
      childrenNodeIds: NodeId[]
    },
  ) {
    super(markDirty, nodeId)
    this.type = "internal"
    this._keys = keys
    this._childrenNodeIds = childrenNodeIds
  }

  serialize(): SerializedInternalNode<K, NodeId> {
    return ["internal", this._keys, this._childrenNodeIds]
  }

  insertNode(key: K, nodeId: NodeId, compare: Comparator<K>) {
    const i = binarySearch(this.keys, key, compare)
    this._keys.splice(i, 0, key)
    this._childrenNodeIds.splice(i + 1, 0, nodeId)
    this.markDirty()
  }
}

export class LeafBTreeNode<K, V, NodeId> extends INode<NodeId> {
  readonly type: "leaf"

  private _keyvals: { key: K; vals: V[] }[]
  private _nextLeafNodeId: NodeId | null

  get keyvals(): readonly { key: K; vals: readonly V[] }[] {
    return this._keyvals
  }

  get nextLeafNodeId(): NodeId | null {
    return this._nextLeafNodeId
  }

  serialize(): SerializedLeafNode<K, V, NodeId> {
    return ["leaf", this._keyvals, this._nextLeafNodeId]
  }

  constructor(
    markDirty: () => void,
    nodeId: NodeId,
    { keyvals, nextLeafNodeId }: {
      keyvals: { key: K; vals: V[] }[]
      nextLeafNodeId: NodeId | null
    },
  ) {
    super(markDirty, nodeId)
    this.type = "leaf"
    this._keyvals = keyvals
    this._nextLeafNodeId = nextLeafNodeId
  }

  pushKey(key: K, vals: readonly V[], compare: Comparator<K>) {
    this._keyvals.push({ key, vals: vals.slice() })
    this._keyvals.sort((a, b) => compare(a.key, b.key))
    this.markDirty()
  }

  pushValue(keyIndex: number, value: V) {
    this._keyvals[keyIndex].vals.push(value)
    this.markDirty()
  }

  removeValue(keyIndex: number, value: V, isEqual: EqualityChecker<V>) {
    const keyval = this._keyvals[keyIndex]
    keyval.vals = keyval.vals.filter((v) => !isEqual(v, value))
    this.markDirty()
  }

  copyKeyvals(start: number, end?: number) {
    return this._keyvals.slice(start, end).map((kv) => ({
      ...kv,
      vals: kv.vals.slice(),
    }))
  }

  removeKey(index: number) {
    this._keyvals.splice(index, 1)
    this.markDirty()
  }
}

export type BTreeNode<K, V, NodeId> =
  | InternalBTreeNode<K, NodeId>
  | LeafBTreeNode<K, V, NodeId>

type DumpedNode<K, V, NodeId> =
  | { type: "leaf"; nodeId: NodeId; keyvals: [K, readonly V[]][] }
  | {
    type: "internal"
    nodeId: NodeId
    keys: readonly K[]
    children: DumpedNode<K, V, NodeId>[]
  }

class LinkedList<V> {
  constructor(
    public head: V,
    public tail: LinkedList<V> | null = null,
  ) {}
}

function indexOfInSortedArray<T, V>(
  arr: readonly T[],
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
  arr: readonly T[],
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
  markDirty(node: BTreeNode<K, V, NodeId>): void
  createLeafNode(
    nodeId: NodeId,
    state: { keyvals: { key: K; vals: V[] }[]; nextLeafNodeId: NodeId | null },
  ): LeafBTreeNode<K, V, NodeId>
  createInternalNode(
    nodeId: NodeId,
    state: { keys: K[]; childrenNodeIds: NodeId[] },
  ): InternalBTreeNode<K, NodeId>
  commit(): void
}

type SerializedLeafNode<K, V, NodeId> = readonly [
  "leaf",
  readonly { key: K; vals: readonly V[] }[],
  NodeId | null,
]
type SerializedInternalNode<K, NodeId> = readonly [
  "internal",
  readonly K[],
  readonly NodeId[],
]
type SerializedNode<K, V, NodeId> =
  | SerializedLeafNode<K, V, NodeId>
  | SerializedInternalNode<K, NodeId>

class InMemoryNodeList<K, V> implements INodeList<K, V, number> {
  private nextNodeId = 0
  constructor(private _nodes: SerializedNode<K, V, number>[]) {}

  get size() {
    return this._nodes.length
  }

  private dirtyNodes = new Map<number, BTreeNode<K, V, number>>()

  markDirty(node: BTreeNode<K, V, number>): void {
    this.dirtyNodes.set(node.nodeId, node)
  }

  getNextNodeId(): number {
    return this.nextNodeId++
  }

  private _nodeCache = new Map<number, BTreeNode<K, V, number>>()

  get(nodeId: number): BTreeNode<K, V, number> {
    const serialized = this._nodes[nodeId]
    const existingDirty = this.dirtyNodes.get(nodeId)
    if (existingDirty != null) {
      return existingDirty
    }
    const existing = this._nodeCache.get(nodeId) as BTreeNode<K, V, number>
    if (existing != null) {
      return existing
    }
    if (serialized[0] === "leaf") {
      const node = new LeafBTreeNode(
        () => {
          this.markDirty(node)
        },
        nodeId,
        {
          keyvals: [
            ...serialized[1].map((value) => ({
              ...value,
              vals: value.vals.slice(),
            })),
          ],
          nextLeafNodeId: serialized[2],
        },
      )
      // TODO this shouldn't be necessary
      // this._nodeCache.set(nodeId, node)
      return node
    }
    const node = new InternalBTreeNode(
      () => {
        this.markDirty(node)
      },
      nodeId,
      {
        keys: [...serialized[1]],
        childrenNodeIds: [...serialized[2]],
      },
    )
    // TODO this shouldn't be necessary
    // this._nodeCache.set(nodeId, node)
    return node
  }

  set(nodeId: number, node: BTreeNode<K, V, number>) {
    if (node.type === "leaf") {
      this._nodes[nodeId] = ["leaf", node.keyvals, node.nextLeafNodeId]
    } else {
      this._nodes[nodeId] = ["internal", node.keys, node.childrenNodeIds]
    }
    this.markDirty(node)
  }

  createLeafNode(
    nodeId: number,
    state: { keyvals: { key: K; vals: V[] }[]; nextLeafNodeId: number | null },
  ): LeafBTreeNode<K, V, number> {
    const node = new LeafBTreeNode(() => this.markDirty(node), nodeId, state)
    this._nodeCache.set(nodeId, node)
    this.markDirty(node)
    return node
  }

  createInternalNode(
    nodeId: number,
    state: { keys: K[]; childrenNodeIds: number[] },
  ): InternalBTreeNode<K, number> {
    const node = new InternalBTreeNode(
      () => this.markDirty(node),
      nodeId,
      state,
    )
    this._nodeCache.set(nodeId, node)
    this.markDirty(node)
    return node
  }

  commit(): void {
    for (const node of this.dirtyNodes.values()) {
      if (node.type === "leaf") {
        this._nodes[node.nodeId] = ["leaf", node.keyvals, node.nextLeafNodeId]
      } else {
        this._nodes[node.nodeId] = ["internal", node.keys, node.childrenNodeIds]
      }
    }
    this.dirtyNodes.clear()
    this._nodeCache.clear()
  }
}

export type Comparator<V> = (a: V, b: V) => number
export type EqualityChecker<V> = (a: V, b: V) => boolean
export type Range<K> = { gte?: K; gt?: K; lte?: K; lt?: K }
export type InMemoryBTreeConfig<K, V> = {
  order?: number
  compare?: Comparator<K>
  isEqual?: EqualityChecker<V>
}
export type InMemoryBTree<K, V> = ReturnType<typeof BTree.inMemory<K, V>>

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
  public readonly compare: (a: K, b: K) => number
  public readonly isEqual: (a: V, b: V) => boolean

  get rootNode(): InternalBTreeNode<K, NodeId> {
    return this.nodes.get(this.rootNodeId) as InternalBTreeNode<K, NodeId>
  }

  static inMemory<K, V>(
    {
      order = 2,
      compare = (a, b) => a < b ? -1 : a > b ? 1 : 0,
      isEqual = (a, b) => a === b,
    }: InMemoryBTreeConfig<K, V> = {},
  ) {
    return new BTree<K, V, number, InMemoryNodeList<K, V>>({
      order,
      compare,
      isEqual,
      nodes: new InMemoryNodeList<K, V>([]),
    })
  }

  getNodeWithId(nodeId: NodeId): BTreeNode<K, V, NodeId> {
    return this.nodes.get(nodeId)
  }

  constructor(
    {
      order = 2,
      compare,
      isEqual,
      nodes,
    }: {
      order?: number
      compare: (a: K, b: K) => number
      isEqual: (a: V, b: V) => boolean
      nodes: NodeListT
    },
  ) {
    this.compare = compare
    this.isEqual = isEqual
    this.nodes = nodes
    this.order = order
    this.rootNodeId = this.nodes.getNextNodeId()
    const childNodeId = this.nodes.getNextNodeId()
    this.nodes.createInternalNode(this.rootNodeId, {
      keys: [],
      childrenNodeIds: [childNodeId],
    })
    this.nodes.createLeafNode(childNodeId, {
      keyvals: [],
      nextLeafNodeId: null,
    })
    this.nodes.commit()
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
    keyval: Readonly<{ key: K; vals: readonly V[] }> | null
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
      found.node.pushValue(found.keyIndex, value)
      this.nodes.commit()
      return
    }
    const { node } = found

    node.pushKey(key, [value], this.compare)
    if (node.keyvals.length <= this.order * 2) {
      this.nodes.commit()
      return
    }
    this.splitNode(found.nodeId, found.parents)
    this.nodes.commit()
  }

  private insertIntoParent(
    parent: InternalBTreeNode<K, NodeId>,
    grandParents: LinkedList<NodeId>,
    node: BTreeNode<K, V, NodeId>,
    key: K,
    depth: number,
  ) {
    parent.insertNode(key, node.nodeId, this.compare)
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
    const L2 = this.nodes.createLeafNode(this.nodes.getNextNodeId(), {
      keyvals: node.copyKeyvals(this.order),
      nextLeafNodeId: node.nextLeafNodeId,
    })
    this.nodes.createLeafNode(node.nodeId, {
      keyvals: node.copyKeyvals(0, this.order),
      nextLeafNodeId: L2.nodeId,
    })
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
      parentNode = this.nodes.createInternalNode(
        this.nodes.getNextNodeId(),
        {
          keys: [],
          childrenNodeIds: [node.nodeId],
        },
      )
      this.rootNodeId = parentNode.nodeId
      parents = new LinkedList(parentNode.nodeId)
    } else {
      parentNode = this.nodes.get(parents.head) as InternalBTreeNode<
        K,
        NodeId
      >
    }

    const keyToMove = node.keys[this.order]
    const L2 = this.nodes.createInternalNode(
      this.nodes.getNextNodeId(),
      {
        keys: node.keys.slice(this.order + 1),
        childrenNodeIds: node.childrenNodeIds.slice(this.order + 1),
      },
    )
    this.nodes.createInternalNode(node.nodeId, {
      keys: node.keys.slice(0, this.order),
      childrenNodeIds: node.childrenNodeIds.slice(0, this.order + 1),
    })
    parentNode.insertNode(keyToMove, L2.nodeId, this.compare)
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
    if (found.keyval == null) {
      return
    }
    const { node, keyIndex } = found
    node.removeKey(keyIndex)
    this.nodes.commit()
  }

  remove(key: K, value: V) {
    const found = this._get(this.rootNodeId, key)
    if (found.keyval == null) {
      return
    }
    found.node.removeValue(found.keyIndex, value, this.isEqual)
    this.nodes.commit()
  }

  has(key: K): boolean {
    const found = this._get(this.rootNodeId, key)
    return found.keyval !== null && found.keyval.vals.length > 0
  }

  get(key: K): readonly V[] {
    return this._get(this.rootNodeId, key).keyval?.vals ?? []
  }

  private getMinNode(
    node: BTreeNode<K, V, NodeId> = this.rootNode,
  ): LeafBTreeNode<K, V, NodeId> {
    if (node.type === "leaf") {
      return node
    }
    return this.getMinNode(this.nodes.get(node.childrenNodeIds[0]))
  }

  getRange(
    { gt, gte, lte, lt }: Range<K>,
  ): { key: K; vals: readonly V[] }[] {
    const results: { key: K; vals: readonly V[] }[] = []

    let current: LeafBTreeNode<K, V, NodeId>
    let isGt: ((k: K) => boolean) | null = null
    let ltCheck: ((k: K) => boolean) | null = null
    if (gt !== undefined) {
      if (gte !== undefined) {
        throw new Error("Cannot have both gt and gte")
      }
      isGt = (k) => this.compare(k, gt) > 0
      current = this._get(this.rootNodeId, gt).node
    } else if (gte !== undefined) {
      isGt = (k) => this.compare(k, gte) >= 0
      current = this._get(this.rootNodeId, gte).node
    } else {
      current = this.getMinNode()
    }

    if (lt !== undefined) {
      if (lte !== undefined) {
        throw new Error("Cannot have both lt and lte")
      }
      ltCheck = (k) => this.compare(k, lt) >= 0
    } else if (lte !== undefined) {
      ltCheck = (k) => this.compare(k, lte) > 0
    }

    if (isGt != null) {
      for (const keyval of current.keyvals) {
        if (!isGt(keyval.key)) {
          continue
        }
        if (ltCheck != null && ltCheck(keyval.key)) {
          return results
        }
        results.push(keyval)
      }
      if (current.nextLeafNodeId == null) return results
      current = this.nodes.get(current.nextLeafNodeId) as LeafBTreeNode<
        K,
        V,
        NodeId
      >
    }

    if (ltCheck == null) {
      while (true) {
        results.push(...current.keyvals)
        if (current.nextLeafNodeId == null) break
        current = this.nodes.get(current.nextLeafNodeId) as LeafBTreeNode<
          K,
          V,
          NodeId
        >
      }
      return results
    }

    while (true) {
      for (const keyval of current.keyvals) {
        if (ltCheck != null && ltCheck(keyval.key)) {
          return results
        }
        results.push(keyval)
      }
      if (current.nextLeafNodeId == null) break
      current = this.nodes.get(current.nextLeafNodeId) as LeafBTreeNode<
        K,
        V,
        NodeId
      >
    }
    return results
  }
}
