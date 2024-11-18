import { binarySearch } from "./binarySearch.ts"
import { Comparator, EqualityChecker } from "./types.ts"

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

export interface INodeList<K, V, NodeId> {
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

export class InMemoryNodeList<K, V> implements INodeList<K, V, number> {
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
      this._nodeCache.set(nodeId, node)
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
    this._nodeCache.set(nodeId, node)
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
