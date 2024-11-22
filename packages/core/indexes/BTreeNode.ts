import { binarySearch } from "../binarySearch.ts"
import { Comparator, EqualityChecker } from "../types.ts"

class INode<NodeId> {
  constructor(
    protected readonly markDirty: () => void,
    readonly nodeId: NodeId,
  ) {}
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
export type SerializedNode<K, V, NodeId> =
  | SerializedLeafNode<K, V, NodeId>
  | SerializedInternalNode<K, NodeId>

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
