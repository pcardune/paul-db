import { binarySearch } from "../binarySearch.ts"
import { Comparator, EqualityChecker } from "../types.ts"

export interface INodeId {
  equals(other: INodeId): boolean
  serialize(): string
  toString(): string
}

class INode<NodeId extends INodeId> {
  constructor(
    protected readonly markDirty: () => void,
    readonly nodeId: NodeId,
  ) {}
}

export type SerializedLeafNode<K, V> = readonly [
  "leaf",
  readonly { key: K; vals: readonly V[] }[],
  ReturnType<INodeId["serialize"]> | null,
  ReturnType<INodeId["serialize"]> | null,
]
type SerializedInternalNode<K> = readonly [
  "internal",
  readonly K[],
  readonly ReturnType<INodeId["serialize"]>[],
]
export type SerializedNode<K, V> =
  | SerializedLeafNode<K, V>
  | SerializedInternalNode<K>

export class InternalBTreeNode<K, NodeId extends INodeId>
  extends INode<NodeId> {
  readonly type: "internal"
  private _keys: K[]
  private _childrenNodeIds: NodeId[]

  get keys(): K[] {
    return this._keys
  }

  get childrenNodeIds(): NodeId[] {
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

  /**
   * Replaces the given child node id with a new one.
   */
  swapChildNodeId(oldNodeId: NodeId, newNodeId: NodeId) {
    const index = this._childrenNodeIds.findIndex((id) => id.equals(oldNodeId))
    if (index < 0) {
      throw new Error(`Child node with id ${oldNodeId.serialize()} not found`)
    }
    this._childrenNodeIds[index] = newNodeId
    this.markDirty()
  }

  serialize(): SerializedInternalNode<K> {
    return [
      "internal",
      this._keys,
      this._childrenNodeIds.map((c) => c.serialize()),
    ]
  }

  override toString() {
    return `InternalBTreeNode(${JSON.stringify(this.nodeId.serialize())}, ${
      JSON.stringify({
        keys: this._keys,
        childrenNodeIds: this._childrenNodeIds.map((c) => c.serialize()),
      })
    })`
  }

  /**
   * Returns data for a new node that is the result of inserting new child node
   */
  withInsertedNode(key: K, nodeId: NodeId, compare: Comparator<K>) {
    const i = binarySearch(this.keys, key, compare)
    const keys = this._keys.slice()
    keys.splice(i, 0, key)
    const childrenNodeIds = this._childrenNodeIds.slice()
    childrenNodeIds.splice(i + 1, 0, nodeId)
    return { keys, childrenNodeIds }
  }
}

export type KeyVals<K, V> = { key: K; vals: V[] }

export class LeafBTreeNode<K, V, NodeId extends INodeId> extends INode<NodeId> {
  readonly type: "leaf"

  private _keyvals: { key: K; vals: V[] }[]
  private _nextLeafNodeId: NodeId | null
  private _prevLeafNodeId: NodeId | null

  get keyvals(): KeyVals<K, V>[] {
    return this._keyvals
  }

  get nextLeafNodeId(): NodeId | null {
    return this._nextLeafNodeId
  }

  set nextLeafNodeId(nodeId: NodeId | null) {
    this._nextLeafNodeId = nodeId
    this.markDirty()
  }

  get prevLeafNodeId(): NodeId | null {
    return this._prevLeafNodeId
  }

  set prevLeafNodeId(nodeId: NodeId | null) {
    this._prevLeafNodeId = nodeId
    this.markDirty()
  }

  serialize(): SerializedLeafNode<K, V> {
    return [
      "leaf",
      this._keyvals,
      this._nextLeafNodeId?.serialize() ?? null,
      this._prevLeafNodeId?.serialize() ?? null,
    ]
  }
  override toString() {
    return `LeafBTreeNode(${JSON.stringify(this.nodeId.serialize())}, ${
      JSON.stringify({
        keys: this._keyvals,
        nextLeafNodeId: this._nextLeafNodeId?.serialize(),
        prevLeafNodeId: this._prevLeafNodeId?.serialize(),
      })
    })`
  }

  constructor(
    markDirty: () => void,
    nodeId: NodeId,
    { keyvals, nextLeafNodeId, prevLeafNodeId }: {
      keyvals: { key: K; vals: V[] }[]
      nextLeafNodeId: NodeId | null
      prevLeafNodeId: NodeId | null
    },
  ) {
    super(markDirty, nodeId)
    this.type = "leaf"
    this._keyvals = keyvals
    this._nextLeafNodeId = nextLeafNodeId
    this._prevLeafNodeId = prevLeafNodeId
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

export type BTreeNode<K, V, NodeId extends INodeId> =
  | InternalBTreeNode<K, NodeId>
  | LeafBTreeNode<K, V, NodeId>
