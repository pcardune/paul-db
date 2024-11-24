import {
  BTreeNode,
  INodeId,
  InternalBTreeNode,
  LeafBTreeNode,
  SerializedNode,
} from "./BTreeNode.ts"

export interface INodeList<K, V, NodeId extends INodeId> {
  get(nodeId: NodeId): Promise<BTreeNode<K, V, NodeId>>
  markDirty(node: BTreeNode<K, V, NodeId>): void
  deleteNode(nodeId: NodeId): Promise<void>
  createLeafNode(
    state: {
      keyvals: { key: K; vals: V[] }[]
      nextLeafNodeId: NodeId | null
      prevLeafNodeId: NodeId | null
    },
  ): Promise<LeafBTreeNode<K, V, NodeId>>
  createInternalNode(
    state: { keys: K[]; childrenNodeIds: NodeId[] },
  ): Promise<InternalBTreeNode<K, NodeId>>
  commit(): Promise<void>
}

export class InMemoryNodeId implements INodeId {
  constructor(readonly index: number) {}

  equals(other: INodeId): boolean {
    return other instanceof InMemoryNodeId && other.index === this.index
  }

  serialize(): string {
    return this.index.toString()
  }

  toString(): string {
    return this.serialize()
  }
}
export class InMemoryNodeList<K, V> implements INodeList<K, V, InMemoryNodeId> {
  private nextNodeId = 0
  constructor(private _nodes: Map<number, SerializedNode<K, V>>) {}

  get size() {
    return this._nodes.size
  }

  private dirtyNodes = new Map<number, BTreeNode<K, V, InMemoryNodeId>>()

  markDirty(node: BTreeNode<K, V, InMemoryNodeId>): void {
    this.dirtyNodes.set(node.nodeId.index, node)
  }

  private getNextNodeId(): InMemoryNodeId {
    return new InMemoryNodeId(this.nextNodeId++)
  }

  private _nodeCache = new Map<number, BTreeNode<K, V, InMemoryNodeId>>()

  get(nodeId: InMemoryNodeId): Promise<BTreeNode<K, V, InMemoryNodeId>> {
    const existingDirty = this.dirtyNodes.get(nodeId.index)
    if (existingDirty != null) {
      return Promise.resolve(existingDirty)
    }
    const existing = this._nodeCache.get(nodeId.index) as BTreeNode<
      K,
      V,
      InMemoryNodeId
    >
    if (existing != null) {
      return Promise.resolve(existing)
    }
    const serialized = this._nodes.get(nodeId.index)
    if (serialized == null) {
      throw new Error(`Node ${nodeId.index} not found`)
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
          nextLeafNodeId: serialized[2] == null
            ? null
            : new InMemoryNodeId(parseInt(serialized[2])),
          prevLeafNodeId: serialized[3] == null
            ? null
            : new InMemoryNodeId(parseInt(serialized[3])),
        },
      )
      // TODO this shouldn't be necessary
      this._nodeCache.set(nodeId.index, node)
      return Promise.resolve(node)
    }
    const node = new InternalBTreeNode(
      () => {
        this.markDirty(node)
      },
      nodeId,
      {
        keys: [...serialized[1]],
        childrenNodeIds: [
          ...serialized[2].map((id) => new InMemoryNodeId(parseInt(id))),
        ],
      },
    )
    // TODO this shouldn't be necessary
    this._nodeCache.set(nodeId.index, node)
    return Promise.resolve(node)
  }

  createLeafNode(
    state: {
      keyvals: { key: K; vals: V[] }[]
      nextLeafNodeId: InMemoryNodeId | null
      prevLeafNodeId: InMemoryNodeId | null
    },
  ): Promise<LeafBTreeNode<K, V, InMemoryNodeId>> {
    const nodeId = this.getNextNodeId()
    const node = new LeafBTreeNode(() => this.markDirty(node), nodeId, state)
    this._nodeCache.set(nodeId.index, node)
    this.markDirty(node)
    return Promise.resolve(node)
  }

  createInternalNode(
    state: { keys: K[]; childrenNodeIds: InMemoryNodeId[] },
  ): Promise<InternalBTreeNode<K, InMemoryNodeId>> {
    const nodeId = this.getNextNodeId()
    const node = new InternalBTreeNode(
      () => this.markDirty(node),
      nodeId,
      state,
    )
    this._nodeCache.set(nodeId.index, node)
    this.markDirty(node)
    return Promise.resolve(node)
  }

  commit(): Promise<void> {
    for (const node of this.dirtyNodes.values()) {
      this._nodes.set(node.nodeId.index, node.serialize())
    }
    this.dirtyNodes.clear()
    this._nodeCache.clear()
    return Promise.resolve()
  }

  deleteNode(nodeId: InMemoryNodeId): Promise<void> {
    this._nodes.delete(nodeId.index)
    this.dirtyNodes.delete(nodeId.index)
    this._nodeCache.delete(nodeId.index)
    return Promise.resolve()
  }
}
