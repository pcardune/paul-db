import {
  BTreeNode,
  InternalBTreeNode,
  LeafBTreeNode,
  SerializedNode,
} from "./BTreeNode.ts"

export interface INodeList<K, V, NodeId> {
  size: number
  get(nodeId: NodeId): Promise<BTreeNode<K, V, NodeId>>
  markDirty(node: BTreeNode<K, V, NodeId>): void
  createLeafNode(
    state: { keyvals: { key: K; vals: V[] }[]; nextLeafNodeId: NodeId | null },
    replaceNodeId?: NodeId,
  ): Promise<LeafBTreeNode<K, V, NodeId>>
  createInternalNode(
    state: { keys: K[]; childrenNodeIds: NodeId[] },
    replaceNodeId?: NodeId,
  ): Promise<InternalBTreeNode<K, NodeId>>
  commit(): Promise<void>
}

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

  private getNextNodeId(): number {
    return this.nextNodeId++
  }

  private _nodeCache = new Map<number, BTreeNode<K, V, number>>()

  get(nodeId: number): Promise<BTreeNode<K, V, number>> {
    const serialized = this._nodes[nodeId]
    const existingDirty = this.dirtyNodes.get(nodeId)
    if (existingDirty != null) {
      return Promise.resolve(existingDirty)
    }
    const existing = this._nodeCache.get(nodeId) as BTreeNode<K, V, number>
    if (existing != null) {
      return Promise.resolve(existing)
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
      return Promise.resolve(node)
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
    return Promise.resolve(node)
  }

  createLeafNode(
    state: { keyvals: { key: K; vals: V[] }[]; nextLeafNodeId: number | null },
    replaceNodeId?: number,
  ): Promise<LeafBTreeNode<K, V, number>> {
    const nodeId = replaceNodeId ?? this.getNextNodeId()
    const node = new LeafBTreeNode(() => this.markDirty(node), nodeId, state)
    this._nodeCache.set(nodeId, node)
    this.markDirty(node)
    return Promise.resolve(node)
  }

  createInternalNode(
    state: { keys: K[]; childrenNodeIds: number[] },
    replaceNodeId?: number,
  ): Promise<InternalBTreeNode<K, number>> {
    const nodeId = replaceNodeId ?? this.getNextNodeId()
    const node = new InternalBTreeNode(
      () => this.markDirty(node),
      nodeId,
      state,
    )
    this._nodeCache.set(nodeId, node)
    this.markDirty(node)
    return Promise.resolve(node)
  }

  commit(): Promise<void> {
    for (const node of this.dirtyNodes.values()) {
      if (node.type === "leaf") {
        this._nodes[node.nodeId] = ["leaf", node.keyvals, node.nextLeafNodeId]
      } else {
        this._nodes[node.nodeId] = ["internal", node.keys, node.childrenNodeIds]
      }
    }
    this.dirtyNodes.clear()
    this._nodeCache.clear()
    return Promise.resolve()
  }
}
