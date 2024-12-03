import {
  BTreeNode,
  INodeId,
  InternalBTreeNode,
  LeafBTreeNode,
  SerializedNode,
} from "./BTreeNode.ts"
import { Promisable } from "npm:type-fest"

export interface INodeList<K, V, NodeId extends INodeId> {
  get(nodeId: NodeId): Promisable<BTreeNode<K, V, NodeId>>
  markDirty(node: BTreeNode<K, V, NodeId>): void
  deleteNode(nodeId: NodeId): Promisable<void>
  createLeafNode(
    state: {
      keyvals: { key: K; vals: V[] }[]
      nextLeafNodeId: NodeId | null
      prevLeafNodeId: NodeId | null
    },
  ): Promisable<LeafBTreeNode<K, V, NodeId>>
  createInternalNode(
    state: { keys: K[]; childrenNodeIds: NodeId[] },
  ): Promisable<InternalBTreeNode<K, NodeId>>
  commit(): Promisable<void>
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

  get(nodeId: InMemoryNodeId): BTreeNode<K, V, InMemoryNodeId> {
    const existingDirty = this.dirtyNodes.get(nodeId.index)
    if (existingDirty != null) {
      return existingDirty
    }
    const existing = this._nodeCache.get(nodeId.index) as BTreeNode<
      K,
      V,
      InMemoryNodeId
    >
    if (existing != null) {
      return existing
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
      return node
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
    return node
  }

  createLeafNode(
    state: {
      keyvals: { key: K; vals: V[] }[]
      nextLeafNodeId: InMemoryNodeId | null
      prevLeafNodeId: InMemoryNodeId | null
    },
  ): LeafBTreeNode<K, V, InMemoryNodeId> {
    const nodeId = this.getNextNodeId()
    const node = new LeafBTreeNode(() => this.markDirty(node), nodeId, state)
    this._nodeCache.set(nodeId.index, node)
    this.markDirty(node)
    return node
  }

  createInternalNode(
    state: { keys: K[]; childrenNodeIds: InMemoryNodeId[] },
  ): InternalBTreeNode<K, InMemoryNodeId> {
    const nodeId = this.getNextNodeId()
    const node = new InternalBTreeNode(
      () => this.markDirty(node),
      nodeId,
      state,
    )
    this._nodeCache.set(nodeId.index, node)
    this.markDirty(node)
    return node
  }

  commit(): void {
    for (const node of this.dirtyNodes.values()) {
      this._nodes.set(node.nodeId.index, node.serialize())
    }
    this.dirtyNodes.clear()
    this._nodeCache.clear()
  }

  deleteNode(nodeId: InMemoryNodeId): void {
    this._nodes.delete(nodeId.index)
    this.dirtyNodes.delete(nodeId.index)
    this._nodeCache.delete(nodeId.index)
  }
}
