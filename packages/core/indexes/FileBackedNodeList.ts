import { IStruct } from "../binary/Struct.ts"
import { IBufferPool } from "../pages/BufferPool.ts"
import { HeapPageFile } from "../pages/HeapPageFile.ts"
import {
  VariableLengthRecordPage,
  VariableLengthRecordPageAllocInfo,
} from "../pages/VariableLengthRecordPage.ts"
import { BTreeNode, InternalBTreeNode, LeafBTreeNode } from "./BTreeNode.ts"
import { INodeList } from "./NodeList.ts"
import {
  FileNodeId,
  internalBTreeNodeStruct,
  leafBTreeNodeStruct,
  WrongNodeTypeError,
} from "./Serializers.ts"

export class FileBackedNodeList<K, V> implements INodeList<K, V, FileNodeId> {
  private leafNodeSerializer: ReturnType<typeof leafBTreeNodeStruct<K, V>>
  private internalNodeSerializer: ReturnType<
    typeof internalBTreeNodeStruct<K>
  >

  constructor(
    private bufferPool: IBufferPool,
    private heapPageFile: HeapPageFile<VariableLengthRecordPageAllocInfo>,
    keyStruct: IStruct<K>,
    valStruct: IStruct<V>,
  ) {
    this.leafNodeSerializer = leafBTreeNodeStruct(keyStruct, valStruct)
    this.internalNodeSerializer = internalBTreeNodeStruct(keyStruct)
  }

  // TODO: this was copied from HeapFileTableStorage,
  // so maybe it should be refactored into a shared utility
  private async getRecordView(id: FileNodeId) {
    const view = await this.bufferPool.getPageView(id.pageId)
    const recordPage = new VariableLengthRecordPage(view)
    const slot = recordPage.getSlotEntry(id.slotIndex)
    if (slot.length === 0) return undefined // this was deleted
    return new DataView(view.buffer, view.byteOffset + slot.offset, slot.length)
  }

  async get(nodeId: FileNodeId): Promise<BTreeNode<K, V, FileNodeId>> {
    const existingDirty = this.dirtyNodes.get(this.cacheKey(nodeId))
    if (existingDirty != null) {
      return Promise.resolve(existingDirty)
    }

    const view = await this.getRecordView(nodeId)
    if (view == null) throw new Error(`Node ${this.cacheKey(nodeId)} not found`)
    try {
      const data = this.leafNodeSerializer.readAt(view, 0)

      const node = new LeafBTreeNode<K, V, FileNodeId>(
        () => {
          this.markDirty(node)
        },
        nodeId,
        data,
      )
      return node
    } catch (e) {
      if (!(e instanceof WrongNodeTypeError)) {
        throw e
      }
      // If it's not a leaf node, try reading it as an internal node
    }

    const data = this.internalNodeSerializer.readAt(view, 0)
    const node = new InternalBTreeNode(
      () => {
        this.markDirty(node)
      },
      nodeId,
      data,
    )
    return node
  }

  private dirtyNodes = new Map<string, BTreeNode<K, V, FileNodeId>>()
  private cacheKey(nodeId: FileNodeId): string {
    return `${nodeId.pageId}-${nodeId.slotIndex}`
  }

  markDirty(node: BTreeNode<K, V, FileNodeId>): void {
    this.dirtyNodes.set(this.cacheKey(node.nodeId), node)
    this.bufferPool.markDirty(node.nodeId.pageId)
  }

  async createLeafNode(
    data: {
      keyvals: { key: K; vals: V[] }[]
      nextLeafNodeId: FileNodeId | null
    },
    replaceNodeId?: FileNodeId,
  ): Promise<LeafBTreeNode<K, V, FileNodeId>> {
    let nodeId: FileNodeId
    if (replaceNodeId) {
      const view = await this.getRecordView(replaceNodeId)
      if (view == null) throw new Error("Node not found")
      this.leafNodeSerializer.writeAt(data, view, 0)
      nodeId = replaceNodeId
    } else {
      const buffer = new ArrayBuffer(this.leafNodeSerializer.sizeof(data))
      const { pageId, allocInfo: { slot, slotIndex } } = await this.heapPageFile
        .allocateSpace(
          buffer.byteLength,
        )
      const page = await this.bufferPool.getPage(pageId)
      // TODO: instead of writing to buffer first and then copying,
      // just write directly to the page.
      this.leafNodeSerializer.writeAt(data, new DataView(buffer), 0)
      page.set(new Uint8Array(buffer), slot.offset)
      nodeId = { pageId, slotIndex }
    }
    this.bufferPool.markDirty(nodeId.pageId)
    const node = new LeafBTreeNode<K, V, FileNodeId>(
      () => this.markDirty(node),
      nodeId,
      data,
    )
    this.markDirty(node)
    return node
  }

  async createInternalNode(
    data: { keys: K[]; childrenNodeIds: FileNodeId[] },
    replaceNodeId?: FileNodeId,
  ): Promise<InternalBTreeNode<K, FileNodeId>> {
    let nodeId: FileNodeId
    if (replaceNodeId) {
      const view = await this.getRecordView(replaceNodeId)
      if (view == null) throw new Error("Node not found")
      this.internalNodeSerializer.writeAt(data, view, 0)
      nodeId = replaceNodeId
    } else {
      const buffer = new ArrayBuffer(this.internalNodeSerializer.sizeof(data))
      const { pageId, allocInfo: { slot, slotIndex } } = await this.heapPageFile
        .allocateSpace(
          buffer.byteLength,
        )
      const page = await this.bufferPool.getPage(pageId)
      // TODO: instead of writing to buffer first and then copying,
      // just write directly to the page.
      this.internalNodeSerializer.writeAt(data, new DataView(buffer), 0)
      page.set(new Uint8Array(buffer), slot.offset)
      nodeId = { pageId, slotIndex }
    }
    this.bufferPool.markDirty(nodeId.pageId)
    const node = new InternalBTreeNode<K, FileNodeId>(
      () => this.markDirty(node),
      nodeId,
      data,
    )
    this.markDirty(node)
    return node
  }

  async commit(): Promise<void> {
    for (const node of this.dirtyNodes.values()) {
      const view = await this.getRecordView(node.nodeId)
      if (view == null) throw new Error("Node not found")
      if (node instanceof LeafBTreeNode) {
        this.leafNodeSerializer.writeAt(node, view, 0)
      } else {
        this.internalNodeSerializer.writeAt(node, view, 0)
      }
      this.dirtyNodes.delete(this.cacheKey(node.nodeId))
      this.bufferPool.markDirty(node.nodeId.pageId)
    }
    await this.bufferPool.commit()
  }
}
