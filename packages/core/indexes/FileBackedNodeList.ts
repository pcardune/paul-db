import { ReadonlyDataView, WriteableDataView } from "../binary/dataview.ts"
import { IStruct } from "../binary/Struct.ts"
import { debugJson, debugLog } from "../logging.ts"
import { IBufferPool } from "../pages/BufferPool.ts"
import { HeapPageFile } from "../pages/HeapPageFile.ts"
import {
  ReadonlyVariableLengthRecordPage,
  VariableLengthRecordPageAllocInfo,
  WriteableVariableLengthRecordPage,
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

  // this is only here for types safety
  private async getRecordView(
    id: FileNodeId,
  ): Promise<ReadonlyDataView | undefined> {
    const view = await this.bufferPool.getPageView(id.pageId)
    const recordPage = new ReadonlyVariableLengthRecordPage(view)
    const slot = recordPage.getSlotEntry(id.slotIndex)
    if (slot.length === 0) return undefined // this was deleted
    return view.slice(slot.offset, slot.length)
  }

  private async writeToRecord(
    id: FileNodeId,
    writer: (view: WriteableDataView) => void | Promise<void>,
  ): Promise<void> {
    await this.bufferPool.writeToPage(id.pageId, (view) => {
      const recordPage = new WriteableVariableLengthRecordPage(view)
      const slot = recordPage.getSlotEntry(id.slotIndex)
      if (slot.length === 0) {
        throw new Error("Node not found")
      }
      return writer(view.slice(slot.offset, slot.length))
    })
  }

  async get(nodeId: FileNodeId): Promise<BTreeNode<K, V, FileNodeId>> {
    debugLog(`INodelist.get(${nodeId})`)
    const existingDirty = this.dirtyNodes.get(this.cacheKey(nodeId))
    if (existingDirty != null) {
      debugLog(() => `  -> found in dirty nodes: ${existingDirty}`)
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
      debugLog(() => `  -> found leaf node: ${node}`)
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
    debugLog(() => `  -> found internal node: ${node}`)
    return node
  }

  private dirtyNodes = new Map<string, BTreeNode<K, V, FileNodeId>>()
  private cacheKey(nodeId: FileNodeId): string {
    return nodeId.serialize()
  }

  markDirty(node: BTreeNode<K, V, FileNodeId>): void {
    this.dirtyNodes.set(this.cacheKey(node.nodeId), node)
    this.bufferPool.markDirty(node.nodeId.pageId)
  }

  async createLeafNode(
    data: {
      keyvals: { key: K; vals: V[] }[]
      nextLeafNodeId: FileNodeId | null
      prevLeafNodeId: FileNodeId | null
    },
  ): Promise<LeafBTreeNode<K, V, FileNodeId>> {
    debugLog(
      () =>
        `createLeafNode(${
          data.keyvals.map((kv) => `${kv.key}: ${debugJson(kv.vals)}`)
            .join(
              ", ",
            )
        })`,
    )
    const size = this.leafNodeSerializer.sizeof(data)
    const { pageId, allocInfo: { slot, slotIndex } } = await this.heapPageFile
      .allocateSpace(size)
    await this.bufferPool.writeToPage(pageId, (view) => {
      this.leafNodeSerializer.writeAt(data, view, slot.offset)
    })
    const nodeId = new FileNodeId({ pageId, slotIndex })

    this.bufferPool.markDirty(nodeId.pageId)
    const node = new LeafBTreeNode<K, V, FileNodeId>(
      () => this.markDirty(node),
      nodeId,
      data,
    )
    // if (this.dirtyNodes.has(this.cacheKey(node.nodeId))) {
    //   throw new Error(`Node ${node.nodeId} already dirty??`)
    // }
    debugLog(() => `  -> created node with size ${size} ${node}`)
    this.markDirty(node)
    return node
  }

  async createInternalNode(
    data: { keys: K[]; childrenNodeIds: FileNodeId[] },
  ): Promise<InternalBTreeNode<K, FileNodeId>> {
    const size = this.internalNodeSerializer.sizeof(data)
    const { pageId, allocInfo: { slot, slotIndex } } = await this.heapPageFile
      .allocateSpace(size)
    await this.bufferPool.writeToPage(pageId, (view) => {
      this.internalNodeSerializer.writeAt(data, view, slot.offset)
    })

    const nodeId = new FileNodeId({ pageId, slotIndex })

    this.bufferPool.markDirty(nodeId.pageId)
    const node = new InternalBTreeNode<K, FileNodeId>(
      () => this.markDirty(node),
      nodeId,
      data,
    )
    this.markDirty(node)
    debugLog(
      `createInernalNode() created node with size ${size} ${node}`,
    )
    return node
  }

  async commit(): Promise<void> {
    debugLog("INodeLIst.commit()")
    for (const node of this.dirtyNodes.values()) {
      await this.writeToRecord(node.nodeId, (view) => {
        if (node instanceof LeafBTreeNode) {
          const size = this.leafNodeSerializer.sizeof(node)
          if (size > view.byteLength) {
            throw new Error(
              `Leaf Node ${node.nodeId.serialize()} too large: ${size} > ${view.byteLength}`,
            )
          }
          this.leafNodeSerializer.writeAt(node, view, 0)
        } else {
          const size = this.internalNodeSerializer.sizeof(node)
          if (this.internalNodeSerializer.sizeof(node) > view.byteLength) {
            throw new Error(
              `Internal Node ${node.nodeId.serialize()} too large: ${size} > ${view.byteLength}`,
            )
          }
          this.internalNodeSerializer.writeAt(node, view, 0)
        }
      })
      this.dirtyNodes.delete(this.cacheKey(node.nodeId))
    }
    await this.bufferPool.commit()
  }

  async deleteNode(nodeId: FileNodeId): Promise<void> {
    debugLog(`deleteNode(${nodeId})`)
    await this.bufferPool.writeToPage(nodeId.pageId, (view) => {
      const recordPage = new WriteableVariableLengthRecordPage(view)
      recordPage.freeSlot(nodeId.slotIndex)
    })
    this.dirtyNodes.delete(this.cacheKey(nodeId))
  }
}
