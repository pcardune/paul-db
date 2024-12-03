import { IBufferPool, PageId } from "../pages/BufferPool.ts"
import { FileNodeId, fileNodeIdStruct } from "./Serializers.ts"
import { IStruct, Struct } from "../binary/Struct.ts"
import { FileBackedNodeList } from "./FileBackedNodeList.ts"
import { BTree, InMemoryBTreeConfig } from "./BTree.ts"
import { AsyncIterableWrapper } from "../async.ts"
import { Droppable, IDroppable } from "../droppable.ts"

const indexInfoStruct = Struct.record(
  {
    heapPageFilePageId: [0, Struct.bigUint64],
    rootNodeId: [1, fileNodeIdStruct],
  },
)

export class FileBackedBTree<K, V> implements IDroppable {
  static async create<K, V>(
    bufferPool: IBufferPool,
    pageId: PageId,
    keyStruct: IStruct<K>,
    valStruct: IStruct<V>,
    {
      order = 2,
      compare = (a, b) => a < b ? -1 : a > b ? 1 : 0,
      isEqual = (a, b) => a === b,
    }: InMemoryBTreeConfig<K, V> = {},
  ) {
    // let heapPageFilePageId: PageId = 0n
    // let rootNodeId: FileNodeId | null = null
    const view = await bufferPool.getPageView(pageId)
    let { heapPageFilePageId, rootNodeId } = indexInfoStruct.readAt(view, 0)

    if (heapPageFilePageId === 0n) {
      // this is the first time this btree is being loaded.
      // create the header page
      heapPageFilePageId = await bufferPool.allocatePage()
      await bufferPool.writeToPage(heapPageFilePageId, (view) => {
        indexInfoStruct.writeAt({ heapPageFilePageId, rootNodeId }, view, 0)
      })
    }
    const nodes = new FileBackedNodeList<K, V>(
      bufferPool,
      heapPageFilePageId,
      keyStruct,
      valStruct,
    )
    if (rootNodeId == null) {
      // this is the first time this btree is being loaded.
      // create the initial set of nodes
      const childNode = await nodes.createLeafNode({
        keyvals: [],
        nextLeafNodeId: null,
        prevLeafNodeId: null,
      })
      const rootNode = await nodes.createInternalNode({
        keys: [],
        childrenNodeIds: [childNode.nodeId],
      })
      await nodes.commit()
      rootNodeId = rootNode.nodeId
      await bufferPool.writeToPage(pageId, (view) => {
        indexInfoStruct.writeAt({ heapPageFilePageId, rootNodeId }, view, 0)
      })
    }
    await bufferPool.commit()
    return new FileBackedBTree(
      bufferPool,
      pageId,
      { order, compare, isEqual },
      nodes,
      rootNodeId,
      heapPageFilePageId,
    )
  }

  readonly btree: BTree<K, V, FileNodeId, FileBackedNodeList<K, V>>
  private droppable: Droppable

  private constructor(
    readonly bufferPool: IBufferPool,
    readonly pageId: PageId,
    readonly config: Required<InMemoryBTreeConfig<K, V>>,
    readonly nodes: FileBackedNodeList<K, V>,
    rootNodeId: FileNodeId,
    readonly heapPageFilePageId: PageId,
  ) {
    this.btree = new BTree<K, V, FileNodeId, FileBackedNodeList<K, V>>(
      {
        ...config,
        nodes,
        rootNodeId,
        onRootNodeChanged: this.onRootNodeChanged,
      },
    )
    this.droppable = new Droppable(async () => {
      await this.nodes.drop()
      await this.bufferPool.freePage(this.pageId)
    })
  }

  async drop() {
    await this.droppable.drop()
  }

  private onRootNodeChanged = async () => {
    const rootNode = await this.btree.getRootNode()
    await this.bufferPool.writeToPage(this.pageId, (view) => {
      indexInfoStruct.writeAt(
        {
          heapPageFilePageId: this.heapPageFilePageId,
          rootNodeId: rootNode.nodeId,
        },
        view,
        0,
      )
    })
  }

  /**
   * Iterate over all the page ids used by this btree.
   */
  pageIdsIter(): AsyncIterableWrapper<PageId> {
    const thisPageId = this.pageId
    const headerPageRefs = this.nodes.heapPageFile.headerPageRefsIter()
    return new AsyncIterableWrapper(async function* () {
      for await (const headerPageRef of headerPageRefs) {
        const headerPage = await headerPageRef.get()
        yield* headerPage.entries.map((entry) => entry.pageId)
        yield headerPageRef.pageId
      }
      yield thisPageId
    })
  }
}
