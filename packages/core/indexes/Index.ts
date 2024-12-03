import { BTree, InMemoryBTreeConfig } from "./BTree.ts"
import { Range } from "../types.ts"
import { INodeId } from "./BTreeNode.ts"
import { InMemoryNodeId } from "./NodeList.ts"
import { IBufferPool, PageId } from "../pages/BufferPool.ts"
import { IStruct } from "../binary/Struct.ts"
import { FileNodeId } from "./Serializers.ts"
import { FileBackedBTree } from "./FileBackedBTree.ts"
import { Promisable } from "npm:type-fest"
import { Droppable, IDroppable } from "../droppable.ts"

interface IIndex<K, V> extends IDroppable {
  insertMany(entries: Iterable<[K, V]>): Promisable<void>

  insert(key: K, value: V): Promisable<void>

  get(key: K): Promisable<readonly V[]>

  has(key: K): Promisable<boolean>

  remove(key: K, value: V): Promisable<void>

  getRange(range: Range<K>): Promisable<{
    key: K
    vals: readonly V[]
  }[]>
}

export class Index<K, V, NodeId extends INodeId> implements IIndex<K, V> {
  private constructor(
    private data: BTree<K, V, NodeId>,
    private droppable: Droppable,
  ) {}

  static inMemory<K, V>(
    config: InMemoryBTreeConfig<K, V>,
  ): Index<K, V, InMemoryNodeId> {
    return new Index(
      BTree.inMemory<K, V>({
        compare: config.compare ?? ((a, b) => a < b ? -1 : a > b ? 1 : 0),
        isEqual: config.isEqual ?? ((a, b) => a === b),
        order: config.order,
      }),
      new Droppable(() => {}),
    )
  }

  static async inFile<K, V>(
    bufferPool: IBufferPool,
    indexPageId: PageId,
    keyStruct: IStruct<K>,
    valStruct: IStruct<V>,
    config: InMemoryBTreeConfig<K, V>,
  ): Promise<Index<K, V, FileNodeId>> {
    const fbbt = await FileBackedBTree.create<K, V>(
      bufferPool,
      indexPageId,
      keyStruct,
      valStruct,
      {
        compare: config.compare ?? ((a, b) => a < b ? -1 : a > b ? 1 : 0),
        isEqual: config.isEqual ?? ((a, b) => a === b),
        order: config.order,
      },
    )

    return new Index(fbbt.btree, new Droppable(() => fbbt.drop()))
  }

  async insertMany(entries: Iterable<[K, V]>): Promise<void> {
    this.droppable.assertNotDropped("Index has been dropped")
    await this.data.insertMany(entries)
  }

  async insert(key: K, value: V): Promise<void> {
    this.droppable.assertNotDropped("Index has been dropped")
    await this.data.insert(key, value)
  }

  get(key: K): Promise<readonly V[]> {
    this.droppable.assertNotDropped("Index has been dropped")
    return this.data.get(key)
  }

  has(key: K): Promise<boolean> {
    this.droppable.assertNotDropped("Index has been dropped")
    return this.data.has(key)
  }

  async remove(key: K, value: V): Promise<void> {
    this.droppable.assertNotDropped("Index has been dropped")
    await this.data.remove(key, value)
  }

  async getRange(range: Range<K>): Promise<{
    key: K
    vals: readonly V[]
  }[]> {
    this.droppable.assertNotDropped("Index has been dropped")
    return await this.data.getRange(range)
  }

  async drop(): Promise<void> {
    await this.droppable.drop()
  }
}
