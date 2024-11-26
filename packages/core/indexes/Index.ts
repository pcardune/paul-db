import { BTree, InMemoryBTreeConfig } from "./BTree.ts"
import { Range } from "../types.ts"
import { INodeId } from "./BTreeNode.ts"
import { InMemoryNodeId } from "./NodeList.ts"
import { IBufferPool, PageId } from "../pages/BufferPool.ts"
import { IStruct } from "../binary/Struct.ts"
import { FileNodeId } from "./Serializers.ts"

export class Index<K, V, NodeId extends INodeId> {
  private data: BTree<K, V, NodeId>
  constructor(data: BTree<K, V, NodeId>) {
    this.data = data
  }

  static async inMemory<K, V>(
    config: InMemoryBTreeConfig<K, V>,
  ): Promise<Index<K, V, InMemoryNodeId>> {
    return new Index(
      await BTree.inMemory<K, V>({
        compare: config.compare ?? ((a, b) => a < b ? -1 : a > b ? 1 : 0),
        isEqual: config.isEqual ?? ((a, b) => a === b),
      }),
    )
  }

  static async inFile<K, V>(
    bufferPool: IBufferPool,
    indexPageId: PageId,
    keyStruct: IStruct<K>,
    valStruct: IStruct<V>,
    config: InMemoryBTreeConfig<K, V>,
  ): Promise<Index<K, V, FileNodeId>> {
    return new Index(
      await BTree.inFile<K, V>(
        bufferPool,
        indexPageId,
        keyStruct,
        valStruct,
        {
          compare: config.compare ?? ((a, b) => a < b ? -1 : a > b ? 1 : 0),
          isEqual: config.isEqual ?? ((a, b) => a === b),
        },
      ),
    )
  }

  async insert(key: K, value: V): Promise<void> {
    await this.data.insert(key, value)
  }

  get(key: K): Promise<readonly V[]> {
    return this.data.get(key)
  }

  has(key: K): Promise<boolean> {
    return this.data.has(key)
  }

  async remove(key: K, value: V): Promise<void> {
    await this.data.remove(key, value)
  }

  async getRange(range: Range<K>): Promise<{
    key: K
    vals: readonly V[]
  }[]> {
    return await this.data.getRange(range)
  }
}
