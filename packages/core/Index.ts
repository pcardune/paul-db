import { BTree, InMemoryBTreeConfig } from "./BTree.ts"
import { InMemoryNodeList } from "./NodeList.ts"
import { Range } from "./types.ts"

export class Index<K, V, NodeId> {
  private data: BTree<K, V, NodeId>
  constructor(data: BTree<K, V, NodeId>) {
    this.data = data
  }

  static inMemory<K, V>(
    config: InMemoryBTreeConfig<K, V>,
  ): Index<K, V, number> {
    return new Index(
      new BTree<K, V, number>({
        compare: config.compare ?? ((a, b) => a < b ? -1 : a > b ? 1 : 0),
        isEqual: config.isEqual ?? ((a, b) => a === b),
        nodes: new InMemoryNodeList<K, V>([]),
      }),
    )
  }

  public insert(key: K, value: V): void {
    this.data.insert(key, value)
  }

  public get(key: K): readonly V[] {
    return this.data.get(key)
  }

  public has(key: K): boolean {
    return this.data.has(key)
  }

  public remove(key: K, value: V): void {
    this.data.remove(key, value)
  }

  public getRange(range: Range<K>): {
    key: K
    vals: readonly V[]
  }[] {
    return this.data.getRange(range)
  }
}
