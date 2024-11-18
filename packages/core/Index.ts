import {
  BTree,
  InMemoryBTree,
  InMemoryBTreeConfig,
  Range,
} from "./DiskBTree.ts"

export class Index<K, V> {
  private data: InMemoryBTree<K, V>
  constructor(config: InMemoryBTreeConfig<K, V>) {
    this.data = BTree.inMemory<K, V>(config)
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
