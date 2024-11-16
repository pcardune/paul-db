// deno-lint-ignore-file no-explicit-any
import { InMemoryBTreeConfig } from "./DiskBTree.ts"
import { Index } from "./Index.ts"

type InternalRowId = bigint

type TableIndex<R extends Record<string, any>, V> = {
  getValue: (record: R) => V
  config?: InMemoryBTreeConfig<V, InternalRowId>
}

export class Table<
  R extends Record<string, any>,
  IndexesT extends Record<string, TableIndex<R, any>>,
> {
  private data: Map<InternalRowId, R>
  private nextId: InternalRowId
  private indexes: IndexesT
  _indexesByName: {
    [K in keyof IndexesT]: Index<
      ReturnType<IndexesT[K]["getValue"]>,
      InternalRowId
    >
  }
  private _allIndexes: Index<unknown, InternalRowId>[]

  constructor(init: {
    indexes: typeof Table.prototype.indexes
    nextId: typeof Table.prototype.nextId
    data: typeof Table.prototype.data
  }) {
    this.nextId = init.nextId
    this.data = init.data
    this.indexes = init.indexes

    this._indexesByName = {} as any
    this._allIndexes = []
    for (const key in this.indexes) {
      const index = new Index(this.indexes[key].config ?? {})
      this._indexesByName[key] = index
      this._allIndexes.push(index)
    }
  }

  static create<
    R extends Record<string, any>,
    IndexesT extends Record<string, any>,
  >(
    indexes?: { [K in keyof IndexesT]: TableIndex<R, IndexesT[K]> },
  ) {
    return new Table<R, { [K in keyof IndexesT]: TableIndex<R, IndexesT[K]> }>({
      indexes: indexes,
      nextId: 1n,
      data: new Map(),
    })
  }

  public insert(record: R): void {
    const id = this.nextId++
    this.data.set(id, record)
    for (const [indexName, config] of Object.entries(this.indexes)) {
      const index = this._indexesByName[indexName]
      index.insert(config.getValue(record), id)
    }
  }

  public get(id: InternalRowId): R | undefined {
    return this.data.get(id)
  }

  public findMany<
    IName extends keyof typeof this._indexesByName,
    ValueT extends Parameters<typeof this._indexesByName[IName]["get"]>[0],
  >(
    indexName: IName,
    value: ValueT,
  ): Readonly<R>[] {
    return this._indexesByName[indexName].get(value).map((id) => {
      return this.data.get(id)!
    })
  }
}
