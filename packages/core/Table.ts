// deno-lint-ignore-file no-explicit-any
import { InMemoryBTreeConfig } from "./DiskBTree.ts"
import { Index } from "./Index.ts"
import { RecordForTableSchema, TableSchema } from "./schema.ts"

type InternalRowId = bigint

type TableIndex<R extends Record<string, any>, V> = {
  getValue: (record: R) => V
  config?: InMemoryBTreeConfig<V, InternalRowId>
}

export class Table<
  SchemaT extends TableSchema<any, any>,
  IndexesT extends Record<
    string,
    TableIndex<RecordForTableSchema<SchemaT>, any>
  >,
> {
  private schema: SchemaT
  private data: Map<InternalRowId, RecordForTableSchema<SchemaT>>
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
    schema: SchemaT
    indexes: IndexesT
    nextId: typeof Table.prototype.nextId
    data: typeof Table.prototype.data
  }) {
    this.schema = init.schema
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
    IndexesT extends Record<string, any>,
    SchemaT extends TableSchema<any, any>,
  >(
    schema: SchemaT,
    indexes: {
      [K in keyof IndexesT]: TableIndex<
        RecordForTableSchema<SchemaT>,
        IndexesT[K]
      >
    },
  ) {
    return new Table<
      SchemaT,
      {
        [K in keyof IndexesT]: TableIndex<
          RecordForTableSchema<SchemaT>,
          IndexesT[K]
        >
      }
    >({
      schema,
      indexes: indexes,
      nextId: 1n,
      data: new Map(),
    })
  }

  public insert(record: RecordForTableSchema<SchemaT>): InternalRowId {
    if (!this.schema.isValidRecord(record)) {
      throw new Error("Invalid record")
    }
    const id = this.nextId++
    this.data.set(id, record)
    for (const [indexName, config] of Object.entries(this.indexes)) {
      const index = this._indexesByName[indexName]
      index.insert(config.getValue(record), id)
    }
    return id
  }

  public get(id: InternalRowId): RecordForTableSchema<SchemaT> | undefined {
    return this.data.get(id)
  }

  public findMany<
    IName extends keyof typeof this._indexesByName,
    ValueT extends Parameters<typeof this._indexesByName[IName]["get"]>[0],
  >(
    indexName: IName,
    value: ValueT,
  ): Readonly<RecordForTableSchema<SchemaT>>[] {
    return this._indexesByName[indexName].get(value).map((id) => {
      return this.data.get(id)!
    })
  }
}
