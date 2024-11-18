// deno-lint-ignore-file no-explicit-any
import { Index } from "./Index.ts"
import {
  OutputForComputedColumnSchema,
  RecordForTableSchema,
  SomeColumnSchema,
  SomeComputedColumnSchema,
  TableSchema,
  ValueForColumnSchema,
} from "./schema.ts"
import { FilterTuple } from "./typetools.ts"

type InternalRowId = bigint

export class Table<
  TName extends string,
  ColumnSchemasT extends SomeColumnSchema[],
  ComputedColumnSchemasT extends SomeComputedColumnSchema[],
  SchemaT extends TableSchema<TName, ColumnSchemasT, ComputedColumnSchemasT>,
> {
  private schema: SchemaT
  private data: Map<InternalRowId, RecordForTableSchema<SchemaT>>
  private nextId: InternalRowId
  private _allIndexes: Map<string, Index<unknown, InternalRowId, unknown>>

  constructor(init: {
    schema: SchemaT
    nextId: typeof Table.prototype.nextId
    data: typeof Table.prototype.data
  }) {
    this.schema = init.schema
    this.nextId = init.nextId
    this.data = init.data

    this._allIndexes = new Map()
    for (const column of this.schema.columns) {
      if (column.indexed) {
        this._allIndexes.set(
          column.name,
          Index.inMemory({
            isEqual: column.type.isEqual,
            compare: column.type.compare,
          }),
        )
      }
    }
    for (const column of this.schema.computedColumns) {
      if (column.indexed) {
        this._allIndexes.set(
          column.name,
          Index.inMemory({}),
        )
      }
    }
  }

  static create<
    TName extends string,
    ColumnSchemasT extends SomeColumnSchema[],
    ComputedColumnSchemasT extends SomeComputedColumnSchema[],
    SchemaT extends TableSchema<any, any, any>,
  >(
    schema: SchemaT,
  ) {
    return new Table<
      TName,
      ColumnSchemasT,
      ComputedColumnSchemasT,
      SchemaT
    >({
      schema,
      nextId: 1n,
      data: new Map(),
    })
  }

  public insertMany(records: RecordForTableSchema<SchemaT>[]): InternalRowId[] {
    return records.map((record) => this.insert(record))
  }

  public insert(record: RecordForTableSchema<SchemaT>): InternalRowId {
    if (!this.schema.isValidRecord(record)) {
      throw new Error("Invalid record")
    }
    for (const column of this.schema.columns) {
      if (column.unique) {
        const index = this._allIndexes.get(column.name)
        if (!index) {
          throw new Error(
            `Column ${column.name} is not indexed but is marked as unique`,
          )
        }
        const value = (record as any)[column.name]
        if (index.has(value)) {
          throw new Error(
            `Record with given ${column.name} value already exists`,
          )
        }
      }
    }

    const id = this.nextId++
    this.data.set(id, record)
    for (const column of this.schema.columns) {
      const index = this._allIndexes.get(column.name)
      if (index) {
        index.insert((record as any)[column.name], id)
      } else if (column.indexed) {
        throw new Error(`Column ${column.name} is not indexed`)
      }
    }
    for (const column of this.schema.computedColumns) {
      const index = this._allIndexes.get(column.name)
      if (index) {
        index.insert(column.compute(record), id)
      } else if (column.indexed) {
        throw new Error(`Column ${column.name} is not indexed`)
      }
    }
    return id
  }

  public get(id: InternalRowId): RecordForTableSchema<SchemaT> | undefined {
    return this.data.get(id)
  }

  public lookup<
    IName extends FilterTuple<SchemaT["columns"], { indexed: true }>["name"],
    ValueT extends ValueForColumnSchema<
      FilterTuple<SchemaT["columns"], { name: IName }>
    >,
  >(
    indexName: IName,
    value: ValueT,
  ): Readonly<RecordForTableSchema<SchemaT>>[] {
    const index = this._allIndexes.get(indexName)
    if (!index) {
      throw new Error(`Index ${indexName} does not exist`)
    }
    return index.get(value).map((id) => {
      return this.data.get(id)!
    })
  }

  public lookupComputed<
    IName extends FilterTuple<
      SchemaT["computedColumns"],
      { indexed: true }
    >["name"],
    ValueT extends OutputForComputedColumnSchema<
      FilterTuple<SchemaT["computedColumns"], { name: IName }>
    >,
  >(indexName: IName, value: ValueT) {
    const index = this._allIndexes.get(indexName)
    if (!index) {
      throw new Error(`Index ${indexName} does not exist`)
    }
    const column = this.schema.computedColumns.find((c) => c.name === indexName)
    if (!column) {
      throw new Error(`Column ${indexName} is not a computed column`)
    }
    return index.get(value).map((id) => this.data.get(id)!)
  }

  public iterate(): IteratorObject<RecordForTableSchema<SchemaT>, void, void> {
    return this.data.values()
  }

  public scanIter<
    IName extends ColumnSchemasT[number]["name"],
    IValue extends RecordForTableSchema<SchemaT>[IName],
  >(
    columnName: IName,
    value: IValue,
  ): IteratorObject<RecordForTableSchema<SchemaT>, void, void> {
    const columnType =
      this.schema.columns.find((c) => c.name === columnName)!.type
    return this.iterate().filter((record) =>
      columnType.isEqual(record[columnName], value)
    )
  }

  public scan<
    IName extends ColumnSchemasT[number]["name"],
    IValue extends RecordForTableSchema<SchemaT>[IName],
  >(
    columnName: IName,
    value: IValue,
  ): RecordForTableSchema<SchemaT>[] {
    return Array.from(this.scanIter(columnName, value))
  }
}
