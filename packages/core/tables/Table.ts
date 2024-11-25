// deno-lint-ignore-file no-explicit-any
import { Index } from "../indexes/Index.ts"
import {
  OutputForComputedColumnSchema,
  RecordForTableSchema,
  SomeColumnSchema,
  SomeComputedColumnSchema,
  TableSchema,
  ValueForColumnSchema,
} from "../schema/schema.ts"
import { ITableStorage } from "./TableStorage.ts"
import { FilterTuple } from "../typetools.ts"
import { INodeId } from "../indexes/BTreeNode.ts"

/**
 * A helper type that lets you declare a table type from a given
 * schema and storage type.
 */
export type TableInfer<SchemaT, StorageT> = SchemaT extends
  TableSchema<infer TName, infer ColumnSchemasT, infer ComputedColumnSchemasT>
  ? StorageT extends ITableStorage<infer RowIdT, RecordForTableSchema<SchemaT>>
    ? Table<
      RowIdT,
      TName,
      ColumnSchemasT,
      ComputedColumnSchemasT,
      SchemaT,
      ITableStorage<RowIdT, RecordForTableSchema<SchemaT>>
    >
  : never
  : never

export class Table<
  RowIdT,
  TName extends string,
  ColumnSchemasT extends SomeColumnSchema[],
  ComputedColumnSchemasT extends SomeComputedColumnSchema[],
  SchemaT extends TableSchema<TName, ColumnSchemasT, ComputedColumnSchemasT>,
  StorageT extends ITableStorage<RowIdT, RecordForTableSchema<SchemaT>>,
> {
  private schema: SchemaT
  private data: StorageT
  private _allIndexes: Map<string, Index<unknown, RowIdT, INodeId>>

  constructor(init: {
    schema: SchemaT
    data: StorageT
    indexes: Map<string, Index<unknown, RowIdT, INodeId>>
  }) {
    this.schema = init.schema
    this.data = init.data

    this._allIndexes = init.indexes
  }

  async insertMany(
    records: RecordForTableSchema<SchemaT>[],
  ): Promise<RowIdT[]> {
    const rowIds: RowIdT[] = []
    for (const record of records) {
      rowIds.push(await this.insert(record))
    }
    return rowIds
  }

  async insert(record: RecordForTableSchema<SchemaT>): Promise<RowIdT> {
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
        if (await index.has(value)) {
          throw new Error(
            `Record with given ${column.name} value already exists`,
          )
        }
      }
    }

    const id = await this.data.insert(record)
    for (const column of this.schema.columns) {
      const index = this._allIndexes.get(column.name)
      if (index) {
        await index.insert((record as any)[column.name], id)
      } else if (column.indexed) {
        throw new Error(`Column ${column.name} is not indexed`)
      }
    }
    for (const column of this.schema.computedColumns) {
      const index = this._allIndexes.get(column.name)
      if (index) {
        await index.insert(column.compute(record), id)
      } else if (column.indexed) {
        throw new Error(`Column ${column.name} is not indexed`)
      }
    }
    await this.data.commit()
    return id
  }

  get(id: RowIdT): Promise<RecordForTableSchema<SchemaT> | undefined> {
    return this.data.get(id)
  }

  async remove(id: RowIdT): Promise<void> {
    await this.data.remove(id)
    await this.data.commit()
  }

  async lookup<
    IName extends FilterTuple<SchemaT["columns"], { indexed: true }>["name"],
    ValueT extends ValueForColumnSchema<
      FilterTuple<SchemaT["columns"], { name: IName }>
    >,
  >(
    indexName: IName,
    value: ValueT,
  ): Promise<Readonly<RecordForTableSchema<SchemaT>>[]> {
    const index = this._allIndexes.get(indexName)
    if (!index) {
      throw new Error(`Index ${indexName} does not exist`)
    }
    return Promise.all(
      (await index.get(value)).map((id) => {
        return this.data.get(id) as Promise<RecordForTableSchema<SchemaT>>
      }),
    )
  }

  async lookupComputed<
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
    return Promise.all(
      (await index.get(value)).map((id) =>
        this.data.get(id) as Promise<RecordForTableSchema<SchemaT>>
      ),
    )
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
