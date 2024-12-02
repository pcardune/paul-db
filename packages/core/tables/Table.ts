// deno-lint-ignore-file no-explicit-any
import { Index } from "../indexes/Index.ts"
import {
  InsertRecordForTableSchema,
  StoredRecordForTableSchema,
  TableSchema,
} from "../schema/schema.ts"
import { ITableStorage } from "./TableStorage.ts"
import { FilterTuple } from "../typetools.ts"
import { INodeId } from "../indexes/BTreeNode.ts"
import { AsyncIterableWrapper } from "../async.ts"
import { Column, Index as CIndex } from "../schema/ColumnSchema.ts"
import { Promisable } from "npm:type-fest"
/**
 * A helper type that lets you declare a table type from a given
 * schema and storage type.
 */
export type TableInfer<SchemaT, StorageT> = SchemaT extends
  TableSchema<infer TName, infer ColumnSchemasT, infer ComputedColumnSchemasT>
  ? StorageT extends
    ITableStorage<infer RowIdT, StoredRecordForTableSchema<SchemaT>> ? Table<
      RowIdT,
      TName,
      ColumnSchemasT,
      ComputedColumnSchemasT,
      SchemaT,
      ITableStorage<RowIdT, StoredRecordForTableSchema<SchemaT>>
    >
  : never
  : never

export class Table<
  RowIdT,
  TName extends string,
  ColumnSchemasT extends Column.Stored[],
  ComputedColumnSchemasT extends Column.Computed.Any[],
  SchemaT extends TableSchema<TName, ColumnSchemasT, ComputedColumnSchemasT>,
  StorageT extends ITableStorage<RowIdT, StoredRecordForTableSchema<SchemaT>>,
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

  async insertManyAndReturn(
    records: InsertRecordForTableSchema<SchemaT>[],
  ): Promise<StoredRecordForTableSchema<SchemaT>[]> {
    const rows: StoredRecordForTableSchema<SchemaT>[] = []
    for (const record of records) {
      rows.push(await this.insertAndReturn(record))
    }
    return rows
  }

  async insertMany(
    records: InsertRecordForTableSchema<SchemaT>[],
  ): Promise<RowIdT[]> {
    const rowIds: RowIdT[] = []
    for (const record of records) {
      rowIds.push(await this.insert(record))
    }
    return rowIds
  }

  async insertAndReturn(
    record: InsertRecordForTableSchema<SchemaT>,
  ): Promise<StoredRecordForTableSchema<SchemaT>> {
    const id = await this.insert(record)
    return this.data.get(id) as Promise<StoredRecordForTableSchema<SchemaT>>
  }

  async insert(record: InsertRecordForTableSchema<SchemaT>): Promise<RowIdT> {
    const validation = this.schema.isValidInsertRecord(record)
    if (!validation.valid) {
      throw new Error("Invalid record: " + validation.reason)
    }

    for (const column of this.schema.columns) {
      if ((record as any)[column.name] == null && column.defaultValueFactory) {
        const value = column.defaultValueFactory()
        if (!column.type.isValid(value)) {
          throw new Error(`Default value for ${column.name} is invalid`)
        }
        ;(record as any)[column.name] = value
      }
    }

    for (const column of this.schema.columns) {
      if (!column.isUnique) continue
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
    for (const column of this.schema.computedColumns) {
      if (!column.isUnique) continue
      const index = this._allIndexes.get(column.name)
      if (!index) {
        throw new Error(
          `Column ${column.name} is not indexed but is marked as unique`,
        )
      }
      const value = column.compute(record as any)
      if (await index.has(value)) {
        throw new Error(
          `Record with given ${column.name} value already exists`,
        )
      }
    }

    const id = await this.data.insert(record)
    for (const column of this.schema.columns) {
      const index = this._allIndexes.get(column.name)
      if (index) {
        await index.insert((record as any)[column.name], id)
      } else if (column.indexed.shouldIndex) {
        throw new Error(`Column ${column.name} is not indexed`)
      }
    }
    for (const column of this.schema.computedColumns) {
      const index = this._allIndexes.get(column.name)
      if (index) {
        await index.insert(column.compute(record), id)
      } else if (column.indexed.shouldIndex) {
        throw new Error(`Column ${column.name} is not indexed`)
      }
    }
    await this.data.commit()
    return id
  }

  get(id: RowIdT): Promisable<StoredRecordForTableSchema<SchemaT> | undefined> {
    return this.data.get(id)
  }

  async set(
    id: RowIdT,
    record: StoredRecordForTableSchema<SchemaT>,
  ): Promise<RowIdT> {
    const newRowId = await this.data.set(id, record)
    await this.data.commit()
    return newRowId
  }

  async removeWhere<
    IName extends FilterTuple<
      SchemaT["columns"],
      { indexed: CIndex.Config }
    >["name"],
    ValueT extends Column.GetValue<
      FilterTuple<SchemaT["columns"], { name: IName }>
    >,
  >(
    indexName: IName,
    value: ValueT,
  ): Promise<void> {
    const index = this._allIndexes.get(indexName)
    if (!index) {
      throw new Error(`Index ${indexName} does not exist`)
    }
    const ids = await index.get(value)
    for (const id of ids) {
      await this.data.remove(id)
    }
    await this.data.commit()
  }

  async remove(id: RowIdT): Promise<void> {
    await this.data.remove(id)
    await this.data.commit()
  }

  async lookupUniqueOrThrow<
    IName extends Column.FindUnique<
      SchemaT["columns"] | SchemaT["computedColumns"]
    >["name"],
    ValueT extends
      | Column.GetValue<Column.FindWithName<SchemaT["columns"], IName>>
      | Column.Computed.GetInput<
        Column.FindWithName<SchemaT["computedColumns"], IName>
      >,
  >(
    indexName: IName,
    value: ValueT,
  ): Promise<Readonly<StoredRecordForTableSchema<SchemaT>>> {
    const result = await this.lookupUnique(indexName, value)
    if (!result) {
      console.error(`Record not found for`, indexName, value)
      throw new Error(`Record not found`)
    }
    return result
  }

  async lookupUnique<
    IName extends Column.FindUnique<
      SchemaT["columns"] | SchemaT["computedColumns"]
    >["name"],
    ValueT extends
      | Column.GetValue<Column.FindWithName<SchemaT["columns"], IName>>
      | Column.Computed.GetInput<
        Column.FindWithName<SchemaT["computedColumns"], IName>
      >,
  >(
    indexName: IName,
    value: ValueT,
  ): Promise<Readonly<StoredRecordForTableSchema<SchemaT>> | undefined> {
    const rowIds = await this.lookupInIndex(indexName, value)
    if (rowIds.length === 0) {
      return
    }
    return this.data.get(rowIds[0])
  }

  /**
   * @param indexName Index to look in
   * @param value indexed value to lookup
   * @returns list of physical row ids that match the value
   */
  async lookupInIndex<
    IName extends Column.FindIndexed<SchemaT["columns"]>["name"],
    ValueT extends
      | Column.GetValue<Column.FindWithName<SchemaT["columns"], IName>>
      | Column.Computed.GetInput<
        Column.FindWithName<SchemaT["computedColumns"], IName>
      >,
  >(
    indexName: IName,
    value: ValueT,
  ): Promise<readonly RowIdT[]> {
    const index = this._allIndexes.get(indexName)
    if (!index) {
      throw new Error(`Index ${indexName} does not exist`)
    }
    const computedColumn = this.schema.computedColumns.find((c) =>
      c.name === indexName
    )
    let valueToLookup = value
    if (computedColumn != null) {
      valueToLookup = computedColumn.compute(value)
    }
    return await index.get(valueToLookup)
  }

  async lookup<
    IName extends Column.FindIndexed<SchemaT["columns"]>["name"],
    ValueT extends Column.GetValue<
      Column.FindWithName<SchemaT["columns"], IName>
    >,
  >(
    indexName: IName,
    value: ValueT,
  ): Promise<Readonly<StoredRecordForTableSchema<SchemaT>>[]> {
    const ids = await this.lookupInIndex(indexName, value)
    return Promise.all(ids.map((id) => this.data.get(id))) as Promise<
      StoredRecordForTableSchema<SchemaT>[]
    >
  }

  async lookupComputed<
    IName extends Column.FindIndexed<SchemaT["computedColumns"]>["name"],
    ValueT extends Column.Computed.GetOutput<
      Column.FindWithName<SchemaT["computedColumns"], IName>
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
        this.data.get(id) as Promise<StoredRecordForTableSchema<SchemaT>>
      ),
    )
  }

  iterate(): AsyncIterableWrapper<StoredRecordForTableSchema<SchemaT>> {
    return this.data.iterate().map(([_rowId, record]) =>
      record
    ) as AsyncIterableWrapper<StoredRecordForTableSchema<SchemaT>>
  }

  scanIter<
    IName extends ColumnSchemasT[number]["name"],
    IValue extends StoredRecordForTableSchema<SchemaT>[IName],
  >(
    columnName: IName,
    value: IValue,
  ): AsyncIterableWrapper<StoredRecordForTableSchema<SchemaT>> {
    const columnType =
      this.schema.columns.find((c) => c.name === columnName)!.type
    return this.iterate().filter((record) =>
      columnType.isEqual(record[columnName], value)
    )
  }

  scan<
    IName extends ColumnSchemasT[number]["name"],
    IValue extends StoredRecordForTableSchema<SchemaT>[IName],
  >(
    columnName: IName,
    value: IValue,
  ): Promise<StoredRecordForTableSchema<SchemaT>[]> {
    return this.scanIter(columnName, value).toArray()
  }
}
