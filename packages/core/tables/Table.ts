// deno-lint-ignore-file no-explicit-any
import {
  ColumnRecord,
  ComputedColumnRecord,
  InsertRecordForTableSchema,
  SomeTableSchema,
  StoredRecordForTableSchema,
  TableSchema,
} from "../schema/TableSchema.ts"
import { ITableStorage } from "./TableStorage.ts"
import { AsyncIterableWrapper } from "../async.ts"
import * as Column from "../schema/columns/index.ts"
import type { Promisable } from "type-fest"
import { SerialIdGenerator } from "../serial.ts"
import { SerialUInt32ColumnType } from "../schema/columns/ColumnType.ts"
import { IndexProvider } from "../indexes/IndexProvider.ts"
import { Droppable, IDroppable } from "../droppable.ts"

/**
 * A helper type that lets you declare a table type from a given
 * schema and storage type.
 */
export type TableInfer<SchemaT extends SomeTableSchema, StorageT> =
  SchemaT extends
    TableSchema<infer TName, infer ColumnSchemasT, infer ComputedColumnSchemasT>
    ? StorageT extends
      ITableStorage<infer RowIdT, StoredRecordForTableSchema<SchemaT>> ? Table<
        RowIdT,
        TName,
        ColumnSchemasT,
        ComputedColumnSchemasT,
        ITableStorage<RowIdT, StoredRecordForTableSchema<SchemaT>>
      >
    : never
    : never

export type TableConfig<
  RowIdT,
  N extends string,
  C extends ColumnRecord,
  CC extends ComputedColumnRecord,
  StorageT extends ITableStorage<
    RowIdT,
    StoredRecordForTableSchema<TableSchema<N, C, CC>>
  >,
> = {
  schema: TableSchema<N, C, CC>
  data: StorageT
  indexProvider: IndexProvider<RowIdT>
  serialIdGenerator?: SerialIdGenerator
  droppable?: IDroppable
}

export class Table<
  RowIdT,
  N extends string,
  C extends ColumnRecord,
  CC extends ComputedColumnRecord,
  StorageT extends ITableStorage<
    RowIdT,
    StoredRecordForTableSchema<
      TableSchema<N, C, CC>
    >
  >,
> implements IDroppable {
  readonly schema: TableSchema<N, C, CC>
  readonly data: StorageT
  private serialIdGenerator?: SerialIdGenerator
  private indexProvider: IndexProvider<RowIdT>
  private droppable: Droppable

  constructor(
    init: TableConfig<
      RowIdT,
      N,
      C,
      CC,
      StorageT
    >,
  ) {
    this.schema = init.schema
    this.data = init.data
    this.serialIdGenerator = init.serialIdGenerator
    this.indexProvider = init.indexProvider
    this.droppable = new Droppable(async () => {
      await init.droppable?.drop()
    })
  }

  drop(): Promisable<void> {
    return this.droppable.drop()
  }

  async insertManyAndReturn(
    records: InsertRecordForTableSchema<this["schema"]>[],
  ): Promise<StoredRecordForTableSchema<this["schema"]>[]> {
    this.droppable.assertNotDropped("Table has been dropped")

    const rows: StoredRecordForTableSchema<this["schema"]>[] = []
    for (const record of records) {
      rows.push(await this.insertAndReturn(record))
    }
    return rows
  }

  async insertMany(
    records: InsertRecordForTableSchema<this["schema"]>[],
  ): Promise<RowIdT[]> {
    this.droppable.assertNotDropped("Table has been dropped")

    const rowIds: RowIdT[] = []
    for (const record of records) {
      rowIds.push(await this.insert(record))
    }
    return rowIds
  }

  async insertAndReturn(
    record: InsertRecordForTableSchema<this["schema"]>,
  ): Promise<StoredRecordForTableSchema<this["schema"]>> {
    this.droppable.assertNotDropped("Table has been dropped")

    const id = await this.insert(record)
    return this.data.get(id) as Promise<
      StoredRecordForTableSchema<this["schema"]>
    >
  }

  async insert(
    record: InsertRecordForTableSchema<this["schema"]>,
  ): Promise<RowIdT> {
    this.droppable.assertNotDropped("Table has been dropped")

    const validation = this.schema.isValidInsertRecord(record)
    if (!validation.valid) {
      throw new Error("Invalid record: " + validation.reason)
    }

    for (const column of this.schema.columns) {
      if ((record as any)[column.name] == null) {
        if (column.type instanceof SerialUInt32ColumnType) {
          if (this.serialIdGenerator) {
            ;(record as any)[column.name] = await this.serialIdGenerator.next(
              column.name,
            )
          } else {
            throw new Error(
              `No serial ID generator provided for column ${column.name}`,
            )
          }
        } else if (column.defaultValueFactory) {
          const value = column.defaultValueFactory()
          if (!column.type.isValid(value)) {
            throw new Error(`Default value for ${column.name} is invalid`)
          }
          ;(record as any)[column.name] = value
        }
      }
    }

    for (const column of this.schema.columns) {
      if (!column.isUnique) continue
      const index = await this.indexProvider.getIndexForColumn(column.name)
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
      const index = await this.indexProvider.getIndexForColumn(column.name)
      if (!index) {
        throw new Error(
          `Column ${column.name} is not indexed but is marked as unique`,
        )
      }
      const value = column.compute(record as any)
      if (await index.has(value)) {
        throw new Error(
          `Record with given ${column.name} value of ${value} already exists in ${this.schema.name}`,
        )
      }
    }

    const id = await this.data.insert(
      record as StoredRecordForTableSchema<this["schema"]>,
    )
    for (const column of this.schema.columns) {
      const index = await this.indexProvider.getIndexForColumn(column.name)
      if (index) {
        await index.insert((record as any)[column.name], id)
      } else if (column.indexed.shouldIndex) {
        throw new Error(`Column ${column.name} is not indexed`)
      }
    }
    for (const column of this.schema.computedColumns) {
      const index = await this.indexProvider.getIndexForColumn(column.name)
      if (index) {
        await index.insert(column.compute(record), id)
      } else if (column.indexed.shouldIndex) {
        throw new Error(`Column ${column.name} is not indexed`)
      }
    }
    await this.data.commit()
    return id
  }

  get(
    id: RowIdT,
  ): Promisable<StoredRecordForTableSchema<this["schema"]> | undefined> {
    this.droppable.assertNotDropped("Table has been dropped")
    return this.data.get(id)
  }

  async set(
    id: RowIdT,
    newRecord: StoredRecordForTableSchema<this["schema"]>,
  ): Promise<RowIdT> {
    this.droppable.assertNotDropped("Table has been dropped")

    const oldRecord = await this.data.get(id) as typeof newRecord
    const newRowId = await this.data.set(id, newRecord)

    for (const column of this.schema.columns) {
      const index = await this.indexProvider.getIndexForColumn(column.name)
      if (index) {
        if (
          column.type.isEqual(oldRecord[column.name], newRecord[column.name])
        ) {
          continue
        }
        await index.remove(oldRecord[column.name], id)
        await index.insert(newRecord[column.name], id)
      } else if (column.indexed.shouldIndex) {
        throw new Error(`Column ${column.name} is not indexed`)
      }
    }
    for (const column of this.schema.computedColumns) {
      const index = await this.indexProvider.getIndexForColumn(column.name)
      if (index) {
        const oldComputed = column.compute(oldRecord)
        const newComputed = column.compute(newRecord)
        if (column.type.isEqual(oldComputed, newComputed)) {
          continue
        }
        await index.remove(oldComputed, id)
        await index.insert(newComputed, id)
      } else if (column.indexed.shouldIndex) {
        throw new Error(`Column ${column.name} is not indexed`)
      }
    }

    await this.data.commit()
    return newRowId
  }

  async updateWhere<IName extends Column.FindIndexedNames<C & CC>>(
    indexName: IName,
    value: Column.GetInput<(C & CC)[IName]>,
    updatedValue: Partial<StoredRecordForTableSchema<this["schema"]>>,
  ): Promise<void> {
    this.droppable.assertNotDropped("Table has been dropped")

    const index = await this.indexProvider.getIndexForColumn(
      indexName as string,
    )
    if (!index) {
      throw new Error(`Index ${indexName as string} does not exist`)
    }
    const ids = await this._lookupInIndex(indexName, value)
    for (const id of ids) {
      const data = await this.get(id)
      if (!data) {
        throw new Error(`Record not found for id ${id}`)
      }
      await this.set(id, { ...data, ...updatedValue })
    }
  }

  async removeWhere<IName extends Column.FindIndexedNames<C & CC>>(
    indexName: IName,
    value: Column.GetInput<(C & CC)[IName]>,
  ): Promise<void> {
    this.droppable.assertNotDropped("Table has been dropped")

    const index = await this.indexProvider.getIndexForColumn(
      indexName as string,
    )
    if (!index) {
      throw new Error(`Index ${indexName as string} does not exist`)
    }
    const ids = await this._lookupInIndex(indexName, value)

    for (const id of ids) {
      await this.remove(id)
    }
  }

  async remove(id: RowIdT): Promise<void> {
    this.droppable.assertNotDropped("Table has been dropped")

    const oldRecord = await this.data.get(id)
    if (oldRecord == null) return // already removed?
    for (const column of this.schema.columns) {
      const index = await this.indexProvider.getIndexForColumn(column.name)
      await index?.remove(oldRecord[column.name], id)
    }
    for (const column of this.schema.computedColumns) {
      const index = await this.indexProvider.getIndexForColumn(column.name)
      const oldComputed = column.compute(oldRecord)
      await index?.remove(oldComputed, id)
    }
    await this.data.remove(id)
    await this.data.commit()
  }

  async lookupUniqueOrThrow<IName extends Column.FindUniqueNames<C & CC>>(
    indexName: IName,
    value: Column.GetInput<(C & CC)[IName]>,
  ): Promise<Readonly<StoredRecordForTableSchema<this["schema"]>>> {
    this.droppable.assertNotDropped("Table has been dropped")
    const rowIds = await this._lookupInIndex(indexName, value)
    if (rowIds.length === 0) {
      console.error(`Record not found for`, indexName, value)
      throw new Error(`Record not found`)
    }
    const record = await this.data.get(rowIds[0])
    if (record == null) {
      console.error(`Record not found for`, indexName, value)
      throw new Error(`Record not found`)
    }
    return record
  }

  async lookupUnique<IName extends Column.FindUniqueNames<C & CC>>(
    indexName: IName,
    value: Column.GetInput<(C & CC)[IName]>,
  ): Promise<Readonly<StoredRecordForTableSchema<this["schema"]>> | undefined> {
    this.droppable.assertNotDropped("Table has been dropped")
    const rowIds = await this._lookupInIndex(indexName, value)
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
  lookupInIndex<IName extends Column.FindIndexedNames<C & CC>>(
    indexName: IName,
    value: Column.GetInput<(C & CC)[IName]>,
  ): Promise<readonly RowIdT[]> {
    return this._lookupInIndex(indexName, value)
  }

  private async _lookupInIndex<IName extends keyof (C & CC)>(
    indexName: IName,
    value: Column.GetInput<(C & CC)[IName]>,
  ): Promise<readonly RowIdT[]> {
    this.droppable.assertNotDropped("Table has been dropped")
    const index = await this.indexProvider.getIndexForColumn(
      indexName as string,
    )
    if (!index) {
      throw new Error(`Index ${indexName as string} does not exist`)
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

  async lookup<IName extends Column.FindIndexedNames<C & CC>>(
    indexName: IName,
    value: Column.GetInput<(C & CC)[IName]>,
  ): Promise<Readonly<StoredRecordForTableSchema<this["schema"]>>[]> {
    this.droppable.assertNotDropped("Table has been dropped")

    const ids = await this._lookupInIndex(indexName, value)
    return Promise.all(ids.map((id) => this.data.get(id))) as Promise<
      StoredRecordForTableSchema<this["schema"]>[]
    >
  }

  async lookupComputed<
    IName extends Column.FindIndexedNames<CC>,
    ValueT extends Column.Computed.GetOutput<CC[IName]>,
  >(
    indexName: IName,
    value: ValueT,
  ): Promise<StoredRecordForTableSchema<this["schema"]>[]> {
    this.droppable.assertNotDropped("Table has been dropped")

    const index = await this.indexProvider.getIndexForColumn(
      indexName as string,
    )
    if (!index) {
      throw new Error(`Index ${indexName as string} does not exist`)
    }
    const column = this.schema.computedColumns.find((c) => c.name === indexName)
    if (!column) {
      throw new Error(`Column ${indexName as string} is not a computed column`)
    }
    return Promise.all(
      (await index.get(value)).map((id) =>
        this.data.get(id) as Promise<StoredRecordForTableSchema<this["schema"]>>
      ),
    )
  }

  iterate(): AsyncIterableWrapper<StoredRecordForTableSchema<this["schema"]>> {
    this.droppable.assertNotDropped("Table has been dropped")

    return this.data.iterate().map(([_rowId, record]) =>
      record
    ) as AsyncIterableWrapper<StoredRecordForTableSchema<this["schema"]>>
  }

  scanIter<
    IName extends C[string]["name"],
    IValue extends StoredRecordForTableSchema<this["schema"]>[IName],
  >(
    columnName: IName,
    value: IValue,
  ): AsyncIterableWrapper<StoredRecordForTableSchema<this["schema"]>> {
    this.droppable.assertNotDropped("Table has been dropped")

    const columnType =
      this.schema.columns.find((c) => c.name === columnName)!.type
    return this.iterate().filter((record) =>
      columnType.isEqual(record[columnName], value)
    )
  }

  scan<
    IName extends C[string]["name"],
    IValue extends StoredRecordForTableSchema<this["schema"]>[IName],
  >(
    columnName: IName,
    value: IValue,
  ): Promise<StoredRecordForTableSchema<this["schema"]>[]> {
    this.droppable.assertNotDropped("Table has been dropped")

    return this.scanIter(columnName, value).toArray()
  }
}
