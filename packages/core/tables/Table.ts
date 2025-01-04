// deno-lint-ignore-file no-explicit-any
import {
  ComputedColumnRecord,
  InsertRecordForTableSchema,
  SomeTableSchema,
  StoredColumnRecord,
  StoredRecordForTableSchema,
  TableSchema,
} from "../schema/TableSchema.ts"
import { ITableStorage } from "./TableStorage.ts"
import { AsyncIterableWrapper, Mutex } from "../async.ts"
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
  C extends StoredColumnRecord,
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

type WriteEvent<SchemaT extends SomeTableSchema> = {
  event: "insert"
  record: StoredRecordForTableSchema<SchemaT>
} | {
  event: "update"
  newRecord: StoredRecordForTableSchema<SchemaT>
} | {
  event: "delete"
  deletedRecord: StoredRecordForTableSchema<SchemaT>
}

export class Table<
  RowIdT,
  N extends string,
  C extends StoredColumnRecord,
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
  private writeMutex = new Mutex()

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

  private subscriptions = new Set<
    (event: WriteEvent<TableSchema<N, C, CC>>) => void
  >()
  subscribe(fn: (event: WriteEvent<TableSchema<N, C, CC>>) => void): void {
    this.subscriptions.add(fn)
  }
  unsubscribe(fn: (event: WriteEvent<TableSchema<N, C, CC>>) => void): void {
    this.subscriptions.delete(fn)
  }
  private emit(event: WriteEvent<TableSchema<N, C, CC>>): void {
    for (const fn of this.subscriptions) {
      fn(event)
    }
  }

  async insertManyAndReturn(
    records: InsertRecordForTableSchema<this["schema"]>[],
  ): Promise<StoredRecordForTableSchema<this["schema"]>[]> {
    this.droppable.assertNotDropped("Table has been dropped")
    using _lock = await this.writeMutex.useLock()

    const rows: StoredRecordForTableSchema<this["schema"]>[] = []
    for (const record of records) {
      const rowId = await this._insert(record)
      rows.push(
        await this.data.get(rowId) as StoredRecordForTableSchema<
          this["schema"]
        >,
      )
    }
    return rows
  }

  async insertMany(
    records: InsertRecordForTableSchema<this["schema"]>[],
  ): Promise<void> {
    this.droppable.assertNotDropped("Table has been dropped")
    using _lock = await this.writeMutex.useLock()

    for (const record of records) {
      await this._insert(record)
    }
  }

  async insertAndReturn(
    record: InsertRecordForTableSchema<this["schema"]>,
  ): Promise<StoredRecordForTableSchema<this["schema"]>> {
    this.droppable.assertNotDropped("Table has been dropped")
    using _lock = await this.writeMutex.useLock()

    const id = await this._insert(record)
    return this.data.get(id) as Promise<
      StoredRecordForTableSchema<this["schema"]>
    >
  }

  async insert(
    record: InsertRecordForTableSchema<this["schema"]>,
  ): Promise<void> {
    this.droppable.assertNotDropped("Table has been dropped")
    using _lock = await this.writeMutex.useLock()
    await this._insert(record)
  }

  async _insert(
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

    const storedRecord = record as unknown as StoredRecordForTableSchema<
      this["schema"]
    >
    const id = await this.data.insert(storedRecord)
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
    this.emit({ event: "insert", record: storedRecord })
    return id
  }

  _get(
    id: RowIdT,
  ): Promisable<StoredRecordForTableSchema<this["schema"]> | undefined> {
    this.droppable.assertNotDropped("Table has been dropped")
    return this.data.get(id) as
      | StoredRecordForTableSchema<this["schema"]>
      | undefined
  }

  async _set(
    id: RowIdT,
    newRecord: StoredRecordForTableSchema<this["schema"]>,
  ): Promise<RowIdT> {
    this.droppable.assertNotDropped("Table has been dropped")

    const oldRecord = await this.data.get(id) as typeof newRecord
    const newRowId = await this.data.set(id, newRecord)

    for (const column of this.schema.columns) {
      const columnName = column.name as keyof typeof newRecord
      const index = await this.indexProvider.getIndexForColumn(column.name)
      if (index) {
        if (
          column.type.isEqual(oldRecord[columnName], newRecord[columnName])
        ) {
          continue
        }
        await index.remove(oldRecord[columnName], id)
        await index.insert(newRecord[columnName], id)
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
    this.emit({ event: "update", newRecord })
    return newRowId
  }

  async updateWhere<IName extends Column.FindIndexedNames<C & CC>>(
    indexName: IName,
    value: Column.GetInput<(C & CC)[IName]>,
    updatedValue: Partial<StoredRecordForTableSchema<TableSchema<N, C, CC>>>,
  ): Promise<void> {
    this.droppable.assertNotDropped("Table has been dropped")
    using _lock = await this.writeMutex.useLock()

    const index = await this.indexProvider.getIndexForColumn(
      indexName as string,
    )
    if (!index) {
      throw new Error(`Index ${indexName as string} does not exist`)
    }
    const ids = await this._lookupInIndex(indexName, value)
    for (const id of ids) {
      const data = await this._get(id)
      if (!data) {
        throw new Error(`Record not found for id ${id}`)
      }
      await this._set(id, { ...data, ...updatedValue })
    }
  }

  async removeWhere<IName extends Column.FindIndexedNames<C & CC>>(
    indexName: IName,
    value: Column.GetInput<(C & CC)[IName]>,
  ): Promise<void> {
    this.droppable.assertNotDropped("Table has been dropped")
    using _lock = await this.writeMutex.useLock()

    const index = await this.indexProvider.getIndexForColumn(
      indexName as string,
    )
    if (!index) {
      throw new Error(`Index ${indexName as string} does not exist`)
    }
    const ids = await this._lookupInIndex(indexName, value)

    for (const id of ids) {
      await this._remove(id)
    }
  }

  async _remove(id: RowIdT): Promise<void> {
    this.droppable.assertNotDropped("Table has been dropped")

    const oldRecord = await this.data.get(id)
    if (oldRecord == null) return // already removed?
    for (const column of this.schema.columns) {
      const index = await this.indexProvider.getIndexForColumn(column.name)
      await index?.remove(oldRecord[column.name as keyof typeof oldRecord], id)
    }
    for (const column of this.schema.computedColumns) {
      const index = await this.indexProvider.getIndexForColumn(column.name)
      const oldComputed = column.compute(oldRecord)
      await index?.remove(oldComputed, id)
    }
    await this.data.remove(id)
    await this.data.commit()
    this.emit({ event: "delete", deletedRecord: oldRecord })
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
    return record as StoredRecordForTableSchema<this["schema"]>
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
    return this.data.get(rowIds[0]) as StoredRecordForTableSchema<
      this["schema"]
    >
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

  scanIter<IName extends keyof StoredRecordForTableSchema<this["schema"]>>(
    columnName: IName,
    value: StoredRecordForTableSchema<this["schema"]>[IName],
  ): AsyncIterableWrapper<StoredRecordForTableSchema<this["schema"]>> {
    this.droppable.assertNotDropped("Table has been dropped")

    const columnType =
      this.schema.columns.find((c) => c.name === columnName)!.type
    return this.iterate().filter((record) =>
      columnType.isEqual(record[columnName], value)
    )
  }

  scan<IName extends keyof StoredRecordForTableSchema<this["schema"]>>(
    columnName: IName,
    value: StoredRecordForTableSchema<this["schema"]>[IName],
  ): Promise<StoredRecordForTableSchema<this["schema"]>[]> {
    this.droppable.assertNotDropped("Table has been dropped")

    return this.scanIter(columnName, value).toArray()
  }
}
