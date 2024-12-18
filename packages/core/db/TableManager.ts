import { IBufferPool } from "../pages/BufferPool.ts"
import {
  ComputedColumnRecord,
  makeTableSchemaStruct,
  SomeTableSchema,
  StoredColumnRecord,
  StoredRecordForTableSchema,
} from "../schema/TableSchema.ts"
import { Table, TableConfig } from "../tables/Table.ts"
import {
  HeapFileRowId,
  HeapFileTableInfer,
  HeapFileTableStorage,
  ITableStorage,
} from "../tables/TableStorage.ts"
import type { UnknownRecord } from "type-fest"
import * as schemas from "./metadataSchemas.ts"
import { SYSTEM_DB, TableRecord } from "./metadataSchemas.ts"
import { IndexManager } from "./IndexManager.ts"
import { DBFileSerialIdGenerator } from "../serial.ts"
import { HeapFileBackedIndexProvider } from "../indexes/IndexProvider.ts"
import { Droppable } from "../droppable.ts"
import { TableSchema } from "../mod.ts"

export class TableManager {
  private tables = new Map<
    string,
    Table<
      HeapFileRowId,
      string,
      StoredColumnRecord,
      ComputedColumnRecord,
      ITableStorage<HeapFileRowId, UnknownRecord>
    >
  >()

  constructor(
    readonly bufferPool: IBufferPool,
    readonly tablesTable: HeapFileTableInfer<typeof schemas.dbTables>,
    readonly indexManager: IndexManager,
  ) {
    this.tables = new Map()
  }

  getTableRecord(db: string, name: string): Promise<TableRecord | undefined> {
    return this.tablesTable.lookupUnique("_db_name", { db, name })
  }

  async hasTableRecord(db: string, name: string): Promise<boolean> {
    return (await this.getTableRecord(db, name)) != null
  }

  async renameTable(db: string, oldName: string, newName: string) {
    const tableRecord = await this.getTableRecord(db, oldName)
    if (tableRecord == null) {
      throw new Error(`Table ${db}.${oldName} not found`)
    }
    await this.tablesTable.updateWhere(
      "_db_name",
      { db, name: oldName },
      { name: newName },
    )
    const existingRef = this.tables.get(`${db}.${oldName}`)
    if (existingRef != null) {
      this.tables.delete(`${db}.${oldName}`)
      this.tables.set(`${db}.${newName}`, existingRef)
    }
  }

  private async createTableRecord(db: string, name: string) {
    const pageId = await this.bufferPool.allocatePage()
    await this.bufferPool.commit()
    const tableRecord = await this.tablesTable.insertAndReturn({
      name: name,
      heapPageId: pageId,
      db,
    })
    return tableRecord
  }

  async getTableStorage<
    N extends string,
    C extends StoredColumnRecord,
    CC extends ComputedColumnRecord,
  >(
    db: string,
    schema: TableSchema<N, C, CC>,
  ): Promise<
    null | {
      tableRecord: TableRecord
      storage: TableConfig<
        HeapFileRowId,
        N,
        C,
        CC,
        HeapFileTableStorage<StoredRecordForTableSchema<TableSchema<N, C, CC>>>
      >
    }
  > {
    const tableRecord = await this.tablesTable.lookupUnique("_db_name", {
      db,
      name: schema.name,
    })
    if (tableRecord == null) return null
    if (makeTableSchemaStruct(schema) == null) {
      throw new Error("Schema is not serializable")
    }
    return {
      tableRecord,
      storage: {
        ...getTableConfig(
          this.bufferPool,
          db,
          schema,
          tableRecord,
          this.indexManager,
          new Droppable(async () => {
            const schemaTable = await this.getTable(
              SYSTEM_DB,
              schemas.dbSchemas,
            )
            const columnsTable = await this.getTable(
              SYSTEM_DB,
              schemas.dbTableColumns,
            )
            if (schemaTable == null || columnsTable == null) {
              throw new Error(
                "Schema tables not found. Perhaps you are trying to drop a system table? That's not allowed",
              )
            }

            // delete metadata from schema tables
            for (
              const schemaRecord of await schemaTable.scan(
                "tableId",
                tableRecord.id,
              )
            ) {
              for (
                const column of await columnsTable.scan(
                  "schemaId",
                  schemaRecord.id,
                )
              ) {
                if (column.indexed && !column.indexInMemory) {
                  this.indexManager
                }
                await columnsTable.removeWhere("id", column.id)
              }
              await columnsTable.removeWhere("schemaId", schemaRecord.id)
              await schemaTable.removeWhere("id", schemaRecord.id)
            }

            // delete the table record
            await this.tablesTable.removeWhere("id", tableRecord.id)
          }),
        ),
        serialIdGenerator: new DBFileSerialIdGenerator(
          this,
          `${db}.${schema.name}`,
        ),
      },
    }
  }

  async getOrCreateTableStorage<
    N extends string,
    C extends StoredColumnRecord,
    CC extends ComputedColumnRecord,
  >(
    db: string,
    schema: TableSchema<N, C, CC>,
  ): Promise<{
    storage: TableConfig<
      HeapFileRowId,
      N,
      C,
      CC,
      HeapFileTableStorage<StoredRecordForTableSchema<TableSchema<N, C, CC>>>
    >
    created: boolean
  }> {
    let storage = await this.getTableStorage(db, schema)
    let created = false
    if (storage == null) {
      await this.createTableRecord(db, schema.name)
      storage = (await this.getTableStorage(db, schema))!
      created = true
    }
    return { ...storage, created }
  }

  async createTable<SchemaT extends SomeTableSchema>(
    db: string,
    schema: SchemaT,
  ): Promise<HeapFileTableInfer<SchemaT>> {
    const tableRecord = await this.createTableRecord(db, schema.name)
    const { storage } = (await this.getTableStorage(db, schema))!
    const table = new Table(storage)
    this.tables.set(`${db}.${schema.name}`, table)

    const schemaTable = await this.getTable(SYSTEM_DB, schemas.dbSchemas)
    const columnsTable = await this.getTable(SYSTEM_DB, schemas.dbTableColumns)
    if (schemaTable == null || columnsTable == null) {
      return table as HeapFileTableInfer<SchemaT>
    }

    const existingIds = await schemaTable.iterate().map((s) => s.id).toArray()
    const schemaRecord = await schemaTable.insertAndReturn({
      // TODO: make id generation sane, but can't use serial,
      // because serial depends on this!
      id: existingIds.length === 0 ? 0 : Math.max(...existingIds) + 1,
      tableId: tableRecord.id,
      version: 0,
    })
    const records = getColumnRecordsForSchema(schema)
    await columnsTable.insertMany(records.map((record) => ({
      ...record,
      schemaId: schemaRecord.id,
    })))

    return table as HeapFileTableInfer<SchemaT>
  }

  async getTable<SchemaT extends SomeTableSchema>(
    db: string,
    schema: SchemaT,
  ): Promise<HeapFileTableInfer<SchemaT> | null> {
    const existing = this.tables.get(`${db}.${schema.name}`)
    if (existing != null) return existing as HeapFileTableInfer<SchemaT>
    const storage = await this.getTableStorage(db, schema)
    if (storage == null) return null
    const table = new Table(storage.storage)
    this.tables.set(`${db}.${schema.name}`, table)
    return table as HeapFileTableInfer<SchemaT>
  }

  async getOrCreateTable<SchemaT extends SomeTableSchema>(
    db: string,
    schema: SchemaT,
  ): Promise<HeapFileTableInfer<SchemaT>> {
    const table = await this.getTable(db, schema)
    if (table != null) return table
    return this.createTable(db, schema)
  }

  async dropTable(db: string, name: string) {
    const table = this.tables.get(`${db}.${name}`)
    if (table == null) {
      throw new Error(``)
    }
    await table.drop()
    this.tables.delete(`${db}.${name}`)
  }
}

export function getTableConfig<
  N extends string,
  C extends StoredColumnRecord,
  CC extends ComputedColumnRecord,
>(
  bufferPool: IBufferPool,
  db: string,
  schema: TableSchema<N, C, CC>,
  tableRecord: Pick<TableRecord, "id" | "heapPageId">,
  indexManager?: IndexManager,
  droppable?: Droppable,
): TableConfig<
  HeapFileRowId,
  N,
  C,
  CC,
  HeapFileTableStorage<StoredRecordForTableSchema<TableSchema<N, C, CC>>>
> {
  const recordStruct = makeTableSchemaStruct(schema)
  if (recordStruct == null) {
    throw new Error("Schema is not serializable")
  }

  const data = new HeapFileTableStorage<
    StoredRecordForTableSchema<TableSchema<N, C, CC>>
  >(
    bufferPool,
    tableRecord.heapPageId,
    recordStruct as any,
    /* schemaId = */ 0,
  )

  const indexProvider = new HeapFileBackedIndexProvider(
    bufferPool,
    db,
    schema,
    tableRecord.id,
    data,
    indexManager,
  )

  return {
    data,
    schema,
    indexProvider,
    droppable: new Droppable(async () => {
      await data.drop()
      await indexProvider.drop()
      await droppable?.drop()
    }),
  }
}

function getColumnRecordsForSchema<SchemaT extends SomeTableSchema>(
  schema: SchemaT,
) {
  return schema.columns.map((column, i) => ({
    name: column.name,
    unique: column.isUnique,
    indexed: column.indexed.shouldIndex,
    indexInMemory: column.indexed.shouldIndex && column.indexed.inMemory,
    computed: false,
    type: column.type.name,
    order: i,
  }))
}
