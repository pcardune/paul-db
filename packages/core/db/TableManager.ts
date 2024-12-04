import { IBufferPool, PageId } from "../pages/BufferPool.ts"
import { Column } from "../schema/ColumnSchema.ts"
import {
  makeTableSchemaStruct,
  SomeTableSchema,
  StoredRecordForTableSchema,
} from "../schema/schema.ts"
import { Table, TableConfig } from "../tables/Table.ts"
import {
  HeapFileRowId,
  HeapFileTableInfer,
  HeapFileTableStorage,
  ITableStorage,
} from "../tables/TableStorage.ts"
import { UnknownRecord } from "npm:type-fest"
import { schemas, SYSTEM_DB } from "./metadataSchemas.ts"
import { IndexManager } from "./IndexManager.ts"
import { DBFileSerialIdGenerator } from "../serial.ts"
import { HeapFileBackedIndexProvider } from "../indexes/IndexProvider.ts"

export class TableManager {
  private tables = new Map<
    string,
    Table<
      HeapFileRowId,
      string,
      Column.Stored[],
      Column.Computed.Any[],
      SomeTableSchema,
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

  async hasTable(db: string, name: string) {
    const tableRecord = await this.tablesTable.lookupUnique("_db_name", {
      db,
      name,
    })
    return tableRecord != null
  }

  private async createTableRecord(db: string, name: string) {
    const pageId = await this.bufferPool.allocatePage()
    await this.bufferPool.commit()
    const tableRecord = await this.tablesTable.insertAndReturn({
      id: `${db}.${name}`,
      name: name,
      heapPageId: pageId,
      db,
    })
    return tableRecord
  }

  async getTableStorage<SchemaT extends SomeTableSchema>(
    db: string,
    schema: SchemaT,
    schemaId: number,
  ) {
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
          tableRecord.heapPageId,
          this.indexManager,
          schemaId,
        ),
        serialIdGenerator: new DBFileSerialIdGenerator(
          this,
          `${db}.${schema.name}`,
        ),
      },
    }
  }

  async getOrCreateTableStorage<SchemaT extends SomeTableSchema>(
    db: string,
    schema: SchemaT,
    schemaId: number,
  ) {
    let storage = await this.getTableStorage(db, schema, schemaId)
    let created = false
    if (storage == null) {
      await this.createTableRecord(db, schema.name)
      storage = (await this.getTableStorage(db, schema, schemaId))!
      created = true
    }
    return { ...storage, created }
  }

  async createTable<SchemaT extends SomeTableSchema>(
    db: string,
    schema: SchemaT,
  ) {
    const tableRecord = await this.createTableRecord(db, schema.name)
    const { storage } = (await this.getTableStorage(db, schema, 0))!
    const table = new Table(storage)
    this.tables.set(`${db}.${schema.name}`, table)

    const schemaTable = await this.getTable(SYSTEM_DB, schemas.dbSchemas)
    const columnsTable = await this.getTable(SYSTEM_DB, schemas.dbTableColumns)
    if (schemaTable == null || columnsTable == null) {
      return table
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

    return table
  }

  async getTable<SchemaT extends SomeTableSchema>(
    db: string,
    schema: SchemaT,
  ): Promise<HeapFileTableInfer<SchemaT> | null> {
    const existing = this.tables.get(`${db}.${schema.name}`)
    if (existing != null) return existing as HeapFileTableInfer<SchemaT>
    const storage = await this.getTableStorage(db, schema, 0)
    if (storage == null) return null
    const table = new Table(storage.storage)
    this.tables.set(`${db}.${schema.name}`, table)
    return table as HeapFileTableInfer<SchemaT>
  }

  async getOrCreateTable<SchemaT extends SomeTableSchema>(
    db: string,
    schema: SchemaT,
  ) {
    const table = await this.getTable(db, schema)
    if (table != null) return table
    return this.createTable(db, schema)
  }
}

export function getTableConfig<SchemaT extends SomeTableSchema>(
  bufferPool: IBufferPool,
  db: string,
  schema: SchemaT,
  heapPageId: PageId,
  indexManager?: IndexManager,
  schemaId: number = 0,
): TableConfig<
  HeapFileRowId,
  SchemaT,
  HeapFileTableStorage<StoredRecordForTableSchema<SchemaT>>
> {
  const recordStruct = makeTableSchemaStruct(schema)
  if (recordStruct == null) {
    throw new Error("Schema is not serializable")
  }

  const data = new HeapFileTableStorage<StoredRecordForTableSchema<SchemaT>>(
    bufferPool,
    heapPageId,
    recordStruct,
    schemaId,
  )

  return {
    data,
    schema,
    indexProvider: new HeapFileBackedIndexProvider(
      bufferPool,
      db,
      schema,
      data,
      indexManager,
    ),
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
