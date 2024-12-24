import { Struct } from "../binary/Struct.ts"
import { readBytesAt, writeBytesAt } from "../io.ts"
import {
  FileBackedBufferPool,
  IBufferPool,
  InMemoryBufferPool,
  LocalStorageBackedBufferPool,
  PageId,
} from "../pages/BufferPool.ts"
import { getColumnTypeFromString } from "../schema/columns/ColumnType.ts"
import {
  SomeTableSchema,
  StoredRecordForTableSchema,
  TableSchema,
} from "../schema/TableSchema.ts"
import { Table } from "../tables/Table.ts"
import { HeapFileTableInfer } from "../tables/TableStorage.ts"
import { ReadonlyDataView } from "../binary/dataview.ts"
import { debugLog } from "../logging.ts"
import { SYSTEM_DB } from "./metadataSchemas.ts"
import * as schemas from "./metadataSchemas.ts"

import { IndexManager } from "./IndexManager.ts"
import { getTableConfig, TableManager } from "./TableManager.ts"
import { DBSchema, IDBSchema } from "../schema/DBSchema.ts"
import type { Simplify } from "type-fest"
import { Json } from "../types.ts"
import { IndexedDBBackedBufferPool } from "../pages/IndexedDBBackedBufferPool.ts"
import { IndexedDBWrapper } from "../pages/IndexedDbWrapper.ts"

const headerStruct = Struct.record({
  pageSize: [0, Struct.uint32],
  headerPageId: [1, Struct.bigUint64],
})

export class DbFile {
  private constructor(
    readonly bufferPool: IBufferPool,
    readonly dbPageIdsTable: HeapFileTableInfer<typeof schemas.dbPageIds>,
    readonly indexManager: IndexManager,
    readonly tableManager: TableManager,
  ) {}

  async getSchemasTable(): Promise<{
    schemaTable: HeapFileTableInfer<typeof schemas.dbSchemas>
    columnsTable: HeapFileTableInfer<typeof schemas.dbTableColumns>
  }> {
    let schemaTable = await this.tableManager.getTable(
      SYSTEM_DB,
      schemas.dbSchemas,
    )
    let columnsTable = await this.tableManager.getTable(
      SYSTEM_DB,
      schemas.dbTableColumns,
    )
    if (schemaTable == null || columnsTable == null) {
      schemaTable = await this.tableManager.createTable(
        SYSTEM_DB,
        schemas.dbSchemas,
      )
      columnsTable = await this.tableManager.createTable(
        SYSTEM_DB,
        schemas.dbTableColumns,
      )
    }
    return { schemaTable, columnsTable }
  }

  async importRecords(
    rows: AsyncIterable<{ db: string; table: string; record: Json }>,
  ) {
    for await (const row of rows) {
      const schemas = await this.getSchemasOrThrow(row.db, row.table)
      const table = await this.tableManager.getTable(row.db, schemas[0].schema)
      if (table == null) {
        throw new Error(`Table ${row.db}.${row.table} not found`)
      }
      const record = table.data.recordStruct.fromJSON(row.record)
      await table.insert(record)
    }
  }

  async *exportRecords(
    filter: { db?: string; table?: string } = {},
  ): AsyncGenerator<
    {
      table: string
      db: string
      record: Json
    },
    void,
    unknown
  > {
    debugLog("DbFile.export()")
    const tables = await this.tableManager.tablesTable.iterate().toArray()
    for (const tableRecord of tables) {
      if (filter.db && tableRecord.db !== filter.db) {
        continue
      }
      if (filter.table && tableRecord.name !== filter.table) {
        continue
      }
      debugLog("  -> Exporting", tableRecord)
      const schemas = await this.getSchemasOrThrow(
        tableRecord.db,
        tableRecord.name,
      )
      if (schemas.length === 0) {
        console.warn("No schema found for", tableRecord)
        continue
      }
      const table = await this.tableManager.getTable(
        tableRecord.db,
        schemas[0].schema,
      )
      if (table == null) {
        throw new Error(
          `Table ${tableRecord.db}.${schemas[0].schema.name} not found`,
        )
      }
      for await (const [_rowId, record] of table.data.iterate()) {
        const json = table.data.recordStruct.toJSON(record)
        yield { table: tableRecord.name, db: tableRecord.db, record: json }
      }
    }
  }

  static async open(config: StorageConfig): Promise<DbFile> {
    let storageLayer: StorageLayer
    if (config.type === "file") {
      storageLayer = await openFile(config)
    } else if (config.type === "memory") {
      const bufferPool = new InMemoryBufferPool(4096)
      const headerPageId = bufferPool.allocatePage()
      storageLayer = {
        bufferPool: bufferPool,
        headerPageId: headerPageId,
        needsCreation: true,
      }
    } else if (config.type === "localstorage") {
      const prefix = config.prefix ?? "pauldb"
      const bufferPool = new LocalStorageBackedBufferPool(prefix)
      const header = localStorage.getItem(`${prefix}-header`)
      if (header == null) {
        const headerPageId = bufferPool.allocatePage()
        localStorage.setItem(`${prefix}-header`, headerPageId.toString())
        storageLayer = {
          bufferPool,
          headerPageId,
          needsCreation: true,
        }
      } else {
        storageLayer = {
          bufferPool,
          headerPageId: BigInt(header),
          needsCreation: false,
        }
      }
    } else if (config.type === "indexeddb") {
      const prefix = config.name
      const wrapper = await IndexedDBWrapper.open(prefix, config.indexedDB)
      const bufferPool = await IndexedDBBackedBufferPool.create(wrapper)
      const header = await wrapper.getKeyVal<bigint>("header")
      if (header == null) {
        const headerPageId = bufferPool.allocatePage()
        await wrapper.setKeyVal("header", headerPageId)
        storageLayer = {
          bufferPool,
          headerPageId,
          needsCreation: true,
        }
      } else {
        storageLayer = {
          bufferPool,
          headerPageId: header,
          needsCreation: false,
        }
      }
    } else {
      throw new Error(`Unknown storage type`)
    }

    const { bufferPool, headerPageId, needsCreation } = storageLayer

    const dbPageIdsTable = new Table(
      getTableConfig(
        bufferPool,
        SYSTEM_DB,
        schemas.dbPageIds,
        { heapPageId: headerPageId, id: "$dbPageIds" },
      ),
    )
    async function getOrCreatePageIdForPageType(pageType: string) {
      const page = await dbPageIdsTable.lookupUnique("pageType", pageType)
      let pageId = page?.pageId
      if (pageId == null) {
        pageId = await bufferPool.allocatePage()
        await bufferPool.commit()
        await dbPageIdsTable.insert({ pageId, pageType })
      }
      return pageId
    }
    const dbIndexesTable = new Table(
      getTableConfig(
        bufferPool,
        SYSTEM_DB,
        schemas.dbIndexes,
        {
          heapPageId: await getOrCreatePageIdForPageType("indexesTable"),
          id: "$dbIndexes",
        },
      ),
    )
    const dbTablesTable = new Table(
      getTableConfig(
        bufferPool,
        SYSTEM_DB,
        schemas.dbTables,
        {
          heapPageId: await getOrCreatePageIdForPageType("tablesTable"),
          id: "$dbTables",
        },
      ),
    )
    const indexManager = new IndexManager(dbIndexesTable)
    const dbFile = new DbFile(
      bufferPool,
      dbPageIdsTable,
      indexManager,
      new TableManager(bufferPool, dbTablesTable, indexManager),
    )
    if (needsCreation) {
      await dbTablesTable.insertMany([
        {
          id: `${SYSTEM_DB}.__dbPageIds`,
          db: SYSTEM_DB,
          name: "__dbPageIds",
          heapPageId: headerPageId,
        },
      ])
      await dbFile.getSchemasTable()
    }

    return dbFile
  }

  close() {
    this.bufferPool.close?.()
  }

  [Symbol.dispose](): void {
    this.close()
  }

  getOrCreateTable<SchemaT extends SomeTableSchema>(
    schema: SchemaT,
    { db = "default" }: { db?: string } = {},
  ): Promise<HeapFileTableInfer<SchemaT>> {
    return this.tableManager.getOrCreateTable(db, schema)
  }

  async getDBModel<DBSchemaT extends DBSchema>(
    dbSchema: DBSchemaT,
  ): Promise<DBModel<DBSchemaT>> {
    const tables: Record<string, HeapFileTableInfer<SomeTableSchema>> = {}
    for (const schema of Object.values(dbSchema.schemas)) {
      tables[schema.name] = await this.getOrCreateTable(schema, {
        db: dbSchema.name,
      })
    }
    return tables as {
      [K in keyof DBSchemaT["schemas"]]: HeapFileTableInfer<
        DBSchemaT["schemas"][K]
      >
    }
  }

  async getSchemasOrThrow(db: string, tableName: string): Promise<{
    schema: SomeTableSchema
    columnRecords: StoredRecordForTableSchema<typeof schemas.dbTableColumns>[]
    schemaRecord: StoredRecordForTableSchema<typeof schemas.dbSchemas>
  }[]> {
    const schemasData = await this.getSchemas(db, tableName)
    if (schemasData == null) {
      throw new Error(`Table ${db}.${tableName} not found`)
    }
    return schemasData
  }

  async getSchemas(db: string, tableName: string): Promise<
    | null
    | {
      schema: SomeTableSchema
      columnRecords: StoredRecordForTableSchema<typeof schemas.dbTableColumns>[]
      schemaRecord: StoredRecordForTableSchema<typeof schemas.dbSchemas>
    }[]
  > {
    const tableRecord = await this.tableManager.tablesTable.lookupUnique(
      "_db_name",
      {
        db,
        name: tableName,
      },
    )
    if (tableRecord == null) {
      return null
    }
    const schemaTable = await this.getSchemasTable()
    const schemaRecords = await schemaTable.schemaTable.scan(
      "tableId",
      tableRecord.id,
    )
    return await Promise.all(schemaRecords.map(async (schemaRecord) => {
      const columnRecords = await schemaTable.columnsTable.scan(
        "schemaId",
        schemaRecord.id,
      )
      columnRecords.sort((a, b) => a.order - b.order)

      let schema: SomeTableSchema = TableSchema.create(tableName)
      for (const columnRecord of columnRecords) {
        schema = schema.with({
          kind: "stored",
          name: columnRecord.name,
          type: getColumnTypeFromString(columnRecord.type),
          isUnique: columnRecord.unique,
          indexed: columnRecord.indexed
            ? {
              shouldIndex: true,
              order: 2,
              storage: columnRecord.indexInMemory ? "memory" : "disk",
            }
            : { shouldIndex: false },
          defaultValueFactory: undefined,
        })
      }

      return { schema, columnRecords, schemaRecord }
    }))
  }

  async renameTable(
    oldTableName: string,
    newTableName: string,
    { db = "default" }: { db?: string } = {},
  ): Promise<void> {
    return await this.tableManager.renameTable(db, oldTableName, newTableName)
  }

  async migrate<M extends Migration>(migration: M): Promise<M> {
    // TODO: Lock the database
    const migrations = await this.getOrCreateTable(schemas.dbMigrations, {
      db: SYSTEM_DB,
    })
    const existingMigration = await migrations.lookupUnique(
      "name",
      migration.name,
    )
    if (existingMigration) {
      return migration
    }
    await migration.migrate(this)
    await migrations.insert({
      name: migration.name,
      db: migration.db,
      completedAt: new Date(),
    })
    return migration
  }
}

type Migration = {
  db: string
  name: string
  migrate: (db: DbFile) => Promise<void>
}

/**
 * A model for reading and writing to various tables in a database
 */
export type DBModel<DBSchemaT extends IDBSchema> = Simplify<
  {
    [K in keyof DBSchemaT["schemas"]]: HeapFileTableInfer<
      DBSchemaT["schemas"][K]
    >
  }
>

export type StorageConfig = {
  type: "file"
  path: string
  create?: boolean
  truncate?: boolean
} | {
  type: "memory"
} | {
  type: "localstorage"
  prefix?: string
} | {
  type: "indexeddb"
  name: string
  indexedDB?: IDBFactory
}

async function openFile(
  { path, create = false, truncate = false }: Extract<
    StorageConfig,
    { type: "file" }
  >,
): Promise<StorageLayer> {
  const file = await Deno.open(path, {
    read: true,
    write: true,
    create,
    truncate,
  })

  let bufferPool: FileBackedBufferPool
  let headerPageId: PageId

  /** Where the buffer pool starts in the file */
  const bufferPoolOffset = headerStruct.size

  const fileInfo = await file.stat()
  const needsCreation = fileInfo.size === 0
  if (needsCreation) {
    if (!create && !truncate) {
      throw new Error(
        "File is empty and neither create nor truncate flag were set",
      )
    }
    // write the header
    const pageSize: number = 4096
    await writeBytesAt(
      file,
      0,
      headerStruct.toUint8Array({
        pageSize: pageSize,
        headerPageId: 0n,
      }),
    )

    bufferPool = await FileBackedBufferPool.create(
      file,
      pageSize,
      bufferPoolOffset,
    )
    headerPageId = await bufferPool.allocatePage()
    await bufferPool.commit()
    await writeBytesAt(
      file,
      0,
      headerStruct.toUint8Array({
        pageSize: pageSize,
        headerPageId: headerPageId,
      }),
    )
  } else {
    // read the header
    const view = new ReadonlyDataView(
      (await readBytesAt(file, 0, headerStruct.size)).buffer,
    )
    const header = headerStruct.readAt(view, 0)
    const { pageSize: pageSize } = header
    headerPageId = header.headerPageId
    bufferPool = await FileBackedBufferPool.create(
      file,
      pageSize,
      bufferPoolOffset,
    )
  }

  return {
    bufferPool,
    headerPageId,
    needsCreation,
  }
}

type StorageLayer = {
  bufferPool: IBufferPool
  headerPageId: PageId
  needsCreation: boolean
}
