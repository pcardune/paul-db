import { Struct } from "../binary/Struct.ts"
import { readBytesAt, writeBytesAt } from "../io.ts"
import { FileBackedBufferPool, PageId } from "../pages/BufferPool.ts"
import { getColumnTypeFromString } from "../schema/columns/ColumnType.ts"
import { SomeTableSchema, TableSchema } from "../schema/schema.ts"
import { Table } from "../tables/Table.ts"
import { HeapFileTableInfer } from "../tables/TableStorage.ts"
import { ReadonlyDataView } from "../binary/dataview.ts"
import { debugLog } from "../logging.ts"
import { schemas, SYSTEM_DB } from "./metadataSchemas.ts"
import { IndexManager } from "./IndexManager.ts"
import { getTableConfig, TableManager } from "./TableManager.ts"

const headerStruct = Struct.record({
  pageSize: [0, Struct.uint32],
  headerPageId: [1, Struct.bigUint64],
})

export class DbFile {
  private constructor(
    private file: Deno.FsFile,
    readonly bufferPool: FileBackedBufferPool,
    readonly dbPageIdsTable: HeapFileTableInfer<typeof schemas.dbPageIds>,
    readonly indexManager: IndexManager,
    readonly tableManager: TableManager,
  ) {}

  async getSchemasTable() {
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

  async *export(filter: { db?: string; table?: string } = {}) {
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

  static async open(
    path: string,
    { create = false, truncate = false }: {
      create?: boolean
      truncate?: boolean
    } = {},
  ) {
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
      file,
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
    this.file.close()
  }

  [Symbol.dispose](): void {
    this.close()
  }

  getOrCreateTable<SchemaT extends SomeTableSchema>(
    schema: SchemaT,
    { db = "default" }: { db?: string } = {},
  ) {
    return this.tableManager.getOrCreateTable(db, schema)
  }

  async getSchemasOrThrow(db: string, tableName: string) {
    const schemas = await this.getSchemas(db, tableName)
    if (schemas == null) {
      throw new Error(`Table ${db}.${tableName} not found`)
    }
    return schemas
  }

  async getSchemas(db: string, tableName: string) {
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
  ) {
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
