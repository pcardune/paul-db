import { Struct } from "../binary/Struct.ts"
import { readBytesAt, writeBytesAt } from "../io.ts"
import {
  FileBackedBufferPool,
  IBufferPool,
  PageId,
} from "../pages/BufferPool.ts"
import { getColumnTypeFromString } from "../schema/ColumnType.ts"
import {
  makeTableSchemaStruct,
  SomeTableSchema,
  StoredRecordForTableSchema,
  TableSchema,
} from "../schema/schema.ts"
import { Table, TableConfig } from "../tables/Table.ts"
import {
  HeapFileRowId,
  HeapFileTableInfer,
  HeapFileTableStorage,
} from "../tables/TableStorage.ts"
import { ReadonlyDataView } from "../binary/dataview.ts"
import { debugLog } from "../logging.ts"
import { DBFileSerialIdGenerator } from "../serial.ts"
import { schemas } from "./metadataSchemas.ts"
import { IndexManager } from "./IndexManager.ts"
import { HeapFileBackedIndexProvider } from "../indexes/IndexProvider.ts"

const SYSTEM_DB = "system"

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
    readonly tablesTable: HeapFileTableInfer<typeof schemas.dbTables>,
  ) {}

  /**
   * Gets table storage, while lazily creating the table if it doesn't exist.
   * Note: ths does not store the schema metadata.
   */
  private async _getTableStorage<SchemaT extends SomeTableSchema>(
    schema: SchemaT,
    db: string,
    schemaId: number,
  ) {
    let tableRecord = await this.tablesTable.lookupUnique("_db_name", {
      db,
      name: schema.name,
    })
    let created = false
    if (tableRecord == null) {
      const pageId = await this.bufferPool.allocatePage()
      await this.bufferPool.commit()
      tableRecord = await this.tablesTable.insertAndReturn({
        id: `${db}.${schema.name}`,
        name: schema.name,
        heapPageId: pageId,
        db,
      })
      created = true
    }

    if (makeTableSchemaStruct(schema) == null) {
      throw new Error("Schema is not serializable")
    }

    return {
      tableRecord,
      created,
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

  private async writeSchemaMetadata<SchemaT extends SomeTableSchema>(
    db: string,
    schema: SchemaT,
    schemaTable: HeapFileTableInfer<typeof schemas.dbSchemas>,
    columnsTable: HeapFileTableInfer<typeof schemas.dbTableColumns>,
  ) {
    const table = await this.tablesTable.lookupUnique("_db_name", {
      db,
      name: schema.name,
    })
    if (table == null) {
      console.debug(
        "table records are",
        await this.tablesTable.iterate().toArray(),
      )
      throw new Error(`Table ${schema.name} not found`)
    }
    const existingIds = await schemaTable.iterate().map((s) => s.id).toArray()
    const schemaRecord = await schemaTable.insertAndReturn({
      // TODO: make id generation sane, but can't use serial,
      // because serial depends on this!
      id: existingIds.length === 0 ? 0 : Math.max(...existingIds) + 1,
      tableId: table.id,
      version: 0,
    })
    const records = getColumnRecordsForSchema(schema)
    await columnsTable.insertMany(records.map((record) => ({
      ...record,
      schemaId: schemaRecord.id,
    })))
  }

  async getSchemasTable() {
    const schemaTableStorage = await this._getTableStorage(
      schemas.dbSchemas,
      SYSTEM_DB,
      0,
    )
    const schemaTable = new Table(schemaTableStorage.storage)
    const columnsTableStorage = await this._getTableStorage(
      schemas.dbTableColumns,
      SYSTEM_DB,
      0,
    )
    const columnsTable = new Table(columnsTableStorage.storage)
    if (schemaTableStorage.created) {
      await this.writeSchemaMetadata(
        SYSTEM_DB,
        schemas.dbSchemas,
        schemaTable,
        columnsTable,
      )
      await this.writeSchemaMetadata(
        SYSTEM_DB,
        schemas.dbTableColumns,
        schemaTable,
        columnsTable,
      )
    }
    return { schemaTable, columnsTable }
  }

  private async getTableStorage<SchemaT extends SomeTableSchema>(
    db: string,
    schema: SchemaT,
  ) {
    const { created, storage } = await this._getTableStorage(
      schema,
      db,
      0,
    )
    if (created) {
      // table was just created, lets add the schema metadata as well
      const schemaTables = await this.getSchemasTable()
      await this.writeSchemaMetadata(
        db,
        schema,
        schemaTables.schemaTable,
        schemaTables.columnsTable,
      )
    } else {
      const existingSchemas = await this.getSchemasOrThrow(db, schema.name)
      if (existingSchemas.length === 0) {
        throw new Error(`No schema found for ${db}.${schema.name}`)
      }
      const existingColumns = existingSchemas[0].columnRecords
      const newColumns = getColumnRecordsForSchema(schema)
      for (const [i, existingColumn] of existingColumns.entries()) {
        const column = newColumns[i]
        if (column == null) {
          throw new Error("Column mismatch")
        }
        if (column.name !== existingColumn.name) {
          throw new Error(
            `Column name mismatch: ${column.name} !== ${existingColumn.name}`,
          )
        }
        if (column.type !== existingColumn.type) {
          throw new Error(
            `Column type mismatch: ${column.type} !== ${existingColumn.type}`,
          )
        }
        if (column.unique !== existingColumn.unique) {
          throw new Error(
            `Column isUnique mismatch: ${column.unique} !== ${existingColumn.unique}`,
          )
        }
        if (column.indexed !== existingColumn.indexed) {
          throw new Error(
            `Column indexed mismatch: ${column.indexed} !== ${existingColumn.indexed}`,
          )
        }
      }
      if (newColumns.length > existingColumns.length) {
        throw new Error(
          `Column length mismatch. Found new column(s) ${
            newColumns.slice(existingColumns.length).map((c) => `"${c.name}"`)
              .join(", ")
          }`,
        )
      }
    }
    return storage
  }

  async *export(filter: { db?: string; table?: string } = {}) {
    debugLog("DbFile.export()")
    const tables = await this.tablesTable.iterate().toArray()
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
      const storage = await this.getTableStorage(
        tableRecord.db,
        schemas[0].schema,
      )
      for await (const [_rowId, record] of storage.data.iterate()) {
        const json = storage.data.recordStruct.toJSON(record)
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
        headerPageId,
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
        await getOrCreatePageIdForPageType("indexesTable"),
      ),
    )
    const dbTablesTable = new Table(
      getTableConfig(
        bufferPool,
        SYSTEM_DB,
        schemas.dbTables,
        await getOrCreatePageIdForPageType("tablesTable"),
      ),
    )
    const dbFile = new DbFile(
      file,
      bufferPool,
      dbPageIdsTable,
      new IndexManager(dbIndexesTable),
      dbTablesTable,
    )
    if (needsCreation) {
      await dbFile.tablesTable.insertMany([
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

  async getOrCreateTable<SchemaT extends SomeTableSchema>(
    schema: SchemaT,
    { db = "default" }: { db?: string } = {},
  ) {
    const storage = await this.getTableStorage(db, schema)
    return new Table(storage)
  }

  async getSchemasOrThrow(db: string, tableName: string) {
    const schemas = await this.getSchemas(db, tableName)
    if (schemas == null) {
      throw new Error(`Table ${db}.${tableName} not found`)
    }
    return schemas
  }

  async getSchemas(db: string, tableName: string) {
    const tableRecord = await this.tablesTable.lookupUnique("_db_name", {
      db,
      name: tableName,
    })
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

function getTableConfig<SchemaT extends SomeTableSchema>(
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
