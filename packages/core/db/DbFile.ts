import { ulid } from "jsr:@std/ulid"
import { Struct } from "../binary/Struct.ts"
import { readBytesAt, writeBytesAt } from "../io.ts"
import { FileBackedBufferPool, PageId } from "../pages/BufferPool.ts"
import { ColumnTypes, getColumnTypeFromString } from "../schema/ColumnType.ts"
import {
  SomeTableSchema,
  StoredRecordForTableSchema,
  TableSchema,
} from "../schema/schema.ts"
import { Table, TableInfer } from "../tables/Table.ts"
import {
  HeapFileTableInfer,
  HeapFileTableStorage,
} from "../tables/TableStorage.ts"
import { ReadonlyDataView } from "../binary/dataview.ts"
import { debugLog } from "../logging.ts"
import { column } from "../schema/ColumnSchema.ts"

const SYSTEM_DB = "system"

const headerStruct = Struct.record({
  pageSize: [0, Struct.uint32],
  headerPageId: [1, Struct.bigUint64],
  pageTypeIndexPageId: [2, Struct.bigUint64],
  dbTables_id_IndexPageId: [3, Struct.bigUint64],
  dbTables_db_name_IndexPageId: [4, Struct.bigUint64],
  dbIndexesIndexPageId: [5, Struct.bigUint64],
})

const dbPageIdsTableSchema = TableSchema.create("__dbPageIds")
  .with(column("pageType", ColumnTypes.string()).unique())
  .with(column("pageId", ColumnTypes.uint64()))

const ulidIdColumn = column("id", ColumnTypes.string()).unique()
  .defaultTo(() => ulid())

const dbTablesTableSchema = TableSchema.create("__dbTables")
  .with(ulidIdColumn)
  .with(column("db", ColumnTypes.string()))
  .with(column("name", ColumnTypes.string()))
  .with(column("heapPageId", ColumnTypes.uint64()))
  .withUniqueConstraint(
    "_db_name",
    ColumnTypes.string(),
    ["db", "name"],
    (input: { db: string; name: string }) => `${input.db}.${input.name}`,
  )

const dbIndexesTableSchema = TableSchema.create("__dbIndexes")
  .with(column("indexName", ColumnTypes.string()).unique())
  .with(column("heapPageId", ColumnTypes.uint64()))

const dbSchemasTableSchema = TableSchema.create("__dbSchemas")
  .with(ulidIdColumn)
  .with(column("tableId", ColumnTypes.string()))
  .with(column("version", ColumnTypes.uint32()))
  .withUniqueConstraint(
    "tableId_version",
    ColumnTypes.string(),
    ["tableId", "version"],
    (input) => `${input.tableId}@${input.version}`,
  )

const dbTableColumnsTableSchema = TableSchema.create("__dbTableColumns")
  .with(ulidIdColumn)
  .with(column("schemaId", ColumnTypes.string()).index())
  .with(column("name", ColumnTypes.string()))
  .with(column("type", ColumnTypes.string()))
  .with(column("unique", ColumnTypes.boolean()))
  .with(column("indexed", ColumnTypes.boolean()))
  .with(column("computed", ColumnTypes.boolean()))
  .with(column("order", ColumnTypes.uint16()))
  .withUniqueConstraint("schemaId_name", ColumnTypes.string(), [
    "schemaId",
    "name",
  ], (input) => `${input.schemaId}.${input.name}`)

export class DbFile {
  private constructor(
    private file: Deno.FsFile,
    readonly bufferPool: FileBackedBufferPool,
    readonly dbPageIdsTable: TableInfer<
      typeof dbPageIdsTableSchema,
      HeapFileTableStorage<
        StoredRecordForTableSchema<typeof dbPageIdsTableSchema>
      >
    >,
    readonly indexesTable: TableInfer<
      typeof dbIndexesTableSchema,
      HeapFileTableStorage<
        StoredRecordForTableSchema<typeof dbIndexesTableSchema>
      >
    >,
    readonly tablesTable: TableInfer<
      typeof dbTablesTableSchema,
      HeapFileTableStorage<
        StoredRecordForTableSchema<typeof dbTablesTableSchema>
      >
    >,
  ) {}

  async getIndexStorage(
    tableName: string,
    columnName: string,
  ): Promise<PageId> {
    const indexName = `${tableName}_${columnName}`
    const indexRecord = await this.indexesTable.lookupUnique(
      "indexName",
      indexName,
    )
    let pageId = indexRecord?.heapPageId
    if (pageId == null) {
      pageId = await this.bufferPool.allocatePage()
      await this.bufferPool.commit()
      await this.indexesTable.insert({ indexName, heapPageId: pageId })
    }
    return pageId
  }

  /**
   * Gets table storage, while lazily creating the table if it doesn't exist.
   * Note: ths does not store the schema metadata.
   */
  private async _getTableStorage<SchemaT extends SomeTableSchema>(
    schema: SchemaT,
    db: string,
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
    return {
      tableRecord,
      created,
      storage: await HeapFileTableStorage.open(
        this,
        this.bufferPool,
        schema,
        tableRecord.heapPageId,
      ),
    }
  }

  private async writeSchemaMetadata<SchemaT extends SomeTableSchema>(
    db: string,
    schema: SchemaT,
    schemaTable: HeapFileTableInfer<typeof dbSchemasTableSchema>,
    columnsTable: HeapFileTableInfer<typeof dbTableColumnsTableSchema>,
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
    const schemaRecord = await schemaTable.insertAndReturn({
      id: `${table.id}@0`,
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
      dbSchemasTableSchema,
      SYSTEM_DB,
    )
    const schemaTable = new Table(schemaTableStorage.storage)
    const columnsTableStorage = await this._getTableStorage(
      dbTableColumnsTableSchema,
      SYSTEM_DB,
    )
    const columnsTable = new Table(columnsTableStorage.storage)
    if (schemaTableStorage.created) {
      await this.writeSchemaMetadata(
        SYSTEM_DB,
        dbSchemasTableSchema,
        schemaTable,
        columnsTable,
      )
      await this.writeSchemaMetadata(
        SYSTEM_DB,
        dbTableColumnsTableSchema,
        schemaTable,
        columnsTable,
      )
    }
    return { schemaTable, columnsTable }
  }

  async getTableStorage<SchemaT extends SomeTableSchema>(
    schema: SchemaT,
    db: string = "default",
  ) {
    const { created, storage } = await this._getTableStorage(schema, db)
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
      const existingSchemas = await this.getSchemas(db, schema.name)
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
      const schemas = await this.getSchemas(tableRecord.db, tableRecord.name)
      if (schemas.length === 0) {
        console.warn("No schema found for", tableRecord)
        continue
      }
      const storage = await this.getTableStorage(
        schemas[0].schema,
        tableRecord.db,
      )
      for await (const [_rowId, record] of storage.data.iterate()) {
        const json = storage.data.serializer.toJSON(record)
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
    let pageTypeIndexPageId: PageId
    let dbTables_id_IndexPageId: PageId
    let dbTables_db_name_IndexPageId: PageId
    let dbIndexesIndexPageId: PageId

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
          pageTypeIndexPageId: 0n,
          dbTables_id_IndexPageId: 0n,
          dbTables_db_name_IndexPageId: 0n,
          dbIndexesIndexPageId: 0n,
        }),
      )

      bufferPool = await FileBackedBufferPool.create(
        file,
        pageSize,
        bufferPoolOffset,
      )
      headerPageId = await bufferPool.allocatePage()
      pageTypeIndexPageId = await bufferPool.allocatePage()
      dbTables_id_IndexPageId = await bufferPool.allocatePage()
      dbTables_db_name_IndexPageId = await bufferPool.allocatePage()
      dbIndexesIndexPageId = await bufferPool.allocatePage()
      await bufferPool.commit()
      await writeBytesAt(
        file,
        0,
        headerStruct.toUint8Array({
          pageSize: pageSize,
          headerPageId: headerPageId,
          pageTypeIndexPageId: pageTypeIndexPageId,
          dbTables_id_IndexPageId: dbTables_id_IndexPageId,
          dbTables_db_name_IndexPageId: dbTables_db_name_IndexPageId,
          dbIndexesIndexPageId: dbIndexesIndexPageId,
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
      pageTypeIndexPageId = header.pageTypeIndexPageId
      dbTables_id_IndexPageId = header.dbTables_id_IndexPageId
      dbTables_db_name_IndexPageId = header.dbTables_db_name_IndexPageId
      dbIndexesIndexPageId = header.dbIndexesIndexPageId
      bufferPool = await FileBackedBufferPool.create(
        file,
        pageSize,
        bufferPoolOffset,
      )
    }

    const dbPageIdsTable = new Table(
      await HeapFileTableStorage.__openWithIndexPageIds(
        bufferPool,
        dbPageIdsTableSchema,
        headerPageId,
        { pageType: pageTypeIndexPageId },
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
      await HeapFileTableStorage.__openWithIndexPageIds(
        bufferPool,
        dbIndexesTableSchema,
        await getOrCreatePageIdForPageType("indexesTable"),
        { indexName: dbIndexesIndexPageId },
      ),
    )
    const dbTablesTable = new Table(
      await HeapFileTableStorage.__openWithIndexPageIds(
        bufferPool,
        dbTablesTableSchema,
        await getOrCreatePageIdForPageType("tablesTable"),
        { id: dbTables_id_IndexPageId, _db_name: dbTables_db_name_IndexPageId },
      ),
    )
    const dbFile = new DbFile(
      file,
      bufferPool,
      dbPageIdsTable,
      dbIndexesTable,
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
        {
          id: `${SYSTEM_DB}.__dbIndexes`,
          db: SYSTEM_DB,
          name: "__dbIndexes",
          heapPageId: dbIndexesIndexPageId,
        },
        {
          id: `${SYSTEM_DB}.__dbTables`,
          db: SYSTEM_DB,
          name: "__dbTables",
          heapPageId: dbTables_id_IndexPageId,
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

  async createTable<SchemaT extends SomeTableSchema>(
    schema: SchemaT,
    db: string = "default",
  ) {
    const storage = await this.getTableStorage(schema, db)
    return new Table(storage)
  }

  async getSchemas(db: string, tableName: string) {
    const tableRecord = await this.tablesTable.lookupUnique("_db_name", {
      db,
      name: tableName,
    })
    if (tableRecord == null) {
      throw new Error(`No table found named ${tableName}`)
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
        if (columnRecord.name === "id") {
          // THIS IS A HACK.
          // the schema comes with a default primary key column
          // TODO: handle the default primary key column better
          continue
        }
        schema = schema.with({
          kind: "stored",
          name: columnRecord.name,
          type: getColumnTypeFromString(columnRecord.type),
          isUnique: columnRecord.unique,
          indexed: columnRecord.indexed
            ? {
              shouldIndex: true,
              order: 2,
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
    computed: false,
    type: column.type.name,
    order: i,
  }))
}
