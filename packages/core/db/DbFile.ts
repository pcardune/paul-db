import { ulid } from "jsr:@std/ulid"
import { Struct } from "../binary/Struct.ts"
import { readBytesAt, writeBytesAt } from "../io.ts"
import { FileBackedBufferPool, PageId } from "../pages/BufferPool.ts"
import { ColumnTypes, getColumnTypeFromString } from "../schema/ColumnType.ts"
import {
  column,
  ColumnSchema,
  SomeTableSchema,
  StoredRecordForTableSchema,
  TableSchema,
} from "../schema/schema.ts"
import { Table, TableInfer } from "../tables/Table.ts"
import {
  HeapFileTableInfer,
  HeapFileTableStorage,
} from "../tables/TableStorage.ts"

const SYSTEM_DB = "system"

const headerStruct = Struct.record({
  pageSize: [0, Struct.uint32],
  headerPageId: [1, Struct.bigUint64],
  pageTypeIndexPageId: [2, Struct.bigUint64],
  dbTables_id_IndexPageId: [3, Struct.bigUint64],
  dbTables_db_name_IndexPageId: [4, Struct.bigUint64],
  dbIndexesIndexPageId: [5, Struct.bigUint64],
})

const dbPageIdsTableSchema = TableSchema.create(
  "__dbPageIds",
  column("pageType", ColumnTypes.string()).makeUnique(),
)
  .withColumn(column("pageId", ColumnTypes.uint64()))

const ulidIdColumn = column("id", ColumnTypes.string()).makeUnique()
  .withDefaultValue(() => ulid())

const dbTablesTableSchema = TableSchema.create(
  "__dbTables",
  ulidIdColumn,
)
  .withColumn(column("db", ColumnTypes.string()))
  .withColumn(column("name", ColumnTypes.string()))
  .withColumn(column("heapPageId", ColumnTypes.uint64()))
  .withUniqueConstraint(
    "_db_name",
    ColumnTypes.string(),
    ["db", "name"],
    (input: { db: string; name: string }) => `${input.db}.${input.name}`,
  )

const dbIndexesTableSchema = TableSchema.create(
  "__dbIndexes",
  column("indexName", ColumnTypes.string()).makeUnique(),
)
  .withColumn(column("heapPageId", ColumnTypes.uint64()))

const dbSchemasTableSchema = TableSchema.create(
  "__dbSchemas",
  ulidIdColumn,
)
  .withColumn(column("tableId", ColumnTypes.string()))
  .withColumn(column("version", ColumnTypes.uint32()))
  .withUniqueConstraint(
    "tableId_version",
    ColumnTypes.string(),
    ["tableId", "version"],
    (input) => `${input.tableId}@${input.version}`,
  )

const dbTableColumnsTableSchema = TableSchema.create(
  "__dbTableColumns",
  ulidIdColumn,
)
  .withColumn(column("schemaId", ColumnTypes.string()).makeIndexed())
  .withColumn(column("name", ColumnTypes.string()))
  .withColumn(column("type", ColumnTypes.string()))
  .withColumn(column("unique", ColumnTypes.boolean()))
  .withColumn(column("indexed", ColumnTypes.boolean()))
  .withColumn(column("computed", ColumnTypes.boolean()))
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
      tableId: table.id,
      version: 0,
    })
    for (const column of schema.columns) {
      await columnsTable.insert({
        schemaId: schemaRecord.id,
        name: column.name,
        unique: column.unique,
        indexed: Boolean(column.indexed),
        computed: false,
        type: column.type.name,
      })
    }
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
    }
    return storage
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
      const view: DataView = new DataView(
        (await readBytesAt(file, 0, 4096)).buffer,
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
    if (needsCreation) {
      await dbTablesTable.insertMany([
        {
          id: "__dbPageIds",
          db: SYSTEM_DB,
          name: "__dbPageIds",
          heapPageId: headerPageId,
        },
        {
          id: "__dbIndexes",
          db: SYSTEM_DB,
          name: "__dbIndexes",
          heapPageId: dbIndexesIndexPageId,
        },
        {
          id: "__dbTables",
          db: SYSTEM_DB,
          name: "__dbTables",
          heapPageId: dbTables_id_IndexPageId,
        },
      ])
    }

    return new DbFile(
      file,
      bufferPool,
      dbPageIdsTable,
      dbIndexesTable,
      dbTablesTable,
    )
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

      let schema: SomeTableSchema = TableSchema.create(tableName)
      for (const columnRecord of columnRecords) {
        if (columnRecord.name === "id") {
          // THIS IS A HACK.
          // the schema comes with a default primary key column
          // TODO: handle the default primary key column better
          continue
        }
        schema = schema.withColumn(
          new ColumnSchema(
            columnRecord.name,
            getColumnTypeFromString(columnRecord.type),
            columnRecord.unique,
            columnRecord.indexed ? { order: 2 } : false,
          ),
        )
      }

      return { schema, columnRecords, schemaRecord: schemaRecord }
    }))
  }
}
