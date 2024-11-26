import { Struct } from "../binary/Struct.ts"
import { readBytesAt, writeBytesAt } from "../io.ts"
import { FileBackedBufferPool, PageId } from "../pages/BufferPool.ts"
import { ColumnTypes } from "../schema/ColumnType.ts"
import {
  column,
  SomeTableSchema,
  StoredRecordForTableSchema,
  TableSchema,
} from "../schema/schema.ts"
import { Table, TableInfer } from "../tables/Table.ts"
import { HeapFileTableStorage } from "../tables/TableStorage.ts"

const headerStruct = Struct.tuple(
  Struct.uint32, // pageId
  Struct.bigUint64, // headerPageId
  Struct.bigUint64, // __dbPageIds_pageType index pageId
  Struct.bigUint64, // __dbTables index pageId
  Struct.bigUint64, // __dbIndexes index pageId
)

const dbPageIdsTableSchema = TableSchema.create(
  "__dbPageIds",
  column("pageType", ColumnTypes.string()).makeUnique(),
)
  .withColumn(column("pageId", ColumnTypes.uint64()))

const dbTablesTableSchema = TableSchema.create(
  "__dbTables",
  column("tableName", ColumnTypes.string()).makeUnique(),
)
  .withColumn(column("heapPageId", ColumnTypes.uint64()))

const dbIndexesTableSchema = TableSchema.create(
  "__dbIndexes",
  column("indexName", ColumnTypes.string()).makeUnique(),
)
  .withColumn(column("heapPageId", ColumnTypes.uint64()))

export class DbFile {
  private constructor(
    private file: Deno.FsFile,
    readonly bufferPool: FileBackedBufferPool,
    private dbPageIdsTable: TableInfer<
      typeof dbPageIdsTableSchema,
      HeapFileTableStorage<
        StoredRecordForTableSchema<typeof dbPageIdsTableSchema>
      >
    >,
    private indexesTable: TableInfer<
      typeof dbIndexesTableSchema,
      HeapFileTableStorage<
        StoredRecordForTableSchema<typeof dbIndexesTableSchema>
      >
    >,
    private tablesTable: TableInfer<
      typeof dbTablesTableSchema,
      HeapFileTableStorage<
        StoredRecordForTableSchema<typeof dbTablesTableSchema>
      >
    >,
  ) {}

  private async getOrCreatePageIdForPageType(pageType: string) {
    const page = await this.dbPageIdsTable.lookupUnique("pageType", pageType)
    let pageId = page?.pageId
    if (pageId == null) {
      pageId = await this.bufferPool.allocatePage()
      await this.bufferPool.commit()
      await this.dbPageIdsTable.insert({ pageId, pageType })
    }
    return pageId
  }

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

  async getTableStorage<SchemaT extends SomeTableSchema>(schema: SchemaT) {
    const page = await this.tablesTable.lookupUnique("tableName", schema.name)
    let pageId = page?.heapPageId
    if (pageId == null) {
      pageId = await this.bufferPool.allocatePage()
      await this.bufferPool.commit()
      await this.tablesTable.insert({
        tableName: schema.name,
        heapPageId: pageId,
      })
    }
    return await HeapFileTableStorage.open(
      this,
      this.bufferPool,
      schema,
      pageId,
    )
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
    let dbTablesIndexPageId: PageId
    let dbIndexesIndexPageId: PageId

    /** Where the buffer pool starts in the file */
    const bufferPoolOffset = headerStruct.sizeof([1, 0n, 0n, 0n, 0n])

    const fileInfo = await file.stat()
    if (fileInfo.size === 0) {
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
        headerStruct.toUint8Array([pageSize, 0n, 0n, 0n, 0n]),
      )

      bufferPool = await FileBackedBufferPool.create(
        file,
        pageSize,
        bufferPoolOffset,
      )
      headerPageId = await bufferPool.allocatePage()
      pageTypeIndexPageId = await bufferPool.allocatePage()
      dbTablesIndexPageId = await bufferPool.allocatePage()
      dbIndexesIndexPageId = await bufferPool.allocatePage()
      await bufferPool.commit()
      await writeBytesAt(
        file,
        0,
        headerStruct.toUint8Array([
          pageSize,
          headerPageId,
          pageTypeIndexPageId,
          dbTablesIndexPageId,
          dbIndexesIndexPageId,
        ]),
      )
    } else {
      // read the header
      const view: DataView = new DataView(
        (await readBytesAt(file, 0, 4096)).buffer,
      )
      const header = headerStruct.readAt(view, 0)
      const pageSize = header[0]
      headerPageId = header[1]
      pageTypeIndexPageId = header[2]
      dbTablesIndexPageId = header[3]
      dbIndexesIndexPageId = header[4]
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
        { tableName: dbTablesIndexPageId },
      ),
    )

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
}
