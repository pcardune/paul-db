import { PageId } from "../pages/BufferPool.ts"
import { HeapFileTableInfer } from "../tables/TableStorage.ts"
import { schemas } from "./metadataSchemas.ts"

export type IndexId = { db: string; table: string; column: string }

function indexName({ db, table, column }: IndexId) {
  return `${db}.${table}.${column}`
}

export class IndexManager {
  constructor(
    readonly indexesTable: HeapFileTableInfer<typeof schemas.dbIndexes>,
  ) {}

  /**
   * Gets the page id of the index storage page for the given index id,
   * or null if none has been allocated yet
   */
  async getIndexStoragePageId(id: IndexId): Promise<PageId | null> {
    const record = await this.indexesTable.lookupUnique(
      "indexName",
      indexName(id),
    )
    return record?.heapPageId ?? null
  }

  /**
   * Gets the page id of the index storage page for the given index id,
   * allocating a new page if necessary.
   */
  async getOrAllocateIndexStoragePageId(id: IndexId): Promise<PageId> {
    const existing = await this.getIndexStoragePageId(id)
    if (existing) {
      return existing
    }
    const pageId = await this.indexesTable.data.bufferPool.allocatePage()
    await this.indexesTable.insert({
      indexName: indexName(id),
      heapPageId: pageId,
    })
    return pageId
  }
}
