import { PageId } from "../pages/BufferPool.ts"
import { HeapFileTableInfer } from "../tables/TableStorage.ts"
import { schemas } from "./metadataSchemas.ts"

export type IndexId = { tableId: string; indexName: string }

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
      "_tableId_indexName",
      id,
    )
    return record?.heapPageId ?? null
  }

  async freeIndexStoragePageId(id: IndexId) {
    await this.indexesTable.removeWhere("_tableId_indexName", id)
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
      indexName: id.indexName,
      tableId: id.tableId,
      heapPageId: pageId,
    })
    return pageId
  }
}
