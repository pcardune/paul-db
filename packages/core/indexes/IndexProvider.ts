import { Promisable, UnknownRecord } from "type-fest"
import { INodeId } from "./BTreeNode.ts"
import { Index } from "./Index.ts"
import {
  SomeTableSchema,
  StoredRecordForTableSchema,
} from "../schema/TableSchema.ts"
import {
  HeapFileRowId,
  heapFileRowIdStruct,
  HeapFileTableStorage,
} from "../tables/TableStorage.ts"
import { IBufferPool } from "../pages/BufferPool.ts"
import { Droppable, IDroppable } from "../droppable.ts"
import { IndexManager } from "../db/IndexManager.ts"

export interface IndexProvider<RowIdT> extends IDroppable {
  getIndexForColumn(
    column: string,
  ): Promisable<Index<unknown, RowIdT, INodeId> | null>
}

abstract class CachingIndexProvider<RowIdT> implements IndexProvider<RowIdT> {
  protected indexes: Map<string, Index<unknown, RowIdT, INodeId>>
  protected droppable: Droppable

  constructor() {
    this.indexes = new Map()
    this.droppable = new Droppable(async () => {
      for (const [key, index] of this.indexes.entries()) {
        await index.drop()
        this.indexes.delete(key)
      }
    })
  }

  drop() {
    return this.droppable.drop()
  }

  abstract getIndexForColumn(
    column: string,
  ): Promisable<Index<unknown, RowIdT, INodeId> | null>
}

export class InMemoryIndexProvider<RowIdT, SchemaT extends SomeTableSchema>
  extends CachingIndexProvider<RowIdT>
  implements IndexProvider<RowIdT> {
  constructor(private schema: SchemaT) {
    super()
  }

  getIndexForColumn(name: string): Index<unknown, RowIdT, INodeId> | null {
    this.droppable.assertNotDropped("IndexProvider has been dropped")
    let index = this.indexes.get(name) ?? null
    if (index == null) {
      const column = this.schema.getColumnByName(name)
      if (column == null || !column.indexed.shouldIndex) return null
      index = Index.inMemory({
        isEqual: column.type.isEqual,
        compare: column.type.compare,
      }) as Index<unknown, RowIdT, INodeId>
      this.indexes.set(name, index)
    }
    return index
  }
}

export class HeapFileBackedIndexProvider<
  SchemaT extends SomeTableSchema,
> extends CachingIndexProvider<HeapFileRowId>
  implements IndexProvider<HeapFileRowId> {
  constructor(
    private bufferPool: IBufferPool,
    private db: string,
    private schema: SchemaT,
    private tableId: string,
    private tableData: HeapFileTableStorage<
      StoredRecordForTableSchema<SchemaT>
    >,
    private indexManager?: IndexManager,
  ) {
    super()
  }

  override async drop(): Promise<void> {
    if (this.indexManager) {
      for (const column of this.indexes.keys()) {
        await this.indexManager.freeIndexStoragePageId({
          tableId: this.tableId,
          indexName: column,
        })
      }
    }
    await super.drop()
  }

  async getIndexForColumn(
    name: string,
  ): Promise<Index<unknown, HeapFileRowId, INodeId> | null> {
    this.droppable.assertNotDropped("IndexProvider has been dropped")
    {
      const index = this.indexes.get(name) ?? null
      if (index != null) return index
    }

    const column = this.schema.getColumnByName(name)
    if (column == null || !column.indexed.shouldIndex) return null

    if (column.indexed.inMemory) {
      const index = Index.inMemory({
        isEqual: column.type.isEqual,
        compare: column.type.compare,
        order: column.indexed.order,
      })
      // build the index
      await index.insertMany(
        await this.tableData.iterate().map(
          ([rowId, record]): [unknown, HeapFileRowId] => {
            if (column.kind === "computed") {
              return [column.compute(record), rowId]
            } else {
              return [(record as UnknownRecord)[column.name], rowId]
            }
          },
        ).toArray(),
      )
      this.indexes.set(column.name, index)
      return index
    }

    if (!column.type.serializer) {
      throw new Error("Type must have a serializer")
    }

    if (!this.indexManager) {
      throw new Error(
        "No index manager was provided. maybe we're bootstrapping?",
      )
    }

    const pageId = await this.indexManager.getOrAllocateIndexStoragePageId({
      tableId: this.tableId,
      indexName: column.name,
    })

    const index = await Index.inFile(
      this.bufferPool,
      pageId,
      column.type.serializer!,
      heapFileRowIdStruct,
      {
        isEqual: column.type.isEqual,
        compare: column.type.compare,
        order: column.indexed.order,
      },
    )
    this.indexes.set(
      column.name,
      index,
    )
    return index
  }
}
