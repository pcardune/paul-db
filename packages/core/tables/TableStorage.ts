import { AsyncIterableWrapper } from "../async.ts"
import { IStruct, Struct } from "../binary/Struct.ts"
import { DbFile } from "../db/DbFile.ts"
import { INodeId } from "../indexes/BTreeNode.ts"
import { Index } from "../indexes/Index.ts"
import { IBufferPool, PageId } from "../pages/BufferPool.ts"
import { HeaderPageRef, HeapPageFile } from "../pages/HeapPageFile.ts"
import {
  ReadonlyVariableLengthRecordPage,
  VariableLengthRecordPageAllocInfo,
  WriteableVariableLengthRecordPage,
} from "../pages/VariableLengthRecordPage.ts"
import {
  makeTableSchemaSerializer,
  SomeTableSchema,
  StoredRecordForTableSchema,
} from "../schema/schema.ts"
import { TableInfer } from "./Table.ts"

export interface ITableStorage<RowId, RowData> {
  get(id: RowId): Promise<RowData | undefined>
  set(id: RowId, data: RowData): Promise<void>
  insert(data: RowData): Promise<RowId>
  remove(id: RowId): Promise<void>
  commit(): Promise<void>
  iterate(): AsyncIterableWrapper<[RowId, RowData]>
}

export class JsonFileTableStorage<RowData>
  implements ITableStorage<number, RowData> {
  private dirtyRecords: Map<number, RowData>
  private deletedRecords: Set<number>

  private _data: Record<number, RowData> | null = null

  private get data(): Record<number, RowData> {
    if (this._data != null) return this._data
    // check if the file exists
    try {
      Deno.statSync(this.filename)
    } catch (e) {
      if (e instanceof Deno.errors.NotFound) {
        this._data = {}
        return this._data
      }
      throw e
    }
    const data = Deno.readTextFileSync(this.filename)
    this._data = JSON.parse(data) as Record<number, RowData>
    return this._data
  }

  constructor(private filename: string) {
    this.dirtyRecords = new Map()
    this.deletedRecords = new Set()
  }

  static async forSchema<
    SchemaT extends SomeTableSchema,
  >(
    schema: SchemaT,
    filename: string,
  ): Promise<{
    data: JsonFileTableStorage<StoredRecordForTableSchema<SchemaT>>
    schema: SchemaT
    indexes: Map<string, Index<unknown, number, INodeId>>
  }> {
    const indexes = new Map<string, Index<unknown, number, INodeId>>()
    for (const column of [...schema.columns, ...schema.computedColumns]) {
      if (column.indexed.shouldIndex) {
        indexes.set(
          column.name,
          await Index.inMemory({
            isEqual: column.type.isEqual,
            compare: column.type.compare,
          }),
        )
      }
    }
    return {
      schema,
      data: new JsonFileTableStorage<StoredRecordForTableSchema<SchemaT>>(
        filename,
      ),
      indexes,
    }
  }

  get(id: number): Promise<RowData | undefined> {
    if (this.deletedRecords.has(id)) {
      return Promise.resolve(undefined)
    }
    return Promise.resolve(this.data[id])
  }
  set(id: number, data: RowData): Promise<void> {
    this.dirtyRecords.set(id, data)
    this.deletedRecords.delete(id)
    return Promise.resolve()
  }

  insert(data: RowData): Promise<number> {
    const id = Math.max(...Object.keys(this.data).map(Number), 0) + 1
    this.set(id, data)
    return Promise.resolve(id)
  }

  remove(id: number): Promise<void> {
    this.dirtyRecords.delete(id)
    this.deletedRecords.add(id)
    return Promise.resolve()
  }

  async commit(): Promise<void> {
    for (const [id, data] of this.dirtyRecords) {
      this.data[id] = data
    }
    this.dirtyRecords.clear()
    for (const id of this.deletedRecords) {
      delete this.data[id]
    }
    await Deno.writeTextFile(this.filename, JSON.stringify(this.data, null, 2))
  }

  iterate(): AsyncIterableWrapper<[number, RowData]> {
    const rows = this.data
    return new AsyncIterableWrapper(async function* iter() {
      for (const [id, data] of Object.entries(rows)) {
        yield [Number(id), data] as [number, RowData]
      }
    })
  }
}

export class InMemoryTableStorage<RowId, RowData>
  implements ITableStorage<RowId, RowData> {
  private dirtyRecords: Map<RowId, RowData>
  private deletedRecords: Set<RowId>

  constructor(
    private getNextRowId: () => RowId,
    private data: Map<RowId, RowData> = new Map(),
  ) {
    this.dirtyRecords = new Map()
    this.deletedRecords = new Set()
  }

  iterate(): AsyncIterableWrapper<[RowId, RowData]> {
    const rows = this.data
    return new AsyncIterableWrapper(async function* iter() {
      for (const [id, data] of rows.entries()) {
        yield [id, data] as [RowId, RowData]
      }
    })
  }

  static async forSchema<SchemaT extends SomeTableSchema>(
    schema: SchemaT,
  ): Promise<{
    data: InMemoryTableStorage<number, StoredRecordForTableSchema<SchemaT>>
    schema: SchemaT
    indexes: Map<string, Index<unknown, number, INodeId>>
  }> {
    const indexes = new Map<string, Index<unknown, number, INodeId>>()
    for (const column of [...schema.columns, ...schema.computedColumns]) {
      if (column.indexed.shouldIndex) {
        indexes.set(
          column.name,
          await Index.inMemory({
            isEqual: column.type.isEqual,
            compare: column.type.compare,
          }),
        )
      }
    }

    let rowId = 0
    return {
      data: new InMemoryTableStorage(() => rowId++),
      schema,
      indexes,
    }
  }

  get(id: RowId): Promise<RowData | undefined> {
    if (this.deletedRecords.has(id)) {
      return Promise.resolve(undefined)
    }
    return Promise.resolve(this.dirtyRecords.get(id) ?? this.data.get(id))
  }

  set(id: RowId, data: RowData): Promise<void> {
    this.dirtyRecords.set(id, data)
    this.deletedRecords.delete(id)
    return Promise.resolve()
  }

  insert(data: RowData): Promise<RowId> {
    const id = this.getNextRowId()
    this.set(id, data)
    return Promise.resolve(id)
  }

  remove(id: RowId): Promise<void> {
    this.dirtyRecords.delete(id)
    this.deletedRecords.add(id)
    return Promise.resolve()
  }

  commit(): Promise<void> {
    for (const [id, data] of this.dirtyRecords) {
      this.data.set(id, data)
    }
    this.dirtyRecords.clear()
    for (const id of this.deletedRecords) {
      this.data.delete(id)
    }
    return Promise.resolve()
  }
}

export type HeapFileRowId = { pageId: PageId; slotIndex: number }

const heapFileRowIdStruct: IStruct<HeapFileRowId> = Struct.record({
  pageId: [0, Struct.bigUint64],
  slotIndex: [1, Struct.uint32],
})
export class HeapFileTableStorage<RowData>
  implements ITableStorage<HeapFileRowId, RowData> {
  private constructor(
    private bufferPool: IBufferPool,
    private heapPageFile: HeapPageFile<VariableLengthRecordPageAllocInfo>,
    readonly serializer: IStruct<RowData>,
  ) {
  }

  iterate(): AsyncIterableWrapper<[HeapFileRowId, RowData]> {
    const heapPageFile = this.heapPageFile
    const bufferPool = this.bufferPool
    const getRecord = this.get.bind(this)
    return new AsyncIterableWrapper(async function* () {
      let currentDirectoryPageRef: HeaderPageRef | null =
        heapPageFile.headerPageRef
      while (currentDirectoryPageRef != null) {
        const directoryPage = await currentDirectoryPageRef.get()
        for (const entry of directoryPage.entries) {
          const recordPage = new ReadonlyVariableLengthRecordPage(
            await bufferPool.getPageView(entry.pageId),
          )
          for (
            const [_slot, slotIndex] of recordPage.iterSlots().filter((
              [slot],
            ) => slot.length > 0)
          ) {
            const id: HeapFileRowId = { slotIndex, pageId: entry.pageId }
            const data = await getRecord(id)
            if (data != null) {
              yield [id, data]
            }
          }
        }
        currentDirectoryPageRef = await heapPageFile.headerPageRef.getNext()
      }
    })
  }

  private async getRecordView(id: HeapFileRowId) {
    const view = await this.bufferPool.getPageView(id.pageId)
    const recordPage = new ReadonlyVariableLengthRecordPage(view)
    const slot = recordPage.getSlotEntry(id.slotIndex)
    if (slot.length === 0) return undefined // this was deleted
    return view.slice(slot.offset, slot.length)
  }

  async get(id: HeapFileRowId): Promise<RowData | undefined> {
    const view = await this.getRecordView(id)
    if (view == null) return // this was deleted
    return this.serializer.readAt(view, 0)
  }

  async set(
    id: HeapFileRowId,
    _data: RowData,
  ): Promise<void> {
    const view = await this.bufferPool.getPageView(id.pageId)
    const recordPage = new ReadonlyVariableLengthRecordPage(view)
    const slot = recordPage.getSlotEntry(id.slotIndex)
    if (slot.length === 0) {
      throw new Error("Cannot set a deleted record")
    }
    throw new Error("NOT IMPLEMENTED")
  }

  async insert(data: RowData): Promise<HeapFileRowId> {
    const numBytes = this.serializer.sizeof(data)
    // const serialized = this.serializer.serialize(data)
    const { pageId, allocInfo: { slot, slotIndex } } = await this.heapPageFile
      .allocateSpace(numBytes)
    if (slot.length < numBytes) {
      // This should never happen since we just allocated the space
      // but we'll check just in case to make it easier to find bugs.
      throw new Error("Record too large")
    }
    await this.bufferPool.writeToPage(pageId, (view) => {
      this.serializer.writeAt(data, view, slot.offset)
    })
    return { pageId, slotIndex }
  }

  async remove(id: HeapFileRowId): Promise<void> {
    await this.bufferPool.writeToPage(id.pageId, (view) => {
      const recordPage = new WriteableVariableLengthRecordPage(view)
      recordPage.freeSlot(id.slotIndex)
    })
  }

  async commit(): Promise<void> {
    await this.bufferPool.commit()
  }

  static async __openWithIndexPageIds<SchemaT extends SomeTableSchema>(
    bufferPool: IBufferPool,
    schema: SchemaT,
    heapPageId: PageId,
    indexPageIds: Record<string, PageId>,
  ) {
    const serializer = makeTableSchemaSerializer(schema)
    if (serializer == null) {
      throw new Error("Schema is not serializable")
    }
    const heapPageFile = new HeapPageFile(
      bufferPool,
      heapPageId,
      ReadonlyVariableLengthRecordPage.allocator,
    )

    const indexes = new Map<string, Index<unknown, HeapFileRowId, INodeId>>()
    for (const column of [...schema.columns, ...schema.computedColumns]) {
      if (column.indexed.shouldIndex) {
        if (!column.type.serializer) {
          throw new Error("Type must have a serializer")
        }
        const pageId = indexPageIds[column.name]
        if (pageId == null) {
          throw new Error(`No page ID for index ${column.name}`)
        }
        indexes.set(
          column.name,
          await Index.inFile(
            bufferPool,
            pageId,
            column.type.serializer!,
            heapFileRowIdStruct,
            {
              isEqual: column.type.isEqual,
              compare: column.type.compare,
              order: column.indexed.order,
            },
          ),
        )
      }
    }

    return {
      data: new HeapFileTableStorage<StoredRecordForTableSchema<SchemaT>>(
        bufferPool,
        heapPageFile,
        serializer,
      ),
      schema,
      indexes,
    }
  }

  static async open<SchemaT extends SomeTableSchema>(
    dbFile: DbFile,
    bufferPool: IBufferPool,
    schema: SchemaT,
    heapPageId: PageId,
  ): Promise<{
    data: HeapFileTableStorage<StoredRecordForTableSchema<SchemaT>>
    schema: SchemaT
    indexes: Map<string, Index<unknown, HeapFileRowId, INodeId>>
  }> {
    if (makeTableSchemaSerializer(schema) == null) {
      throw new Error("Schema is not serializable")
    }

    const indexPageIds: Record<string, PageId> = {}

    for (const column of [...schema.columns, ...schema.computedColumns]) {
      if (column.indexed.shouldIndex) {
        if (!column.type.serializer) {
          throw new Error("Type must have a serializer")
        }
        const pageId: PageId = await dbFile.getIndexStorage(
          schema.name,
          column.name,
        )
        indexPageIds[column.name] = pageId
      }
    }
    return await HeapFileTableStorage.__openWithIndexPageIds(
      bufferPool,
      schema,
      heapPageId,
      indexPageIds,
    )
  }
}

export type HeapFileTableInfer<SchemaT extends SomeTableSchema> = TableInfer<
  SchemaT,
  HeapFileTableStorage<StoredRecordForTableSchema<SchemaT>>
>
