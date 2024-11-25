import { FixedWidthStruct, IStruct } from "../binary/Struct.ts"
import { INodeId } from "../indexes/BTreeNode.ts"
import { Index } from "../indexes/Index.ts"
import { IBufferPool, PageId } from "../pages/BufferPool.ts"
import { HeapPageFile } from "../pages/HeapPageFile.ts"
import {
  VariableLengthRecordPage,
  VariableLengthRecordPageAllocInfo,
} from "../pages/VariableLengthRecordPage.ts"
import {
  makeTableSchemaSerializer,
  SomeTableSchema,
  StoredRecordForTableSchema,
} from "../schema/schema.ts"

export interface ITableStorage<RowId, RowData> {
  get(id: RowId): Promise<RowData | undefined>
  set(id: RowId, data: RowData): Promise<void>
  insert(data: RowData): Promise<RowId>
  remove(id: RowId): Promise<void>
  commit(): Promise<void>
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
      if (column.indexed) {
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

  static async forSchema<SchemaT extends SomeTableSchema>(
    schema: SchemaT,
  ): Promise<{
    data: InMemoryTableStorage<number, StoredRecordForTableSchema<SchemaT>>
    schema: SchemaT
    indexes: Map<string, Index<unknown, number, INodeId>>
  }> {
    const indexes = new Map<string, Index<unknown, number, INodeId>>()
    for (const column of [...schema.columns, ...schema.computedColumns]) {
      if (column.indexed) {
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

const heapFileRowIdStruct: IStruct<HeapFileRowId> = new FixedWidthStruct({
  size: 8 + 4,
  write: (id, view) => {
    view.setBigUint64(0, id.pageId)
    view.setUint32(8, id.slotIndex)
  },
  read: (view) => ({
    pageId: view.getBigUint64(0),
    slotIndex: view.getUint32(8),
  }),
})
export class HeapFileTableStorage<RowData>
  implements ITableStorage<HeapFileRowId, RowData> {
  private constructor(
    private bufferPool: IBufferPool,
    private heapPageFile: HeapPageFile<VariableLengthRecordPageAllocInfo>,
    private serializer: IStruct<RowData>,
  ) {
  }

  private async getRecordView(id: HeapFileRowId) {
    const page = await this.bufferPool.getPage(id.pageId)
    const view = new DataView(page.buffer)
    const recordPage = new VariableLengthRecordPage(view)
    const slot = recordPage.getSlotEntry(id.slotIndex)
    if (slot.length === 0) return undefined // this was deleted
    return new DataView(page.buffer, slot.offset, slot.length)
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
    const page = await this.bufferPool.getPage(id.pageId)
    const view = new DataView(page.buffer)
    const recordPage = new VariableLengthRecordPage(view)
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
    const page = await this.bufferPool.getPage(pageId)
    this.serializer.writeAt(data, new DataView(page.buffer), slot.offset)
    return { pageId, slotIndex }
  }

  async remove(id: HeapFileRowId): Promise<void> {
    const page = await this.bufferPool.getPage(id.pageId)
    const view = new DataView(page.buffer)
    const recordPage = new VariableLengthRecordPage(view)
    recordPage.freeSlot(id.slotIndex)
  }

  async commit(): Promise<void> {
    await this.bufferPool.commit()
  }

  static async create<SchemaT extends SomeTableSchema>(
    bufferPool: IBufferPool,
    schema: SchemaT,
  ): Promise<{
    data: HeapFileTableStorage<StoredRecordForTableSchema<SchemaT>>
    schema: SchemaT
    indexes: Map<string, Index<unknown, HeapFileRowId, INodeId>>
  }> {
    const serializer = makeTableSchemaSerializer(schema)
    if (serializer == null) {
      throw new Error("Schema is not serializable")
    }
    const heapPageId = await bufferPool.allocatePage()
    const heapPageFile = new HeapPageFile(
      bufferPool,
      heapPageId,
      VariableLengthRecordPage.allocator,
    )

    const indexes = new Map<string, Index<unknown, HeapFileRowId, INodeId>>()
    for (const column of [...schema.columns, ...schema.computedColumns]) {
      if (column.indexed) {
        if (!column.type.serializer) {
          throw new Error("Type must have a serializer")
        }
        indexes.set(
          column.name,
          await Index.inFile(
            bufferPool,
            column.type.serializer!,
            heapFileRowIdStruct,
            {
              isEqual: column.type.isEqual,
              compare: column.type.compare,
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
}
