import { IBufferPool, PageId } from "../pages/BufferPool.ts"
import { HeapPageFile } from "../pages/HeapPageFile.ts"
import { VariableLengthRecordPage } from "../pages/VariableLengthRecordPage.ts"
import {
  makeTableSchemaSerializer,
  RecordForTableSchema,
  SomeTableSchema,
} from "../schema/schema.ts"
import { Serializer } from "../schema/Serializers.ts"

export interface ITableStorage<RowId, RowData> {
  get(id: RowId): Promise<RowData | undefined>
  set(id: RowId, data: RowData): Promise<void>
  insert(data: RowData): Promise<RowId>
  remove(id: RowId): Promise<void>
  values(): IteratorObject<RowData, void, void>
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

  static forSchema<
    SchemaT extends SomeTableSchema,
  >(
    _schema: SchemaT,
    filename: string,
  ): JsonFileTableStorage<RecordForTableSchema<SchemaT>> {
    return new JsonFileTableStorage<RecordForTableSchema<SchemaT>>(filename)
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

  values(): IteratorObject<RowData, void, void> {
    return Object.entries(this.data).values().filter(([id]) =>
      !this.deletedRecords.has(parseInt(id))
    )
      .map(([_, data]) => data)
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

  static forSchema<SchemaT extends SomeTableSchema>(
    _schema: SchemaT,
  ): InMemoryTableStorage<number, RecordForTableSchema<SchemaT>> {
    let rowId = 0
    return new InMemoryTableStorage(() => rowId++)
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

  values(): IteratorObject<RowData, void, void> {
    return this.data.entries().filter(([id]) => !this.deletedRecords.has(id))
      .map(([_, data]) => data)
  }
}

export type HeapFileRowId = { pageId: PageId; slotId: number }
export class HeapFileTableStorage<RowData>
  implements ITableStorage<HeapFileRowId, RowData> {
  private constructor(
    private bufferPool: IBufferPool,
    private heapPageFile: HeapPageFile<{ freeSpace: number; slotId: number }>,
    private serializer: Serializer<RowData>,
  ) {
  }

  private async getRecordView(id: { pageId: PageId; slotId: number }) {
    const page = await this.bufferPool.getPage(id.pageId)
    const view = new DataView(page.buffer)
    const recordPage = new VariableLengthRecordPage(view)
    const slot = recordPage.getSlotEntry(id.slotId)
    if (slot.length === 0) return undefined // this was deleted
    return new DataView(page.buffer, slot.offset, slot.length)
  }

  async get(
    id: { pageId: PageId; slotId: number },
  ): Promise<RowData | undefined> {
    const view = await this.getRecordView(id)
    if (view == null) return // this was deleted
    return this.serializer.deserialize(view)
  }

  async set(
    id: { pageId: PageId; slotId: number },
    data: RowData,
  ): Promise<void> {
    const page = await this.bufferPool.getPage(id.pageId)
    const view = new DataView(page.buffer)
    const recordPage = new VariableLengthRecordPage(view)
    let slot = recordPage.getSlotEntry(id.slotId)
    if (slot.length === 0) {
      throw new Error("Cannot set a deleted record")
    }
    const serialized = this.serializer.serialize(data)
    recordPage.freeSlot(id.slotId)
    recordPage.freeSpace
    try {
      slot = recordPage.allocateSlot(serialized.byteLength)
    } catch {
      // not enough space, need to move the record
      throw new Error("Not implemented")
    }
  }

  async insert(data: RowData): Promise<{ pageId: PageId; slotId: number }> {
    const serialized = this.serializer.serialize(data)
    const { pageId, allocInfo } = await this.heapPageFile.allocateSpace(
      serialized.byteLength,
    )
    const page = await this.bufferPool.getPage(pageId)
    page.set(new Uint8Array(serialized), allocInfo.slotId)
    return { pageId, slotId: allocInfo.slotId }
  }

  async remove(id: { pageId: PageId; slotId: number }): Promise<void> {
    const page = await this.bufferPool.getPage(id.pageId)
    const view = new DataView(page.buffer)
    const recordPage = new VariableLengthRecordPage(view)
    recordPage.freeSlot(id.slotId)
  }

  values(): IteratorObject<RowData, void, void> {
    throw new Error("Method not implemented.")
  }

  async commit(): Promise<void> {
    await this.bufferPool.commit()
  }

  static async create<SchemaT extends SomeTableSchema>(
    bufferPool: IBufferPool,
    schema: SchemaT,
  ): Promise<HeapFileTableStorage<RecordForTableSchema<SchemaT>>> {
    const serializer = makeTableSchemaSerializer(schema)
    if (serializer == null) {
      throw new Error("Schema is not serializable")
    }
    const heapPageFile = await HeapPageFile.create(
      bufferPool,
      async (pageId, bytes) => {
        const page = await bufferPool.getPage(pageId)
        const view = new DataView(page.buffer)
        const recordPage = new VariableLengthRecordPage(view)
        const slot = recordPage.allocateSlot(bytes)
        return { freeSpace: recordPage.freeSpace, slotId: slot.offset }
      },
    )
    return new HeapFileTableStorage<RecordForTableSchema<SchemaT>>(
      bufferPool,
      heapPageFile,
      serializer,
    )
  }
}