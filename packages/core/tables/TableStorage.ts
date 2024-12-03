import { AsyncIterableWrapper } from "../async.ts"
import { IStruct, Struct } from "../binary/Struct.ts"
import { InMemoryIndexProvider } from "../indexes/IndexProvider.ts"
import { IBufferPool, PageId } from "../pages/BufferPool.ts"
import { HeaderPageRef, HeapPageFile } from "../pages/HeapPageFile.ts"
import {
  ReadonlyVariableLengthRecordPage,
  VariableLengthRecordPageAllocInfo,
  WriteableVariableLengthRecordPage,
} from "../pages/VariableLengthRecordPage.ts"
import {
  SomeTableSchema,
  StoredRecordForTableSchema,
  TableSchema,
} from "../schema/schema.ts"
import { Table, TableConfig } from "./Table.ts"
import { Promisable } from "npm:type-fest"

export interface ITableStorage<RowId, RowData> {
  get(id: RowId): Promisable<RowData | undefined>
  set(id: RowId, data: RowData): Promisable<RowId>
  insert(data: RowData): Promisable<RowId>
  remove(id: RowId): Promisable<void>
  commit(): Promisable<void>
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

  static forSchema<
    SchemaT extends SomeTableSchema,
  >(
    schema: SchemaT,
    filename: string,
  ): TableConfig<
    number,
    SchemaT,
    JsonFileTableStorage<StoredRecordForTableSchema<SchemaT>>
  > {
    return {
      schema,
      data: new JsonFileTableStorage<StoredRecordForTableSchema<SchemaT>>(
        filename,
      ),
      indexProvider: new InMemoryIndexProvider(schema),
      serialIdGenerator: {
        next(_name: string): number {
          throw new Error("Not implemented")
        },
      },
    }
  }

  get(id: number): RowData | undefined {
    if (this.deletedRecords.has(id)) {
      return
    }
    return this.data[id]
  }
  set(id: number, data: RowData): number {
    this.dirtyRecords.set(id, data)
    this.deletedRecords.delete(id)
    return id
  }

  insert(data: RowData): number {
    const id = Math.max(...Object.keys(this.data).map(Number), 0) + 1
    this.set(id, data)
    return id
  }

  remove(id: number): void {
    this.dirtyRecords.delete(id)
    this.deletedRecords.add(id)
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

  static forSchema<SchemaT extends SomeTableSchema>(
    schema: SchemaT,
  ): TableConfig<
    number,
    SchemaT,
    InMemoryTableStorage<number, StoredRecordForTableSchema<SchemaT>>
  > {
    let rowId = 0
    const serialIds = new Map<string, number>()
    return {
      data: new InMemoryTableStorage(() => rowId++),
      schema,
      indexProvider: new InMemoryIndexProvider(schema),
      serialIdGenerator: {
        next(name: string) {
          const nextId = (serialIds.get(name) ?? 0) + 1
          serialIds.set(name, nextId)
          return nextId
        },
      },
    }
  }

  get(id: RowId): RowData | undefined {
    if (this.deletedRecords.has(id)) {
      return
    }
    return this.dirtyRecords.get(id) ?? this.data.get(id)
  }

  set(id: RowId, data: RowData): RowId {
    this.dirtyRecords.set(id, data)
    this.deletedRecords.delete(id)
    return id
  }

  insert(data: RowData): RowId {
    const id = this.getNextRowId()
    this.set(id, data)
    return id
  }

  remove(id: RowId): void {
    this.dirtyRecords.delete(id)
    this.deletedRecords.add(id)
  }

  commit(): void {
    for (const [id, data] of this.dirtyRecords) {
      this.data.set(id, data)
    }
    this.dirtyRecords.clear()
    for (const id of this.deletedRecords) {
      this.data.delete(id)
    }
  }
}

export type HeapFileRowId = { pageId: PageId; slotIndex: number }
type HeapFileEntry<RowData> = {
  header: { rowId: HeapFileRowId; forward: boolean; schemaId: number }
  rowData: RowData
}
export const heapFileRowIdStruct: IStruct<HeapFileRowId> = Struct.record({
  pageId: [0, Struct.bigUint64],
  slotIndex: [1, Struct.uint32],
})
const headerStruct: IStruct<HeapFileEntry<unknown>["header"]> = Struct.record({
  forward: [0, Struct.boolean],
  rowId: [1, heapFileRowIdStruct],
  schemaId: [2, Struct.uint32],
})
export class HeapFileTableStorage<RowData>
  implements ITableStorage<HeapFileRowId, RowData> {
  entryStruct: IStruct<HeapFileEntry<RowData>>

  private heapPageFile: HeapPageFile<VariableLengthRecordPageAllocInfo>

  constructor(
    readonly bufferPool: IBufferPool,
    pageId: PageId,
    readonly recordStruct: IStruct<RowData>,
    private schemaId: number,
  ) {
    this.entryStruct = Struct.record({
      header: [0, headerStruct],
      rowData: [1, this.recordStruct],
    })
    this.heapPageFile = new HeapPageFile(
      bufferPool,
      pageId,
      ReadonlyVariableLengthRecordPage.allocator,
    )
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

  private async getTerminalEntry(
    id: HeapFileRowId,
  ): Promise<
    {
      id: HeapFileRowId
      entry: HeapFileEntry<RowData> | null
      path: { id: HeapFileRowId; entry: HeapFileEntry<RowData> | null }[]
    }
  > {
    let entry: HeapFileEntry<RowData> | null = null
    const path: { id: HeapFileRowId; entry: HeapFileEntry<RowData> | null }[] =
      []
    while (entry == null || entry.header.forward) {
      // follow the forward pointer
      const view = await this.getRecordView(
        entry == null ? id : entry.header.rowId,
      )
      if (view == null) return { id, entry, path } // this was deleted
      entry = this.entryStruct.readAt(view, 0)
      path.push({ id, entry })
    }
    return { id, entry, path }
  }

  async get(id: HeapFileRowId): Promise<RowData | undefined> {
    const terminal = await this.getTerminalEntry(id)
    return terminal.entry?.rowData
  }

  async set(
    initialId: HeapFileRowId,
    data: RowData,
  ): Promise<HeapFileRowId> {
    const terminal = await this.getTerminalEntry(initialId)
    if (terminal.entry == null) {
      throw new Error("Cannot set a deleted record")
    }

    const view = await this.bufferPool.getPageView(terminal.id.pageId)
    const recordPage = new ReadonlyVariableLengthRecordPage(view)
    const slot = recordPage.getSlotEntry(terminal.id.slotIndex)
    if (slot.length === 0) {
      throw new Error("Cannot set a deleted record")
    }

    const newEntry = {
      header: {
        forward: false,
        rowId: heapFileRowIdStruct.emptyValue(),
        schemaId: this.schemaId,
      },
      rowData: data,
    }

    if (this.entryStruct.sizeof(newEntry) <= slot.length) {
      await this.bufferPool.writeToPage(terminal.id.pageId, (view) => {
        this.entryStruct.writeAt(newEntry, view, slot.offset)
      })
      return terminal.id
    }
    const forwardId = await this.insert(data)
    const forwardEntry = {
      header: { forward: true, rowId: forwardId, schemaId: this.schemaId },
      rowData: this.recordStruct.emptyValue(),
    }
    const forwardSize = this.entryStruct.sizeof(forwardEntry)
    if (forwardSize > slot.length) {
      throw new Error("Record too large")
    }
    await this.bufferPool.writeToPage(terminal.id.pageId, (view) => {
      this.entryStruct.writeAt(forwardEntry, view, slot.offset)
    })
    return forwardId
  }

  async insert(data: RowData): Promise<HeapFileRowId> {
    const entry = {
      header: {
        forward: false,
        rowId: heapFileRowIdStruct.emptyValue(),
        schemaId: this.schemaId,
      },
      rowData: data,
    }
    const numBytes = this.entryStruct.sizeof(entry)
    // const serialized = this.serializer.serialize(data)
    const { pageId, allocInfo: { slot, slotIndex } } = await this.heapPageFile
      .allocateSpace(numBytes)
    if (slot.length < numBytes) {
      // This should never happen since we just allocated the space
      // but we'll check just in case to make it easier to find bugs.
      throw new Error("Record too large")
    }
    await this.bufferPool.writeToPage(pageId, (view) => {
      this.entryStruct.writeAt(entry, view, slot.offset)
    })
    return { pageId, slotIndex }
  }

  async remove(id: HeapFileRowId): Promise<void> {
    const terminal = await this.getTerminalEntry(id)
    for (const { id, entry } of terminal.path) {
      if (entry == null) {
        continue
      }
      await this.bufferPool.writeToPage(id.pageId, (view) => {
        const recordPage = new WriteableVariableLengthRecordPage(view)
        recordPage.freeSlot(id.slotIndex)
      })
    }
  }

  async commit(): Promise<void> {
    await this.bufferPool.commit()
  }
}

export type HeapFileTableInfer<SchemaT extends SomeTableSchema> =
  SchemaT extends
    TableSchema<infer TName, infer ColumnSchemasT, infer ComputedColumnSchemasT>
    ? Table<
      HeapFileRowId,
      TName,
      ColumnSchemasT,
      ComputedColumnSchemasT,
      SchemaT,
      HeapFileTableStorage<StoredRecordForTableSchema<SchemaT>>
    >
    : never
