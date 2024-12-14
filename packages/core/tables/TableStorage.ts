import { AsyncIterableWrapper } from "../async.ts"
import { IStruct, Struct } from "../binary/Struct.ts"
import { Droppable, IDroppable } from "../droppable.ts"
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
import { Promisable } from "type-fest"

export interface ITableStorage<RowId, RowData> extends IDroppable {
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
  private droppable: Droppable

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
    this.droppable = new Droppable(async () => {
      await Deno.remove(this.filename)
    })
  }

  drop() {
    return this.droppable.drop()
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
    this.droppable.assertNotDropped("TableStorage has been dropped")
    if (this.deletedRecords.has(id)) {
      return
    }
    return this.data[id]
  }
  set(id: number, data: RowData): number {
    this.droppable.assertNotDropped("TableStorage has been dropped")
    this.dirtyRecords.set(id, data)
    this.deletedRecords.delete(id)
    return id
  }

  insert(data: RowData): number {
    this.droppable.assertNotDropped("TableStorage has been dropped")
    const id = Math.max(...Object.keys(this.data).map(Number), 0) + 1
    this.set(id, data)
    return id
  }

  remove(id: number): void {
    this.droppable.assertNotDropped("TableStorage has been dropped")
    this.dirtyRecords.delete(id)
    this.deletedRecords.add(id)
  }

  async commit(): Promise<void> {
    this.droppable.assertNotDropped("TableStorage has been dropped")
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
    this.droppable.assertNotDropped("TableStorage has been dropped")
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
  private droppable: Droppable

  constructor(
    private getNextRowId: () => RowId,
    private data: Map<RowId, RowData> = new Map(),
  ) {
    this.dirtyRecords = new Map()
    this.deletedRecords = new Set()
    this.droppable = new Droppable(() => {
      this.data.clear()
    })
  }

  drop() {
    return this.droppable.drop()
  }

  iterate(): AsyncIterableWrapper<[RowId, RowData]> {
    this.droppable.assertNotDropped("TableStorage has been dropped")
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
    this.droppable.assertNotDropped("TableStorage has been dropped")
    if (this.deletedRecords.has(id)) {
      return
    }
    return this.dirtyRecords.get(id) ?? this.data.get(id)
  }

  set(id: RowId, data: RowData): RowId {
    this.droppable.assertNotDropped("TableStorage has been dropped")
    this.dirtyRecords.set(id, data)
    this.deletedRecords.delete(id)
    return id
  }

  insert(data: RowData): RowId {
    this.droppable.assertNotDropped("TableStorage has been dropped")
    const id = this.getNextRowId()
    this.set(id, data)
    return id
  }

  remove(id: RowId): void {
    this.droppable.assertNotDropped("TableStorage has been dropped")
    this.dirtyRecords.delete(id)
    this.deletedRecords.add(id)
  }

  commit(): void {
    this.droppable.assertNotDropped("TableStorage has been dropped")
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
  header: {
    canonical: boolean
    forwardRowId: HeapFileRowId | null
    schemaId: number
  }
  rowData: RowData
}
export const heapFileRowIdStruct: IStruct<HeapFileRowId> = Struct.record({
  pageId: [0, Struct.bigUint64],
  slotIndex: [1, Struct.uint32],
})
const headerStruct: IStruct<HeapFileEntry<unknown>["header"]> = Struct.record({
  forward: [0, Struct.boolean],
  forwardRowId: [1, heapFileRowIdStruct.nullable()],
  schemaId: [2, Struct.uint32],
  canonical: [3, Struct.boolean],
})
export class HeapFileTableStorage<RowData>
  implements ITableStorage<HeapFileRowId, RowData> {
  entryStruct: IStruct<HeapFileEntry<RowData>>

  private heapPageFile: HeapPageFile<VariableLengthRecordPageAllocInfo>
  private droppable: Droppable

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
    this.droppable = new Droppable(async () => {
      await this.heapPageFile.drop()
    })
  }

  drop() {
    return this.droppable.drop()
  }

  iterate(): AsyncIterableWrapper<[HeapFileRowId, RowData]> {
    this.droppable.assertNotDropped("TableStorage has been dropped")
    const heapPageFile = this.heapPageFile
    const bufferPool = this.bufferPool
    const getTerminalEntry = this.getTerminalEntry.bind(this)
    return new AsyncIterableWrapper(async function* () {
      let currentDirectoryPageRef: HeaderPageRef | null =
        heapPageFile.headerPageRef
      while (currentDirectoryPageRef != null) {
        const directoryPage = await currentDirectoryPageRef.get()
        for (const directoryEntry of directoryPage.entries) {
          const recordPage = new ReadonlyVariableLengthRecordPage(
            await bufferPool.getPageView(directoryEntry.pageId),
          )
          for (
            const [_slot, slotIndex] of recordPage.iterSlots().filter((
              [slot],
            ) => slot.length > 0)
          ) {
            const id: HeapFileRowId = {
              slotIndex,
              pageId: directoryEntry.pageId,
            }
            const terminalEntry = await getTerminalEntry(id)
            if (terminalEntry.canonicalId == null) continue // we're not at the root
            if (terminalEntry.rowData == null) continue // this was deleted
            yield [terminalEntry.canonicalId, terminalEntry.rowData]
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
      canonicalId: HeapFileRowId | null
      terminalId: HeapFileRowId
      rowData: RowData | undefined
    }
  > {
    const view = await this.getRecordView(id)
    if (view == null) {
      return { canonicalId: id, terminalId: id, rowData: undefined }
    }
    const entry = this.entryStruct.readAt(view, 0)
    const canonicalId = entry.header.canonical ? id : null
    if (entry.header.forwardRowId == null) {
      return {
        canonicalId,
        terminalId: id,
        rowData: entry.rowData,
      }
    }
    const forwardView = await this.getRecordView(entry.header.forwardRowId)
    if (forwardView == null) {
      return {
        canonicalId,
        terminalId: entry.header.forwardRowId,
        rowData: undefined,
      }
    }
    const forwardEntry = this.entryStruct.readAt(forwardView, 0)
    if (forwardEntry.header.forwardRowId != null) {
      throw new Error(`Forward chain too long`)
    }
    return {
      canonicalId,
      terminalId: entry.header.forwardRowId,
      rowData: forwardEntry.rowData,
    }
  }

  async get(id: HeapFileRowId): Promise<RowData | undefined> {
    this.droppable.assertNotDropped("TableStorage has been dropped")
    const terminal = await this.getTerminalEntry(id)
    return terminal.rowData ?? undefined
  }

  async set(
    initialId: HeapFileRowId,
    data: RowData,
  ): Promise<HeapFileRowId> {
    this.droppable.assertNotDropped("TableStorage has been dropped")

    const terminal = await this.getTerminalEntry(initialId)
    if (terminal.rowData === undefined) {
      throw new Error("Cannot set a deleted record")
    }
    if (terminal.canonicalId == null) {
      throw new Error("Cannot set a forward record")
    }

    const isShallow = terminal.canonicalId === terminal.terminalId

    const terminalSlot = new ReadonlyVariableLengthRecordPage(
      await this.bufferPool.getPageView(terminal.terminalId.pageId),
    )
      .getSlotEntry(terminal.terminalId.slotIndex)
    if (terminalSlot.length === 0) {
      throw new Error("Cannot set a deleted record")
    }

    // two cases:
    // case 1: this is a shallow entry
    if (isShallow) {
      const newEntry: HeapFileEntry<RowData> = {
        header: {
          canonical: true,
          forwardRowId: null,
          schemaId: this.schemaId,
        },
        rowData: data,
      }
      // case 1.1: the new record fits the existing slot
      if (this.entryStruct.sizeof(newEntry) <= terminalSlot.length) {
        await this.bufferPool.writeToPage(
          terminal.terminalId.pageId,
          (view) => {
            this.entryStruct.writeAt(newEntry, view, terminalSlot.offset)
          },
        )
        return terminal.canonicalId
      }
      // case 1.2: the new record does not fit the existing slot
      const terminalId = await this._insert(data, { canonical: false })
      await this.bufferPool.writeToPage(terminal.canonicalId.pageId, (view) => {
        this.entryStruct.writeAt(
          {
            header: {
              canonical: true,
              forwardRowId: terminalId,
              schemaId: this.schemaId,
            },
            rowData: this.recordStruct.emptyValue(),
          },
          view,
          terminalSlot.offset,
        )
      })
      return terminal.canonicalId
    }

    // case 2: this is a forwarded entry
    const newEntry: HeapFileEntry<RowData> = {
      header: {
        canonical: false,
        forwardRowId: null,
        schemaId: this.schemaId,
      },
      rowData: data,
    }

    if (this.entryStruct.sizeof(newEntry) <= terminalSlot.length) {
      // case 2.1: this is a forwarded entry, and the new record fits the existing slot
      await this.bufferPool.writeToPage(terminal.terminalId.pageId, (view) => {
        this.entryStruct.writeAt(newEntry, view, terminalSlot.offset)
      })
      return terminal.canonicalId
    }
    // case 2.2: this is a forwarded entry, and the new record does not fit the existing slot
    // free up the old slot
    await this.bufferPool.writeToPage(terminal.terminalId.pageId, (view) => {
      const recordPage = new WriteableVariableLengthRecordPage(view)
      recordPage.freeSlot(terminal.terminalId.slotIndex)
    })
    // insert the new record
    const terminalId = await this._insert(data, { canonical: false })
    // update the forward record
    const canoncalSlot = new ReadonlyVariableLengthRecordPage(
      await this.bufferPool.getPageView(terminal.canonicalId.pageId),
    )
      .getSlotEntry(terminal.canonicalId.slotIndex)

    await this.bufferPool.writeToPage(terminal.canonicalId.pageId, (view) => {
      this.entryStruct.writeAt(
        {
          header: {
            canonical: true,
            forwardRowId: terminalId,
            schemaId: this.schemaId,
          },
          rowData: this.recordStruct.emptyValue(),
        },
        view,
        canoncalSlot.offset,
      )
    })
    return terminal.canonicalId
  }

  private async _insert(
    data: RowData,
    { canonical }: { canonical: boolean },
  ): Promise<HeapFileRowId> {
    const entry: HeapFileEntry<RowData> = {
      header: {
        canonical,
        forwardRowId: null,
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

  insert(data: RowData): Promise<HeapFileRowId> {
    this.droppable.assertNotDropped("TableStorage has been dropped")
    return this._insert(data, { canonical: true })
  }

  async remove(id: HeapFileRowId): Promise<void> {
    this.droppable.assertNotDropped("TableStorage has been dropped")
    const terminal = await this.getTerminalEntry(id)
    if (terminal.canonicalId == null) {
      throw new Error("Cannot delete a forward record")
    }
    // delete the root record
    await this.bufferPool.writeToPage(terminal.terminalId.pageId, (view) => {
      const recordPage = new WriteableVariableLengthRecordPage(view)
      recordPage.freeSlot(terminal.terminalId.slotIndex)
    })
    // delete the forward record
    if (
      terminal.terminalId != null &&
      terminal.terminalId !== terminal.canonicalId
    ) {
      await this.bufferPool.writeToPage(terminal.terminalId.pageId, (view) => {
        const recordPage = new WriteableVariableLengthRecordPage(view)
        recordPage.freeSlot(terminal.terminalId.slotIndex)
      })
    }
  }

  async commit(): Promise<void> {
    this.droppable.assertNotDropped("TableStorage has been dropped")
    await this.bufferPool.commit()
  }
}

/**
 * Infers the type of a Table instance for the given schema
 */
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
