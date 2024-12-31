import { AsyncIterableWrapper } from "../async.ts"
import { ReadonlyDataView } from "../binary/dataview.ts"
import { FixedWidthStruct, IStruct, Struct } from "../binary/Struct.ts"
import { Droppable, IDroppable } from "../droppable.ts"
import { InMemoryIndexProvider } from "../indexes/IndexProvider.ts"
import { IBufferPool, PageId } from "../pages/BufferPool.ts"
import { HeaderPageRef, HeapPageFile } from "../pages/HeapPageFile.ts"
import { LinkedPageList } from "../pages/LinkedPageList.ts"
import {
  ReadonlyVariableLengthRecordPage,
  VariableLengthRecordPageAllocInfo,
  WriteableVariableLengthRecordPage,
} from "../pages/VariableLengthRecordPage.ts"
import {
  ComputedColumnRecord,
  SomeTableSchema,
  StoredColumnRecord,
  StoredRecordForTableSchema,
  TableSchema,
} from "../schema/TableSchema.ts"
import { Table, TableConfig } from "./Table.ts"
import type { Promisable } from "type-fest"

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
    N extends string,
    C extends StoredColumnRecord,
    CC extends ComputedColumnRecord,
  >(
    schema: TableSchema<N, C, CC>,
    filename: string,
  ): TableConfig<
    number,
    N,
    C,
    CC,
    JsonFileTableStorage<StoredRecordForTableSchema<TableSchema<N, C, CC>>>
  > {
    return {
      schema,
      data: new JsonFileTableStorage<
        StoredRecordForTableSchema<TableSchema<N, C, CC>>
      >(
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

  static forSchema<
    N extends string,
    C extends StoredColumnRecord,
    CC extends ComputedColumnRecord,
  >(
    schema: TableSchema<N, C, CC>,
  ): TableConfig<
    number,
    N,
    C,
    CC,
    InMemoryTableStorage<
      number,
      StoredRecordForTableSchema<TableSchema<N, C, CC>>
    >
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

type NormalHeader = {
  type: "normal"
  value: { canonical: boolean }
}
type ForwardedHeader = {
  type: "forwarded"
  value: { canonical: boolean; forwardRowId: HeapFileRowId }
}

type OversizedRowHeader = {
  type: "oversized"
  value: { canonical: boolean; headPageId: PageId }
}
type HeapFileEntry<RowData> = {
  header: NormalHeader | ForwardedHeader | OversizedRowHeader
  rowData: RowData
}
export const heapFileRowIdStruct: FixedWidthStruct<HeapFileRowId> = Struct
  .record({
    pageId: [0, Struct.bigUint64],
    slotIndex: [1, Struct.uint32],
  })
const headerStruct: IStruct<HeapFileEntry<unknown>["header"]> = Struct
  .fixedSizeUnion({
    normal: [0, Struct.record({ canonical: [0, Struct.boolean] })],
    forwarded: [
      1,
      Struct.record({
        canonical: [0, Struct.boolean],
        forwardRowId: [1, heapFileRowIdStruct],
      }),
    ],
    oversized: [
      2,
      Struct.record({
        canonical: [0, Struct.boolean],
        headPageId: [1, Struct.bigUint64],
      }),
    ],
  })
export class HeapFileTableStorage<RowData>
  implements ITableStorage<HeapFileRowId, RowData> {
  // entryStruct: IStruct<HeapFileEntry<RowData>>

  private heapPageFile: HeapPageFile<VariableLengthRecordPageAllocInfo>
  private droppable: Droppable

  constructor(
    readonly bufferPool: IBufferPool,
    pageId: PageId,
    readonly recordStruct: IStruct<RowData>,
    private schemaId: number,
  ) {
    // this.entryStruct = Struct.record({
    //   header: [0, headerStruct],
    //   rowData: [1, this.recordStruct],
    // })
    this.heapPageFile = new HeapPageFile(
      bufferPool,
      pageId,
      ReadonlyVariableLengthRecordPage.allocator,
    )
    this.droppable = new Droppable(async () => {
      await this.heapPageFile.drop()
    })
  }

  drop(): Promisable<void> {
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
      header: HeapFileEntry<RowData>["header"] | null
      forwardHeader: HeapFileEntry<RowData>["header"] | null
      canonicalId: HeapFileRowId | null
      terminalId: HeapFileRowId
      rowData: RowData | undefined
    }
  > {
    const view = await this.getRecordView(id)
    if (view == null) {
      return {
        header: null,
        forwardHeader: null,
        canonicalId: id,
        terminalId: id,
        rowData: undefined,
      }
    }
    const header = headerStruct.readAt(view, 0)

    const canonicalId = header.value.canonical ? id : null
    if (header.type !== "forwarded") {
      let rowData: RowData
      if (header.type === "normal") {
        rowData = this.recordStruct.readAt(view, headerStruct.sizeof(header))
      } else if (header.type === "oversized") {
        const rawData = await new LinkedPageList(
          this.bufferPool,
          header.value.headPageId,
        ).readData()
        rowData = this.recordStruct.readAt(
          new ReadonlyDataView(rawData.buffer),
          0,
        )
      } else {
        // @ts-expect-error - this should be exhaustive and never run.
        throw new Error(`Unexpected header type: ${header.type}`)
      }

      return {
        header,
        forwardHeader: null,
        canonicalId,
        terminalId: id,
        rowData,
      }
    }
    const forwardView = await this.getRecordView(
      header.value.forwardRowId,
    )
    if (forwardView == null) {
      return {
        header,
        forwardHeader: null,
        canonicalId,
        terminalId: header.value.forwardRowId,
        rowData: undefined,
      }
    }
    const forwardHeader = headerStruct.readAt(forwardView, 0)
    if (forwardHeader.type === "forwarded") {
      throw new Error(`Forward chain too long`)
    }
    let rowData: RowData
    if (forwardHeader.type === "normal") {
      rowData = this.recordStruct.readAt(
        forwardView,
        headerStruct.sizeof(forwardHeader),
      )
    } else if (forwardHeader.type === "oversized") {
      const rawData = await new LinkedPageList(
        this.bufferPool,
        forwardHeader.value.headPageId,
      ).readData()
      rowData = this.recordStruct.readAt(
        new ReadonlyDataView(rawData.buffer),
        0,
      )
    } else {
      // @ts-expect-error - this should be exhaustive and never run.
      throw new Error(`Unexpected header type: ${forwardHeader.type}`)
    }
    return {
      header,
      forwardHeader,
      canonicalId,
      terminalId: header.value.forwardRowId,
      rowData: rowData,
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
      const newHeader: HeapFileEntry<RowData>["header"] = {
        type: "normal",
        value: { canonical: true },
      }
      const newSize = headerStruct.sizeof(newHeader) +
        this.recordStruct.sizeof(data)
      // case 1.1: the new record fits the existing slot
      if (newSize <= terminalSlot.length) {
        await this.bufferPool.writeToPage(
          terminal.terminalId.pageId,
          (view) => {
            headerStruct.writeAt(newHeader, view, terminalSlot.offset)
            this.recordStruct.writeAt(
              data,
              view,
              terminalSlot.offset + headerStruct.sizeof(newHeader),
            )
          },
        )
        if (terminal.header?.type === "oversized") {
          // the old record was oversized, so we need to free it
          await new LinkedPageList(
            this.bufferPool,
            terminal.header.value.headPageId,
          ).drop()
        }
        return terminal.canonicalId
      }
      // case 1.2: the new record does not fit the existing slot
      if (newSize > this.heapPageFile.maxAllocSize) {
        // case 1.2.1: the new record is oversized
        let linkedPageList: LinkedPageList
        if (terminal.header?.type === "oversized") {
          // the existing record is oversized too, so we can just reuse it
          linkedPageList = new LinkedPageList(
            this.bufferPool,
            terminal.header.value.headPageId,
          )
        } else {
          linkedPageList = new LinkedPageList(
            this.bufferPool,
            await this.bufferPool.allocatePage(),
          )
        }
        await linkedPageList.writeData(this.recordStruct.toUint8Array(data))
        const oversizedHeader: HeapFileEntry<RowData>["header"] = {
          type: "oversized",
          value: { canonical: true, headPageId: linkedPageList.headPageId },
        }
        if (headerStruct.sizeof(oversizedHeader) > terminalSlot.length) {
          throw new Error(
            "Can't even store the oversized header in existing slot. This should never happen.",
          )
        }
        await this.bufferPool.writeToPage(
          terminal.canonicalId.pageId,
          (view) => {
            headerStruct.writeAt(oversizedHeader, view, terminalSlot.offset)
          },
        )
        return terminal.canonicalId
      }
      // case 1.2.2: the new record is not oversized
      const terminalId = await this._insert(data, { canonical: false })
      await this.bufferPool.writeToPage(terminal.canonicalId.pageId, (view) => {
        headerStruct.writeAt(
          {
            type: "forwarded",
            value: { canonical: true, forwardRowId: terminalId },
          },
          view,
          terminalSlot.offset,
        )
      })
      if (terminal.header?.type === "oversized") {
        // the old record was oversized, so we need to free it
        await new LinkedPageList(
          this.bufferPool,
          terminal.header.value.headPageId,
        ).drop()
      }
      return terminal.canonicalId
    }

    // case 2: this is a forwarded entry
    const newHeader: HeapFileEntry<RowData>["header"] = {
      type: "normal",
      value: { canonical: false },
    }
    const newSize = headerStruct.sizeof(newHeader) +
      this.recordStruct.sizeof(data)
    if (newSize <= terminalSlot.length) {
      // case 2.1: this is a forwarded entry, and the new record fits the existing slot
      await this.bufferPool.writeToPage(terminal.terminalId.pageId, (view) => {
        headerStruct.writeAt(newHeader, view, terminalSlot.offset)
        this.recordStruct.writeAt(
          data,
          view,
          terminalSlot.offset + headerStruct.sizeof(newHeader),
        )
      })
      if (terminal.forwardHeader?.type === "oversized") {
        // the old record was oversized, so we need to free it
        await new LinkedPageList(
          this.bufferPool,
          terminal.forwardHeader.value.headPageId,
        ).drop()
      }
      return terminal.canonicalId
    }
    // case 2.2: this is a forwarded entry, and the new record does not fit the existing slot
    // free up the old slot
    await this.bufferPool.writeToPage(terminal.terminalId.pageId, (view) => {
      const recordPage = new WriteableVariableLengthRecordPage(view)
      recordPage.freeSlot(terminal.terminalId.slotIndex)
    })
    if (terminal.forwardHeader?.type === "oversized") {
      // the old record was oversized, so we need to free it
      await new LinkedPageList(
        this.bufferPool,
        terminal.forwardHeader.value.headPageId,
      ).drop()
    }
    // insert the new record
    const terminalId = await this._insert(data, { canonical: false })
    // update the forward record
    const canoncalSlot = new ReadonlyVariableLengthRecordPage(
      await this.bufferPool.getPageView(terminal.canonicalId.pageId),
    )
      .getSlotEntry(terminal.canonicalId.slotIndex)

    await this.bufferPool.writeToPage(terminal.canonicalId.pageId, (view) => {
      headerStruct.writeAt(
        {
          type: "forwarded",
          value: {
            canonical: true,
            forwardRowId: terminalId,
          },
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
    const header: HeapFileEntry<RowData>["header"] = {
      type: "normal",
      value: { canonical },
    }
    const numBytes = headerStruct.sizeof(header) +
      this.recordStruct.sizeof(data)

    if (numBytes > this.heapPageFile.maxAllocSize) {
      // Well, we can't fit this record in a single page, so we'll just
      // split the record up across multiple pages.
      const linkedPageListHead = await this.bufferPool.allocatePage()
      const linkedPageList = new LinkedPageList(
        this.bufferPool,
        linkedPageListHead,
      )
      await linkedPageList.writeData(this.recordStruct.toUint8Array(data))

      const oversizedHeader: HeapFileEntry<RowData>["header"] = {
        type: "oversized",
        value: { canonical, headPageId: linkedPageListHead },
      }
      const { pageId, allocInfo: { slot, slotIndex } } = await this.heapPageFile
        .allocateSpace(headerStruct.sizeof(oversizedHeader))
      await this.bufferPool.writeToPage(pageId, (view) => {
        headerStruct.writeAt(oversizedHeader, view, slot.offset)
      })
      return { pageId, slotIndex }
    }

    const { pageId, allocInfo: { slot, slotIndex } } = await this.heapPageFile
      .allocateSpace(numBytes)
    if (slot.length < numBytes) {
      // This should never happen since we just allocated the space
      // but we'll check just in case to make it easier to find bugs.
      throw new Error("Record too large")
    }
    await this.bufferPool.writeToPage(pageId, (view) => {
      headerStruct.writeAt(header, view, slot.offset)
      this.recordStruct.writeAt(
        data,
        view,
        slot.offset + headerStruct.sizeof(header),
      )
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
      HeapFileTableStorage<StoredRecordForTableSchema<SchemaT>>
    >
    : never
