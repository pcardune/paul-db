import { RecordForTableSchema, SomeTableSchema } from "./schema/schema.ts"

export interface ITableStorage<RowId, RowData> {
  get(id: RowId): RowData | undefined
  set(id: RowId, data: RowData): void
  remove(id: RowId): void
  values(): IteratorObject<RowData, void, void>
  commit(): void
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

  get(id: number): RowData | undefined {
    if (this.deletedRecords.has(id)) {
      return undefined
    }
    return this.data[id]
  }
  set(id: number, data: RowData): void {
    this.dirtyRecords.set(id, data)
    this.deletedRecords.delete(id)
  }

  remove(id: number): void {
    this.dirtyRecords.delete(id)
    this.deletedRecords.add(id)
  }

  commit(): void {
    for (const [id, data] of this.dirtyRecords) {
      this.data[id] = data
    }
    this.dirtyRecords.clear()
    for (const id of this.deletedRecords) {
      delete this.data[id]
    }
    Deno.writeTextFileSync(this.filename, JSON.stringify(this.data, null, 2))
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

  constructor(private data: Map<RowId, RowData> = new Map()) {
    this.dirtyRecords = new Map()
    this.deletedRecords = new Set()
  }

  static forSchema<SchemaT extends SomeTableSchema>(
    _schema: SchemaT,
  ): InMemoryTableStorage<number, RecordForTableSchema<SchemaT>> {
    return new InMemoryTableStorage()
  }

  get(id: RowId): RowData | undefined {
    if (this.deletedRecords.has(id)) {
      return undefined
    }
    return this.dirtyRecords.get(id) ?? this.data.get(id)
  }

  set(id: RowId, data: RowData): void {
    this.dirtyRecords.set(id, data)
    this.deletedRecords.delete(id)
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

  values(): IteratorObject<RowData, void, void> {
    return this.data.entries().filter(([id]) => !this.deletedRecords.has(id))
      .map(([_, data]) => data)
  }
}
