// deno-lint-ignore-file no-explicit-any
import { PushTuple } from "../typetools.ts"
import { ColumnType } from "./ColumnType.ts"
import { Serializer } from "./Serializers.ts"

export class ColumnSchema<
  Name extends string,
  ValueT,
  UniqueT extends boolean,
  IndexedT extends boolean,
> {
  constructor(
    readonly name: Name,
    readonly type: ColumnType<ValueT>,
    readonly unique: UniqueT,
    readonly indexed: IndexedT,
  ) {}

  withName<NewName extends string>(name: NewName) {
    return new ColumnSchema<NewName, ValueT, UniqueT, IndexedT>(
      name,
      this.type,
      this.unique,
      this.indexed,
    )
  }

  makeUnique(): ColumnSchema<Name, ValueT, true, true> {
    return new ColumnSchema(this.name, this.type, true, true)
  }

  makeIndexed(): ColumnSchema<Name, ValueT, UniqueT, true> {
    return new ColumnSchema(this.name, this.type, this.unique, true)
  }
}

export function column<Name extends string, ValueT>(
  name: Name,
  type: ColumnType<ValueT>,
) {
  return new ColumnSchema(name, type, false, false)
}

class ComputedColumnSchema<
  Name extends string,
  UniqueT extends boolean,
  IndexedT extends boolean,
  InputT,
  OutputT,
> {
  constructor(
    readonly name: Name,
    readonly unique: UniqueT,
    readonly indexed: IndexedT,
    readonly compute: (input: InputT) => OutputT,
  ) {
  }

  makeUnique(): ComputedColumnSchema<Name, true, true, InputT, OutputT> {
    return new ComputedColumnSchema(this.name, true, true, this.compute)
  }

  makeIndexed(): ComputedColumnSchema<Name, UniqueT, true, InputT, OutputT> {
    return new ComputedColumnSchema(this.name, this.unique, true, this.compute)
  }
}

export function computedColumn<
  Name extends string,
  InputT,
  OutputT,
>(
  name: Name,
  compute: (input: InputT) => OutputT,
) {
  return new ComputedColumnSchema(name, false, false, compute)
}

export type ValueForColumnSchema<C> = C extends
  ColumnSchema<any, infer V, any, any> ? V
  : never

export type InputForComputedColumnSchema<C> = C extends
  ComputedColumnSchema<any, any, any, infer I, any> ? I : never
export type OutputForComputedColumnSchema<C> = C extends
  ComputedColumnSchema<any, any, any, any, infer O> ? O : never
export type ColumnSchemaWithValue<V> = ColumnSchema<string, V, false, false>
export type SomeColumnSchema = ColumnSchema<string, any, boolean, boolean>
export type IndexedColumnSchema = ColumnSchema<string, any, any, true>
export type SomeComputedColumnSchema = ComputedColumnSchema<
  string,
  boolean,
  boolean,
  any,
  any
>

export type RecordForColumnSchema<
  CS extends SomeColumnSchema | SomeComputedColumnSchema,
> = CS extends ComputedColumnSchema<string, boolean, boolean, any, any> ? {
    [K in CS["name"]]?: never
  }
  : {
    [K in CS["name"]]: ValueForColumnSchema<CS>
  }
type RecordForColumnSchemas<
  CS extends (SomeColumnSchema | SomeComputedColumnSchema)[],
> = {
  [K in CS[number]["name"]]: ValueForColumnSchema<
    Extract<CS[number], { name: K }>
  >
}

export type RecordForTableSchema<TS extends SomeTableSchema> =
  RecordForColumnSchemas<
    TS["columns"]
  >

export type SomeTableSchema = TableSchema<
  string,
  SomeColumnSchema[],
  SomeComputedColumnSchema[]
>

export class TableSchema<
  TableName extends string,
  ColumnSchemasT extends SomeColumnSchema[],
  ComputedColumnsT extends SomeComputedColumnSchema[],
> {
  private constructor(
    public readonly name: TableName,
    public readonly columns: ColumnSchemasT,
    public readonly computedColumns: ComputedColumnsT,
  ) {}

  getColumns(): ColumnSchemasT {
    return this.columns
  }

  static create<Name extends string>(name: Name) {
    return new TableSchema(name, [], [])
  }

  isValidRecord(record: RecordForColumnSchemas<ColumnSchemasT>): boolean {
    for (const column of this.columns) {
      const value = record[column.name as keyof typeof record]
      if (column instanceof ComputedColumnSchema) {
        continue
      }
      if (!column.type.isValid(value)) {
        return false
      }
    }
    return true
  }

  withComputedColumn<
    CName extends string,
    CUnique extends boolean,
    CIndexed extends boolean,
    COutput,
  >(
    column: ComputedColumnSchema<
      CName,
      CUnique,
      CIndexed,
      RecordForColumnSchemas<ColumnSchemasT>,
      COutput
    >,
  ): TableSchema<
    TableName,
    ColumnSchemasT,
    PushTuple<
      ComputedColumnsT,
      ComputedColumnSchema<
        CName,
        CUnique,
        CIndexed,
        RecordForColumnSchemas<ColumnSchemasT>,
        COutput
      >
    >
  > {
    return new TableSchema(this.name, this.columns, [
      ...this.computedColumns,
      column,
    ])
  }

  withColumn<
    CName extends string,
    CValue,
    CUnique extends boolean,
    CIndexed extends boolean,
  >(
    column: ColumnSchema<CName, CValue, CUnique, CIndexed>,
  ): TableSchema<
    TableName,
    PushTuple<ColumnSchemasT, ColumnSchema<CName, CValue, CUnique, CIndexed>>,
    ComputedColumnsT
  >
  withColumn<CName extends string, CValue>(
    name: CName,
    type: ColumnType<CValue>,
  ): TableSchema<
    TableName,
    PushTuple<ColumnSchemasT, ColumnSchema<CName, CValue, false, false>>,
    ComputedColumnsT
  >
  withColumn<CName extends string, CValue>(
    name: CName,
    type: ColumnType<CValue>,
    options: {
      unique: true
    },
  ): TableSchema<
    TableName,
    PushTuple<ColumnSchemasT, ColumnSchema<CName, CValue, true, true>>,
    ComputedColumnsT
  >
  withColumn<
    CName extends string,
    CValue,
    CUnique extends boolean,
    CIndexed extends boolean,
  >(
    nameOrColumn: CName | ColumnSchema<CName, CValue, CUnique, CIndexed>,
    type?: ColumnType<CValue>,
    options?: { unique: true },
  ): TableSchema<
    TableName,
    PushTuple<ColumnSchemasT, ColumnSchema<CName, CValue, CUnique, CIndexed>>,
    ComputedColumnsT
  > {
    if (typeof nameOrColumn === "string") {
      if (type) {
        if (options) {
          return new TableSchema(this.name, [
            ...this.columns,
            column(nameOrColumn, type)
              .makeUnique() as ColumnSchema<CName, CValue, CUnique, CIndexed>,
          ], this.computedColumns)
        }
        return new TableSchema(this.name, [
          ...this.columns,
          column(nameOrColumn, type) as ColumnSchema<
            CName,
            CValue,
            CUnique,
            CIndexed
          >,
        ], this.computedColumns)
      } else {
        throw new Error("Type and options are required")
      }
    }
    return new TableSchema(this.name, [
      ...this.columns,
      nameOrColumn,
    ], this.computedColumns)
  }
}

export function makeTableSchemaSerializer<SchemaT extends SomeTableSchema>(
  schema: SchemaT,
): Serializer<RecordForTableSchema<SchemaT>> | undefined {
  if (schema.columns.some((c) => c.type.serializer == null)) {
    // can't make a serializer if any of the columns don't have a serializer
    return
  }

  let headerSize = 0
  let fixedSize = 0
  for (const column of schema.columns) {
    const serializer = column.type.serializer!
    if (serializer.fixedLength === false) {
      headerSize += 4
    } else {
      fixedSize += serializer.fixedLength
    }
  }
  return {
    fixedLength: headerSize > 0 ? false : fixedSize,
    serialize: (record: RecordForTableSchema<typeof schema>) => {
      const fixedBuffers: ArrayBuffer[] = []
      const varBuffers: ArrayBuffer[] = []
      for (const column of schema.columns) {
        const serializer = column.type.serializer!
        const columnValue = (record as any)[column.name]
        if (serializer.fixedLength !== false) {
          fixedBuffers.push(serializer.serialize(columnValue))
        } else {
          varBuffers.push(serializer.serialize(columnValue))
        }
      }
      const totalSize = headerSize + fixedSize +
        varBuffers.reduce((acc, b) => acc + b.byteLength, 0)
      const buffer = new ArrayBuffer(totalSize)
      const view = new DataView(buffer)
      let headerOffset = 0
      let fixedValueOffset = headerSize
      let varValueOffset = headerSize + fixedSize

      const bufferUint8 = new Uint8Array(buffer)
      // write the variable length values first
      for (const varBuffer of varBuffers) {
        view.setUint32(headerOffset, varValueOffset)
        headerOffset += 4
        bufferUint8.set(new Uint8Array(varBuffer), varValueOffset)
        varValueOffset += varBuffer.byteLength
      }
      // write the fixed length values
      for (const fixedBuffer of fixedBuffers) {
        bufferUint8.set(new Uint8Array(fixedBuffer), fixedValueOffset)
        fixedValueOffset += fixedBuffer.byteLength
      }
      return buffer
    },
    deserialize: (buffer: DataView) => {
      const record: Record<string, any> = {}
      let headerOffset = 0
      let fixedValueOffset = headerSize
      for (const column of schema.columns) {
        const serializer = column.type.serializer!
        if (serializer.fixedLength !== false) {
          record[column.name] = serializer.deserialize(
            new DataView(
              buffer.buffer,
              buffer.byteOffset + fixedValueOffset,
              serializer.fixedLength,
            ),
          )
          fixedValueOffset += serializer.fixedLength
        } else {
          const varLengthStartPointer = buffer.getUint32(headerOffset)
          // 0 for the first one, then 4 for the next one
          // the total headerSize is 8.
          // so if we are at offset 4, then the length of the thing goes
          // to the end of the buffer. 4 + 4 = 8 vs 0 + 4 = 4
          let varLengthSize: number
          if (headerOffset + 4 >= headerSize) {
            // this is the last variable length value
            varLengthSize = buffer.byteLength - varLengthStartPointer
          } else {
            const nextVarLengthStart = buffer.getUint32(headerOffset + 4)
            varLengthSize = nextVarLengthStart - varLengthStartPointer
          }
          record[column.name] = serializer.deserialize(
            new DataView(
              buffer.buffer,
              buffer.byteOffset + varLengthStartPointer,
              varLengthSize,
            ),
          )
          headerOffset += 4
        }
      }
      return record as RecordForTableSchema<typeof schema>
    },
  }
}
