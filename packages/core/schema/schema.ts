// deno-lint-ignore-file no-explicit-any
import {
  FixedWidthStruct,
  IStruct,
  VariableWidthStruct,
} from "../binary/Struct.ts"
import { PushTuple } from "../typetools.ts"
import { ColumnType } from "./ColumnType.ts"

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
    readonly type: ColumnType<OutputT>,
    readonly unique: UniqueT,
    readonly indexed: IndexedT,
    readonly compute: (input: InputT) => OutputT,
  ) {
  }

  makeUnique(): ComputedColumnSchema<Name, true, true, InputT, OutputT> {
    return new ComputedColumnSchema(
      this.name,
      this.type,
      true,
      true,
      this.compute,
    )
  }

  makeIndexed(): ComputedColumnSchema<Name, UniqueT, true, InputT, OutputT> {
    return new ComputedColumnSchema(
      this.name,
      this.type,
      this.unique,
      true,
      this.compute,
    )
  }
}

export function computedColumn<
  Name extends string,
  InputT,
  OutputT,
>(
  name: Name,
  type: ColumnType<OutputT>,
  compute: (input: InputT) => OutputT,
) {
  return new ComputedColumnSchema(name, type, false, false, compute)
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
): IStruct<RecordForTableSchema<SchemaT>> | undefined {
  if (schema.columns.some((c) => c.type.serializer == null)) {
    // can't make a serializer if any of the columns don't have a serializer
    return
  }

  let headerSize = 0
  let fixedSize = 0
  for (const column of schema.columns) {
    const serializer = column.type.serializer!
    if (serializer instanceof FixedWidthStruct) {
      fixedSize += serializer.size
    } else {
      headerSize += 4
    }
  }

  return new VariableWidthStruct({
    sizeof: (record) => {
      return schema.columns.reduce((acc, column) => {
        return column.type.serializer!.sizeof((record as any)[column.name]) +
          acc
      }, 0)
    },
    write: (record, view) => {
      let offset = 0
      for (const column of schema.columns) {
        const serializer = column.type.serializer!
        const columnValue = (record as any)[column.name]
        serializer.writeAt(columnValue, view, offset)
        offset += serializer.sizeof(columnValue)
      }
    },
    read: (view) => {
      const record: Record<string, any> = {}
      let offset = 0
      for (const column of schema.columns) {
        const serializer = column.type.serializer!
        record[column.name] = serializer.readAt(view, offset)
        offset += serializer.sizeof(record[column.name])
      }
      return record as RecordForTableSchema<typeof schema>
    },
  })
}
