// deno-lint-ignore-file no-explicit-any
import {
  FixedWidthStruct,
  IStruct,
  VariableWidthStruct,
} from "../binary/Struct.ts"
import { PushTuple } from "../typetools.ts"
import { ColumnType, ColumnTypes } from "./ColumnType.ts"
import { ulid } from "jsr:@std/ulid"

export const _internals = {
  ulid,
}

export type ColumnIndexConfig = {
  order: number
}

export const DEFAULT_INDEX_CONFIG: ColumnIndexConfig = { order: 2 }

export class ColumnSchema<
  Name extends string,
  ValueT,
  UniqueT extends boolean,
  IndexedT extends ColumnIndexConfig | false,
  DefaultValueFactoryT extends (() => ValueT) | undefined,
> {
  constructor(
    readonly name: Name,
    readonly type: ColumnType<ValueT>,
    readonly unique: UniqueT,
    readonly indexed: IndexedT,
    readonly defaultValueFactory?: DefaultValueFactoryT,
  ) {}

  withName<NewName extends string>(name: NewName) {
    return new ColumnSchema<
      NewName,
      ValueT,
      UniqueT,
      IndexedT,
      DefaultValueFactoryT
    >(
      name,
      this.type,
      this.unique,
      this.indexed,
    )
  }

  makeUnique(
    indexConfig: ColumnIndexConfig = DEFAULT_INDEX_CONFIG,
  ): ColumnSchema<Name, ValueT, true, ColumnIndexConfig, DefaultValueFactoryT> {
    return new ColumnSchema(
      this.name,
      this.type,
      true,
      indexConfig,
      this.defaultValueFactory,
    )
  }

  makeIndexed(): ColumnSchema<
    Name,
    ValueT,
    UniqueT,
    ColumnIndexConfig,
    DefaultValueFactoryT
  > {
    return new ColumnSchema(
      this.name,
      this.type,
      this.unique,
      DEFAULT_INDEX_CONFIG,
      this.defaultValueFactory,
    )
  }

  withDefaultValue(
    defaultValueFactory: () => ValueT,
  ): ColumnSchema<Name, ValueT, UniqueT, IndexedT, () => ValueT> {
    return new ColumnSchema(
      this.name,
      this.type,
      this.unique,
      this.indexed,
      defaultValueFactory,
    )
  }
}

export function column<Name extends string, ValueT>(
  name: Name,
  type: ColumnType<ValueT>,
): ColumnSchema<Name, ValueT, false, false, undefined> {
  return new ColumnSchema(name, type, false, false)
}

class ComputedColumnSchema<
  Name extends string,
  UniqueT extends boolean,
  IndexedT extends ColumnIndexConfig | false,
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

  makeUnique(
    indexConfig = DEFAULT_INDEX_CONFIG,
  ): ComputedColumnSchema<Name, true, ColumnIndexConfig, InputT, OutputT> {
    return new ComputedColumnSchema(
      this.name,
      this.type,
      true,
      indexConfig,
      this.compute,
    )
  }

  makeIndexed(
    indexConfig = DEFAULT_INDEX_CONFIG,
  ): ComputedColumnSchema<Name, UniqueT, ColumnIndexConfig, InputT, OutputT> {
    return new ComputedColumnSchema(
      this.name,
      this.type,
      this.unique,
      indexConfig,
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
  ColumnSchema<any, infer V, any, any, any> ? V
  : never

export type InputForComputedColumnSchema<C> = C extends
  ComputedColumnSchema<any, any, any, infer I, any> ? I : never
export type OutputForComputedColumnSchema<C> = C extends
  ComputedColumnSchema<any, any, any, any, infer O> ? O : never
export type SomeColumnSchema = ColumnSchema<
  string,
  any,
  boolean,
  ColumnIndexConfig | false,
  undefined | (() => any)
>
export type IndexedColumnSchema = ColumnSchema<
  string,
  any,
  any,
  ColumnIndexConfig,
  undefined | (() => any)
>

type ColumnSchemaWithDefaultValue = ColumnSchema<
  string,
  any,
  boolean,
  ColumnIndexConfig | false,
  () => any
>

export type SomeComputedColumnSchema = ComputedColumnSchema<
  string,
  boolean,
  ColumnIndexConfig | false,
  any,
  any
>

export type RecordForColumnSchema<
  CS extends SomeColumnSchema | SomeComputedColumnSchema,
  DefaultOptional extends boolean = false,
> = CS extends
  ComputedColumnSchema<string, boolean, ColumnIndexConfig | false, any, any> ? {
    [K in CS["name"]]?: never
  }
  : DefaultOptional extends true ? CS extends ColumnSchemaWithDefaultValue ? {
        [K in CS["name"]]?: ValueForColumnSchema<CS>
      }
    : {
      [K in CS["name"]]: ValueForColumnSchema<CS>
    }
  : {
    [K in CS["name"]]: ValueForColumnSchema<CS>
  }
type StoredRecordForColumnSchemas<
  CS extends (SomeColumnSchema)[],
> = {
  [K in CS[number]["name"]]: ValueForColumnSchema<
    Extract<CS[number], { name: K }>
  >
}

type MyColumns = [
  ColumnSchema<"id", number, true, ColumnIndexConfig, () => number>,
  ColumnSchema<"name", string, false, false, undefined>,
  ColumnSchema<"age", number, false, false, undefined>,
  ColumnSchema<"email", string, true, false, undefined>,
]

type foo = InsertRecordForColumnSchemas<MyColumns>
type InsertRecordForColumnSchemas<CS extends SomeColumnSchema[]> =
  & {
    [K in Extract<CS[number], { defaultValueFactory?: undefined }>["name"]]:
      ValueForColumnSchema<Extract<CS[number], { name: K }>>
  }
  & {
    [K in Extract<CS[number], ColumnSchemaWithDefaultValue>["name"]]?:
      ValueForColumnSchema<Extract<CS[number], { name: K }>>
  }

export type InsertRecordForTableSchema<TS extends SomeTableSchema> =
  InsertRecordForColumnSchemas<TS["columns"]>
export type StoredRecordForTableSchema<TS extends SomeTableSchema> =
  StoredRecordForColumnSchemas<TS["columns"]>

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

  static create<Name extends string>(
    name: Name,
  ): TableSchema<
    Name,
    [ColumnSchema<"id", string, true, ColumnIndexConfig, () => string>],
    []
  >
  static create<
    Name extends string,
    PKColumn extends ColumnSchema<
      string,
      any,
      true,
      ColumnIndexConfig,
      undefined | (() => any)
    >,
  >(
    name: Name,
    primaryKeyColumn: PKColumn,
  ): TableSchema<Name, [PKColumn], []>
  static create(
    name: string,
    primaryKeyColumn?: ColumnSchema<
      string,
      any,
      true,
      ColumnIndexConfig,
      undefined | (() => any)
    >,
  ): TableSchema<
    string,
    [
      ColumnSchema<
        string,
        any,
        true,
        ColumnIndexConfig,
        undefined | (() => any)
      >,
    ],
    []
  > {
    if (primaryKeyColumn == null) {
      primaryKeyColumn = column("id", ColumnTypes.string())
        .makeUnique()
        .withDefaultValue(() => _internals.ulid())
    }
    return new TableSchema(name, [primaryKeyColumn], [])
  }

  isValidInsertRecord(
    record: InsertRecordForColumnSchemas<ColumnSchemasT>,
  ): { valid: true } | { valid: false; reason: string } {
    for (const column of this.columns) {
      const value = record[column.name as keyof typeof record]
      if (value == null && column.defaultValueFactory) {
        continue
      }
      if (!column.type.isValid(value)) {
        return {
          valid: false,
          reason: `Invalid value for column ${column.name}`,
        }
      }
    }
    return { valid: true }
  }

  isValidStoredRecord(
    record: StoredRecordForColumnSchemas<ColumnSchemasT>,
  ): boolean {
    for (const column of this.columns) {
      const value = record[column.name as keyof typeof record]
      if (!column.type.isValid(value)) {
        return false
      }
    }
    return true
  }

  withComputedColumn<
    CName extends string,
    CUnique extends boolean,
    CIndexed extends ColumnIndexConfig | false,
    COutput,
  >(
    column: ComputedColumnSchema<
      CName,
      CUnique,
      CIndexed,
      StoredRecordForColumnSchemas<ColumnSchemasT>,
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
        StoredRecordForColumnSchemas<ColumnSchemasT>,
        COutput
      >
    >
  > {
    return new TableSchema(this.name, this.columns, [
      ...this.computedColumns,
      column,
    ])
  }

  withUniqueConstraint<
    CName extends string,
    CInputNames extends ColumnSchemasT[number]["name"][],
    COutput,
  >(
    name: CName,
    type: ColumnType<COutput>,
    _inputColumns: CInputNames, // just here for type inference
    compute: (
      input: Pick<
        StoredRecordForColumnSchemas<ColumnSchemasT>,
        CInputNames[number]
      >,
    ) => COutput,
  ) {
    return new TableSchema(this.name, this.columns, [
      ...this.computedColumns,
      computedColumn(
        name,
        type,
        compute,
      ).makeUnique(),
    ])
  }

  withColumn<
    CName extends string,
    CValue,
    CUnique extends boolean,
    CIndexed extends ColumnIndexConfig | false,
    CDefaultValueFactory extends undefined | (() => CValue),
  >(
    column: ColumnSchema<
      CName,
      CValue,
      CUnique,
      CIndexed,
      CDefaultValueFactory
    >,
  ): TableSchema<
    TableName,
    PushTuple<
      ColumnSchemasT,
      ColumnSchema<CName, CValue, CUnique, CIndexed, CDefaultValueFactory>
    >,
    ComputedColumnsT
  >
  withColumn<CName extends string, CValue>(
    name: CName,
    type: ColumnType<CValue>,
  ): TableSchema<
    TableName,
    PushTuple<
      ColumnSchemasT,
      ColumnSchema<CName, CValue, false, false, undefined>
    >,
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
    PushTuple<
      ColumnSchemasT,
      ColumnSchema<CName, CValue, true, ColumnIndexConfig, undefined>
    >,
    ComputedColumnsT
  >
  withColumn<
    CName extends string,
    CValue,
    CUnique extends boolean,
    CIndexed extends ColumnIndexConfig | false,
  >(
    nameOrColumn:
      | CName
      | ColumnSchema<CName, CValue, CUnique, CIndexed, undefined>,
    type?: ColumnType<CValue>,
    options?: { unique: true },
  ): TableSchema<
    TableName,
    PushTuple<
      ColumnSchemasT,
      ColumnSchema<CName, CValue, CUnique, CIndexed, undefined | (() => CValue)>
    >,
    ComputedColumnsT
  > {
    if (typeof nameOrColumn === "string") {
      if (type) {
        if (options) {
          return new TableSchema(this.name, [
            ...this.columns,
            column(nameOrColumn, type)
              .makeUnique() as ColumnSchema<
                CName,
                CValue,
                CUnique,
                CIndexed,
                undefined
              >,
          ], this.computedColumns)
        }
        return new TableSchema(this.name, [
          ...this.columns,
          column(nameOrColumn, type) as ColumnSchema<
            CName,
            CValue,
            CUnique,
            CIndexed,
            undefined
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
): IStruct<StoredRecordForTableSchema<SchemaT>> | undefined {
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
      return record as StoredRecordForTableSchema<typeof schema>
    },
  })
}
