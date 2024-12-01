// deno-lint-ignore-file no-explicit-any
import { IStruct, Struct } from "../binary/Struct.ts"
import { PushTuple } from "../typetools.ts"
import { ColumnType } from "./ColumnType.ts"

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
    readonly isUnique: UniqueT,
    readonly indexed: IndexedT,
    readonly defaultValueFactory?: DefaultValueFactoryT,
  ) {}

  named<NewName extends string>(name: NewName) {
    return new ColumnSchema<
      NewName,
      ValueT,
      UniqueT,
      IndexedT,
      DefaultValueFactoryT
    >(
      name,
      this.type,
      this.isUnique,
      this.indexed,
    )
  }

  unique(
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

  index(): ColumnSchema<
    Name,
    ValueT,
    UniqueT,
    ColumnIndexConfig,
    DefaultValueFactoryT
  > {
    return new ColumnSchema(
      this.name,
      this.type,
      this.isUnique,
      DEFAULT_INDEX_CONFIG,
      this.defaultValueFactory,
    )
  }

  defaultTo(
    defaultValueFactory: () => ValueT,
  ): ColumnSchema<Name, ValueT, UniqueT, IndexedT, () => ValueT> {
    return new ColumnSchema(
      this.name,
      this.type,
      this.isUnique,
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
    readonly isUnique: UniqueT,
    readonly indexed: IndexedT,
    readonly compute: (input: InputT) => OutputT,
  ) {
  }

  unique(
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

  index(
    indexConfig = DEFAULT_INDEX_CONFIG,
  ): ComputedColumnSchema<Name, UniqueT, ColumnIndexConfig, InputT, OutputT> {
    return new ComputedColumnSchema(
      this.name,
      this.type,
      this.isUnique,
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
    [],
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
    SomeColumnSchema[],
    []
  > {
    return new TableSchema(name, primaryKeyColumn ? [primaryKeyColumn] : [], [])
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
      ).unique(),
    ])
  }

  with<
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
  with<CName extends string, CValue>(
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
  with<CName extends string, CValue>(
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
  with<
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
    const columnName = typeof nameOrColumn === "string"
      ? nameOrColumn
      : nameOrColumn.name
    if (
      this.columns.some((c) => c.name === columnName) ||
      this.computedColumns.some((c) => c.name === columnName)
    ) {
      throw new Error(`Column '${columnName}' already exists`)
    }
    if (typeof nameOrColumn === "string") {
      if (type) {
        if (options) {
          return new TableSchema(this.name, [
            ...this.columns,
            column(nameOrColumn, type)
              .unique() as ColumnSchema<
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

  return Struct.record(
    Object.fromEntries(
      schema.columns.map((c, i) => [c.name, [i, c.type.serializer!]]),
    ),
  ) as unknown as IStruct<StoredRecordForTableSchema<SchemaT>>
}
