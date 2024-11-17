// deno-lint-ignore-file no-explicit-any
import { Comparator, EqualityChecker } from "./DiskBTree.ts"
import { PushTuple } from "./typetools.ts"

export const ColumnTypes = {
  any<T>() {
    return new ColumnType<T>({ isValid: (_value: T): _value is T => true })
  },
  caseInsensitiveString() {
    return new ColumnType<string>({
      isValid: (value) => typeof value === "string",
      equals: (a, b) => a.toLowerCase() === b.toLowerCase(),
      compare: (a, b) => a.toLowerCase().localeCompare(b.toLowerCase()),
    })
  },
  positiveNumber() {
    return new ColumnType<number>({
      isValid: (value) => value > 0,
    })
  },
}

export class ColumnType<T> {
  isValid: (value: T) => boolean
  isEqual: EqualityChecker<T>
  compare: Comparator<T>
  constructor({
    isValid,
    equals = (a: T, b: T) => a === b,
    compare = (a: T, b: T) => (a > b ? 1 : a < b ? -1 : 0),
  }: {
    isValid: (value: T) => boolean
    equals?: EqualityChecker<T>
    compare?: Comparator<T>
  }) {
    this.isValid = isValid
    this.isEqual = equals
    this.compare = compare
  }
}

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

export type SomeColumnSchema = ColumnSchema<string, any, boolean, any>
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

export type RecordForTableSchema<TS extends TableSchema<any, any, any>> =
  RecordForColumnSchemas<
    TS["columns"]
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
