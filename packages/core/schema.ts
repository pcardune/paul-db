// deno-lint-ignore-file no-explicit-any
import { Comparator, EqualityChecker } from "./DiskBTree.ts"

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
> {
  readonly name: Name
  readonly unique: UniqueT
  readonly type: ColumnType<ValueT>

  constructor(
    name: Name,
    type: ColumnType<ValueT>,
    unique: UniqueT,
  ) {
    this.name = name
    this.type = type
    this.unique = unique
  }

  withName<NewName extends string>(name: NewName) {
    return new ColumnSchema<NewName, ValueT, UniqueT>(
      name,
      this.type,
      this.unique,
    )
  }

  makeUnique(): ColumnSchema<Name, ValueT, true> {
    return new ColumnSchema(this.name, this.type, true)
  }
}

export function column<Name extends string, ValueT>(
  name: Name,
  type: ColumnType<ValueT>,
) {
  return new ColumnSchema(name, type, false)
}

class ComputedColumnSchema<
  Name extends string,
  UniqueT extends boolean,
  InputT,
  OutputT,
> {
  constructor(
    readonly name: Name,
    readonly unique: UniqueT,
    readonly compute: (input: InputT) => OutputT,
  ) {
  }

  makeUnique(): ComputedColumnSchema<Name, true, InputT, OutputT> {
    return new ComputedColumnSchema(this.name, true, this.compute)
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
  return new ComputedColumnSchema(name, false, compute)
}

export type ValueForColumnSchema<C> = C extends ColumnSchema<any, infer V, any>
  ? V
  : never

export type SomeColumnSchema = ColumnSchema<string, any, boolean>
export type SomeComputedColumnSchema = ComputedColumnSchema<
  string,
  boolean,
  any,
  any
>
type PushTuple<T extends any[], V> = [...T, V]

export type RecordForColumnSchema<
  CS extends SomeColumnSchema | SomeComputedColumnSchema,
> = CS extends ComputedColumnSchema<string, boolean, any, any> ? {
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
  name: TableName
  columns: ColumnSchemasT
  computedColumns: ComputedColumnsT

  private constructor(
    name: TableName,
    columns: ColumnSchemasT,
    computedColumns: ComputedColumnsT,
  ) {
    this.name = name
    this.columns = columns
    this.computedColumns = computedColumns
  }

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
    COutput,
  >(
    column: ComputedColumnSchema<
      CName,
      CUnique,
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

  withColumn<CName extends string, CValue, CUnique extends boolean>(
    column: ColumnSchema<CName, CValue, CUnique>,
  ): TableSchema<
    TableName,
    PushTuple<ColumnSchemasT, ColumnSchema<CName, CValue, CUnique>>,
    ComputedColumnsT
  >
  withColumn<CName extends string, CValue>(
    name: CName,
    type: ColumnType<CValue>,
  ): TableSchema<
    TableName,
    PushTuple<ColumnSchemasT, ColumnSchema<CName, CValue, false>>,
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
    PushTuple<ColumnSchemasT, ColumnSchema<CName, CValue, true>>,
    ComputedColumnsT
  >
  withColumn<
    CName extends string,
    CValue,
    CUnique extends boolean,
  >(
    nameOrColumn: CName | ColumnSchema<CName, CValue, CUnique>,
    type?: ColumnType<CValue>,
    options?: { unique: true },
  ): TableSchema<
    TableName,
    PushTuple<ColumnSchemasT, ColumnSchema<CName, CValue, CUnique>>,
    ComputedColumnsT
  > {
    if (typeof nameOrColumn === "string") {
      if (type) {
        if (options) {
          return new TableSchema(this.name, [
            ...this.columns,
            column(nameOrColumn, type)
              .makeUnique() as ColumnSchema<CName, CValue, CUnique>,
          ], this.computedColumns)
        }
        return new TableSchema(this.name, [
          ...this.columns,
          column(nameOrColumn, type) as ColumnSchema<
            CName,
            CValue,
            CUnique
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
