// deno-lint-ignore-file no-explicit-any
import { Comparator, EqualityChecker } from "./DiskBTree.ts"

export const ColumnTypes = {
  any<T>() {
    return new ColumnType<T>({ isValid: (_value: T): _value is T => true })
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

  private constructor(
    name: Name,
    type: ColumnType<ValueT>,
    unique: UniqueT,
  ) {
    this.name = name
    this.type = type
    this.unique = unique
  }

  static create<Name extends string, ValueT, UniqueT extends boolean>(
    name: Name,
    type: ColumnType<ValueT>,
    unique: UniqueT,
  ) {
    return new ColumnSchema(name, type, unique)
  }
}

export type SomeColumnSchema = ColumnSchema<string, any, boolean>

type PushTuple<T extends any[], V> = [...T, V]

export type RecordForColumnSchema<CS extends SomeColumnSchema[]> = {
  [K in CS[number]["name"]]:
    Extract<CS[number], { name: K }>["type"]["isValid"] extends (
      value: infer V,
    ) => boolean ? V
      : never
}

export type RecordForTableSchema<TS extends TableSchema<any, any>> =
  RecordForColumnSchema<
    TS["columns"]
  >

export class TableSchema<
  TableName extends string,
  ColumnSchemasT extends SomeColumnSchema[],
> {
  name: TableName
  columns: ColumnSchemasT

  private constructor(name: TableName, columns: ColumnSchemasT) {
    this.name = name
    this.columns = columns
  }

  getColumns(): ColumnSchemasT {
    return this.columns
  }

  static create<Name extends string>(name: Name) {
    return new TableSchema(name, [])
  }

  isValidRecord(record: RecordForColumnSchema<ColumnSchemasT>): boolean {
    for (const column of this.columns) {
      const value = record[column.name as keyof typeof record]
      if (!column.type.isValid(value)) {
        return false
      }
    }
    return true
  }

  withColumn<CName extends string, CValue, CUnique extends boolean>(
    name: CName,
    type: ColumnType<CValue>,
    unique: CUnique,
  ): TableSchema<
    TableName,
    PushTuple<ColumnSchemasT, ColumnSchema<CName, CValue, CUnique>>
  > {
    return new TableSchema(this.name, [
      ...this.columns,
      ColumnSchema.create(name, type, unique),
    ])
  }
}
