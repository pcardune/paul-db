// deno-lint-ignore-file no-explicit-any
export const ColumnTypes = {
  any<T>() {
    return (_value: T): _value is T => true
  },
  positiveNumber() {
    return (value: number): value is number => value > 0
  },
}

export class ColumnSchema<Name extends string, ValueT> {
  name: Name
  isValid: (value: ValueT) => boolean

  private constructor(name: Name, isValid: (value: ValueT) => boolean) {
    this.name = name
    this.isValid = isValid
  }

  static create<Name extends string, ValueT>(
    name: Name,
    isValid: (value: ValueT) => boolean,
  ) {
    return new ColumnSchema(name, isValid)
  }
}

type SomeColumnSchema = ColumnSchema<string, any>

type PushTuple<T extends any[], V> = [...T, V]

export type RecordForColumnSchema<CS extends SomeColumnSchema[]> = {
  [K in CS[number]["name"]]: Extract<CS[number], { name: K }>["isValid"] extends
    (
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

  static create<Name extends string>(name: Name) {
    return new TableSchema(name, [])
  }

  isValidRecord(record: RecordForColumnSchema<ColumnSchemasT>): boolean {
    for (const column of this.columns) {
      const value = record[column.name as keyof typeof record]
      if (!column.isValid(value)) {
        return false
      }
    }
    return true
  }

  withColumn<CName extends string, CValue>(
    name: CName,
    isValid: (value: CValue) => boolean,
  ): TableSchema<
    TableName,
    PushTuple<ColumnSchemasT, ColumnSchema<CName, CValue>>
  > {
    return new TableSchema(this.name, [
      ...this.columns,
      ColumnSchema.create(name, isValid),
    ])
  }
}
