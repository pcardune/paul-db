import { IStruct, Struct } from "../binary/Struct.ts"
import {
  ColumnBuilder,
  computedColumn,
  StoredRecordForColumnSchemas,
} from "./columns/ColumnBuilder.ts"
import { ColumnType } from "./columns/ColumnType.ts"
import type {
  ConditionalPick,
  EmptyObject,
  Merge,
  NonEmptyTuple,
  Simplify,
} from "type-fest"
import * as Column from "./columns/index.ts"

export { Column }

type UnwrapColumnBuilder<T extends Column.Stored.Any> = T extends ColumnBuilder<
  infer Name,
  infer ValueT,
  infer UniqueT,
  infer IndexedT,
  infer DefaultValueFactoryT
> ? Column.Stored.Any<Name, ValueT, UniqueT, IndexedT, DefaultValueFactoryT>
  : T

type RemoveSymbols<T> = Simplify<
  {
    [K in keyof T as K extends symbol ? never : K]: T[K]
  }
>

type ColumnValues<CS extends Record<string, Column.Stored.Any>> = {
  [K in keyof CS]: Column.Stored.GetValue<CS[K]>
}

export type InsertRecordForColumnSchemas<
  CS extends Record<string, Column.Stored.Any>,
> = Simplify<
  & ColumnValues<ConditionalPick<CS, { defaultValueFactory: undefined }>>
  & Partial<
    ColumnValues<ConditionalPick<CS, Column.Stored.WithDefaultValue>>
  >
>

/**
 * Infers the type of a record that can be inserted into a table with the given
 * schema. All columns with default values are optional.
 */
export type InsertRecordForTableSchema<TS extends IHaveStoredColumns> =
  InsertRecordForColumnSchemas<TS["storedColumnsByName"]>

/**
 * Infers the type of a record that is stored in a table with the given schema.
 * This is what you'll get back when querying the table.
 */
export type StoredRecordForTableSchema<TS extends ISchema> =
  StoredRecordForColumnSchemas<TS["columnsByName"]>

/**
 * Represents an arbitrary table schema
 */
export type SomeTableSchema = TableSchema<
  string,
  StoredColumnRecord,
  any
>

export type TableSchemaColumnNames<TS extends ISchema> = Exclude<
  keyof (TS["columnsByName"]),
  symbol
>

interface IHaveStoredColumns<
  ColumnSchemasT extends Record<string, Column.Stored.Any> = Record<
    string,
    Column.Stored.Any
  >,
> {
  readonly storedColumnsByName: ColumnSchemasT
}

export type ColumnRecord = Record<string, Column.Any>
export type StoredColumnRecord = Record<string, Column.Stored.Any>
export type ComputedColumnRecord = Record<string, Column.Computed.Any>

export interface ISchema<
  TName extends string = string,
  ColumnsT extends ColumnRecord = ColumnRecord,
> {
  readonly name: TName
  readonly columnsByName: ColumnsT
}

export class TableSchema<
  TableName extends string,
  ColumnSchemasT extends StoredColumnRecord,
  ComputedColumnsT extends ComputedColumnRecord,
> implements
  IHaveStoredColumns<ColumnSchemasT>,
  ISchema<TableName, Merge<ColumnSchemasT, ComputedColumnsT>> {
  readonly storedColumnsByName: ColumnSchemasT
  readonly computedColumnsByName: ComputedColumnsT
  readonly columnsByName: ColumnSchemasT & ComputedColumnsT

  private constructor(
    public readonly name: TableName,
    public readonly columns: Column.Stored.Any[],
    public readonly computedColumns: Column.Computed.Any[],
  ) {
    this.storedColumnsByName = Object.fromEntries(
      columns.map((c) => [c.name, c]),
    ) as unknown as ColumnSchemasT
    this.computedColumnsByName = Object.fromEntries(
      computedColumns.map((c) => [c.name, c]),
    ) as unknown as ComputedColumnsT
    this.columnsByName = {
      ...this.storedColumnsByName,
      ...this.computedColumnsByName,
    }
  }

  withName<NewName extends string>(
    name: NewName,
  ): TableSchema<NewName, ColumnSchemasT, ComputedColumnsT> {
    return new TableSchema(name, this.columns, this.computedColumns)
  }

  getColumnByName(
    name: Extract<keyof ColumnSchemasT | keyof ComputedColumnsT, string>,
  ): Column.Any | undefined {
    return this.columnsByName[name]
  }

  getColumnByNameOrThrow(
    name: Extract<keyof ColumnSchemasT | keyof ComputedColumnsT, string>,
  ): Column.Any {
    const column = this.getColumnByName(name)
    if (column == null) {
      throw new Error(`Column '${name}' not found`)
    }
    return column
  }

  static create<TableName extends string>(
    name: TableName,
  ): TableSchema<TableName, EmptyObject, EmptyObject> {
    return new TableSchema(name, [], [])
  }

  isValidInsertRecord(
    record: Simplify<InsertRecordForColumnSchemas<ColumnSchemasT>>,
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
    record: Simplify<StoredRecordForColumnSchemas<ColumnSchemasT>>,
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
    CIndexed extends Column.Index.Config,
    COutput,
  >(
    column: Column.Computed.Any<
      CName,
      CUnique,
      CIndexed,
      StoredRecordForColumnSchemas<ColumnSchemasT>,
      COutput
    >,
  ): TableSchema<
    TableName,
    ColumnSchemasT,
    & ComputedColumnsT
    & Record<
      CName,
      Column.Computed.Any<
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
    CInputNames extends ColumnSchemasT[string]["name"][],
    CInput extends StoredRecordForColumnSchemas<
      Pick<ColumnSchemasT, CInputNames[number]>
    >,
    COutput,
  >(
    name: CName,
    type: ColumnType<COutput>,
    _inputColumns: CInputNames, // just here for type inference
    compute: (input: CInput) => COutput,
    indexConfig: Partial<Column.Index.ShouldIndex> = {},
  ): TableSchema<
    TableName,
    ColumnSchemasT,
    & ComputedColumnsT
    & Record<
      CName,
      Column.Computed.Any<
        CName,
        true,
        Column.Index.ShouldIndex,
        CInput,
        COutput
      >
    >
  > {
    return new TableSchema(this.name, this.columns, [
      ...this.computedColumns,
      computedColumn(
        name,
        type,
        compute,
      ).unique(indexConfig),
    ])
  }

  with<ColumnsT extends NonEmptyTuple<Column.Stored.Any>>(
    ...columns: ColumnsT
  ): TableSchema<
    TableName,
    RemoveSymbols<
      Simplify<
        & ColumnSchemasT
        & { [K in ColumnsT[number] as K["name"]]: UnwrapColumnBuilder<K> }
      >
    >,
    ComputedColumnsT
  > {
    for (const column of columns) {
      const columnName = column.name
      if (
        this.columns.some((c) => c.name === columnName) ||
        this.computedColumns.some((c) => c.name === columnName)
      ) {
        throw new Error(`Column '${columnName}' already exists`)
      }
    }
    return new TableSchema(this.name, [
      ...this.columns,
      ...(columns.map((c) =>
        c instanceof ColumnBuilder ? c.finalize() : c
      ) as unknown as ColumnsT),
    ], this.computedColumns)
  }
}

/**
 * Create an empty table schema for a table with the given name.
 *
 * @param name
 */
export function create<TableName extends string>(
  name: TableName,
): TableSchema<TableName, EmptyObject, EmptyObject> {
  return TableSchema.create(name)
}

export function makeTableSchemaStruct<
  ColumnSchemasT extends Record<string, Column.Stored.Any>,
>(
  schema: TableSchema<
    string,
    ColumnSchemasT,
    Record<string, Column.Computed.Any>
  >,
): IStruct<StoredRecordForColumnSchemas<ColumnSchemasT>> | undefined {
  if (schema.columns.some((c) => c.type.serializer == null)) {
    // can't make a serializer if any of the columns don't have a serializer
    return
  }

  return Struct.record(
    Object.fromEntries(
      schema.columns.map((c, i) => [c.name, [i, c.type.serializer!]]),
    ),
  ) as unknown as IStruct<StoredRecordForColumnSchemas<ColumnSchemasT>>
}
