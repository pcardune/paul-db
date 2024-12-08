import { IStruct, Struct } from "../binary/Struct.ts"
import { PushTuple } from "../typetools.ts"
import {
  ColumnBuilder,
  computedColumn,
  StoredRecordForColumnSchemas,
} from "./columns/ColumnBuilder.ts"
import { ColumnType } from "./columns/ColumnType.ts"
import { NonEmptyTuple, Simplify } from "npm:type-fest"
import * as Column from "./columns/index.ts"

type InsertRecordForColumnSchemas<CS extends Column.Stored.Any[]> =
  & {
    [K in Extract<CS[number], { defaultValueFactory: undefined }>["name"]]:
      Column.Stored.GetValue<Extract<CS[number], { name: K }>>
  }
  & {
    [K in Extract<CS[number], Column.Stored.WithDefaultValue>["name"]]?:
      Column.Stored.GetValue<Extract<CS[number], { name: K }>>
  }

/**
 * Infers the type of a record that can be inserted into a table with the given
 * schema. All columns with default values are optional.
 */
export type InsertRecordForTableSchema<TS extends SomeTableSchema> =
  InsertRecordForColumnSchemas<TS["columns"]>

/**
 * Infers the type of a record that is stored in a table with the given schema.
 * This is what you'll get back when querying the table.
 */
export type StoredRecordForTableSchema<TS extends SomeTableSchema> =
  StoredRecordForColumnSchemas<TS["columns"]>

export type SomeTableSchema = TableSchema<
  string,
  Column.Stored.Any[],
  Column.Computed.Any[]
>

export class TableSchema<
  TableName extends string,
  ColumnSchemasT extends Column.Stored.Any[],
  ComputedColumnsT extends Column.Computed.Any[],
> {
  private columnsByName: Record<string, Column.Any> = {}

  private constructor(
    public readonly name: TableName,
    public readonly columns: ColumnSchemasT,
    public readonly computedColumns: ComputedColumnsT,
  ) {
    this.columnsByName = Object.fromEntries(
      [...columns, ...computedColumns].map((c) => [c.name, c]),
    )
  }

  withName<NewName extends string>(
    name: NewName,
  ): TableSchema<NewName, ColumnSchemasT, ComputedColumnsT> {
    return new TableSchema(name, this.columns, this.computedColumns)
  }

  getColumnByName(name: string): Column.Any | null {
    return this.columnsByName[name] ?? null
  }

  static create<TableName extends string>(
    name: TableName,
  ): TableSchema<TableName, [], []> {
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
    PushTuple<
      ComputedColumnsT,
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
    indexConfig: Partial<Column.Index.ShouldIndex> = {},
  ) {
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
    [...ColumnSchemasT, ...ColumnsT],
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

export function makeTableSchemaStruct<SchemaT extends SomeTableSchema>(
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
