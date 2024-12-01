import { IStruct, Struct } from "../binary/Struct.ts"
import { PushTuple } from "../typetools.ts"
import {
  Column,
  computedColumn,
  Index,
  StoredRecordForColumnSchemas,
} from "./ColumnSchema.ts"
import { ColumnType } from "./ColumnType.ts"

type InsertRecordForColumnSchemas<CS extends Column.Stored[]> =
  & {
    [K in Extract<CS[number], { defaultValueFactory: undefined }>["name"]]:
      Column.GetValue<Extract<CS[number], { name: K }>>
  }
  & {
    [K in Extract<CS[number], Column.WithDefaultValue>["name"]]?:
      Column.GetValue<Extract<CS[number], { name: K }>>
  }

export type InsertRecordForTableSchema<TS extends SomeTableSchema> =
  InsertRecordForColumnSchemas<TS["columns"]>
export type StoredRecordForTableSchema<TS extends SomeTableSchema> =
  StoredRecordForColumnSchemas<TS["columns"]>

export type SomeTableSchema = TableSchema<
  string,
  Column.Stored[],
  Column.Computed.Any[]
>

export class TableSchema<
  TableName extends string,
  ColumnSchemasT extends Column.Stored[],
  ComputedColumnsT extends Column.Computed.Any[],
> {
  private constructor(
    public readonly name: TableName,
    public readonly columns: ColumnSchemasT,
    public readonly computedColumns: ComputedColumnsT,
  ) {}

  getColumns(): ColumnSchemasT {
    return this.columns
  }

  static create(name: string): TableSchema<string, [], []> {
    return new TableSchema(name, [], [])
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
    CIndexed extends Index.Config,
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

  with<C extends Column.Stored>(
    column: C,
  ): TableSchema<TableName, PushTuple<ColumnSchemasT, C>, ComputedColumnsT> {
    const columnName = column.name
    if (
      this.columns.some((c) => c.name === columnName) ||
      this.computedColumns.some((c) => c.name === columnName)
    ) {
      throw new Error(`Column '${columnName}' already exists`)
    }
    return new TableSchema(this.name, [
      ...this.columns,
      column,
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
