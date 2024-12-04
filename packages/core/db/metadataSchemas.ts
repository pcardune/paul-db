import { column } from "../schema/ColumnSchema.ts"
import { ColumnTypes } from "../schema/ColumnType.ts"
import { ulid } from "jsr:@std/ulid"
import { TableSchema } from "../schema/schema.ts"

const ulidIdColumn = column("id", ColumnTypes.string())
  .unique({ inMemory: true }).defaultTo(() => ulid())

export const SYSTEM_DB = "system"

export const schemas = {
  dbPageIds: TableSchema.create("__dbPageIds")
    .with(column("pageType", ColumnTypes.string()).unique({ inMemory: true }))
    .with(column("pageId", ColumnTypes.uint64())),

  dbTables: TableSchema.create("__dbTables")
    .with(ulidIdColumn)
    .with(column("db", ColumnTypes.string()))
    .with(column("name", ColumnTypes.string()))
    .with(column("heapPageId", ColumnTypes.uint64()))
    .withUniqueConstraint(
      "_db_name",
      ColumnTypes.string(),
      ["db", "name"],
      (input: { db: string; name: string }) => `${input.db}.${input.name}`,
      { inMemory: true },
    ),

  dbIndexes: TableSchema.create("__dbIndexes")
    .with(column("indexName", ColumnTypes.string()).unique({ inMemory: true }))
    .with(column("heapPageId", ColumnTypes.uint64())),

  dbSchemas: TableSchema.create("__dbSchemas")
    .with(column("id", ColumnTypes.uint32()).unique({ inMemory: true }))
    .with(column("tableId", ColumnTypes.string()))
    .with(column("version", ColumnTypes.uint32()))
    .withUniqueConstraint(
      "tableId_version",
      ColumnTypes.string(),
      ["tableId", "version"],
      (input) => `${input.tableId}@${input.version}`,
      { inMemory: true },
    ),

  dbTableColumns: TableSchema.create("__dbTableColumns")
    .with(ulidIdColumn)
    .with(column("schemaId", ColumnTypes.uint32()).index({ inMemory: true }))
    .with(column("name", ColumnTypes.string()))
    .with(column("type", ColumnTypes.string()))
    .with(column("unique", ColumnTypes.boolean()))
    .with(column("indexed", ColumnTypes.boolean()))
    .with(column("indexInMemory", ColumnTypes.boolean()).defaultTo(() => false))
    .with(column("computed", ColumnTypes.boolean()))
    .with(column("order", ColumnTypes.uint16()))
    .withUniqueConstraint(
      "schemaId_name",
      ColumnTypes.string(),
      [
        "schemaId",
        "name",
      ],
      (input) => `${input.schemaId}.${input.name}`,
      { inMemory: true },
    ),
}
