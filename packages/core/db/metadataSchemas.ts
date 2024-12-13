import { column } from "../schema/columns/ColumnBuilder.ts"
import { ColumnTypes } from "../schema/columns/ColumnType.ts"
import { ulid } from "@std/ulid"
import { StoredRecordForTableSchema, TableSchema } from "../schema/schema.ts"

const ulidIdColumn = column("id", ColumnTypes.string())
  .unique({ inMemory: true }).defaultTo(() => ulid())

export const SYSTEM_DB = "system"

export const schemas = {
  dbPageIds: TableSchema.create("__dbPageIds")
    .with(
      column("pageType", ColumnTypes.string()).unique({ inMemory: true }),
      column("pageId", ColumnTypes.uint64()),
    ),

  dbTables: TableSchema.create("__dbTables")
    .with(
      ulidIdColumn,
      column("db", ColumnTypes.string()),
      column("name", ColumnTypes.string()),
      column("heapPageId", ColumnTypes.uint64()),
    )
    .withUniqueConstraint(
      "_db_name",
      ColumnTypes.string(),
      ["db", "name"],
      (input: { db: string; name: string }) => `${input.db}.${input.name}`,
      { inMemory: true },
    ),

  dbIndexes: TableSchema.create("__dbIndexes")
    .with(
      ulidIdColumn,
      column("indexName", ColumnTypes.string()),
      column("tableId", ColumnTypes.string()),
      column("heapPageId", ColumnTypes.uint64()),
    )
    .withUniqueConstraint(
      "_tableId_indexName",
      ColumnTypes.string(),
      ["tableId", "indexName"],
      (input) => `${input.tableId}.${input.indexName}`,
      { inMemory: true },
    ),

  dbSchemas: TableSchema.create("__dbSchemas")
    .with(
      column("id", ColumnTypes.uint32()).unique({ inMemory: true }),
      column("tableId", ColumnTypes.string()),
      column("version", ColumnTypes.uint32()),
    )
    .withUniqueConstraint(
      "tableId_version",
      ColumnTypes.string(),
      ["tableId", "version"],
      (input) => `${input.tableId}@${input.version}`,
      { inMemory: true },
    ),

  dbTableColumns: TableSchema.create("__dbTableColumns")
    .with(
      ulidIdColumn,
      column("schemaId", ColumnTypes.uint32()).index({ inMemory: true }),
      column("name", ColumnTypes.string()),
      column("type", ColumnTypes.string()),
      column("unique", ColumnTypes.boolean()),
      column("indexed", ColumnTypes.boolean()),
      column("indexInMemory", ColumnTypes.boolean()).defaultTo(() => false),
      column("computed", ColumnTypes.boolean()),
      column("order", ColumnTypes.uint16()),
    )
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

  dbMigrations: TableSchema.create("__dbMigrations")
    .with(
      column("name", ColumnTypes.string()).unique(),
      column("db", ColumnTypes.string()),
      column("completedAt", ColumnTypes.timestamp()),
    ),
}

export type TableRecord = StoredRecordForTableSchema<typeof schemas.dbTables>
