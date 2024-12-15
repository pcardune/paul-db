import { column } from "../schema/columns/ColumnBuilder.ts"
import { ColumnTypes } from "../schema/columns/ColumnType.ts"
import { ulid } from "@std/ulid"
import {
  type Column,
  type StoredRecordForTableSchema,
  TableSchema,
} from "../schema/TableSchema.ts"

type ULIDColumn = Column.Stored.Any<
  "id",
  string,
  true,
  Column.Index.Config,
  () => string
>

const ulidIdColumn: ULIDColumn = column("id", ColumnTypes.string())
  .unique({ inMemory: true }).defaultTo(() => ulid()).finalize()

export const SYSTEM_DB = "system"

export const dbPageIds: TableSchema<
  "__dbPageIds",
  [
    Column.Stored.Any<"pageType", string, true, Column.Index.Config, undefined>,
    Column.Stored.Simple<"pageId", bigint>,
  ],
  []
> = TableSchema.create("__dbPageIds")
  .with(
    column("pageType", ColumnTypes.string()).unique({ inMemory: true })
      .finalize(),
    column("pageId", ColumnTypes.uint64()).finalize(),
  )

export const dbTables: TableSchema<
  "__dbTables",
  [
    ULIDColumn,
    Column.Stored.Simple<"db", string>,
    Column.Stored.Simple<"name", string>,
    Column.Stored.Simple<"heapPageId", bigint>,
  ],
  [
    Column.Computed.Any<
      "_db_name",
      true,
      Column.Index.Config,
      { db: string; name: string },
      string
    >,
  ]
> = TableSchema.create("__dbTables")
  .with(
    ulidIdColumn,
    column("db", ColumnTypes.string()) as Column.Stored.Simple<"db", string>,
    column("name", ColumnTypes.string()) as Column.Stored.Simple<
      "name",
      string
    >,
    column("heapPageId", ColumnTypes.uint64()) as Column.Stored.Simple<
      "heapPageId",
      bigint
    >,
  )
  .withUniqueConstraint(
    "_db_name",
    ColumnTypes.string(),
    ["db", "name"],
    (input: { db: string; name: string }) => `${input.db}.${input.name}`,
    { inMemory: true },
  )

export const dbIndexes: TableSchema<
  "__dbIndexes",
  [
    ULIDColumn,
    Column.Stored.Simple<"indexName", string>,
    Column.Stored.Simple<"tableId", string>,
    Column.Stored.Simple<"heapPageId", bigint>,
  ],
  [
    Column.Computed.Any<
      "_tableId_indexName",
      true,
      Column.Index.Config,
      { tableId: string; indexName: string },
      string
    >,
  ]
> = TableSchema.create("__dbIndexes")
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
  )

export const dbSchemas: TableSchema<
  "__dbSchemas",
  [
    Column.Stored.Any<"id", number, true, Column.Index.Config, undefined>,
    Column.Stored.Simple<"tableId", string>,
    Column.Stored.Simple<"version", number>,
  ],
  [
    Column.Computed.Any<
      "tableId_version",
      true,
      Column.Index.Config,
      { tableId: string; version: number },
      string
    >,
  ]
> = TableSchema.create("__dbSchemas")
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
  )

export const dbTableColumns: TableSchema<
  "__dbTableColumns",
  [
    ULIDColumn,
    Column.Stored.Any<
      "schemaId",
      number,
      false,
      Column.Index.Config,
      undefined
    >,
    Column.Stored.Simple<"name", string>,
    Column.Stored.Simple<"type", string>,
    Column.Stored.Simple<"unique", boolean>,
    Column.Stored.Simple<"indexed", boolean>,
    Column.Stored.Any<
      "indexInMemory",
      boolean,
      false,
      Column.Index.Config,
      () => boolean
    >,
    Column.Stored.Simple<"computed", boolean>,
    Column.Stored.Simple<"order", number>,
  ],
  [
    Column.Computed.Any<
      "schemaId_name",
      true,
      Column.Index.Config,
      { schemaId: number; name: string },
      string
    >,
  ]
> = TableSchema.create("__dbTableColumns")
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
  )

export const dbMigrations = TableSchema.create("__dbMigrations")
  .with(
    column("name", ColumnTypes.string()).unique(),
    column("db", ColumnTypes.string()),
    column("completedAt", ColumnTypes.timestamp()),
  )

export type TableRecord = StoredRecordForTableSchema<typeof dbTables>
