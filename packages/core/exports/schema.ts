/**
 * Column types are available here.
 * @module
 */
export * as type from "../schema/columns/types.ts"

export * as Column from "../schema/columns/index.ts"
export { ColumnType } from "../schema/columns/ColumnType.ts"
export { column, computedColumn } from "../schema/columns/ColumnBuilder.ts"

/**
 * Create an empty table schema for a table with the given name.
 */
export { create as table } from "../schema/TableSchema.ts"

/**
 * Create an empty database schema.
 */
export { create as db } from "../schema/DBSchema.ts"

export type {
  InsertRecordForTableSchema as InferInsertRecord,
  StoredRecordForTableSchema as InferRecord,
} from "../schema/TableSchema.ts"
export type { DBModel } from "../db/DbFile.ts"

export type { HeapFileTableInfer as InferTable } from "../tables/TableStorage.ts"

export type { SomeTableSchema } from "../schema/TableSchema.ts"
