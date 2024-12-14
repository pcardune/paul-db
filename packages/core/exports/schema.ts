export { ColumnTypes as type } from "../schema/columns/ColumnType.ts"
export { column, computedColumn } from "../schema/columns/ColumnBuilder.ts"

import { TableSchema } from "../schema/schema.ts"
export const table = TableSchema.create
export { create as db } from "../schema/DBSchema.ts"

export type {
  InsertRecordForTableSchema as InferInsertRecord,
  StoredRecordForTableSchema as InferRecord,
} from "../schema/schema.ts"
export type { DBModel } from "../db/DbFile.ts"

export type { HeapFileTableInfer as InferTable } from "../tables/TableStorage.ts"
