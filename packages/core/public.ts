import { TableSchema } from "./schema/schema.ts"
import { DBSchema } from "./schema/DBSchema.ts"

export { column, computedColumn } from "./schema/columns/ColumnBuilder.ts"
export const table = TableSchema.create
export const db = DBSchema.create
export { ColumnTypes as type } from "./schema/columns/ColumnType.ts"

export type {
  InsertRecordForTableSchema as InferInsertRecord,
  StoredRecordForTableSchema as InferRecord,
} from "./schema/schema.ts"
export type { DBModel } from "./db/DbFile.ts"

export type { HeapFileTableInfer as InferTable } from "./tables/TableStorage.ts"
