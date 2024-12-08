import { TableSchema } from "./mod.ts"

export { column, computedColumn } from "./schema/columns/ColumnBuilder.ts"
export const table = TableSchema.create
export { ColumnTypes as type } from "./schema/columns/ColumnType.ts"
import { DBSchema } from "./schema/DBSchema.ts"
export const db = DBSchema.create

export type {
  InsertRecordForTableSchema as InferInsertRecord,
  StoredRecordForTableSchema as InferRecord,
} from "./schema/schema.ts"

export type { HeapFileTableInfer as InferTable } from "./tables/TableStorage.ts"
