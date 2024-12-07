import { TableSchema } from "./mod.ts"

export { column, computedColumn } from "./schema/columns/ColumnBuilder.ts"
export const table = TableSchema.create
export { ColumnTypes as type } from "./schema/columns/ColumnType.ts"

export type {
  InsertRecordForTableSchema as InferInsertRecord,
  StoredRecordForTableSchema as InferRecord,
} from "./schema/schema.ts"

export type { HeapFileTableInfer as InferTable } from "./tables/TableStorage.ts"
