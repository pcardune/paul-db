import { column, ColumnTypes, PaulDB, TableSchema } from "@paul-db/core"
import { HeapFileTableInfer } from "../../core/tables/TableStorage.ts"

const todoSchema = TableSchema.create("todos")
  .with(
    column("id", ColumnTypes.uuid())
      .unique()
      .defaultTo(() => crypto.randomUUID()),
  )
  .with(
    column("title", ColumnTypes.string()),
  )
  .with(
    column("createdAt", ColumnTypes.timestamp())
      .defaultTo(() => new Date()),
  )
  .with(
    column("completedAt", ColumnTypes.timestamp().nullable())
      .defaultTo(() => null),
  )

export type Model = {
  todos: HeapFileTableInfer<typeof todoSchema>
}

export async function getModel(db: PaulDB): Promise<Model> {
  return {
    todos: await db.dbFile.getOrCreateTable(todoSchema),
  }
}
