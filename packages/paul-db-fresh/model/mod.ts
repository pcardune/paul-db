import { column, ColumnTypes, PaulDB, Table, TableSchema } from "@paul-db/core"
import { HeapFileTableInfer } from "../../core/tables/TableStorage.ts"

const todoSchema = TableSchema.create("todos")
  .withColumn(
    column("id", ColumnTypes.uuid()).withDefaultValue(() => crypto.randomUUID())
      .makeUnique(),
  ).withColumn(column("title", ColumnTypes.string()))

export type Model = {
  todos: HeapFileTableInfer<typeof todoSchema>
}

export async function getModel(db: PaulDB): Promise<Model> {
  return {
    todos: new Table(await db.dbFile.getTableStorage(todoSchema)),
  }
}
