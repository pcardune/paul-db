import { exists } from "@std/fs/exists"
import { DbFile } from "./db/DbFile.ts"
import * as path from "@std/path"

export { DbFile } from "./db/DbFile.ts"
export { Table } from "./tables/Table.ts"
export { TableSchema } from "./schema/schema.ts"
export * as migrations from "./db/migrations.ts"
export * as s from "./public.ts"
export * as plan from "./query/QueryPlanNode.ts"

export {
  ColumnType,
  getColumnTypeFromSQLType,
  getColumnTypeFromString,
} from "./schema/columns/ColumnType.ts"

export class PaulDB {
  private constructor(readonly dbFile: DbFile) {
  }

  static async inMemory() {
    return new PaulDB(await DbFile.open({ type: "memory" }))
  }

  static async localStorage(prefix?: string) {
    return new PaulDB(await DbFile.open({ type: "localstorage", prefix }))
  }

  static async open(dirName: string, { create = false } = {}) {
    await Deno.mkdir(dirName, { recursive: true })
    if (!(await exists(dirName))) {
      if (create) {
        await Deno.mkdir(dirName, { recursive: true })
      } else {
        throw new Error(`Directory ${dirName} does not exist`)
      }
    }
    const dbFile = await DbFile.open({
      type: "file",
      path: path.join(dirName, "db"),
      create,
    })

    return new PaulDB(dbFile)
  }

  [Symbol.dispose]() {
    this.shutdown()
  }

  shutdown() {
    this.dbFile.close()
  }
}
