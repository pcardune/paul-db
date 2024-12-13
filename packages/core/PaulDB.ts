import { exists } from "@std/fs/exists"
import { DbFile } from "./db/DbFile.ts"
import type { RowData } from "./query/QueryPlanNode.ts"
import * as path from "@std/path"
import { IPlanBuilder } from "./query/QueryBuilder.ts"
import { AsyncIterableWrapper } from "./async.ts"

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

  query<T extends RowData>(
    plan: IPlanBuilder<T>,
  ): AsyncIterableWrapper<T extends { "$0": infer U } ? U : T> {
    return plan.plan().execute(this).map((rowData) =>
      "$0" in rowData ? rowData.$0 : rowData
    ) as AsyncIterableWrapper<T extends { "$0": infer U } ? U : T>
  }
}
