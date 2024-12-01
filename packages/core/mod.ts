import { exists } from "@std/fs/exists"
import { DbFile } from "./db/DbFile.ts"
import { WriteAheadLog } from "./wal.ts"
import * as path from "@std/path"
import { TableSchema } from "./schema/schema.ts"
import { column } from "./schema/ColumnSchema.ts"

export { DbFile } from "./db/DbFile.ts"
export { Table } from "./tables/Table.ts"
export { TableSchema } from "./schema/schema.ts"

export { column, computedColumn } from "./schema/ColumnSchema.ts"

export {
  ColumnType,
  ColumnTypes,
  getColumnTypeFromSQLType,
  getColumnTypeFromString,
} from "./schema/ColumnType.ts"

export const s = {
  table: TableSchema.create,
  column,
}

export class PaulDB {
  private constructor(private wal: WriteAheadLog, readonly dbFile: DbFile) {
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
    const wal = await WriteAheadLog.create(path.join(dirName, "logs"))
    const dbFile = await DbFile.open(path.join(dirName, "db"), { create })

    return new PaulDB(wal, dbFile)
  }

  [Symbol.dispose]() {
    this.shutdown()
  }

  shutdown() {
    this.wal.cleanup()
    this.dbFile.close()
  }

  async insert(key: string, value: string) {
    await this.wal.write({
      type: "insert",
      key,
      value,
    })
  }
}
