import { exists } from "@std/fs/exists"
import { DbFile } from "./db/DbFile.ts"
import { WriteAheadLog } from "./wal.ts"
import * as path from "@std/path"

export function doSomething() {
  console.log("Doing something... from the core package")
}

export { DbFile } from "./db/DbFile.ts"

export class PaulDB {
  private constructor(private wal: WriteAheadLog, readonly dbFile: DbFile) {
    console.log("PaulDB constructor")
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
