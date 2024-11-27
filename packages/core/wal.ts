import { ensureDir } from "@std/fs"
import { WriteOperation } from "./operations.ts"

export class WriteAheadLog {
  private constructor(private file: Deno.FsFile) {}

  static async create(logDir: string) {
    await ensureDir(logDir)

    const file = await Deno.open(`${logDir}/wal.log`, {
      append: true,
      create: true,
      read: true,
    })
    return new WriteAheadLog(file)
  }

  [Symbol.dispose]() {
    this.cleanup()
  }

  cleanup() {
    this.file.close()
  }

  async write(operation: WriteOperation) {
    console.log("Writing to WAL")
    await this.file.lock()
    await this.file.write(
      new TextEncoder().encode(JSON.stringify(operation) + "\n"),
    )
    await this.file.syncData()
    await this.file.unlock()
  }
}
