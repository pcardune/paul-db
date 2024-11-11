import { WriteAheadLog } from "./wal.ts"

export function doSomething() {
  console.log("Doing something... from the core package")
}

export class PaulDB {
  private constructor(private wal: WriteAheadLog) {
    console.log("PaulDB constructor")
  }

  static async create() {
    const wal = await WriteAheadLog.create("logs")
    return new PaulDB(wal)
  }

  shutdown() {
    this.wal.cleanup()
  }

  async insert(key: string, value: string) {
    await this.wal.write({
      type: "insert",
      key,
      value,
    })
  }
}
