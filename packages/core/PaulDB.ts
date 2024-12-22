import { exists } from "@std/fs/exists"
import { DbFile, DBModel } from "./db/DbFile.ts"
import type { RowData } from "./query/QueryPlanNode.ts"
import * as path from "@std/path"
import { IPlanBuilder } from "./query/QueryBuilder.ts"
import { AsyncIterableWrapper } from "./async.ts"
import { DBSchema } from "./schema/DBSchema.ts"
import { Simplify } from "type-fest"

export { DbFile }

/**
 * Remove all symbol keys from an object. These show up when
 * doing complicated things with the query builder due to the use of
 * type-fest's EmptyObject type.
 */
type Clean<T> = Simplify<
  {
    [K in keyof T as K extends symbol ? never : K]: T[K]
  }
>

/**
 * A database instance.
 * @class PaulDB
 */
export class PaulDB {
  private constructor(readonly dbFile: DbFile) {
  }

  /**
   * Constructor for an in-memory database.
   * @returns A new in-memory database instance.
   */
  static async inMemory(): Promise<PaulDB> {
    return new PaulDB(await DbFile.open({ type: "memory" }))
  }

  /**
   * Constructor for a local storage database.
   * @param prefix all local storage keys will be given this prefix. defaults to "pauldb"
   * @returns A new local storage database instance.
   */
  static async localStorage(prefix: string = "pauldb"): Promise<PaulDB> {
    return new PaulDB(await DbFile.open({ type: "localstorage", prefix }))
  }

  /**
   * Constructor for a file system database.
   * @param dirName directory to store the database files
   * @param options options for opening the database
   * @param options.create if true, create the directory/files if they do not exist
   * @returns A new file system database instance.
   */
  static async open(dirName: string, { create = false } = {}): Promise<PaulDB> {
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

  /**
   * Close the database. This is only necessary for file system databases.
   */
  [Symbol.dispose]() {
    this.shutdown()
  }

  /**
   * Close the database. This is only necessary for file system databases.
   */
  shutdown() {
    this.dbFile.close()
  }

  /**
   * Query the database
   * @param plan The query to use
   * @returns the query results
   */
  query<T extends RowData>(
    plan: IPlanBuilder<T>,
  ): AsyncIterableWrapper<Clean<T extends { "$0": infer U } ? U : T>> {
    return plan.plan().execute(this).map((rowData) =>
      "$0" in rowData ? rowData.$0 : rowData
    ) as AsyncIterableWrapper<T extends { "$0": infer U } ? U : T>
  }

  /**
   * Generates a model object with the given schema that can be used
   * to read and write to the database
   * @param dbSchema the schema to use
   * @returns a DBModel objects
   */
  getModelForSchema<DBSchemaT extends DBSchema>(
    dbSchema: DBSchemaT,
  ): Promise<DBModel<DBSchemaT>> {
    return this.dbFile.getDBModel(dbSchema)
  }
}
