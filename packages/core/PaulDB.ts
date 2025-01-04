import { exists } from "@std/fs/exists"
import { DbFile, DBModel, IDbFile } from "./db/DbFile.ts"
import type { RowData } from "./query/QueryPlanNode.ts"
import type { Promisable } from "type-fest"
import * as path from "@std/path"
import { IPlanBuilder, QueryBuilder } from "./query/QueryBuilder.ts"
import { AsyncIterableWrapper } from "./async.ts"
import { DBSchema } from "./schema/DBSchema.ts"
import type { Simplify } from "type-fest"
import { TableNotFoundError } from "./errors.ts"
import { SomeTableSchema } from "./schema/TableSchema.ts"
import { IMigrationHelper } from "./db/MigrationHelper.ts"
import { IQueryPlanNode } from "./query/QueryPlanNode.ts"
import { TableScan } from "./query/QueryPlanNode.ts"

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
  get dbFile(): IDbFile {
    return this._dbFile
  }

  private constructor(private _dbFile: DbFile) {
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
   * Constructor for a local storage database.
   * @param prefix all local storage keys will be given this prefix. defaults to "pauldb"
   * @returns A new local storage database instance.
   */
  static async indexedDB(name: string = "pauldb"): Promise<PaulDB> {
    return new PaulDB(await DbFile.open({ type: "indexeddb", name }))
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
    this._dbFile.close()
  }

  /**
   * Get the schema for a table
   */
  async getSchema(db: string, table: string): Promise<SomeTableSchema> {
    const schemas = await this._dbFile.getSchemas(db, table)
    if (schemas == null || schemas.length === 0) {
      throw new TableNotFoundError(`Table ${db}.${table} not found`)
    }
    return schemas[0].schema
  }

  /**
   * Query the database
   * @param plan The query to use
   * @returns the query results
   * @deprecated use getModelForSchema().$query instead
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
  async getModelForSchema<DBSchemaT extends DBSchema>(
    dbSchema: DBSchemaT,
    version: number = 1,
    onUpgradeNeeded?: (
      helper: IMigrationHelper<DBSchemaT>,
    ) => Promise<void>,
  ): Promise<
    & DBModel<DBSchemaT>
    & {
      $query: <T extends RowData>(
        plan:
          | IPlanBuilder<T>
          | ((schema: QueryBuilder<DBSchemaT>) => IPlanBuilder<T>),
      ) => AsyncIterableWrapper<Clean<T extends { "$0": infer U } ? U : T>>
      $subscribe: <T extends RowData>(
        plan:
          | IPlanBuilder<T>
          | ((schema: QueryBuilder<DBSchemaT>) => IPlanBuilder<T>),
        handler: (
          data: AsyncIterableWrapper<
            Clean<T extends { "$0": infer U } ? U : T>
          >,
        ) => void,
      ) => void
    }
  > {
    const model = await this._dbFile.getDBModel(
      dbSchema,
      version,
      onUpgradeNeeded,
    )

    const result = {
      ...model,
      $query: <T extends RowData>(
        plan:
          | IPlanBuilder<T>
          | ((schema: QueryBuilder<DBSchemaT>) => IPlanBuilder<T>),
      ): AsyncIterableWrapper<Clean<T extends { "$0": infer U } ? U : T>> => {
        if (typeof plan === "function") {
          plan = plan(dbSchema.query())
        }
        return plan.plan().execute(this).map((rowData) =>
          "$0" in rowData ? rowData.$0 : rowData
        ) as AsyncIterableWrapper<T extends { "$0": infer U } ? U : T>
      },
      $subscribe: <T extends RowData>(
        plan:
          | IPlanBuilder<T>
          | ((schema: QueryBuilder<DBSchemaT>) => IPlanBuilder<T>),
        handler: (
          data: AsyncIterableWrapper<
            Clean<T extends { "$0": infer U } ? U : T>
          >,
        ) => Promisable<void>,
      ): void => {
        function getNode() {
          if (typeof plan === "function") {
            return plan(dbSchema.query()).plan()
          }
          return plan.plan()
        }
        const node = getNode()
        function findTables(node: IQueryPlanNode): string[] {
          if (node instanceof TableScan) {
            return [node.table]
          }
          return node.children().flatMap(findTables)
        }
        const tables = new Set(findTables(node))
        const tableEventHandler = () => {
          handler(
            node.execute(this).map((rowData) =>
              "$0" in rowData ? rowData.$0 : rowData
            ) as AsyncIterableWrapper<T extends { "$0": infer U } ? U : T>,
          )
        }
        for (const table of tables) {
          model[table].subscribe(tableEventHandler)
        }
        tableEventHandler()
      },
    }
    return result
  }
}
