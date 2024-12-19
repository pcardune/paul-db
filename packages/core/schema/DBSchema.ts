import type { EmptyObject, NonEmptyTuple, Simplify } from "type-fest"
import { SomeTableSchema } from "./TableSchema.ts"
import { associateBy } from "@std/collections"
import { SYSTEM_DB } from "../db/metadataSchemas.ts"
import { QueryBuilder } from "../query/QueryPlanNode.ts"

type SchemaMap<TableSchemasT extends NonEmptyTuple<SomeTableSchema>> = {
  [K in TableSchemasT[number]["name"]]: Extract<
    TableSchemasT[number],
    { name: K }
  >
}

export interface IDBSchema<
  DBName extends string = string,
  SchemasT extends Record<string, SomeTableSchema> = Record<
    string,
    SomeTableSchema
  >,
> {
  readonly name: DBName
  readonly schemas: SchemasT
}

export class DBSchema<
  DBName extends string = string,
  SchemasT extends Record<string, SomeTableSchema> = Record<
    string,
    SomeTableSchema
  >,
> implements IDBSchema<DBName, SchemasT> {
  private constructor(readonly name: DBName, readonly schemas: SchemasT) {}

  static create(): DBSchema<"default", EmptyObject>
  static create(name: typeof SYSTEM_DB): never
  static create<DBName extends string>(
    name: DBName,
  ): DBSchema<DBName, EmptyObject>
  static create(
    name?: string,
  ): DBSchema<string, EmptyObject> {
    if (name == SYSTEM_DB) {
      throw new Error(`DB name "${SYSTEM_DB}" is reserved`)
    }
    return new DBSchema(name ?? "default", {})
  }

  /**
   * Add tables to the database schema.
   * This will return a new database schema with the aditional tables
   * you provide.
   *
   * ```ts
   * import {schema as s} from "@paul-db/core"
   * const appDBSchema = s.db().withTables(
   *  s.table("users").with(
   *   s.column("id", s.type.serial()),
   *   s.column("name", s.type.string()),
   *  ),
   *  s.table("posts").with(
   *    s.column("id", s.type.serial()),
   *    s.column("title", s.type.string()),
   *  ),
   * )
   * ```
   */
  withTables<TableSchemasT extends NonEmptyTuple<SomeTableSchema>>(
    ...tables: TableSchemasT
  ): DBSchema<DBName, Simplify<SchemasT & SchemaMap<TableSchemasT>>> {
    const map = associateBy(tables, (t) => t.name)
    return new DBSchema(this.name, { ...this.schemas, ...map }) as DBSchema<
      DBName,
      SchemasT & SchemaMap<TableSchemasT>
    >
  }

  /**
   * Creates a QueryBuilder instance based on this schema.
   *
   * ```ts
   * import {dbSchema} from "@paul-db/core/examples"
   * const numRecentlyLoggedInUsersQuery = dbSchema.query()
   *   .from("users")
   *   .where(
   *     (t) => t.column("users.lastLogin")
   *       .gt(new Date(Date.now() - 1000 * 60 * 60 * 24))
   *   )
   *   .aggregate({ count: (agg) => agg.count() })
   * ```
   */
  query(): QueryBuilder<this> {
    return new QueryBuilder(this)
  }
}

/**
 * Create a new database schema for the default database.
 *
 * ```ts
 * import {schema as s} from "@paul-db/core"
 * const appDBSchema = s.db().withTables(
 *   s.table("users").with(
 *     s.column("id", s.type.serial()),
 *     s.column("name", s.type.string()),
 *   ),
 *   s.table("posts").with(
 *    s.column("id", s.type.serial()),
 *    s.column("title", s.type.string()),
 *    s.column("content", s.type.string()),
 *   ),
 * )
 * ```
 *
 * You can optionally pass in a name for the database in case you
 * have multiple databases in your application.
 *
 * ```ts
 * import {schema as s} from "@paul-db/core"
 * const analyticsDBSchema = s.db("analytics").withTables(
 *   s.table("events").with(
 *     s.column("timestamp", s.type.timestamp()),
 *     s.column("type", s.type.string()),
 *     s.column("data", s.type.json()),
 *   ),
 * )
 * ```
 */
export function create(): DBSchema<"default", EmptyObject>
/**
 * Create a new database schema for the system database.
 * This is a reserved name and cannot be used for user-defined databases.
 * @throws Error
 */
export function create(name: typeof SYSTEM_DB): never
/**
 * Create a new database schema for a database with a custom name
 * ```ts
 * import {schema as s} from "@paul-db/core"
 * const myDB = s.db("analytics").withTables(
 *   s.table("events").with(
 *     s.column("timestamp", s.type.timestamp()),
 *     s.column("type", s.type.string()),
 *     s.column("data", s.type.json()),
 *   )
 * )
 * ```
 */
export function create<DBName extends string>(
  name: DBName,
): DBSchema<DBName, EmptyObject>
export function create(name?: string): DBSchema<string, EmptyObject> {
  return DBSchema.create(name as string)
}
