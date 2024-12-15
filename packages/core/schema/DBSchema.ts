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
  withTables<TableSchemasT extends NonEmptyTuple<SomeTableSchema>>(
    ...tables: TableSchemasT
  ): DBSchema<DBName, Simplify<SchemasT & SchemaMap<TableSchemasT>>>
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

  withTables<TableSchemasT extends NonEmptyTuple<SomeTableSchema>>(
    ...tables: TableSchemasT
  ): DBSchema<DBName, Simplify<SchemasT & SchemaMap<TableSchemasT>>> {
    const map = associateBy(tables, (t) => t.name)
    return new DBSchema(this.name, { ...this.schemas, ...map }) as DBSchema<
      DBName,
      SchemasT & SchemaMap<TableSchemasT>
    >
  }

  query(): QueryBuilder<this> {
    return new QueryBuilder(this)
  }
}

/**
 * Create a new database schema for a database named "default"
 */
export function create(): IDBSchema<"default", EmptyObject>
/**
 * Create a new database schema for the system database.
 * This is a reserved name and cannot be used for user-defined databases.
 * @throws Error
 */
export function create(name: typeof SYSTEM_DB): never
/**
 * Create a new database schema for a database with a custom name
 */
export function create<DBName extends string>(
  name: DBName,
): IDBSchema<DBName, EmptyObject>
export function create(name?: string): IDBSchema<string, EmptyObject> {
  return DBSchema.create(name as string)
}
