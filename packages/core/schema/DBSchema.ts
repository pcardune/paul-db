import { EmptyObject, NonEmptyTuple, Simplify } from "npm:type-fest"
import { SomeTableSchema } from "./schema.ts"
import { associateBy } from "@std/collections"
import { SYSTEM_DB } from "../db/metadataSchemas.ts"
import { QueryBuilder } from "../query/QueryPlanNode.ts"

type SchemaMap<TableSchemasT extends NonEmptyTuple<SomeTableSchema>> = {
  [K in TableSchemasT[number]["name"]]: Extract<
    TableSchemasT[number],
    { name: K }
  >
}

export class DBSchema<
  DBName extends string = string,
  SchemasT extends Record<string, SomeTableSchema> = Record<
    string,
    SomeTableSchema
  >,
> {
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
