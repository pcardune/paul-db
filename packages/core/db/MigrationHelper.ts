import { SomeTableSchema } from "../schema/TableSchema.ts"
import { HeapFileTableInfer } from "../tables/TableStorage.ts"

import { DBSchema } from "../schema/DBSchema.ts"
import { TableNotFoundError } from "../errors.ts"
import type { DbFile, DBModel } from "./DbFile.ts"

/**
 * A helper class for running migrations on a database.
 */
export class MigrationHelper<DBSchemaT extends DBSchema> {
  constructor(
    private dbFile: DbFile,
    readonly currentVersion: number,
    private targetSchema: DBSchemaT,
  ) {}

  /**
   * Add all missing tables
   */
  async addMissingTables() {
    const tableNames = Object.keys(this.targetSchema.schemas)
    for (const table of tableNames) {
      await this.dbFile.getOrCreateTable(this.targetSchema.schemas[table], {
        db: this.targetSchema.name,
      })
    }
  }

  private async getExistingSchema(tableName: string) {
    const schemas = await this.dbFile.getSchemas(
      this.targetSchema.name,
      tableName,
    )
    if (schemas == null) return null
    if (schemas.length === 0) return null
    return schemas[0].schema
  }

  /**
   * Add a specific missing column in a specific table
   */
  async addMissingColumn<TName extends keyof DBSchemaT["schemas"]>(
    tableName: TName,
    columnName: keyof DBSchemaT["schemas"][TName]["columnsByName"],
  ) {
    const newSchema = this.targetSchema.schemas[tableName as string]
    const oldSchema = await this.getExistingSchema(tableName as string)
    if (oldSchema == null) {
      throw new TableNotFoundError(
        `Table ${this.targetSchema.name}.${tableName as string} not found in db. Did you forget to call addMissingTables() first?`,
      )
    }
    if (oldSchema.columnsByName[columnName] != null) {
      throw new Error(
        `Column ${this.targetSchema.name}.${tableName as string}.${columnName as string} already exists`,
      )
    }
    if (newSchema.columnsByName[columnName].defaultValueFactory == null) {
      throw new Error(
        `Column ${this.targetSchema.name}.${tableName as string}.${columnName as string} does not have a default value, and can't be added to the db.`,
      )
    }

    const oldTable = await this.dbFile.tableManager.getTable(
      this.targetSchema.name,
      oldSchema,
    )
    if (oldTable == null) throw new Error("Unexpected null table")
    if (newSchema.name === oldSchema.name) {
      // rename the old table so the new table can use its name
      await this.dbFile.renameTable(
        oldSchema.name,
        `$migration_${oldSchema.name}`,
        { db: this.targetSchema.name },
      )
    }
    const newTable = await this.dbFile.getOrCreateTable(newSchema, {
      db: this.targetSchema.name,
    })
    for await (const row of oldTable.iterate()) {
      await newTable.insert(row)
    }
    await this.dbFile.getOrCreateTable(
      oldSchema.withName(`$migration_${oldSchema.name}`),
      { db: this.targetSchema.name },
    )
    await oldTable.drop()
  }

  /**
   * Add all missing columns across all tables
   */
  async addMissingColumns() {
    for (const schema of Object.values(this.targetSchema.schemas)) {
      const schemas = await this.dbFile.getSchemas(
        this.targetSchema.name,
        schema.name,
      )
      if (schemas == null) {
        throw new TableNotFoundError(
          `Table ${this.targetSchema.name}.${schema.name} not found in db. Did you forget to call addMissingTables() first?`,
        )
      }
      const [{ schema: existingSchema }] = schemas

      for (const column of schema.columns) {
        if (!existingSchema.columns.some((c) => c.name === column.name)) {
          this.addMissingColumn(schema.name, column.name)
        }
      }
    }
  }

  /**
   * Try to get a model for the target schema. This will throw an error if
   * the target schema does not match what's in the database.
   */
  async getModel(): Promise<DBModel<DBSchemaT>> {
    const tables: Record<string, HeapFileTableInfer<SomeTableSchema>> = {}
    for (const schema of Object.values(this.targetSchema.schemas)) {
      const table = await this.dbFile.tableManager.getTable(
        this.targetSchema.name,
        schema,
      )
      if (table == null) {
        throw new TableNotFoundError(
          `Table ${this.targetSchema.name}.${schema.name} not found in db. Did you forget to run a migration?`,
        )
      }

      const [{ schema: existingSchema }] = await this.dbFile.getSchemasOrThrow(
        this.targetSchema.name,
        schema.name,
      )
      for (const column of schema.columns) {
        if (!existingSchema.columns.some((c) => c.name === column.name)) {
          throw new Error(
            `Column ${this.targetSchema.name}.${schema.name}.${column.name} not found in db. Did you forget to run a migration?`,
          )
        }
      }

      tables[schema.name] = table
    }
    return { ...tables, $schema: this.targetSchema } as DBModel<DBSchemaT>
  }
}
