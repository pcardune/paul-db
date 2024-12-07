import type {
  InsertRecordForTableSchema,
  SomeTableSchema,
  StoredRecordForTableSchema,
} from "../schema/schema.ts"
import type { DbFile } from "./DbFile.ts"

export function tableSchemaMigration<
  OldSchemaT extends SomeTableSchema,
  NewSchemaT extends SomeTableSchema,
>(
  name: string,
  oldSchema: OldSchemaT,
  newSchema: NewSchemaT | ((oldSchema: OldSchemaT) => NewSchemaT),
  rowMapper: (
    oldRow: StoredRecordForTableSchema<OldSchemaT>,
  ) => InsertRecordForTableSchema<NewSchemaT>,
  { db = "default" }: { db?: string } = {},
) {
  if (typeof newSchema === "function") {
    newSchema = newSchema(oldSchema)
  }

  return {
    db,
    name,
    newSchema,
    migrate: async (dbFile: DbFile) => {
      const oldTable = await dbFile.getOrCreateTable(oldSchema, { db })
      if (newSchema.name === oldSchema.name) {
        // rename the old table so the new table can use its name
        await dbFile.renameTable(
          oldSchema.name,
          `$migration_${oldSchema.name}`,
          {
            db,
          },
        )
      }
      const newTable = await dbFile.getOrCreateTable(newSchema, { db })
      for await (const row of oldTable.iterate()) {
        await newTable.insert(rowMapper(row))
      }
      await oldTable.drop()
    },
  }
}
