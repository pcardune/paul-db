import type {
  InsertRecordForTableSchema,
  SomeTableSchema,
  StoredRecordForTableSchema,
} from "../schema/TableSchema.ts"
import type { IDbFile } from "./DbFile.ts"

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
    migrate: async (dbFile: IDbFile) => {
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
        await newTable.insert(
          rowMapper(row as StoredRecordForTableSchema<OldSchemaT>),
        )
      }
      await dbFile.getOrCreateTable(
        oldSchema.withName(`$migration_${oldSchema.name}`),
        { db },
      )
      await oldTable.drop()
    },
  }
}
