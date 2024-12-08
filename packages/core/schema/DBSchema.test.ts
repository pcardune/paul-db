import { expect } from "jsr:@std/expect"
import { column } from "../public.ts"
import { ColumnTypes } from "./columns/ColumnType.ts"
import { DBSchema } from "./DBSchema.ts"
import { TableSchema } from "./schema.ts"
import { assertType } from "../testing.ts"

Deno.test("DBSchema.create()", () => {
  const people = TableSchema.create("people").with(
    column("name", ColumnTypes.string()),
  )
  const pets = TableSchema.create("pets").with(
    column("name", ColumnTypes.string()),
  )
  const db = DBSchema.create().withTables(
    people,
    pets,
  )
  expect(db.name).toBe("default")
  expect(db.schemas.people).toBe(people)
  expect(db.schemas.pets).toBe(pets)

  assertType<"default">(db.name)
  assertType<{ people: typeof people; pets: typeof pets }>(db.schemas)

  expect(() => DBSchema.create("system")).toThrow(
    'DB name "system" is reserved',
  )
})
