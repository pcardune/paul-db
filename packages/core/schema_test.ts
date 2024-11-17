import { describe, it } from "jsr:@std/testing/bdd"
import { expect } from "jsr:@std/expect"
import { ColumnSchema, ColumnTypes, TableSchema } from "./schema.ts"

describe("Schemas", () => {
  it("can be built iteratively", () => {
    const peopleSchema = TableSchema.create("people")
      .withColumn(
        "name",
        ColumnTypes.any<string>(),
        { unique: false },
      )
      .withColumn("age", ColumnTypes.positiveNumber(), { unique: false })

    // @ts-expect-error: If specifying a column by name, you must
    // also provide a type
    expect(() => peopleSchema.withColumn("bar")).toThrow()

    expect(peopleSchema.columns).toHaveLength(2)
    expect(peopleSchema.isValidRecord({ name: "Alice", age: 12 })).toBe(true)
    expect(peopleSchema.isValidRecord({ name: "Alice", age: -12 })).toBe(false)

    // @ts-expect-error: Can't insert a record with missing columns
    expect(peopleSchema.isValidRecord({ name: "Alice" })).toBe(false)
  })

  it("Lets you specify a column directly", () => {
    const nameColumn = ColumnSchema.create("name", ColumnTypes.any<string>(), {
      unique: false,
    })
    const peopleSchema = TableSchema.create("people").withColumn(nameColumn)
    expect(peopleSchema.columns).toHaveLength(1)
    expect(peopleSchema.isValidRecord({ name: "Alice" })).toBe(true)
  })
})
