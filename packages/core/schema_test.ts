import { describe, it } from "jsr:@std/testing/bdd"
import { expect } from "jsr:@std/expect"
import { ColumnSchema, ColumnTypes, TableSchema } from "./schema.ts"

function assertType<T>(_value: T) {}

describe("ColumnSchemas", () => {
  const nameColumn = ColumnSchema.create(
    "name",
    ColumnTypes.any<string>(),
  )

  it("can be created with a basic type", () => {
    expect(nameColumn.name).toBe("name")
    expect(nameColumn.unique).toBe(false)
    expect(nameColumn.type.isValid("Alice")).toBe(true)
    // @ts-expect-error: Can't insert a number into a string column
    // Note: at runtime this will not throw an error because we expect
    // typescript to catch it
    expect(nameColumn.type.isValid(12)).toBe(true)

    assertType<"name">(nameColumn.name)
    assertType<false>(nameColumn.unique)
  })

  it("lets you make a column unique", () => {
    const uniqueNameColumn = nameColumn.makeUnique()
    expect(uniqueNameColumn.unique).toBe(true)

    // the type system should know that this is true
    assertType<true>(uniqueNameColumn.unique)
  })

  it("lets you copy a column with a new name", () => {
    const lastNameColumn = nameColumn.withName("lastName")
    expect(lastNameColumn.name).toBe("lastName")
    assertType<"lastName">(lastNameColumn.name)
  })
})

describe("Schemas", () => {
  it("can be built iteratively", () => {
    const peopleSchema = TableSchema.create("people")
      .withColumn(
        "name",
        ColumnTypes.any<string>(),
      )
      .withColumn("age", ColumnTypes.positiveNumber())

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
    const nameColumn = ColumnSchema.create("name", ColumnTypes.any<string>())
    const peopleSchema = TableSchema.create("people").withColumn(nameColumn)
    expect(peopleSchema.columns).toHaveLength(1)
    expect(peopleSchema.isValidRecord({ name: "Alice" })).toBe(true)
  })
})
