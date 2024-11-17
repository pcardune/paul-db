import { describe, it } from "jsr:@std/testing/bdd"
import { expect } from "jsr:@std/expect"
import {
  ColumnSchema,
  ColumnTypes,
  TableSchema,
  ValueForColumnSchema,
} from "./schema.ts"
import { RecordForTableSchema } from "./schema.ts"

function assertType<T>(_value: T) {}
type TypeEquals<Actual, Expected> = Actual extends Expected ? true
  : "Types not equal"

function assertTrue<T extends true>() {}
assertTrue<TypeEquals<"green", "green">>()

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
    assertTrue<TypeEquals<string, ValueForColumnSchema<typeof nameColumn>>>()
  })

  it("exposes the types of the column to typescript", () => {
    assertTrue<TypeEquals<string, ValueForColumnSchema<typeof nameColumn>>>()

    // @ts-expect-error: Can't insert a number into a string column
    assertTrue<TypeEquals<number, ValueForColumnSchema<typeof nameColumn>>>()

    const ageColumn = ColumnSchema.create(
      "age",
      ColumnTypes.positiveNumber(),
    )
    assertTrue<TypeEquals<number, ValueForColumnSchema<typeof ageColumn>>>()
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

  it("Uses the underlying column type for validation", () => {
    const peopleSchema = TableSchema.create("people")
      .withColumn("name", ColumnTypes.any<string>())
      .withColumn("age", ColumnTypes.positiveNumber())

    expect(peopleSchema.isValidRecord({ name: "Alice", age: 12 })).toBe(true)
    expect(peopleSchema.isValidRecord({ name: "Alice", age: -12 })).toBe(false)
  })

  it("Exposes the types of records that are stored in the table", () => {
    const peopleSchema = TableSchema.create("people")
      .withColumn(
        "name",
        ColumnTypes.any<string>(),
      )
      .withColumn("age", ColumnTypes.positiveNumber())
    assertTrue<
      TypeEquals<
        { name: string; age: number },
        RecordForTableSchema<typeof peopleSchema>
      >
    >()
  })
})
