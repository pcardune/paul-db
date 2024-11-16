import { describe, it } from "jsr:@std/testing/bdd"
import { expect } from "jsr:@std/expect"
import { ColumnTypes, TableSchema } from "./schema.ts"

describe("Schemas", () => {
  it("can be built iteratively", () => {
    const peopleSchema = TableSchema.create("people")
      .withColumn(
        "name",
        ColumnTypes.any<string>(),
      )
      .withColumn("age", ColumnTypes.positiveNumber())

    expect(peopleSchema.columns).toHaveLength(2)
    expect(peopleSchema.isValidRecord({ name: "Alice", age: 12 })).toBe(true)
    expect(peopleSchema.isValidRecord({ name: "Alice", age: -12 })).toBe(false)
  })
})
