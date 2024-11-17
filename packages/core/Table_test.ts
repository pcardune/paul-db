import { describe, it } from "jsr:@std/testing/bdd"
import { expect } from "jsr:@std/expect"
import { Table } from "./table.ts"
import { ColumnType, ColumnTypes, TableSchema } from "./schema.ts"

const peopleSchema = TableSchema.create("people")
  .withColumn("name", ColumnTypes.any<string>(), { unique: false })
  .withColumn("age", ColumnTypes.positiveNumber(), { unique: false })

describe("Table", () => {
  it("lets you insert and retrieve records", () => {
    const people = Table.create(peopleSchema, {})

    const aliceId = people.insert({ name: "Alice", age: 12 })
    const bobId = people.insert({ name: "Bob", age: 12 })
    expect(people.get(aliceId)).toEqual({ name: "Alice", age: 12 })
    expect(people.get(bobId)).toEqual({ name: "Bob", age: 12 })
  })

  it("lets you use whatever column constraint functions you want", () => {
    const oddPeople = Table.create(
      peopleSchema
        .withColumn(
          "favoriteOdd",
          new ColumnType({ isValid: (value: number) => value % 2 === 1 }),
          { unique: false },
        ),
      {},
    )

    oddPeople.insert({ name: "Alice", age: 13, favoriteOdd: 13 })
    expect(() => {
      oddPeople.insert({ name: "Alice", age: 12, favoriteOdd: 12 })
    }).toThrow("Invalid record")
  })

  it("enforces uniqueness constraints", () => {
    const people = Table.create(
      peopleSchema
        .withColumn("ssn", ColumnTypes.any<string>(), { unique: true }),
      {},
    )

    people.insert({ name: "Alice", age: 12, ssn: "123-45-6789" })
    expect(() => {
      people.insert({ name: "Alice", age: 12, ssn: "123-45-6789" })
    }).toThrow("Record with given ssn value already exists")
    people.insert({ name: "Alice", age: 12, ssn: "123-45-6-7-89" })
  })

  it("lets you customize the uniqueness constraint, by explicitly specifying an index", () => {
    const people = Table.create(
      peopleSchema
        .withColumn("ssn", ColumnTypes.any<string>(), { unique: true }),
      {
        ssn: {
          getValue: (r) => r.ssn.replace(/-/g, ""),
        },
      },
    )
    people.insert({ name: "Alice", age: 12, ssn: "123-45-6789" })
    expect(() => {
      people.insert({ name: "Alice", age: 12, ssn: "123-45-6-7-89" })
    }).toThrow("Record with given ssn value already exists")
  })

  it("should not allow you to insert records with invalid schema", () => {
    const people = Table.create(peopleSchema, {})
    people.insert({ name: "Alice", age: 12 })
    expect(() => {
      people.insert({ name: "Alice", age: -12 })
    }).toThrow("Invalid record")
  })

  it("can query records using an index", () => {
    const people = Table.create(
      peopleSchema,
      {
        name: {
          getValue: (r) => r.name,
        },
        age: {
          getValue: (r) => r.age,
        },
        lowerCaseName: {
          getValue: (r) => r.name.toLowerCase(),
        },
      },
    )

    people.insert({ name: "Alice", age: 12 })
    people.insert({ name: "Bob", age: 12 })
    people.insert({ name: "Charlie", age: 15 })

    expect(people.findMany("age", 12)).toEqual([
      { name: "Alice", age: 12 },
      { name: "Bob", age: 12 },
    ])
    expect(people.findMany("age", 15)).toEqual([{ name: "Charlie", age: 15 }])

    expect(people.findMany("lowerCaseName", "alice")).toEqual([
      { name: "Alice", age: 12 },
    ])
  })
})
