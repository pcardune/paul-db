import { describe, it } from "jsr:@std/testing/bdd"
import { expect } from "jsr:@std/expect"
import { Table } from "./table.ts"
import { ColumnType, ColumnTypes, TableSchema } from "./schema.ts"

const peopleSchema = TableSchema.create("people")
  .withColumn("name", ColumnTypes.any<string>())
  .withColumn("age", ColumnTypes.positiveNumber())

describe("Insert and Retrieve", () => {
  it("lets you insert and retrieve records", () => {
    const people = Table.create(peopleSchema, {})

    const aliceId = people.insert({ name: "Alice", age: 12 })
    const bobId = people.insert({ name: "Bob", age: 12 })
    expect(people.get(aliceId)).toEqual({ name: "Alice", age: 12 })
    expect(people.get(bobId)).toEqual({ name: "Bob", age: 12 })
  })
})

describe("Insert Validation", () => {
  it("should not allow you to insert records with invalid schema", () => {
    const people = Table.create(peopleSchema, {})
    people.insert({ name: "Alice", age: 12 })
    expect(() => {
      people.insert({ name: "Alice", age: -12 })
    }).toThrow("Invalid record")
  })

  it("Throws when inserting values that don't satisfy the column type", () => {
    const oddPeople = Table.create(
      peopleSchema
        .withColumn(
          "favoriteOdd",
          new ColumnType({ isValid: (value: number) => value % 2 === 1 }),
        ),
      {},
    )

    oddPeople.insert({ name: "Alice", age: 13, favoriteOdd: 13 })
    expect(() => {
      oddPeople.insert({ name: "Alice", age: 12, favoriteOdd: 12 })
    }).toThrow("Invalid record")
  })
})

const phoneNumberType = new ColumnType<string>({
  isValid: (value) => /^[\d-]+$/.test(value),
  equals: (a, b) => a.replace(/-/g, "") === b.replace(/-/g, ""),
  compare: (a, b) => a.replace(/-/g, "").localeCompare(b.replace(/-/g, "")),
})

describe("Uniqueness Constraints", () => {
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

  it("utilizes column type to determine uniqueness", () => {
    const people = Table.create(
      peopleSchema
        .withColumn("phone", phoneNumberType, { unique: true }),
      {},
    )
    people.insert({ name: "Alice", age: 12, phone: "123-867-5309" })
    expect(() => {
      people.insert({ name: "Alice", age: 12, phone: "123-8675-309" })
    }).toThrow("Record with given phone value already exists")
  })
})

describe("Querying", () => {
  describe("Table.iterate()", () => {
    it("lets you iterate over the entire contents of the table", () => {
      const people = Table.create(peopleSchema, {})
      people.insertMany([
        { name: "Alice", age: 30 },
        { name: "Bob", age: 30 },
        { name: "Charlie", age: 35 },
      ])

      expect(
        people.iterate().filter((r) => r.name.toLowerCase().includes("a"))
          .toArray(),
      ).toEqual([
        { name: "Alice", age: 30 },
        { name: "Charlie", age: 35 },
      ])
    })
  })

  describe("Table.scan()", () => {
    it("can query records by scanning the entire table", () => {
      const people = Table.create(peopleSchema, {})
      people.insertMany([
        { name: "Alice", age: 30 },
        { name: "Bob", age: 30 },
        { name: "Charlie", age: 35 },
      ])

      expect(people.scan("age", 30)).toEqual([
        { name: "Alice", age: 30 },
        { name: "Bob", age: 30 },
      ])
    })

    it("uses the underlying column type for equality testing", () => {
      const people = Table.create(
        TableSchema.create("people").withColumn(
          "name",
          ColumnTypes.any<string>(),
        ).withColumn("email", ColumnTypes.caseInsensitiveString()),
        {},
      )
      people.insertMany([
        { name: "Alice", email: "alice@example.com" },
        { name: "Alice 2", email: "Alice@example.com" },
        { name: "Bob", email: "bob@example.com" },
        { name: "Charlie", email: "charlie@website.com" },
      ])
      expect(people.scan("email", "ALICE@EXAMPLE.COM")).toEqual([
        { name: "Alice", email: "alice@example.com" },
        { name: "Alice 2", email: "Alice@example.com" },
      ])
    })
  })

  describe("Table.lookup()", () => {
    it("lets you query using an index", () => {
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

      people.insertMany([
        { name: "Alice", age: 12 },
        { name: "Bob", age: 12 },
        { name: "Charlie", age: 15 },
      ])

      expect(people.lookup("age", 12)).toEqual([
        { name: "Alice", age: 12 },
        { name: "Bob", age: 12 },
      ])
      expect(people.lookup("age", 15)).toEqual([{ name: "Charlie", age: 15 }])

      expect(people.lookup("lowerCaseName", "alice")).toEqual([
        { name: "Alice", age: 12 },
      ])
    })
  })
})
