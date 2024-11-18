import { describe, it } from "jsr:@std/testing/bdd"
import { expect } from "jsr:@std/expect"
import { Table } from "./Table.ts"
import {
  column,
  ColumnType,
  ColumnTypes,
  computedColumn,
  TableSchema,
} from "./schema.ts"

const peopleSchema = TableSchema.create("people")
  .withColumn("name", ColumnTypes.any<string>())
  .withColumn("age", ColumnTypes.positiveNumber())

describe("Insert and Retrieve", () => {
  it("lets you insert and retrieve records", () => {
    const people = Table.create(peopleSchema)

    const aliceId = people.insert({ name: "Alice", age: 12 })
    const bobId = people.insert({ name: "Bob", age: 12 })
    expect(people.get(aliceId)).toEqual({ name: "Alice", age: 12 })
    expect(people.get(bobId)).toEqual({ name: "Bob", age: 12 })
  })
})

describe("Insert Validation", () => {
  it("should not allow you to insert records with invalid schema", () => {
    const people = Table.create(peopleSchema)
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
      const people = Table.create(peopleSchema)
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
      const people = Table.create(peopleSchema)
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
    const indexedPeopleSchema = TableSchema.create("people")
      .withColumn(column("name", ColumnTypes.any<string>()).makeIndexed())
      .withColumn(column("phone", phoneNumberType))
      .withColumn(column("age", ColumnTypes.positiveNumber()).makeIndexed())
      .withComputedColumn(
        computedColumn(
          "lowerCaseName",
          (input: { name: string }) => input.name.toLowerCase(),
        ).makeIndexed(),
      )
    const people = Table.create(
      indexedPeopleSchema,
    )

    people.insertMany([
      { name: "Alice", age: 25, phone: "123-456-7890" },
      { name: "Bob", age: 35, phone: "123-456-7891" },
      { name: "Charlie", age: 25, phone: "123-456-7892" },
    ])

    it("lets you query using an index", () => {
      expect(people.lookup("age", 25)).toEqual([
        { name: "Alice", age: 25, phone: "123-456-7890" },
        { name: "Charlie", age: 25, phone: "123-456-7892" },
      ])
      expect(people.lookup("age", 35)).toEqual([
        { name: "Bob", age: 35, phone: "123-456-7891" },
      ])
    })

    it("lets you query using an index on a computed column", () => {
      expect(people.lookupComputed("lowerCaseName", "alice")).toEqual([
        { name: "Alice", age: 25, phone: "123-456-7890" },
      ])
    })

    it.skip("will throw if you lookup a computed indexed column with an invalid value", () => {
      expect(() => people.lookupComputed("lowerCaseName", "ALICE")).toThrow(
        "Invalid value for column lowerCaseName",
      )
    })
  })
})
