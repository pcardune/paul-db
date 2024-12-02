import { beforeAll, describe, it } from "jsr:@std/testing/bdd"
import { expect } from "jsr:@std/expect"
import { assertSnapshot } from "jsr:@std/testing/snapshot"
import { Table, TableInfer } from "./Table.ts"
import { StoredRecordForTableSchema, TableSchema } from "../schema/schema.ts"
import {
  HeapFileRowId,
  InMemoryTableStorage,
  JsonFileTableStorage,
} from "./TableStorage.ts"
import { ColumnType, ColumnTypes } from "../schema/ColumnType.ts"
import { DbFile } from "../db/DbFile.ts"
import { generateTestFilePath } from "../testing.ts"
import { column, computedColumn } from "../schema/ColumnSchema.ts"

const peopleSchema = TableSchema.create("people")
  .with(column("name", ColumnTypes.any<string>()))
  .with(column("age", ColumnTypes.positiveNumber()))

describe("Create, Read, and Delete", () => {
  it("lets you insert and retrieve records", async () => {
    const people = new Table(await InMemoryTableStorage.forSchema(peopleSchema))

    const aliceId = await people.insert({ name: "Alice", age: 12 })
    const bobId = await people.insert({ name: "Bob", age: 12 })
    expect(await people.get(aliceId)).toEqual({
      name: "Alice",
      age: 12,
    })
    expect(await people.get(bobId)).toEqual({
      name: "Bob",
      age: 12,
    })
  })
  it("You can delete records", async () => {
    const people = new Table(
      await InMemoryTableStorage.forSchema(peopleSchema),
    )
    const aliceId = await people.insert({ name: "Alice", age: 12 })
    const bobId = await people.insert({ name: "Bob", age: 12 })
    people.remove(aliceId)
    expect(await people.get(aliceId)).toBeUndefined()
    expect(await people.get(bobId)).toEqual({
      name: "Bob",
      age: 12,
    })
  })
})

describe("Insert Validation", () => {
  it("should not allow you to insert records with invalid schema", async () => {
    const people = new Table(
      await InMemoryTableStorage.forSchema(peopleSchema.with(
        column("uuid", ColumnTypes.uuid()).defaultTo(() => crypto.randomUUID()),
      )),
    )
    await people.insert({ name: "Alice", age: 12 })
    expect(people.insert({ name: "Alice", age: -12 })).rejects.toThrow(
      "Invalid record: Invalid value for column age",
    )
    expect(
      people.insert({
        name: "Alice",
        age: 12,
        uuid: "not-valid-uuid-dispite-typecheck",
      }),
    ).rejects.toThrow(
      "Invalid record: Invalid value for column uuid",
    )
  })

  it("Throws when inserting values that don't satisfy the column type", async () => {
    const schema = peopleSchema
      .with(
        column(
          "favoriteOdd",
          new ColumnType({
            name: "oddNumber",
            isValid: (value: number) => value % 2 === 1,
          }),
        ),
      )
    const oddPeople = new Table(await InMemoryTableStorage.forSchema(schema))

    oddPeople.insert({ name: "Alice", age: 13, favoriteOdd: 13 })
    expect(oddPeople.insert({ name: "Alice", age: 12, favoriteOdd: 12 }))
      .rejects.toThrow("Invalid record")
  })
})

const phoneNumberType = new ColumnType<string>({
  name: "phoneNumber",
  isValid: (value) => /^[\d-]+$/.test(value),
  equals: (a, b) => a.replace(/-/g, "") === b.replace(/-/g, ""),
  compare: (a, b) => a.replace(/-/g, "").localeCompare(b.replace(/-/g, "")),
})

describe("Uniqueness Constraints", () => {
  it("enforces uniqueness constraints", async () => {
    const schema = peopleSchema
      .with(column("ssn", ColumnTypes.any<string>()).unique())
    const people = new Table(await InMemoryTableStorage.forSchema(schema))

    await people.insert({ name: "Alice", age: 12, ssn: "123-45-6789" })
    expect(people.insert({ name: "Alice", age: 12, ssn: "123-45-6789" }))
      .rejects.toThrow("Record with given ssn value already exists")
    await people.insert({ name: "Alice", age: 12, ssn: "123-45-6-7-89" })
  })

  it("utilizes column type to determine uniqueness", async () => {
    const schema = peopleSchema
      .with(column("phone", phoneNumberType).unique())
    const people = new Table(await InMemoryTableStorage.forSchema(schema))
    await people.insert({ name: "Alice", age: 12, phone: "123-867-5309" })
    expect(
      people.insert({ name: "Alice", age: 12, phone: "123-8675-309" }),
    ).rejects.toThrow("Record with given phone value already exists")
  })
})

describe("Querying", () => {
  describe("Table.iterate()", () => {
    it("lets you iterate over the entire contents of the table", async () => {
      const people = new Table(
        await InMemoryTableStorage.forSchema(peopleSchema),
      )
      await people.insertMany([
        { name: "Alice", age: 30 },
        { name: "Bob", age: 30 },
        { name: "Charlie", age: 35 },
      ])

      const data = await people.iterate()
        .filter((r) => r.name.toLowerCase().includes("a"))
        .toArray()

      expect(data).toEqual([
        { name: "Alice", age: 30 },
        { name: "Charlie", age: 35 },
      ])
    })
  })

  describe("Table.scan()", () => {
    it("can query records by scanning the entire table", async () => {
      const people = new Table(
        await InMemoryTableStorage.forSchema(peopleSchema),
      )
      await people.insertMany([
        { name: "Alice", age: 30 },
        { name: "Bob", age: 30 },
        { name: "Charlie", age: 35 },
      ])

      expect(await people.scan("age", 30)).toEqual([
        { name: "Alice", age: 30 },
        { name: "Bob", age: 30 },
      ])
    })

    it("uses the underlying column type for equality testing", async () => {
      const peopleSchema = TableSchema.create("people").with(
        column("name", ColumnTypes.any<string>()),
      ).with(column("email", ColumnTypes.caseInsensitiveString()))
      const people = new Table(
        await InMemoryTableStorage.forSchema(peopleSchema),
      )
      await people.insertMany([
        { name: "Alice", email: "alice@example.com" },
        { name: "Alice 2", email: "Alice@example.com" },
        { name: "Bob", email: "bob@example.com" },
        { name: "Charlie", email: "charlie@website.com" },
      ])
      expect(await people.scan("email", "ALICE@EXAMPLE.COM")).toEqual([
        { name: "Alice", email: "alice@example.com" },
        { name: "Alice 2", email: "Alice@example.com" },
      ])
    })
  })

  describe("Table.lookup()", () => {
    const indexedPeopleSchema = TableSchema.create("people")
      .with(column("name", ColumnTypes.any<string>()).index())
      .with(column("phone", phoneNumberType))
      .with(column("age", ColumnTypes.positiveNumber()).index())
      .withComputedColumn(
        computedColumn(
          "lowerCaseName",
          ColumnTypes.string(),
          (input: { name: string }) => input.name.toLowerCase(),
        ).index(),
      )
    let people: TableInfer<
      typeof indexedPeopleSchema,
      InMemoryTableStorage<
        number,
        StoredRecordForTableSchema<typeof indexedPeopleSchema>
      >
    >
    beforeAll(async () => {
      people = new Table(
        await InMemoryTableStorage.forSchema(indexedPeopleSchema),
      )
    })

    beforeAll(async () => {
      await people.insertMany([
        { name: "Alice", age: 25, phone: "123-456-7890" },
        { name: "Bob", age: 35, phone: "123-456-7891" },
        { name: "Charlie", age: 25, phone: "123-456-7892" },
      ])
    })

    it("lets you query using an index", async () => {
      expect(await people.lookup("age", 25)).toEqual([
        { name: "Alice", age: 25, phone: "123-456-7890" },
        { name: "Charlie", age: 25, phone: "123-456-7892" },
      ])
      expect(await people.lookup("age", 35)).toEqual([
        { name: "Bob", age: 35, phone: "123-456-7891" },
      ])
    })

    it("lets you query using an index on a computed column", async () => {
      expect(await people.lookupComputed("lowerCaseName", "alice"))
        .toEqual([
          { name: "Alice", age: 25, phone: "123-456-7890" },
        ])
    })

    it.skip("will throw if you lookup a computed indexed column with an invalid value", async () => {
      expect(await people.lookupComputed("lowerCaseName", "ALICE")).rejects
        .toThrow(
          "Invalid value for column lowerCaseName",
        )
    })
  })
})

Deno.test({
  name: "json table storage",
  permissions: { read: true, write: true },
  fn: async (t) => {
    using tempFile = generateTestFilePath("people.json")
    const storage = await JsonFileTableStorage.forSchema(
      peopleSchema,
      tempFile.filePath,
    )
    const people = new Table(storage)
    await people.insert({ name: "Alice", age: 12 })
    await people.insert({ name: "Bob", age: 12 })
    const f = Deno.readTextFileSync(tempFile.filePath)
    await assertSnapshot(t, f)
  },
})

Deno.test("HeapFileTableStorage", async (t) => {
  using tempFile = generateTestFilePath("people.data")

  const schema = TableSchema.create("people")
    .with(column("id", ColumnTypes.string()).unique())
    .with(column("firstName", ColumnTypes.string()))
    .with(column("lastName", ColumnTypes.string()))
    .with(column("age", ColumnTypes.uint32()))
    .with(column("likesIceCream", ColumnTypes.boolean()))

  async function useTableResources(
    filePath: string,
    { truncate = false } = {},
  ) {
    const dbFile = await DbFile.open(filePath, { create: true, truncate })
    const table = new Table(await dbFile.getTableStorage(schema))
    return { table, dbFile, [Symbol.dispose]: () => dbFile.close() }
  }

  using resources = await useTableResources(tempFile.filePath, {
    truncate: true,
  })
  const { table: people } = resources
  let aliceRowId: HeapFileRowId
  const otherRowIds: HeapFileRowId[] = []

  await t.step(".insert()", async () => {
    aliceRowId = await people.insert({
      id: "1",
      firstName: "Alice",
      lastName: "Jones",
      age: 25,
      likesIceCream: true,
    })
    for (let i = 2; i <= 100; i++) {
      const rowId = await people.insert({
        id: i.toString(),
        firstName: `Person ${i}`,
        lastName: `Lastname ${i}`,
        age: i,
        likesIceCream: i % 2 === 0,
      })
      otherRowIds.push(rowId)
    }
  })

  await t.step(".set()", async () => {
    const alice = await people.get(aliceRowId)
    let newRowId = await people.set(aliceRowId, { ...alice!, age: 26 })

    // the row id should stay the same because the record fits into
    // the existing slot
    expect(newRowId).toEqual(aliceRowId)
    expect(await people.lookupUnique("id", "1")).toEqual({
      id: "1",
      firstName: "Alice",
      lastName: "Jones",
      age: 26,
      likesIceCream: true,
    })

    // the row id should change because the record no longer fits into
    // the existing slot
    newRowId = await people.set(aliceRowId, {
      ...alice!,
      age: 27,
      lastName: "from the books about wonderland",
    })
    expect(newRowId).not.toMatchObject(aliceRowId)
    expect(await people.get(aliceRowId), "old row id will still work").toEqual({
      id: "1",
      firstName: "Alice",
      lastName: "from the books about wonderland",
      age: 27,
      likesIceCream: true,
    })
    expect(
      await people.lookupUnique("id", "1"),
      "looking up from an index will work",
    ).toEqual({
      id: "1",
      firstName: "Alice",
      lastName: "from the books about wonderland",
      age: 27,
      likesIceCream: true,
    })

    // we can still find the next record that was inserted
    expect(await people.get(otherRowIds[0])).toEqual({
      age: 2,
      firstName: "Person 2",
      id: "2",
      lastName: "Lastname 2",
      likesIceCream: true,
    })
  })

  using resources2 = await useTableResources(tempFile.filePath)
  const fixtures = [
    { resources, name: "table that was written to" },
    { resources: resources2, name: "a just opened file" },
  ]
  for (const { resources: { table: people }, name } of fixtures) {
    await t.step(`reads [${name}]`, async (t) => {
      await t.step(
        "Table.get()",
        async () => {
          expect(await people.get(aliceRowId)).toEqual({
            id: "1",
            firstName: "Alice",
            lastName: "from the books about wonderland",
            age: 27,
            likesIceCream: true,
          })
        },
      )

      await t.step(
        "Table.lookupUnique()",
        async () => {
          expect(await people.lookupUnique("id", "1")).toEqual({
            id: "1",
            firstName: "Alice",
            lastName: "from the books about wonderland",
            age: 27,
            likesIceCream: true,
          })
        },
      )
    })
  }
})
