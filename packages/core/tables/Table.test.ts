import { beforeAll, beforeEach, describe, it } from "jsr:@std/testing/bdd"
import { expect } from "jsr:@std/expect"
import { assertSnapshot } from "jsr:@std/testing/snapshot"
import { Table, TableInfer } from "./Table.ts"
import {
  column,
  computedColumn,
  RecordForTableSchema,
  TableSchema,
} from "../schema/schema.ts"
import {
  HeapFileRowId,
  HeapFileTableStorage,
  InMemoryTableStorage,
  JsonFileTableStorage,
} from "./TableStorage.ts"
import { ColumnType, ColumnTypes } from "../schema/ColumnType.ts"
import { FileBackedBufferPool } from "../pages/BufferPool.ts"

const peopleSchema = TableSchema.create("people")
  .withColumn("name", ColumnTypes.any<string>())
  .withColumn("age", ColumnTypes.positiveNumber())

describe("Create, Read, and Delete", () => {
  it("lets you insert and retrieve records", async () => {
    const people = await Table.create(
      peopleSchema,
      InMemoryTableStorage.forSchema(peopleSchema),
    )

    const aliceId = await people.insert({ name: "Alice", age: 12 })
    const bobId = await people.insert({ name: "Bob", age: 12 })
    expect(await people.get(aliceId)).toEqual({ name: "Alice", age: 12 })
    expect(await people.get(bobId)).toEqual({ name: "Bob", age: 12 })
  })
  it("You can delete records", async () => {
    const people = await Table.create(
      peopleSchema,
      InMemoryTableStorage.forSchema(peopleSchema),
    )
    const aliceId = await people.insert({ name: "Alice", age: 12 })
    const bobId = await people.insert({ name: "Bob", age: 12 })
    people.remove(aliceId)
    expect(await people.get(aliceId)).toBeUndefined()
    expect(await people.get(bobId)).toEqual({ name: "Bob", age: 12 })
  })
})

describe("Insert Validation", () => {
  it("should not allow you to insert records with invalid schema", async () => {
    const people = await Table.create(
      peopleSchema,
      InMemoryTableStorage.forSchema(peopleSchema),
    )
    await people.insert({ name: "Alice", age: 12 })
    expect(people.insert({ name: "Alice", age: -12 })).rejects.toThrow(
      "Invalid record",
    )
  })

  it("Throws when inserting values that don't satisfy the column type", async () => {
    const schema = peopleSchema
      .withColumn(
        "favoriteOdd",
        new ColumnType({ isValid: (value: number) => value % 2 === 1 }),
      )
    const oddPeople = await Table.create(
      schema,
      InMemoryTableStorage.forSchema(schema),
    )

    oddPeople.insert({ name: "Alice", age: 13, favoriteOdd: 13 })
    expect(oddPeople.insert({ name: "Alice", age: 12, favoriteOdd: 12 }))
      .rejects.toThrow("Invalid record")
  })
})

const phoneNumberType = new ColumnType<string>({
  isValid: (value) => /^[\d-]+$/.test(value),
  equals: (a, b) => a.replace(/-/g, "") === b.replace(/-/g, ""),
  compare: (a, b) => a.replace(/-/g, "").localeCompare(b.replace(/-/g, "")),
})

describe("Uniqueness Constraints", () => {
  it("enforces uniqueness constraints", async () => {
    const schema = peopleSchema
      .withColumn("ssn", ColumnTypes.any<string>(), { unique: true })
    const people = await Table.create(
      schema,
      InMemoryTableStorage.forSchema(schema),
    )

    await people.insert({ name: "Alice", age: 12, ssn: "123-45-6789" })
    expect(people.insert({ name: "Alice", age: 12, ssn: "123-45-6789" }))
      .rejects.toThrow("Record with given ssn value already exists")
    await people.insert({ name: "Alice", age: 12, ssn: "123-45-6-7-89" })
  })

  it("utilizes column type to determine uniqueness", async () => {
    const schema = peopleSchema
      .withColumn("phone", phoneNumberType, { unique: true })
    const people = await Table.create(
      schema,
      InMemoryTableStorage.forSchema(schema),
    )
    await people.insert({ name: "Alice", age: 12, phone: "123-867-5309" })
    expect(
      people.insert({ name: "Alice", age: 12, phone: "123-8675-309" }),
    ).rejects.toThrow("Record with given phone value already exists")
  })
})

describe("Querying", () => {
  describe("Table.iterate()", () => {
    it("lets you iterate over the entire contents of the table", async () => {
      const people = await Table.create(
        peopleSchema,
        InMemoryTableStorage.forSchema(peopleSchema),
      )
      await people.insertMany([
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
    it("can query records by scanning the entire table", async () => {
      const people = await Table.create(
        peopleSchema,
        InMemoryTableStorage.forSchema(peopleSchema),
      )
      await people.insertMany([
        { name: "Alice", age: 30 },
        { name: "Bob", age: 30 },
        { name: "Charlie", age: 35 },
      ])

      expect(people.scan("age", 30)).toEqual([
        { name: "Alice", age: 30 },
        { name: "Bob", age: 30 },
      ])
    })

    it("uses the underlying column type for equality testing", async () => {
      const peopleSchema = TableSchema.create("people").withColumn(
        "name",
        ColumnTypes.any<string>(),
      ).withColumn("email", ColumnTypes.caseInsensitiveString())
      const people = await Table.create(
        peopleSchema,
        InMemoryTableStorage.forSchema(peopleSchema),
      )
      await people.insertMany([
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
    let people: TableInfer<
      typeof indexedPeopleSchema,
      InMemoryTableStorage<
        number,
        RecordForTableSchema<typeof indexedPeopleSchema>
      >
    >
    beforeAll(async () => {
      people = await Table.create(
        indexedPeopleSchema,
        InMemoryTableStorage.forSchema(indexedPeopleSchema),
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
    Deno.removeSync("/tmp/people.json")
    const people = await Table.create(
      peopleSchema,
      JsonFileTableStorage.forSchema(peopleSchema, "/tmp/people.json"),
    )
    await people.insert({ name: "Alice", age: 12 })
    await people.insert({ name: "Bob", age: 12 })
    const f = Deno.readTextFileSync("/tmp/people.json")
    await assertSnapshot(t, f)
  },
})

describe("Heap file backed table storage", () => {
  let bufferPool: FileBackedBufferPool
  const filePath = "/tmp/people.data"

  const schema = TableSchema.create("people")
    .withColumn("firstName", ColumnTypes.string())
    .withColumn("lastName", ColumnTypes.string())
    .withColumn("age", ColumnTypes.uint32())
    .withColumn("likesIceCream", ColumnTypes.boolean())

  async function withBufferPool(
    filePath: string,
    fn: (bufferPool: FileBackedBufferPool) => Promise<void>,
  ): Promise<void> {
    bufferPool = await FileBackedBufferPool.create(filePath, 4096)
    try {
      await fn(bufferPool)
    } finally {
      bufferPool.close()
    }
  }

  async function withTable(
    bufferPool: FileBackedBufferPool,
    fn: (
      table: TableInfer<
        typeof schema,
        HeapFileTableStorage<RecordForTableSchema<typeof schema>>
      >,
    ) => Promise<void>,
  ): Promise<void> {
    const table = await Table.create(
      schema,
      await HeapFileTableStorage.create(bufferPool, schema),
    )
    await fn(table)
  }

  async function withTableInFile(
    filePath: string,
    fn: (
      table: TableInfer<
        typeof schema,
        HeapFileTableStorage<RecordForTableSchema<typeof schema>>
      >,
    ) => Promise<void>,
  ): Promise<void> {
    await withBufferPool(filePath, (bufferPool) => withTable(bufferPool, fn))
  }

  beforeEach(() => {
    Deno.openSync(filePath, {
      create: true,
      write: true,
      truncate: true,
    }).close()
  })

  it("lets you insert and read a record", async () => {
    await withTableInFile(filePath, async (people) => {
      const aliceRowId = await people.insert({
        firstName: "Alice",
        lastName: "Jones",
        age: 25,
        likesIceCream: true,
      })
      expect(await people.get(aliceRowId)).toEqual({
        firstName: "Alice",
        lastName: "Jones",
        age: 25,
        likesIceCream: true,
      })
    })
  })

  it("lets you read the records back from disk", async () => {
    let aliceRowId: HeapFileRowId
    await withTableInFile(filePath, async (people) => {
      aliceRowId = await people.insert({
        firstName: "Alice",
        lastName: "Jones",
        age: 25,
        likesIceCream: true,
      })
    })
    await withTableInFile(filePath, async (people) => {
      expect(await people.get(aliceRowId)).toEqual({
        firstName: "Alice",
        lastName: "Jones",
        age: 25,
        likesIceCream: true,
      })
    })
  })
})
