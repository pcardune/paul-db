import { describe, it } from "@std/testing/bdd"
import { expect } from "@std/expect"
import {
  InsertRecordForColumnSchemas,
  InsertRecordForTableSchema,
  ISchema,
  makeTableSchemaStruct,
  TableSchema,
} from "./TableSchema.ts"
import { StoredRecordForTableSchema } from "./TableSchema.ts"
import { ColumnTypes } from "./columns/ColumnType.ts"
import { dumpUint8Buffer } from "../binary/util.ts"
import { WriteableDataView } from "../binary/dataview.ts"
import * as Column from "./columns/index.ts"
import {
  column,
  computedColumn,
  StoredRecordForColumnSchemas,
} from "./columns/ColumnBuilder.ts"
import { DBSchema } from "./DBSchema.ts"
import { assertTrue, assertType, TypeEquals } from "../testing.ts"
import type { ConditionalPick } from "type-fest"

describe("ColumnSchemas", () => {
  const nameColumn = column(
    "name",
    ColumnTypes.string(),
  )

  it("can be created with a basic type", () => {
    expect(nameColumn.name).toBe("name")
    expect(nameColumn.isUnique).toBe(false)
    expect(nameColumn.type.isValid("Alice")).toBe(true)
    // @ts-expect-error: Can't insert a number into a string column
    // Note: at runtime this will not throw an error because we expect
    // typescript to catch it
    expect(nameColumn.type.isValid(25)).toBe(false)

    assertType<"name">(nameColumn.name)
    assertType<false>(nameColumn.isUnique)
    assertTrue<TypeEquals<string, Column.Stored.GetValue<typeof nameColumn>>>()
  })

  it("exposes the types of the column to typescript", () => {
    assertTrue<TypeEquals<string, Column.Stored.GetValue<typeof nameColumn>>>()

    // @ts-expect-error: Can't insert a number into a string column
    assertTrue<TypeEquals<number, Column.GetValue<typeof nameColumn>>>()

    const ageColumn = column(
      "age",
      ColumnTypes.uint32(),
    )
    const nameColumn = column(
      "name",
      ColumnTypes.string(),
    )
    const createdAtColumn = column(
      "createdAt",
      ColumnTypes.date(),
    ).defaultTo(() => new Date())
    const nameAndAgeColumn = computedColumn(
      "nameAndAnge",
      ColumnTypes.string(),
      (input: { name: string; age: number }) => `${input.name} ${input.age}`,
    )
    assertTrue<TypeEquals<number, Column.Stored.GetValue<typeof ageColumn>>>()

    type StoredRecord = StoredRecordForColumnSchemas<
      { age: typeof ageColumn; name: typeof nameColumn }
    >
    assertTrue<TypeEquals<{ age: number; name: string }, StoredRecord>>()
    assertTrue<TypeEquals<undefined, typeof ageColumn["defaultValueFactory"]>>()

    assertTrue<TypeEquals<string, Column.GetOutput<typeof nameAndAgeColumn>>>()
    assertTrue<TypeEquals<number, Column.GetOutput<typeof ageColumn>>>()
    assertTrue<
      TypeEquals<
        { age: number },
        Column.GetRecordContainingColumn<typeof ageColumn>
      >
    >()
    assertTrue<
      TypeEquals<
        { age: number; name: string },
        Column.GetRecordContainingColumn<typeof nameAndAgeColumn>
      >
    >()

    assertTrue<
      TypeEquals<
        { age: number; name: string; createdAt?: Date | undefined },
        InsertRecordForColumnSchemas<
          {
            age: typeof ageColumn
            name: typeof nameColumn
            createdAt: typeof createdAtColumn
          }
        >
      >
    >()
  })

  it("lets you make a column unique", () => {
    const uniqueNameColumn = nameColumn.unique()
    expect(uniqueNameColumn.isUnique).toBe(true)

    // the type system should know that this is true
    assertType<true>(uniqueNameColumn.isUnique)
  })

  it("lets you copy a column with a new name", () => {
    const lastNameColumn = nameColumn.named("lastName")
    expect(lastNameColumn.name).toBe("lastName")
    assertType<"lastName">(lastNameColumn.name)
  })

  it("Lets you create a serial type column", () => {
    const idCol = column("id", "serial")
    expect(idCol.isUnique).toBe(true)
    expect(idCol.indexed.shouldIndex).toBe(true)
    expect(idCol.defaultValueFactory).toBeDefined()
  })

  it("has helper types for searching through columns", () => {
    const columns = {
      id: column("id", ColumnTypes.int16()).unique(),
      foo: column("foo", ColumnTypes.string()),
      bar: column("bar", ColumnTypes.string()).index(),
    }
    const computedCols = {
      foobar: computedColumn(
        "foobar",
        ColumnTypes.string(),
        ({ foo, bar }: { foo: string; bar: string }) => foo + bar,
      ).unique(),
    }

    type AllCols = typeof columns & typeof computedCols

    type IndexedCols = Column.FindIndexedNames<AllCols>
    assertTrue<TypeEquals<"id" | "bar" | "foobar", IndexedCols>>()
    type UniqueCols = Column.FindUniqueNames<AllCols>
    assertTrue<TypeEquals<"id" | "foobar", UniqueCols>>()
  })
})

describe("Computed column schemas", () => {
  it("can be used to compute values from other values", () => {
    const nameColumn = computedColumn(
      "name",
      ColumnTypes.string(),
      (input: { firstName: string; lastName: string }) =>
        `${input.firstName} ${input.lastName}`,
    )

    expect(nameColumn.compute({ firstName: "Alice", lastName: "Smith" })).toBe(
      "Alice Smith",
    )

    // @ts-expect-error: typescript will catch an invalid type,
    // but at runtime you are on your own
    expect(() => nameColumn.compute(1)).not.toThrow()
  })

  it("has a never value type", () => {
    const nameColumn = computedColumn(
      "name",
      ColumnTypes.string(),
      (input: { firstName: string; lastName: string }) =>
        `${input.firstName} ${input.lastName}`,
    )
    assertTrue<TypeEquals<never, Column.Stored.GetValue<typeof nameColumn>>>()
  })
})

describe("Schemas", () => {
  it("can be built iteratively", () => {
    const peopleSchema = TableSchema.create("people")
      .with(column("name", ColumnTypes.string()))
      .with(column("age", ColumnTypes.uint32()))

    expect(peopleSchema.columns).toHaveLength(2)
    expect(peopleSchema.isValidInsertRecord({ name: "Alice", age: 25 }).valid)
      .toBe(
        true,
      )
    expect(peopleSchema.isValidInsertRecord({ name: "Alice", age: -25 }).valid)
      .toBe(
        false,
      )

    // @ts-expect-error: Can't insert a record with missing columns
    expect(peopleSchema.isValidInsertRecord({ name: "Alice" }).valid).toBe(
      false,
    )

    const otherPeopleSchema = TableSchema.create("people")
      .with(
        column("name", ColumnTypes.string()),
        column("age", ColumnTypes.uint32()),
      )
    assertTrue<TypeEquals<typeof peopleSchema, typeof otherPeopleSchema>>()
  })

  it("Lets you specify a column directly", () => {
    const nameColumn = column("name", ColumnTypes.string())
    const peopleSchema = TableSchema.create("people").with(nameColumn)
    expect(peopleSchema.columns).toHaveLength(1)
    expect(peopleSchema.isValidInsertRecord({ name: "Alice" }).valid).toBe(true)
  })

  it("Uses the underlying column type for validation", () => {
    const peopleSchema = TableSchema.create("people")
      .with(column("name", ColumnTypes.string()))
      .with(column("age", ColumnTypes.uint32()))

    expect(peopleSchema.isValidInsertRecord({ name: "Alice", age: 25 }).valid)
      .toBe(
        true,
      )
    expect(peopleSchema.isValidInsertRecord({ name: "Alice", age: -25 }).valid)
      .toBe(
        false,
      )
  })

  it("Exposes the types of records that are stored in the table", () => {
    const peopleSchema = TableSchema.create("people")
      .with(column("name", ColumnTypes.string()))
      .with(column("age", ColumnTypes.uint32()))

    peopleSchema.columnsByName.name
    type foo = ISchema<
      typeof peopleSchema["name"],
      typeof peopleSchema.columnsByName
    >
    type Blah = ConditionalPick<foo["columnsByName"], Column.Stored.Any>

    type blahCols = StoredRecordForColumnSchemas<Blah>

    type Person = StoredRecordForTableSchema<typeof peopleSchema>
    assertTrue<
      TypeEquals<
        { id: string; name: string; age: number },
        Person
      >
    >()

    assertTrue<
      TypeEquals<
        { id?: string; name: string; age: number },
        InsertRecordForTableSchema<typeof peopleSchema>
      >
    >()
  })

  describe("Computed columns", () => {
    it("can be added using withComputedColumn()", () => {
      const peopleSchema = TableSchema.create("people")
        .with(column("firstName", ColumnTypes.string()))
        .with(column("lastName", ColumnTypes.string()))
        .withComputedColumn(
          computedColumn(
            "name",
            ColumnTypes.string(),
            (input: { firstName: string; lastName: string }) =>
              `${input.firstName} ${input.lastName}`,
          ),
        )

      assertTrue<
        TypeEquals<
          { firstName: string; lastName: string; name: never },
          InsertRecordForTableSchema<typeof peopleSchema>
        >
      >()

      expect(
        peopleSchema.isValidInsertRecord({
          firstName: "Alice",
          lastName: "Smith",
        }),
      )
    })

    it("can only use the record type up to the current point", () => {
      const peopleSchema = TableSchema.create("people")
        .with(column("firstName", ColumnTypes.string()))

      const nameColumn = computedColumn(
        "name",
        ColumnTypes.string(),
        (input: { firstName: string; lastName: string }) =>
          `${input.firstName} ${input.lastName}`,
      )

      peopleSchema
        .withComputedColumn(
          // @ts-expect-error: lastName is not in the schema yet
          nameColumn,
        )

      peopleSchema.with(column("lastName", ColumnTypes.string()))
        .withComputedColumn(nameColumn)

      // Inference will also work
      peopleSchema.withComputedColumn(
        computedColumn(
          "uppercaseName",
          ColumnTypes.string(),
          (input) => {
            assertTrue<
              TypeEquals<{ id: string; firstName: string }, typeof input>
            >()
            return input.firstName.toUpperCase()
          },
        ),
      )
    })
  })
})

describe("unique constraints", () => {
  it("can be added to a table schema", () => {
    const schema = TableSchema.create("people")
      .with(column("name", ColumnTypes.string()))
      .with(column("age", ColumnTypes.uint32()))
      .withUniqueConstraint(
        "nameAndAge",
        ColumnTypes.string(),
        ["name", "age"],
        (input) => `${input.name}:${input.age}`,
      )

    expect(schema.computedColumnsByName.nameAndAge).toBeDefined()
    const nameAndAgeCol = schema.computedColumnsByName.nameAndAge
    expect(nameAndAgeCol.isUnique).toBe(true)
    expect(nameAndAgeCol.indexed.shouldIndex).toBe(true)
    expect(nameAndAgeCol.compute({ name: "Alice", age: 25 })).toBe("Alice:25")
  })
})

describe("Serializing and deserializing records", () => {
  it("can serialize and deserialize records", () => {
    const peopleSchema = TableSchema.create("people")
      .with(column("age", ColumnTypes.uint32()))
      .with(column("likesIceCream", ColumnTypes.boolean()))
      .with(column("name", ColumnTypes.string()))

    const serializer = makeTableSchemaStruct(peopleSchema)!
    expect(serializer).toBeDefined()

    const recordToWrite = {
      name: "Alice",
      age: 25,
      likesIceCream: true,
    }
    const data = new ArrayBuffer(serializer.sizeof(recordToWrite))
    const view = new WriteableDataView(data)
    serializer.writeAt(recordToWrite, view, 0)
    // deno-fmt-ignore
    expect(dumpUint8Buffer(data)).toEqual([
        0,   0,   0,  14,      // length of the record (excluding the length itself)
        0,   0,   0,  25,      // age=25
        1,                     // likesIceCream=true
        0,   0,   0,   5,      // length of "Alice"
       65, 108, 105,  99, 101, // name="Alice"
    ])

    const record = serializer.readAt(view, 0)
    expect(record).toEqual({
      name: "Alice",
      age: 25,
      likesIceCream: true,
    })
  })
})

describe("DBSchema", () => {
  it("lets you combine multiple table schemas", () => {
    const people = TableSchema.create("people").with(
      column("name", ColumnTypes.string()),
    )
    const pets = TableSchema.create("pets").with(
      column("name", ColumnTypes.string()).finalize(),
    )
    const db = DBSchema.create().withTables(
      people,
      pets,
    )
    expect(db.name).toBe("default")
    expect(db.schemas.people).toBe(people)
    expect(db.schemas.pets).toBe(pets)
  })
})
