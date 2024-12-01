import { describe, it } from "jsr:@std/testing/bdd"
import { expect } from "jsr:@std/expect"
import {
  InsertRecordForTableSchema,
  makeTableSchemaSerializer,
  TableSchema,
} from "./schema.ts"
import { StoredRecordForTableSchema } from "./schema.ts"
import { ColumnTypes } from "./ColumnType.ts"
import { dumpUint8Buffer } from "../binary/util.ts"
import { WriteableDataView } from "../binary/dataview.ts"
import {
  Column,
  column,
  computedColumn,
  StoredRecordForColumnSchemas,
} from "./ColumnSchema.ts"

function assertType<T>(_value: T) {}
type TypeEquals<Actual, Expected> = Actual extends Expected ? true
  : "Types not equal"

function assertTrue<T extends true>() {}
assertTrue<TypeEquals<"green", "green">>()

describe("ColumnSchemas", () => {
  const nameColumn = column(
    "name",
    ColumnTypes.any<string>(),
  )

  it("can be created with a basic type", () => {
    expect(nameColumn.name).toBe("name")
    expect(nameColumn.isUnique).toBe(false)
    expect(nameColumn.type.isValid("Alice")).toBe(true)
    // @ts-expect-error: Can't insert a number into a string column
    // Note: at runtime this will not throw an error because we expect
    // typescript to catch it
    expect(nameColumn.type.isValid(25)).toBe(true)

    assertType<"name">(nameColumn.name)
    assertType<false>(nameColumn.isUnique)
    assertTrue<TypeEquals<string, Column.GetValue<typeof nameColumn>>>()
  })

  it("exposes the types of the column to typescript", () => {
    assertTrue<TypeEquals<string, Column.GetValue<typeof nameColumn>>>()

    // @ts-expect-error: Can't insert a number into a string column
    assertTrue<TypeEquals<number, Column.GetValue<typeof nameColumn>>>()

    const ageColumn = column(
      "age",
      ColumnTypes.positiveNumber(),
    )
    const nameColumn = column(
      "name",
      ColumnTypes.string(),
    )
    assertTrue<TypeEquals<number, Column.GetValue<typeof ageColumn>>>()

    type StoredRecord = StoredRecordForColumnSchemas<
      [typeof ageColumn, typeof nameColumn]
    >
    assertTrue<TypeEquals<{ age: number; name: string }, StoredRecord>>()
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
    assertTrue<TypeEquals<never, Column.GetValue<typeof nameColumn>>>()
  })
})

describe("Schemas", () => {
  it("can be built iteratively", () => {
    const peopleSchema = TableSchema.create("people")
      .with(column("name", ColumnTypes.any<string>()))
      .with(column("age", ColumnTypes.positiveNumber()))

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
  })

  it("Lets you specify a column directly", () => {
    const nameColumn = column("name", ColumnTypes.any<string>())
    const peopleSchema = TableSchema.create("people").with(nameColumn)
    expect(peopleSchema.columns).toHaveLength(1)
    expect(peopleSchema.isValidInsertRecord({ name: "Alice" }).valid).toBe(true)
  })

  it("Uses the underlying column type for validation", () => {
    const peopleSchema = TableSchema.create("people")
      .with(column("name", ColumnTypes.any<string>()))
      .with(column("age", ColumnTypes.positiveNumber()))

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
      .with(column("name", ColumnTypes.any<string>()))
      .with(column("age", ColumnTypes.positiveNumber()))
    assertTrue<
      TypeEquals<
        { id: string; name: string; age: number },
        StoredRecordForTableSchema<typeof peopleSchema>
      >
    >()

    type foo = InsertRecordForTableSchema<typeof peopleSchema>

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
        .with(column("firstName", ColumnTypes.any<string>()))
        .with(column("lastName", ColumnTypes.any<string>()))
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
        .with(column("firstName", ColumnTypes.any<string>()))

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

      peopleSchema.with(column("lastName", ColumnTypes.any<string>()))
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

describe("Serializing and deserializing records", () => {
  it("can serialize and deserialize records", () => {
    const peopleSchema = TableSchema.create("people")
      .with(column("age", ColumnTypes.uint32()))
      .with(column("likesIceCream", ColumnTypes.boolean()))
      .with(column("name", ColumnTypes.string()))

    const serializer = makeTableSchemaSerializer(peopleSchema)!
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
