import { expect } from "@std/expect"
import { ColumnTypes, getColumnTypeFromString } from "./ColumnType.ts"
import { ReadonlyDataView } from "../../binary/dataview.ts"

Deno.test("string/TEXT columns", () => {
  const stringColumn = ColumnTypes.string()
  expect(stringColumn.isValid("hello")).toBe(true)
  // @ts-expect-error - 123 obviously isn't a string
  expect(stringColumn.isValid(123)).toBe(false)
  expect(getColumnTypeFromString("string").name).toEqual("string")
})

Deno.test("timestamp columns", () => {
  const dateColumn = ColumnTypes.timestamp()
  const date = new Date()
  const buffer = dateColumn.serializer?.toUint8Array(date)
  expect(
    dateColumn.serializer?.readAt(new ReadonlyDataView(buffer!.buffer), 0)
      .getTime(),
  )
    .toEqual(date.getTime())
})

Deno.test("array columns", () => {
  const stringArray = ColumnTypes.string().array()
  expect(stringArray.name).toEqual("string[]")
  expect(stringArray.isValid(["hello", "world"])).toBe(true)
  expect(getColumnTypeFromString("string[]").name).toEqual("string[]")
})

Deno.test("nullable columns", () => {
  const nullableString = ColumnTypes.string().nullable()
  expect(nullableString.name).toEqual("string?")
  expect(nullableString.isValid(null)).toBe(true)
  expect(nullableString.isValid("hello")).toBe(true)
  expect(getColumnTypeFromString("string?").name).toEqual("string?")
})

Deno.test("nullable array columns", () => {
  const nullableStringArray = ColumnTypes.string().array().nullable()
  expect(nullableStringArray.name).toEqual("string[]?")
  expect(nullableStringArray.isValid(null)).toBe(true)
  expect(nullableStringArray.isValid(["hello", "world"])).toBe(true)
  expect(getColumnTypeFromString("string[]?").name).toEqual("string[]?")
})

Deno.test("nullable array of nullable strings", () => {
  const nullableStringArray = ColumnTypes.string().nullable().array().nullable()
  expect(nullableStringArray.name).toEqual("string?[]?")
  expect(nullableStringArray.isValid(null)).toBe(true)
  expect(nullableStringArray.isValid(["hello", "world"])).toBe(true)
  expect(nullableStringArray.isValid([null, "world"])).toBe(true)
  expect(getColumnTypeFromString("string?[]?").name).toEqual("string?[]?")
})
