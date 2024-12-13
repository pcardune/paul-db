import { expect } from "@std/expect"
import {
  ColumnTypes,
  getColumnTypeFromSQLType,
  getColumnTypeFromString,
} from "./ColumnType.ts"

Deno.test("string/TEXT columns", () => {
  const stringColumn = ColumnTypes.string()
  expect(stringColumn.isValid("hello")).toBe(true)
  // @ts-expect-error - 123 obviously isn't a string
  expect(stringColumn.isValid(123)).toBe(false)
  expect(getColumnTypeFromSQLType("TEXT").name).toEqual("string")
  expect(getColumnTypeFromString("string").name).toEqual("string")
})

Deno.test("array columns", () => {
  const stringArray = ColumnTypes.string().array()
  expect(stringArray.name).toEqual("string[]")
  expect(stringArray.isValid(["hello", "world"])).toBe(true)
  expect(getColumnTypeFromSQLType("TEXT[]").name).toEqual("string[]")
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
