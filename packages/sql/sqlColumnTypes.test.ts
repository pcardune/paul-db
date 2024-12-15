import { expect } from "@std/expect"
import { getColumnTypeFromSQLType } from "./sqlColumnTypes.ts"

Deno.test("string/TEXT columns", () => {
  expect(getColumnTypeFromSQLType("TEXT").name).toEqual("string")
})

Deno.test("array columns", () => {
  expect(getColumnTypeFromSQLType("TEXT[]").name).toEqual("string[]")
})
