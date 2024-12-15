/**
 * This module provides a set of built-in column types that can be used to
 * define the schema of a table.
 * @module
 */

import { Struct } from "../../binary/Struct.ts"
import { Json } from "../../types.ts"
import { ColumnType, SerialUInt32ColumnType } from "./ColumnType.ts"
import * as stdUUID from "@std/uuid"

/**
 * A column type for storing case-insensitive strings.
 *
 * Note that the stored value will have the same case as the original value
 * but comparisons (including for indexing) will be case-insensitive
 */
export function caseInsensitiveString(): ColumnType<string> {
  return new ColumnType<string>({
    name: "caseInsensitiveString",
    isValid: (value) => typeof value === "string",
    equals: (a, b) => a.toLowerCase() === b.toLowerCase(),
    compare: (a, b) => a.toLowerCase().localeCompare(b.toLowerCase()),
  })
}

/**
 * A column type for storing UUIDs
 */
export function uuid(): ColumnType<string> {
  return new ColumnType<string>({
    name: "uuid",
    isValid: (value) => stdUUID.validate(value),
    serializer: Struct.unicodeStringStruct,
    minValue: "00000000-0000-0000-0000-000000000000",
  })
}

/**
 * A column type for storing boolean values
 */
export function boolean(): ColumnType<boolean> {
  return new ColumnType<boolean>({
    name: "boolean",
    isValid: (value) => typeof value === "boolean",
    serializer: Struct.boolean,
    minValue: false,
  })
}

/**
 * A column type for storing strings (UTF-8 encoded)
 */
export function string(): ColumnType<string> {
  return new ColumnType<string>({
    name: "string",
    isValid: (value) => typeof value === "string",
    serializer: Struct.unicodeStringStruct,
    minValue: "",
  })
}

/**
 * A column type for storing signed 64-bit floating point numbers
 */
export function float(): ColumnType<number> {
  return new ColumnType<number>({
    name: "float",
    isValid: (value) => typeof value === "number",
    serializer: Struct.float64,
    minValue: -Infinity,
  })
}

/**
 * A column type for storing signed 16-bit integers
 */
export function int16(): ColumnType<number> {
  return new ColumnType<number>({
    name: "int16",
    isValid: (value) =>
      typeof value === "number" && Number.isInteger(value) &&
      value >= -(2 ** 15 - 1) && value <= 2 ** 15 - 1,
    serializer: Struct.int16,
    minValue: -(2 ** 15 - 1),
  })
}

/**
 * A column type for storing unsigned 16-bit integers
 */
export function uint16(): ColumnType<number> {
  return new ColumnType<number>({
    name: "uint16",
    isValid: (value) =>
      typeof value === "number" && Number.isInteger(value) && value >= 0 &&
      value <= 2 ** 16 - 1,
    serializer: Struct.uint16,
    minValue: 0,
  })
}

/**
 * A column type for storing signed 32-bit integers
 */
export function int32(): ColumnType<number> {
  return new ColumnType<number>({
    name: "int32",
    isValid: (value) =>
      typeof value === "number" && Number.isInteger(value) &&
      value >= -(2 ** 31 - 1) && value <= 2 ** 31 - 1,
    serializer: Struct.int32,
    minValue: -(2 ** 31 - 1),
  })
}

/**
 * A column type for storing unsigned 32-bit integers
 */
export function uint32(): ColumnType<number> {
  return new ColumnType<number>({
    name: "uint32",
    isValid: (value) =>
      typeof value === "number" && Number.isInteger(value) && value >= 0 &&
      value <= 2 ** 32 - 1,
    serializer: Struct.uint32,
    minValue: 0,
  })
}

/**
 * A column type for storing signed 64-bit integers (i.e. bigints)
 */
export function int64(): ColumnType<bigint> {
  return new ColumnType<bigint>({
    name: "int64",
    isValid: (value) =>
      typeof value === "bigint" && value >= -(2n ** 63n - 1n) &&
      value <= 2n ** 63n - 1n,
    serializer: Struct.bigInt64,
    minValue: -(2n ** 63n - 1n),
  })
}

/**
 * A column type for storing unsigned 64-bit integers (i.e. bigints)
 */
export function uint64(): ColumnType<bigint> {
  return new ColumnType<bigint>({
    name: "uint64",
    isValid: (value) =>
      typeof value === "bigint" && value >= 0 && value <= 2n ** 64n - 1n,
    serializer: Struct.bigUint64,
    minValue: 0n,
  })
}

/**
 * A column type for storing arbitrary JSON data
 */
export function json(): ColumnType<Json> {
  return new ColumnType<Json>({
    name: "json",
    isValid: (value) => {
      try {
        JSON.stringify(value)
        return true
      } catch {
        return false
      }
    },
    serializer: Struct.json,
  })
}

/**
 * A column type for storing timestamps
 */
export function timestamp(): ColumnType<Date> {
  return new ColumnType<Date>({
    name: "timestamp",
    isValid: (value) => value instanceof Date,
    serializer: Struct.timestamp,
    minValue: new Date(-Infinity),
  })
}

/**
 * A column type for storing dates (i.e. day/month/year and not time)
 */
export function date(): ColumnType<Date> {
  return new ColumnType<Date>({
    name: "date",
    isValid: (value) => value instanceof Date,
    serializer: Struct.date,
    minValue: new Date(-Infinity),
  })
}

/**
 * A column type for storing binary data
 */
export function blob(): ColumnType<Uint8Array> {
  return new ColumnType<Uint8Array>({
    name: "blob",
    isValid: (value) => value instanceof Uint8Array,
    serializer: Struct.bytes,
  })
}

export function serial(): SerialUInt32ColumnType {
  return new SerialUInt32ColumnType()
}
