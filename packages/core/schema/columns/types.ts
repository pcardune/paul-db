import { Struct } from "../../binary/Struct.ts"
import { Json } from "../../types.ts"
import { ColumnType } from "./ColumnType.ts"
import * as stdUUID from "@std/uuid"

export function any<T>() {
  return new ColumnType<T>({
    name: "any",
    isValid: (_value: T): _value is T => true,
  })
}

export function caseInsensitiveString() {
  return new ColumnType<string>({
    name: "caseInsensitiveString",
    isValid: (value) => typeof value === "string",
    equals: (a, b) => a.toLowerCase() === b.toLowerCase(),
    compare: (a, b) => a.toLowerCase().localeCompare(b.toLowerCase()),
  })
}

export function uuid() {
  return new ColumnType<string>({
    name: "uuid",
    isValid: (value) => stdUUID.validate(value),
    serializer: Struct.unicodeStringStruct,
    minValue: "00000000-0000-0000-0000-000000000000",
  })
}

export function positiveNumber() {
  return new ColumnType<number>({
    name: "positiveNumber",
    isValid: (value) => value > 0,
    minValue: 0,
  })
}

export function boolean() {
  return new ColumnType<boolean>({
    name: "boolean",
    isValid: (value) => typeof value === "boolean",
    serializer: Struct.boolean,
    minValue: false,
  })
}

export function string() {
  return new ColumnType<string>({
    name: "string",
    isValid: (value) => typeof value === "string",
    serializer: Struct.unicodeStringStruct,
    minValue: "",
  })
}

export function float() {
  return new ColumnType<number>({
    name: "float",
    isValid: (value) => typeof value === "number",
    serializer: Struct.float64,
    minValue: -Infinity,
  })
}

export function int16() {
  return new ColumnType<number>({
    name: "int16",
    isValid: (value) =>
      typeof value === "number" && Number.isInteger(value) &&
      value >= -(2 ** 15 - 1) && value <= 2 ** 15 - 1,
    serializer: Struct.int16,
    minValue: -(2 ** 15 - 1),
  })
}

export function uint16() {
  return new ColumnType<number>({
    name: "uint16",
    isValid: (value) =>
      typeof value === "number" && Number.isInteger(value) && value >= 0 &&
      value <= 2 ** 16 - 1,
    serializer: Struct.uint16,
    minValue: 0,
  })
}

export function int32() {
  return new ColumnType<number>({
    name: "int32",
    isValid: (value) =>
      typeof value === "number" && Number.isInteger(value) &&
      value >= -(2 ** 31 - 1) && value <= 2 ** 31 - 1,
    serializer: Struct.int32,
    minValue: -(2 ** 31 - 1),
  })
}

export function uint32() {
  return new ColumnType<number>({
    name: "uint32",
    isValid: (value) =>
      typeof value === "number" && Number.isInteger(value) && value >= 0 &&
      value <= 2 ** 32 - 1,
    serializer: Struct.uint32,
    minValue: 0,
  })
}

export function int64() {
  return new ColumnType<bigint>({
    name: "int64",
    isValid: (value) =>
      typeof value === "bigint" && value >= -(2n ** 63n - 1n) &&
      value <= 2n ** 63n - 1n,
    serializer: Struct.bigInt64,
    minValue: -(2n ** 63n - 1n),
  })
}

export function uint64() {
  return new ColumnType<bigint>({
    name: "uint64",
    isValid: (value) =>
      typeof value === "bigint" && value >= 0 && value <= 2n ** 64n - 1n,
    serializer: Struct.bigUint64,
    minValue: 0n,
  })
}

export function json() {
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

export function timestamp() {
  return new ColumnType<Date>({
    name: "timestamp",
    isValid: (value) => value instanceof Date,
    serializer: Struct.timestamp,
    minValue: new Date(-Infinity),
  })
}

export function date() {
  return new ColumnType<Date>({
    name: "date",
    isValid: (value) => value instanceof Date,
    serializer: Struct.date,
    minValue: new Date(-Infinity),
  })
}

export function blob() {
  return new ColumnType<Uint8Array>({
    name: "blob",
    isValid: (value) => value instanceof Uint8Array,
    serializer: Struct.bytes,
  })
}
