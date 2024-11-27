import { IStruct, Struct } from "../binary/Struct.ts"
import { Comparator, EqualityChecker } from "../types.ts"
import * as uuid from "jsr:@std/uuid"

export const ColumnTypes = {
  any<T>() {
    return new ColumnType<T>({
      name: "any",
      isValid: (_value: T): _value is T => true,
    })
  },
  caseInsensitiveString() {
    return new ColumnType<string>({
      name: "caseInsensitiveString",
      isValid: (value) => typeof value === "string",
      equals: (a, b) => a.toLowerCase() === b.toLowerCase(),
      compare: (a, b) => a.toLowerCase().localeCompare(b.toLowerCase()),
    })
  },
  uuid() {
    return new ColumnType<`${string}-${string}-${string}-${string}-${string}`>({
      name: "uuid",
      isValid: (value) => uuid.validate(value),
    })
  },
  positiveNumber() {
    return new ColumnType<number>({
      name: "positiveNumber",
      isValid: (value) => value > 0,
    })
  },
  boolean() {
    return new ColumnType<boolean>({
      name: "boolean",
      isValid: (value) => typeof value === "boolean",
      serializer: Struct.boolean,
    })
  },
  string() {
    return new ColumnType<string>({
      name: "string",
      isValid: (value) => typeof value === "string",
      serializer: Struct.unicodeStringStruct,
    })
  },
  float() {
    return new ColumnType<number>({
      name: "float",
      isValid: (value) => typeof value === "number",
      serializer: Struct.float64,
    })
  },
  int32() {
    return new ColumnType<number>({
      name: "int32",
      isValid: (value) => typeof value === "number" && Number.isInteger(value),
      serializer: Struct.int32,
    })
  },
  uint32() {
    return new ColumnType<number>({
      name: "uint32",
      isValid: (value) =>
        typeof value === "number" && Number.isInteger(value) && value >= 0,
      serializer: Struct.uint32,
    })
  },
  int64() {
    return new ColumnType<bigint>({
      name: "int64",
      isValid: (value) => typeof value === "bigint",
      serializer: Struct.bigInt64,
    })
  },
  uint64() {
    return new ColumnType<bigint>({
      name: "uint64",
      isValid: (value) => typeof value === "bigint" && value >= 0,
      serializer: Struct.bigUint64,
    })
  },
}

export class ColumnType<T> {
  readonly name: string
  isValid: (value: T) => boolean
  isEqual: EqualityChecker<T>
  compare: Comparator<T>
  serializer?: IStruct<T>

  constructor({
    name,
    isValid,
    equals = (a: T, b: T) => a === b,
    compare = (a: T, b: T) => (a > b ? 1 : a < b ? -1 : 0),
    serializer,
  }: {
    name: string
    isValid: (value: T) => boolean
    equals?: EqualityChecker<T>
    compare?: Comparator<T>
    serializer?: IStruct<T>
  }) {
    this.name = name
    this.isValid = isValid
    this.isEqual = equals
    this.compare = compare
    this.serializer = serializer
  }
}
