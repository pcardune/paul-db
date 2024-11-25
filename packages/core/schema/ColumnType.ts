import { IStruct, Struct } from "../binary/Struct.ts"
import { Comparator, EqualityChecker } from "../types.ts"
import * as uuid from "jsr:@std/uuid"

export const ColumnTypes = {
  any<T>() {
    return new ColumnType<T>({ isValid: (_value: T): _value is T => true })
  },
  caseInsensitiveString() {
    return new ColumnType<string>({
      isValid: (value) => typeof value === "string",
      equals: (a, b) => a.toLowerCase() === b.toLowerCase(),
      compare: (a, b) => a.toLowerCase().localeCompare(b.toLowerCase()),
    })
  },
  uuid() {
    return new ColumnType<`${string}-${string}-${string}-${string}-${string}`>({
      isValid: (value) => uuid.validate(value),
    })
  },
  positiveNumber() {
    return new ColumnType<number>({
      isValid: (value) => value > 0,
    })
  },
  boolean() {
    return new ColumnType<boolean>({
      isValid: (value) => typeof value === "boolean",
      serializer: Struct.boolean,
    })
  },
  string() {
    return new ColumnType<string>({
      isValid: (value) => typeof value === "string",
      serializer: Struct.unicodeStringStruct,
    })
  },
  float() {
    return new ColumnType<number>({
      isValid: (value) => typeof value === "number",
      serializer: Struct.float64,
    })
  },
  int32() {
    return new ColumnType<number>({
      isValid: (value) => typeof value === "number" && Number.isInteger(value),
      serializer: Struct.int32,
    })
  },
  uint32() {
    return new ColumnType<number>({
      isValid: (value) =>
        typeof value === "number" && Number.isInteger(value) && value >= 0,
      serializer: Struct.uint32,
    })
  },
  int64() {
    return new ColumnType<bigint>({
      isValid: (value) => typeof value === "bigint",
      serializer: Struct.bigInt64,
    })
  },
  uint64() {
    return new ColumnType<bigint>({
      isValid: (value) => typeof value === "bigint" && value >= 0,
      serializer: Struct.bigUint64,
    })
  },
}

export class ColumnType<T> {
  isValid: (value: T) => boolean
  isEqual: EqualityChecker<T>
  compare: Comparator<T>
  serializer?: IStruct<T>

  constructor({
    isValid,
    equals = (a: T, b: T) => a === b,
    compare = (a: T, b: T) => (a > b ? 1 : a < b ? -1 : 0),
    serializer,
  }: {
    isValid: (value: T) => boolean
    equals?: EqualityChecker<T>
    compare?: Comparator<T>
    serializer?: IStruct<T>
  }) {
    this.isValid = isValid
    this.isEqual = equals
    this.compare = compare
    this.serializer = serializer
  }
}
