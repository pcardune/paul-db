import { Comparator, EqualityChecker } from "../types.ts"
import {
  booleanSerializer,
  floatSerializer,
  int32Serializer,
  int64Serializer,
  Serializer,
  stringSerializer,
  uint32Serializer,
  uint64Serializer,
} from "./Serializers.ts"

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
  positiveNumber() {
    return new ColumnType<number>({
      isValid: (value) => value > 0,
    })
  },
  boolean() {
    return new ColumnType<boolean>({
      isValid: (value) => typeof value === "boolean",
      serializer: booleanSerializer,
    })
  },
  string() {
    return new ColumnType<string>({
      isValid: (value) => typeof value === "string",
      serializer: stringSerializer,
    })
  },
  float() {
    return new ColumnType<number>({
      isValid: (value) => typeof value === "number",
      serializer: floatSerializer,
    })
  },
  int32() {
    return new ColumnType<number>({
      isValid: (value) => typeof value === "number" && Number.isInteger(value),
      serializer: int32Serializer,
    })
  },
  uint32() {
    return new ColumnType<number>({
      isValid: (value) =>
        typeof value === "number" && Number.isInteger(value) && value >= 0,
      serializer: uint32Serializer,
    })
  },
  int64() {
    return new ColumnType<bigint>({
      isValid: (value) => typeof value === "bigint",
      serializer: int64Serializer,
    })
  },
  uint64() {
    return new ColumnType<bigint>({
      isValid: (value) => typeof value === "bigint" && value >= 0,
      serializer: uint64Serializer,
    })
  },
}

export class ColumnType<T> {
  isValid: (value: T) => boolean
  isEqual: EqualityChecker<T>
  compare: Comparator<T>
  serializer?: Serializer<T>
  constructor({
    isValid,
    equals = (a: T, b: T) => a === b,
    compare = (a: T, b: T) => (a > b ? 1 : a < b ? -1 : 0),
    serializer,
  }: {
    isValid: (value: T) => boolean
    equals?: EqualityChecker<T>
    compare?: Comparator<T>
    serializer?: Serializer<T>
  }) {
    this.isValid = isValid
    this.isEqual = equals
    this.compare = compare
    this.serializer = serializer
  }
}
