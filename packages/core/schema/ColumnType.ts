import { IStruct, Struct, VariableWidthStruct } from "../binary/Struct.ts"
import { Comparator, EqualityChecker, Json } from "../types.ts"
import * as uuid from "jsr:@std/uuid"

export function getColumnTypeFromString(type: string): ColumnType<any> {
  const t = type.toLowerCase()
  if (t in ColumnTypes) {
    return (ColumnTypes as Record<string, () => ColumnType<any>>)[t]()
  }
  throw new Error(`Unknown type: ${type}`)
}

export function getColumnTypeFromSQLType(sqlType: string): ColumnType<any> {
  switch (sqlType) {
    case "TEXT":
    case "VARCHAR":
    case "CHAR":
      return ColumnTypes.string()
    case "SMALLINT":
      return ColumnTypes.int16()
    case "INT":
    case "INTEGER":
      return ColumnTypes.int32()
    case "FLOAT":
    case "REAL":
    case "DOUBLE":
      return ColumnTypes.float()
    case "BOOLEAN":
      return ColumnTypes.boolean()
    case "UUID":
      return ColumnTypes.uuid()
    case "JSON":
    case "JSONB":
      return ColumnTypes.json()
    case "DATE":
      return ColumnTypes.date()
    case "TIMESTAMP":
      return ColumnTypes.timestamp()
    default:
      throw new Error(`Unknown SQL type: ${sqlType}`)
  }
}

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
  int16() {
    return new ColumnType<number>({
      name: "int16",
      isValid: (value) =>
        typeof value === "number" && Number.isInteger(value) &&
        value >= -(2 ** 15 - 1) && value <= 2 ** 15 - 1,
      serializer: Struct.int16,
    })
  },
  uint16() {
    return new ColumnType<number>({
      name: "uint16",
      isValid: (value) =>
        typeof value === "number" && Number.isInteger(value) && value >= 0 &&
        value <= 2 ** 16 - 1,
      serializer: Struct.uint16,
    })
  },
  int32() {
    return new ColumnType<number>({
      name: "int32",
      isValid: (value) =>
        typeof value === "number" && Number.isInteger(value) &&
        value >= -(2 ** 31 - 1) && value <= 2 ** 31 - 1,
      serializer: Struct.int32,
    })
  },
  uint32() {
    return new ColumnType<number>({
      name: "uint32",
      isValid: (value) =>
        typeof value === "number" && Number.isInteger(value) && value >= 0 &&
        value <= 2 ** 32 - 1,
      serializer: Struct.uint32,
    })
  },
  int64() {
    return new ColumnType<bigint>({
      name: "int64",
      isValid: (value) =>
        typeof value === "bigint" && value >= -(2n ** 63n - 1n) &&
        value <= 2n ** 63n - 1n,
      serializer: Struct.bigInt64,
    })
  },
  uint64() {
    return new ColumnType<bigint>({
      name: "uint64",
      isValid: (value) =>
        typeof value === "bigint" && value >= 0 && value <= 2n ** 64n - 1n,
      serializer: Struct.bigUint64,
    })
  },
  json() {
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
  },
  timestamp() {
    return new ColumnType<Date>({
      name: "timestamp",
      isValid: (value) => value instanceof Date,
      serializer: new VariableWidthStruct({
        sizeof: () => 8,
        read: (view) => new Date(Struct.unicodeStringStruct.readAt(view, 0)),
        write: (value, view) =>
          Struct.unicodeStringStruct.writeAt(value.toISOString(), view, 0),
      }),
    })
  },
  date() {
    return new ColumnType<Date>({
      name: "date",
      isValid: (value) => value instanceof Date,
      serializer: Struct.date,
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
