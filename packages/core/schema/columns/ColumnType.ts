import { IStruct, Struct } from "../../binary/Struct.ts"
import { Comparator, EqualityChecker } from "../../types.ts"
import * as ColumnTypes from "./types.ts"

export { ColumnTypes }

/**
 * Parses a string into a ColumnType instance
 *
 * @throws Error if the type is unknown
 */
export function getColumnTypeFromString(
  type: `${string}[]`,
): ColumnType<unknown[]>
export function getColumnTypeFromString(
  type: `${string}?`,
): ColumnType<unknown | null>
export function getColumnTypeFromString(type: string): ColumnType<unknown>
export function getColumnTypeFromString(
  type: `${string}[]` | string,
): ColumnType<unknown> | ColumnType<unknown[]> {
  const t = type.toLowerCase()

  if (t.endsWith("?")) {
    return getColumnTypeFromString(t.slice(0, -1)).nullable()
  }
  if (t.endsWith("[]")) {
    return getColumnTypeFromString(t.slice(0, -2)).array()
  }

  if (t in ColumnTypes) {
    return (ColumnTypes as Record<string, () => ColumnType<unknown>>)[t]()
  }
  if (t === "serial") {
    return new SerialUInt32ColumnType() as ColumnType<unknown>
  }
  throw new Error(`Unknown type: ${type}`)
}

/**
 * Converts a SQL type to a ColumnType instance
 */
export function getColumnTypeFromSQLType(sqlType: string): ColumnType<any> {
  if (sqlType.endsWith("[]")) {
    return getColumnTypeFromSQLType(sqlType.slice(0, -2)).array()
  }
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
    case "BLOB":
      return ColumnTypes.blob()
    case "SERIAL":
      return new SerialUInt32ColumnType()
    default:
      throw new Error(`Unknown SQL type: ${sqlType}`)
  }
}

/**
 * Represents a column type in a database table
 */
export class ColumnType<T = unknown> {
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

  /**
   * Takes a ColumnType and returns a new ColumnType that allows arrays of values
   */
  array(): ColumnType<T[]> {
    return new ColumnType<T[]>({
      name: this.name + "[]",
      isValid: (value) => Array.isArray(value) && value.every(this.isValid),
      equals: (a, b) =>
        a.length === b.length && a.every((v, i) => this.isEqual(v, b[i])),
      compare: (a, b) => {
        for (let i = 0; i < Math.min(a.length, b.length); i++) {
          const cmp = this.compare(a[i], b[i])
          if (cmp !== 0) return cmp
        }
        return a.length - b.length
      },
      serializer: this.serializer?.array(),
    })
  }

  /**
   * Takes a ColumnType and returns a new ColumnType that allows null values
   */
  nullable(): ColumnType<T | null> {
    return new ColumnType<T | null>({
      name: this.name + "?",
      isValid: (value) => value === null || this.isValid(value),
      equals: (a, b) =>
        a === b || (a !== null && b !== null && this.isEqual(a, b)),
      compare: (a, b) => {
        if (a === b) return 0
        if (a === null) return -1
        if (b === null) return 1
        return this.compare(a, b)
      },
      serializer: this.serializer?.nullable(),
    })
  }
}

export class SerialUInt32ColumnType extends ColumnType<number> {
  constructor() {
    super({
      name: "serial",
      isValid: (value) => value >= 0,
      serializer: Struct.uint32,
    })
  }
}
