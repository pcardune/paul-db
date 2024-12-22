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
 * Represents a column type in a database table
 */
export class ColumnType<T = unknown> {
  /**
   * The name of the column type
   */
  readonly name: string

  /**
   * Checks if a value is valid for this column type
   */
  isValid: (value: T) => boolean

  /**
   * Checks if two values are equal
   */
  isEqual: EqualityChecker<T>

  /**
   * Compares two values
   */
  compare: Comparator<T>

  /**
   * When doing aggregation queries, the minimum value is used to
   * initialize the aggregation
   */
  minValue?: T

  /**
   * @ignore
   */
  serializer?: IStruct<T>

  /**
   * Constructs a new column type
   *
   * @ignore
   */
  constructor({
    name,
    isValid,
    minValue,
    equals = (a: T, b: T) => a === b,
    compare = (a: T, b: T) => (a > b ? 1 : a < b ? -1 : 0),
    serializer,
  }: {
    name: string
    isValid: (value: T) => boolean
    minValue?: T
    equals?: EqualityChecker<T>
    compare?: Comparator<T>
    serializer?: IStruct<T>
  }) {
    this.name = name
    this.isValid = isValid
    this.minValue = minValue
    this.isEqual = equals
    this.compare = compare
    this.serializer = serializer
  }

  /**
   * Takes a ColumnType and returns a new ColumnType that allows arrays of values
   */
  array(): ArrayColumnType<T> {
    return new ArrayColumnType<T>(this)
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

export class ArrayColumnType<T> extends ColumnType<T[]> {
  constructor(readonly type: ColumnType<T>) {
    super({
      name: type.name + "[]",
      isValid: (value) => Array.isArray(value) && value.every(type.isValid),
      equals: (a, b) =>
        a.length === b.length && a.every((v, i) => type.isEqual(v, b[i])),
      compare: (a, b) => {
        for (let i = 0; i < Math.min(a.length, b.length); i++) {
          const cmp = type.compare(a[i], b[i])
          if (cmp !== 0) return cmp
        }
        return a.length - b.length
      },
      serializer: type.serializer?.array(),
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

export type ColValueOf<T extends ColumnType<any>> = T extends
  ColumnType<infer V> ? V
  : never
