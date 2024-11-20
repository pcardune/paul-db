import { Comparator, EqualityChecker } from "../types.ts"

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
}

export class ColumnType<T> {
  isValid: (value: T) => boolean
  isEqual: EqualityChecker<T>
  compare: Comparator<T>
  constructor({
    isValid,
    equals = (a: T, b: T) => a === b,
    compare = (a: T, b: T) => (a > b ? 1 : a < b ? -1 : 0),
  }: {
    isValid: (value: T) => boolean
    equals?: EqualityChecker<T>
    compare?: Comparator<T>
  }) {
    this.isValid = isValid
    this.isEqual = equals
    this.compare = compare
  }
}
