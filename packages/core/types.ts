import {
  type Merge,
  type Simplify,
  type StringKeyOf,
  type UnionToTuple,
} from "type-fest"
export {
  type JsonObject as JsonRecord,
  type JsonPrimitive,
  type JsonValue as Json,
} from "type-fest"
export type Comparator<V> = (a: V, b: V) => number
export type EqualityChecker<V> = (a: V, b: V) => boolean
export type Range<K> = { gte?: K; gt?: K; lte?: K; lt?: K }

export function assertUnreachable(_x: never): never {
  throw new Error("Unreachable code reached")
}

type RenameKeys<
  T extends Record<string, Record<string, any>>,
  Separator extends string,
> = Simplify<
  {
    [K in StringKeyOf<T>]: {
      [K2 in StringKeyOf<T[K]> as `${K}${Separator}${K2}`]: T[K][K2]
    }
  }
>

type MergeRecords<T extends any[]> = Simplify<
  T extends [infer A, infer B, ...infer Rest]
    ? Merge<A, MergeRecords<[B, ...Rest]>>
    : T extends [infer A] ? A
    : never
>

/**
 * Flattens a nested object into a single level object, by concatenating the
 * keys with an underscore.
 *
 * For example:
 * ```ts
 * import {assertTrue} from "./testing.ts"
 * import {IsEqual} from "type-fest"
 * type SomeType = {
 *   one: {a: number, b: string}
 *   two: {c: boolean}
 * }
 *
 * type SomeTypeFlattened = Flattened<SomeType>
 *
 * assertTrue<IsEqual<SomeTypeFlattened, {
 *   one_a: number
 *   one_b: string
 *   two_c: boolean
 * }>>()
 * ```
 */
export type Flattened<
  T extends Record<string, Record<string, any>>,
  Separator extends string = "_",
> = Simplify<
  MergeRecords<
    UnionToTuple<RenameKeys<T, Separator>[keyof RenameKeys<T, Separator>]>
  >
>
