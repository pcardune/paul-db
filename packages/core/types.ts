import type { Simplify } from "type-fest"
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

/**
 * Remove all symbol keys from an object. These show up when
 * doing complicated things with the query builder due to the use of
 * type-fest's EmptyObject type.
 */
export type Clean<T> = Simplify<
  {
    [K in keyof T as K extends symbol ? never : K]: T[K]
  }
>
