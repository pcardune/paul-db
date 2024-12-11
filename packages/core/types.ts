export type JsonPrimitive = string | number | boolean | null
export type Json = JsonPrimitive | { [key: string]: Json } | Json[]
export type JsonRecord = { [key: string]: Json }
export type Comparator<V> = (a: V, b: V) => number
export type EqualityChecker<V> = (a: V, b: V) => boolean
export type Range<K> = { gte?: K; gt?: K; lte?: K; lt?: K }

export function assertUnreachable(_x: never): never {
  throw new Error("Unreachable code reached")
}
