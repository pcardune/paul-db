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
