export type Comparator<V> = (a: V, b: V) => number
export type EqualityChecker<V> = (a: V, b: V) => boolean
export type Range<K> = { gte?: K; gt?: K; lte?: K; lt?: K }
