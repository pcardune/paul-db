// deno-lint-ignore-file no-explicit-any

export type PushTuple<T extends any[], V> = [...T, V]
export type FilterTuple<T extends any[], U> = Extract<T[number], U>
