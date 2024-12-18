/**
 * Types for columns in a schema
 * @module
 */

import * as Stored from "./Stored.ts"
import * as Computed from "./Computed.ts"
import * as Index from "./IndexConfig.ts"
import type { ConditionalKeys, ConditionalPick } from "type-fest"

export * as Index from "./IndexConfig.ts"
export * as Computed from "./Computed.ts"

export * as Stored from "./Stored.ts"

/**
 * Represents all stored and computed columns
 */
export type Any = Stored.Any | Computed.Any

/**
 * Find all unique columns from a tuple of columns
 */
export type FindUnique<T> = ConditionalPick<T, { isUnique: true }>
export type FindUniqueNames<T> = ConditionalKeys<T, { isUnique: true }>

/**
 * Find all indexed columns from a tuple of columns
 */
export type FindIndexed<T> = ConditionalPick<T, { indexed: Index.ShouldIndex }>
export type FindIndexedNames<T> = ConditionalKeys<
  T,
  { indexed: Index.ShouldIndex }
>

/**
 * Get the value type of a column. If the column is computed, this will be the
 * output type of the computation. If the column is stored, this will be the
 * value type of the column.
 */
export type GetOutput<C extends Any> = C extends Computed.Any
  ? Computed.GetOutput<C>
  : Stored.GetValue<C>

export type GetInput<C extends Any> = C extends Computed.Any
  ? Computed.GetInput<C>
  : Stored.GetValue<C>

/**
 * Gets the record you would need if you wanted it to contain this column.
 */
export type GetRecordContainingColumn<C extends Any> = C extends Computed.Any
  ? Computed.GetInput<C>
  : {
    [Property in C["name"]]: Stored.GetValue<C>
  }
