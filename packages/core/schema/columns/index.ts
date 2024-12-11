import * as Stored from "./Stored.ts"
import * as Computed from "./Computed.ts"
import { FilterTuple } from "../../typetools.ts"

export * as Index from "./IndexConfig.ts"
export * as Computed from "./Computed.ts"
export * as Stored from "./Stored.ts"

/**
 * Represents all stored and computed columns
 */
export type Any = Stored.Any | Computed.Any

/**
 * Find a column with a given name from any tuple of columns
 */
export type FindWithName<CS extends Any[], Name extends string> = FilterTuple<
  CS,
  { name: Name }
>

/**
 * Find all unique columns from a tuple of columns
 */
export type FindUnique<CS extends Any[]> = FilterTuple<CS, { isUnique: true }>

/**
 * Find all indexed columns from a tuple of columns
 */
export type FindIndexed<CS extends Any[]> = Exclude<
  CS[number],
  { indexed: { shouldIndex: false } }
>

/**
 * Get the value type of a column. If the column is computed, this will be the
 * output type of the computation. If the column is stored, this will be the
 * value type of the column.
 */
export type GetOutput<C extends Any> = C extends Computed.Any
  ? Computed.GetOutput<C>
  : Stored.GetValue<C>

/**
 * Gets the record you would need if you wanted it to contain this column.
 */
export type GetRecordContainingColumn<C extends Any> = C extends Computed.Any
  ? Computed.GetInput<C>
  : {
    [Property in C["name"]]: Stored.GetValue<C>
  }
