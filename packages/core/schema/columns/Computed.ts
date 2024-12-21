/**
 * Type definitions for computed columns
 *
 * @module
 */
import type { ColumnType } from "./ColumnType.ts"
import * as Index from "./IndexConfig.ts"

/**
 * Represents any computed column
 */
export type Any<
  Name extends string = string,
  UniqueT extends boolean = boolean,
  IndexedT extends Index.Config = Index.Config,
  InputT = any,
  OutputT = any,
> = {
  kind: "computed"
  name: Name
  type: ColumnType<OutputT>
  isUnique: UniqueT
  indexed: IndexedT
  compute: (input: InputT) => OutputT
}

/**
 * Converts a computed column type to a nullable type
 */
export type MakeNullable<C extends Any> = C extends Any<
  infer Name,
  infer Unique,
  infer Indexed,
  infer Input,
  infer Output
> ? Any<Name, Unique, Indexed, Input | null, Output | null>
  : never

/**
 * Infer the output type of a computed column
 */
export type GetOutput<C> = C extends
  Any<string, boolean, Index.Config, any, infer O> ? O
  : never

/**
 * Infer the input type of a computed column
 */
export type GetInput<C> = C extends Any<string, boolean, Index.Config, infer I>
  ? I
  : never
