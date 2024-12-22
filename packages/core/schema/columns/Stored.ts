/**
 * Types for columns that are stored in the database
 * @module
 */

import type { OverrideProperties } from "type-fest"
import * as Index from "./IndexConfig.ts"
import type { ColumnType } from "./ColumnType.ts"

/**
 * The configuration for a default value of a stored column
 */
export type DefaultValueConfig<ValueT = unknown> = (() => ValueT) | undefined

/**
 * Represents any stored column
 */
export type Any<
  Name extends string = string,
  ValueT = any,
  ColT extends ColumnType<ValueT> = ColumnType<ValueT>,
  UniqueT extends boolean = boolean,
  IndexedT extends Index.Config = Index.Config,
  DefaultValueFactoryT extends DefaultValueConfig<ValueT> = DefaultValueConfig<
    ValueT
  >,
> = {
  kind: "stored"
  name: Name
  type: ColT
  isUnique: UniqueT
  indexed: IndexedT
  defaultValueFactory: DefaultValueFactoryT
}

/**
 * @ignore
 */
export type Simple<Name extends string = string, ValueT = any> = Any<
  Name,
  ValueT,
  ColumnType<ValueT>,
  false,
  Index.ShouldNotIndex,
  undefined
>

/**
 * Converts a stored column type to a nullable stored column type
 */
export type MakeNullable<C extends Any> = C extends Any<
  infer Name,
  infer Value,
  infer ColT,
  infer Unique,
  infer Indexed,
  infer DefaultValueFactory
> ? Any<
    Name,
    Value | null,
    ColumnType<Value | null>,
    Unique,
    Indexed,
    DefaultValueFactory
  >
  : never

/**
 * Represents any stored column with a default value
 */
export type WithDefaultValue<ValueT = unknown> = OverrideProperties<
  Any,
  { defaultValueFactory: () => ValueT }
>

/**
 * Infer the value type of a stored column
 */
export type GetValue<C> = C extends Any<string, infer V> ? V : never
