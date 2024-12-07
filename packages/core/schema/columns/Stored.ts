import { OverrideProperties } from "npm:type-fest"
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
  UniqueT extends boolean = boolean,
  IndexedT extends Index.Config = Index.Config,
  DefaultValueFactoryT extends DefaultValueConfig<ValueT> = DefaultValueConfig<
    ValueT
  >,
> = {
  kind: "stored"
  name: Name
  type: ColumnType<ValueT>
  isUnique: UniqueT
  indexed: IndexedT
  defaultValueFactory: DefaultValueFactoryT
}

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
