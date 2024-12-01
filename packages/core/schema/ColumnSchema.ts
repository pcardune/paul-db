import { ColumnType } from "./ColumnType.ts"

// deno-lint-ignore no-namespace
export namespace Index {
  export type ShouldIndex = {
    shouldIndex: true
    order?: number
  }

  export type ShouldNotIndex = {
    shouldIndex: false
  }

  export type Config = Index.ShouldIndex | ShouldNotIndex
}

export const DEFAULT_INDEX_CONFIG: Index.ShouldIndex = {
  shouldIndex: true,
  order: 2,
}

type DefaultValueConfig<ValueT = unknown> = (() => ValueT) | undefined

export class ColumnSchema<
  Name extends string = string,
  ValueT = any,
  UniqueT extends boolean = boolean,
  IndexedT extends Index.Config = Index.Config,
  DefaultValueFactoryT extends DefaultValueConfig<ValueT> = DefaultValueConfig<
    ValueT
  >,
> {
  constructor(
    readonly name: Name,
    readonly type: ColumnType<ValueT>,
    readonly isUnique: UniqueT,
    readonly indexed: IndexedT,
    readonly defaultValueFactory?: DefaultValueFactoryT,
  ) {}

  named<NewName extends string>(name: NewName) {
    return new ColumnSchema<
      NewName,
      ValueT,
      UniqueT,
      IndexedT,
      DefaultValueFactoryT
    >(
      name,
      this.type,
      this.isUnique,
      this.indexed,
    )
  }

  unique(
    indexConfig: Index.Config = DEFAULT_INDEX_CONFIG,
  ): ColumnSchema<Name, ValueT, true, Index.Config, DefaultValueFactoryT> {
    return new ColumnSchema(
      this.name,
      this.type,
      true,
      indexConfig,
      this.defaultValueFactory,
    )
  }

  index(): ColumnSchema<
    Name,
    ValueT,
    UniqueT,
    Index.Config,
    DefaultValueFactoryT
  > {
    return new ColumnSchema(
      this.name,
      this.type,
      this.isUnique,
      DEFAULT_INDEX_CONFIG,
      this.defaultValueFactory,
    )
  }

  defaultTo(
    defaultValueFactory: () => ValueT,
  ): ColumnSchema<Name, ValueT, UniqueT, IndexedT, () => ValueT> {
    return new ColumnSchema(
      this.name,
      this.type,
      this.isUnique,
      this.indexed,
      defaultValueFactory,
    )
  }
}

export function column<Name extends string, ValueT>(
  name: Name,
  type: ColumnType<ValueT>,
): ColumnSchema<Name, ValueT, false, Index.ShouldNotIndex, undefined> {
  return new ColumnSchema(name, type, false, { shouldIndex: false })
}

export class ComputedColumnSchema<
  Name extends string = string,
  UniqueT extends boolean = boolean,
  IndexedT extends Index.Config = Index.Config,
  InputT = any,
  OutputT = any,
> {
  constructor(
    readonly name: Name,
    readonly type: ColumnType<OutputT>,
    readonly isUnique: UniqueT,
    readonly indexed: IndexedT,
    readonly compute: (input: InputT) => OutputT,
  ) {
  }

  unique(
    indexConfig = DEFAULT_INDEX_CONFIG,
  ): ComputedColumnSchema<Name, true, Index.Config, InputT, OutputT> {
    return new ComputedColumnSchema(
      this.name,
      this.type,
      true,
      indexConfig,
      this.compute,
    )
  }

  index(
    indexConfig = DEFAULT_INDEX_CONFIG,
  ): ComputedColumnSchema<Name, UniqueT, Index.Config, InputT, OutputT> {
    return new ComputedColumnSchema(
      this.name,
      this.type,
      this.isUnique,
      indexConfig,
      this.compute,
    )
  }
}

export function computedColumn<
  Name extends string,
  InputT,
  OutputT,
>(
  name: Name,
  type: ColumnType<OutputT>,
  compute: (input: InputT) => OutputT,
) {
  return new ComputedColumnSchema(
    name,
    type,
    false,
    { shouldIndex: false },
    compute,
  )
}

export type ValueForColumnSchema<C> = C extends ColumnSchema<string, infer V>
  ? V
  : never

export type InputForComputedColumnSchema<C> = C extends
  ComputedColumnSchema<string, boolean, Index.Config, infer I> ? I : never
export type OutputForComputedColumnSchema<C> = C extends
  ComputedColumnSchema<string, boolean, Index.Config, any, infer O> ? O
  : never

export type SomeColumnSchema = ColumnSchema
export type IndexedColumnSchema = ColumnSchema<
  string,
  any,
  boolean,
  Exclude<Index.Config, false>
>

export type ColumnSchemaWithDefaultValue = ColumnSchema<
  string,
  any,
  boolean,
  Index.Config,
  () => any
>

export type SomeComputedColumnSchema = ComputedColumnSchema<
  string,
  boolean,
  Index.Config,
  any,
  any
>

export type RecordForColumnSchema<
  CS extends SomeColumnSchema | SomeComputedColumnSchema,
  DefaultOptional extends boolean = false,
> = CS extends ComputedColumnSchema<string, boolean, Index.Config, any, any> ? {
    [K in CS["name"]]?: never
  }
  : DefaultOptional extends true ? CS extends ColumnSchemaWithDefaultValue ? {
        [K in CS["name"]]?: ValueForColumnSchema<CS>
      }
    : {
      [K in CS["name"]]: ValueForColumnSchema<CS>
    }
  : {
    [K in CS["name"]]: ValueForColumnSchema<CS>
  }

export type StoredRecordForColumnSchemas<
  CS extends (SomeColumnSchema)[],
> = {
  [K in CS[number]["name"]]: ValueForColumnSchema<
    Extract<CS[number], { name: K }>
  >
}
