// deno-lint-ignore-file no-namespace
import { FilterTuple } from "../typetools.ts"
import { ColumnType } from "./ColumnType.ts"
import { OverrideProperties } from "npm:type-fest"

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

export namespace Column {
  export namespace Computed {
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

    export type GetOutput<C> = C extends
      Column.Computed.Any<string, boolean, Index.Config, any, infer O> ? O
      : never

    export type GetInput<C> = C extends
      Column.Computed.Any<string, boolean, Index.Config, infer I> ? I : never
  }

  export type Stored<
    Name extends string = string,
    ValueT = any,
    UniqueT extends boolean = boolean,
    IndexedT extends Index.Config = Index.Config,
    DefaultValueFactoryT extends DefaultValueConfig<ValueT> =
      DefaultValueConfig<
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

  export type Any = Stored | Computed.Any

  export type FindWithName<CS extends Any[], Name extends string> = FilterTuple<
    CS,
    { name: Name }
  >

  export type FindUnique<CS extends Any[]> = FilterTuple<CS, { isUnique: true }>
  export type FindIndexed<CS extends Any[]> = Exclude<
    CS[number],
    { indexed: { shouldIndex: false } }
  >

  export type WithDefaultValue<ValueT = any> = OverrideProperties<
    Stored,
    { defaultValueFactory: () => ValueT }
  >

  export type GetValue<C> = C extends Stored<string, infer V> ? V : never
}

class ColumnBuilder<
  Name extends string = string,
  ValueT = any,
  UniqueT extends boolean = boolean,
  IndexedT extends Index.Config = Index.Config,
  DefaultValueFactoryT extends DefaultValueConfig<ValueT> = DefaultValueConfig<
    ValueT
  >,
> {
  readonly kind: "stored" = "stored"
  constructor(
    readonly name: Name,
    readonly type: ColumnType<ValueT>,
    readonly isUnique: UniqueT,
    readonly indexed: IndexedT,
    readonly defaultValueFactory: DefaultValueFactoryT,
  ) {}

  named<NewName extends string>(name: NewName) {
    return new ColumnBuilder<
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
      this.defaultValueFactory,
    )
  }

  unique(
    indexConfig: Index.Config = DEFAULT_INDEX_CONFIG,
  ): ColumnBuilder<Name, ValueT, true, Index.Config, DefaultValueFactoryT> {
    return new ColumnBuilder(
      this.name,
      this.type,
      true,
      indexConfig,
      this.defaultValueFactory,
    )
  }

  index(): ColumnBuilder<
    Name,
    ValueT,
    UniqueT,
    Index.Config,
    DefaultValueFactoryT
  > {
    return new ColumnBuilder(
      this.name,
      this.type,
      this.isUnique,
      DEFAULT_INDEX_CONFIG,
      this.defaultValueFactory,
    )
  }

  defaultTo(
    defaultValueFactory: () => ValueT,
  ): ColumnBuilder<Name, ValueT, UniqueT, IndexedT, () => ValueT> {
    return new ColumnBuilder(
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
): ColumnBuilder<Name, ValueT, false, Index.ShouldNotIndex, undefined> {
  return new ColumnBuilder(name, type, false, { shouldIndex: false }, undefined)
}

class ComputedColumnBuilder<
  Name extends string = string,
  UniqueT extends boolean = boolean,
  IndexedT extends Index.Config = Index.Config,
  InputT = any,
  OutputT = any,
> {
  readonly kind: "computed" = "computed"
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
  ): ComputedColumnBuilder<Name, true, Index.Config, InputT, OutputT> {
    return new ComputedColumnBuilder(
      this.name,
      this.type,
      true,
      indexConfig,
      this.compute,
    )
  }

  index(
    indexConfig = DEFAULT_INDEX_CONFIG,
  ): ComputedColumnBuilder<Name, UniqueT, Index.Config, InputT, OutputT> {
    return new ComputedColumnBuilder(
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
  return new ComputedColumnBuilder(
    name,
    type,
    false,
    { shouldIndex: false },
    compute,
  )
}

export type StoredRecordForColumnSchemas<CS extends Column.Stored[]> = {
  [K in CS[number]["name"]]: Column.GetValue<Column.FindWithName<CS, K>>
}
