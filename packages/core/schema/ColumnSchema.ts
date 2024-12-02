// deno-lint-ignore-file no-namespace
import { FilterTuple } from "../typetools.ts"
import { ColumnType, SerialUInt32ColumnType } from "./ColumnType.ts"
import { OverrideProperties, Simplify } from "npm:type-fest"

export namespace Index {
  export type ShouldIndex = {
    shouldIndex: true
    order?: number
    inMemory?: boolean
  }

  export type ShouldNotIndex = {
    shouldIndex: false
  }

  export type Config = Index.ShouldIndex | ShouldNotIndex
}

export const DEFAULT_INDEX_CONFIG: Index.ShouldIndex = {
  shouldIndex: true,
  order: 2,
  inMemory: false,
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

export class ColumnBuilder<
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

  finalize(): Column.Stored<
    Name,
    ValueT,
    UniqueT,
    IndexedT,
    DefaultValueFactoryT
  > {
    return {
      kind: "stored",
      name: this.name,
      type: this.type,
      isUnique: this.isUnique,
      indexed: this.indexed,
      defaultValueFactory: this.defaultValueFactory,
    }
  }

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
    indexConfig: Partial<Index.Config> = {},
  ): ColumnBuilder<Name, ValueT, true, Index.Config, DefaultValueFactoryT> {
    return new ColumnBuilder(
      this.name,
      this.type,
      true,
      { ...DEFAULT_INDEX_CONFIG, ...indexConfig },
      this.defaultValueFactory,
    )
  }

  index(indexConfig: Partial<Index.Config> = {}): ColumnBuilder<
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
      { ...DEFAULT_INDEX_CONFIG, ...indexConfig },
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
function column<Name extends string, ValueT>(
  name: Name,
  type: "serial",
): ColumnBuilder<Name, number, true, Index.ShouldIndex, () => number>
function column<Name extends string, ValueT>(
  name: Name,
  type: ColumnType<ValueT>,
): ColumnBuilder<Name, ValueT, false, Index.ShouldNotIndex, undefined>
function column<Name extends string>(
  name: Name,
  type: ColumnType<any> | "serial",
): ColumnBuilder<
  Name,
  any,
  boolean,
  Index.Config,
  DefaultValueConfig<any>
> {
  if (type === "serial") {
    return new ColumnBuilder(
      name,
      new SerialUInt32ColumnType(),
      true,
      DEFAULT_INDEX_CONFIG,
      () => -1, // This will be overridden by the database
    )
  }
  return new ColumnBuilder(name, type, false, { shouldIndex: false }, undefined)
}

export { column }

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
    indexConfig: Partial<Index.ShouldIndex> = {},
  ): ComputedColumnBuilder<Name, true, Index.Config, InputT, OutputT> {
    return new ComputedColumnBuilder(
      this.name,
      this.type,
      true,
      { ...DEFAULT_INDEX_CONFIG, ...indexConfig },
      this.compute,
    )
  }

  index(
    indexConfig: Partial<Index.ShouldIndex> = {},
  ): ComputedColumnBuilder<Name, UniqueT, Index.Config, InputT, OutputT> {
    return new ComputedColumnBuilder(
      this.name,
      this.type,
      this.isUnique,
      { ...DEFAULT_INDEX_CONFIG, ...indexConfig },
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

export type StoredRecordForColumnSchemas<CS extends Column.Stored[]> = Simplify<
  {
    [K in CS[number]["name"]]: Column.GetValue<Column.FindWithName<CS, K>>
  }
>
