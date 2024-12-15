import { ColumnType, SerialUInt32ColumnType } from "./ColumnType.ts"
import type { Simplify } from "type-fest"
import * as Column from "./index.ts"

export const DEFAULT_INDEX_CONFIG: Column.Index.ShouldIndex = {
  shouldIndex: true,
  order: 2,
  inMemory: false,
}

export class ColumnBuilder<
  Name extends string = string,
  ValueT = unknown,
  UniqueT extends boolean = boolean,
  IndexedT extends Column.Index.Config = Column.Index.Config,
  DefaultValueFactoryT extends Column.Stored.DefaultValueConfig<ValueT> =
    Column.Stored.DefaultValueConfig<
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

  finalize(): Column.Stored.Any<
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

  named<NewName extends string>(name: NewName): ColumnBuilder<
    NewName,
    ValueT,
    UniqueT,
    IndexedT,
    DefaultValueFactoryT
  > {
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
    indexConfig: Partial<Column.Index.Config> = {},
  ): ColumnBuilder<
    Name,
    ValueT,
    true,
    Column.Index.Config,
    DefaultValueFactoryT
  > {
    return new ColumnBuilder(
      this.name,
      this.type,
      true,
      { ...DEFAULT_INDEX_CONFIG, ...indexConfig },
      this.defaultValueFactory,
    )
  }

  index(indexConfig: Partial<Column.Index.Config> = {}): ColumnBuilder<
    Name,
    ValueT,
    UniqueT,
    Column.Index.Config,
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

/**
 * Creates a unique, indexed uint32 column that auto-increments.
 * @param name name of the column
 * @param type "serial" to indicate a serial column
 */
export function column<Name extends string, ValueT>(
  name: Name,
  type: "serial",
): ColumnBuilder<Name, number, true, Column.Index.ShouldIndex, () => number>
/**
 * Creates a column with the given name and type.
 * @param name name of the column
 * @param type type of the column
 */
export function column<Name extends string, ValueT>(
  name: Name,
  type: ColumnType<ValueT>,
): ColumnBuilder<Name, ValueT, false, Column.Index.ShouldNotIndex, undefined>
/**
 * Creates a column with the given name and type.
 * @param name name of the column
 * @param type type of the column
 */
export function column<Name extends string>(
  name: Name,
  type: ColumnType | "serial",
): ColumnBuilder<
  Name,
  any,
  boolean,
  Column.Index.Config,
  Column.Stored.DefaultValueConfig<unknown>
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

class ComputedColumnBuilder<
  Name extends string = string,
  UniqueT extends boolean = boolean,
  IndexedT extends Column.Index.Config = Column.Index.Config,
  InputT = unknown,
  OutputT = unknown,
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
    indexConfig: Partial<Column.Index.ShouldIndex> = {},
  ): ComputedColumnBuilder<Name, true, Column.Index.Config, InputT, OutputT> {
    return new ComputedColumnBuilder(
      this.name,
      this.type,
      true,
      { ...DEFAULT_INDEX_CONFIG, ...indexConfig },
      this.compute,
    )
  }

  index(
    indexConfig: Partial<Column.Index.ShouldIndex> = {},
  ): ComputedColumnBuilder<
    Name,
    UniqueT,
    Column.Index.Config,
    InputT,
    OutputT
  > {
    return new ComputedColumnBuilder(
      this.name,
      this.type,
      this.isUnique,
      { ...DEFAULT_INDEX_CONFIG, ...indexConfig },
      this.compute,
    )
  }
}

/**
 * Creates a computed column.
 * @param name name of the column
 * @param type type of the column (after it's computed)
 * @param compute function that computes the column's value from other data
 */
export function computedColumn<
  Name extends string,
  InputT,
  OutputT,
>(
  name: Name,
  type: ColumnType<OutputT>,
  compute: (input: InputT) => OutputT,
): ComputedColumnBuilder<
  Name,
  false,
  Column.Index.ShouldNotIndex,
  InputT,
  OutputT
> {
  return new ComputedColumnBuilder(
    name,
    type,
    false,
    { shouldIndex: false },
    compute,
  )
}

export type StoredRecordForColumnSchemas<CS extends Column.Stored.Any[]> =
  Simplify<
    {
      [K in CS[number]["name"]]: Column.Stored.GetValue<
        Column.FindWithName<CS, K>
      >
    }
  >
