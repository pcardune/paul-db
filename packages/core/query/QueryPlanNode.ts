import type { Promisable, UnknownRecord } from "type-fest"
import { AsyncIterableWrapper } from "../async.ts"
import { Json } from "../types.ts"
import type { MultiAggregation } from "./Aggregation.ts"
import { PaulDB } from "../PaulDB.ts"
import { Expr } from "./Expr.ts"
import { BTree } from "../indexes/BTree.ts"
import { ColumnType } from "../schema/columns/ColumnType.ts"

export * from "./Aggregation.ts"
export * from "./Expr.ts"

export { QueryBuilder } from "./QueryBuilder.ts"

/**
 * A RowData is a record of table names to row data.
 */
export type RowData = Record<string, UnknownRecord>

/**
 * A QueryPlanNode is a node in a query plan that can be executed
 */
export interface IQueryPlanNode<T extends RowData = RowData> {
  /**
   * Returns a human-readable description of the query plan
   */
  describe(): string

  /**
   * Returns a JSON representation of the query plan
   */
  toJSON(): Json

  /**
   * Executes the query plan and returns an async iterable of the results
   */
  execute(ctx: ExecutionContext | PaulDB): AsyncIterableWrapper<T>

  children(): IQueryPlanNode[]
}

/**
 * @ignore
 */
export abstract class AbstractQueryPlan<T extends RowData>
  implements IQueryPlanNode<T> {
  /**
   * Returns a human-readable description of the query plan
   */
  abstract describe(): string
  /**
   * Returns a JSON representation of the query plan
   */
  abstract toJSON(): Json

  /**
   * @ignore
   */
  abstract getIter(ctx: ExecutionContext): Promisable<AsyncIterableWrapper<T>>

  abstract children(): IQueryPlanNode[]

  /**
   * Executes the query plan and returns an async iterable of the results
   */
  execute(ctx: ExecutionContext | PaulDB): AsyncIterableWrapper<T> {
    if (!(ctx instanceof ExecutionContext)) {
      ctx = new ExecutionContext(ctx, {})
    }
    const iter = this.getIter(ctx)
    return new AsyncIterableWrapper(async function* () {
      const wrapper = await iter
      for await (const row of wrapper) {
        yield row
      }
    })
  }
}

/**
 * A TableScan node scans a table in the database.
 */
export class TableScan<T extends RowData> extends AbstractQueryPlan<T> {
  /**
   * The alias of the table. i.e. "SELECT * FROM table AS alias"
   */
  readonly alias: string

  /**
   * Constructs a new TableScan node with the given database and table.
   */
  constructor(db: string, table: string)
  /**
   * Constructs a new TableScan node with the given database, table and alias
   */
  constructor(db: string, table: string, alias: string)
  /**
   * Constructs a new TableScan node with the given database and table with
   * an optional alias.
   */
  constructor(readonly db: string, readonly table: string, alias?: string) {
    super()
    this.alias = alias ?? table
  }
  /**
   * Returns a human-readable description of the query plan
   */
  describe(): string {
    return `TableScan(${this.db}.${this.table})`
  }

  /**
   * Returns a JSON representation of the query plan
   */
  toJSON(): Json {
    return {
      type: "TableScan",
      db: this.db,
      table: this.table,
      alias: this.alias,
    }
  }

  override children(): IQueryPlanNode[] {
    return []
  }

  /**
   * @ignore
   */
  override async getIter(
    ctx: ExecutionContext,
  ): Promise<AsyncIterableWrapper<T>> {
    const schema = await ctx.db.getSchema(this.db, this.table)
    const tableInstance = await ctx.db.dbFile.getOrCreateTable(schema, {
      db: this.db,
    })
    return tableInstance.iterate().map((row) => ({
      [this.alias]: row,
    })) as AsyncIterableWrapper<T>
  }
}

/**
 * An Aggregate node aggregates the results of the child node based on the given
 * aggregation.
 */
export class Aggregate<T extends UnknownRecord, AliasT extends string>
  extends AbstractQueryPlan<Record<AliasT, T>> {
  /**
   * Constructs a new Aggregate node with the given child and aggregation.
   * @param child
   * @param aggregation
   */
  constructor(
    readonly child: IQueryPlanNode,
    readonly aggregation: MultiAggregation<T>,
    readonly alias: AliasT,
  ) {
    super()
  }
  /**
   * Returns a human-readable description of the query plan
   */
  describe(): string {
    return `Aggregate(${this.child.describe()}, ${this.aggregation.describe()}) AS ${this.alias}`
  }

  /**
   * Returns a JSON representation of the query plan
   */
  override toJSON(): Json {
    return {
      type: "Aggregate",
      child: this.child.toJSON(),
      aggregation: this.aggregation.toJSON(),
      alias: this.alias,
    }
  }

  override children(): IQueryPlanNode[] {
    return [this.child]
  }

  /**
   * @ignore
   */
  override async getIter(
    ctx: ExecutionContext,
  ): Promise<AsyncIterableWrapper<Record<AliasT, T>>> {
    const aggregation = this.aggregation
    let accumulator: T | undefined = undefined
    for await (const row of this.child.execute(ctx)) {
      accumulator = await aggregation.update(accumulator, ctx.withRowData(row))
    }
    return new AsyncIterableWrapper([
      { [this.alias]: aggregation.result(accumulator) } as Record<AliasT, T>,
    ])
  }
}

/**
 * ExecutionContext is the context in which a query or an expression is executed.
 * It holds the data available to the query or expression.
 */
export class ExecutionContext<RowDataT extends RowData = RowData> {
  /**
   * Constructs a new ExecutionContext with the given database and row data.
   */
  constructor(readonly db: PaulDB, readonly rowData: RowDataT) {}

  /**
   * Returns a new ExecutionContext with the given row data added.
   */
  withRowData<RowDataT2 extends RowData>(
    rowData: RowDataT2,
  ): ExecutionContext<RowDataT2> {
    return new ExecutionContext(this.db, { ...this.rowData, ...rowData })
  }
}

/**
 * A Filter node filters the results of the child node based on a predicate.
 */
export class Filter<T extends RowData = RowData> extends AbstractQueryPlan<T> {
  /**
   * Constructs a new Filter node with the given child and predicate.
   * @param child
   * @param predicate
   */
  constructor(
    readonly child: IQueryPlanNode<T>,
    readonly predicate: Expr<ColumnType<boolean>>,
  ) {
    super()
  }

  override children(): IQueryPlanNode[] {
    return [this.child]
  }

  /**
   * Returns a human-readable description of the query plan
   */
  describe(): string {
    return `Filter(${this.child.describe()}, ${this.predicate.describe()})`
  }
  /**
   * @ignore
   */
  override getIter(ctx: ExecutionContext): AsyncIterableWrapper<T> {
    return this.child.execute(ctx).filter(
      (row) => this.predicate.resolve(ctx.withRowData(row)),
    )
  }

  /**
   * Returns a JSON representation of the query plan
   */
  toJSON(): Json {
    return {
      type: "Filter",
      child: this.child.toJSON(),
      predicate: this.predicate.toJSON(),
    }
  }
}

export class GroupBy<
  GroupKey extends UnknownRecord = UnknownRecord,
  AggregateT extends UnknownRecord = UnknownRecord,
  RowDataT extends RowData = RowData,
  AliasT extends string = string,
> extends AbstractQueryPlan<
  Record<AliasT, GroupKey & AggregateT>
> {
  constructor(
    readonly child: IQueryPlanNode<RowDataT>,
    readonly groupByExpr: {
      [Key in keyof GroupKey]: Expr<ColumnType<GroupKey[Key]>>
    },
    readonly aggregation: MultiAggregation<AggregateT>,
    readonly alias: AliasT,
  ) {
    super()
  }

  override children(): IQueryPlanNode[] {
    return [this.child]
  }

  override getIter(
    ctx: ExecutionContext,
  ): AsyncIterableWrapper<
    Record<AliasT, GroupKey & AggregateT>
  > {
    const childIter = this.child.execute(ctx)
    const groupByExpr = this.groupByExpr

    const groups: Array<
      { groupKey: GroupKey; accumulator: AggregateT }
    > = []

    const btree = BTree.inMemory<GroupKey, number>({
      compare: (a, b) => {
        for (const key of Object.keys(a) as (keyof GroupKey)[]) {
          const cmp = groupByExpr[key].getType().compare(a[key], b[key])
          if (cmp !== 0) return cmp
        }
        return 0
      },
    })
    const aggregation = this.aggregation
    const alias = this.alias
    return new AsyncIterableWrapper(async function* () {
      for await (const row of childIter) {
        const groupKey = {} as GroupKey
        for (const key of Object.keys(groupByExpr) as (keyof GroupKey)[]) {
          groupKey[key] = await groupByExpr[key].resolve(ctx.withRowData(row))
        }
        const existingValue = await btree.get(groupKey)
        if (existingValue.length === 0) {
          const accumulator = await aggregation.update(
            undefined,
            ctx.withRowData(row),
          )
          groups.push({ groupKey, accumulator })
          await btree.insert(groupKey, groups.length - 1)
        } else {
          const group = groups[existingValue[0]]
          group.accumulator = await aggregation.update(
            group.accumulator,
            ctx.withRowData(row),
          )
        }
      }
      for (const { groupKey, accumulator } of groups) {
        yield {
          [alias]: { ...groupKey, ...aggregation.result(accumulator) },
        } as Record<
          AliasT,
          GroupKey & AggregateT
        >
      }
    })
  }

  override describe(): string {
    return `GroupBy(${this.child.describe()}, ${
      Object.entries(this.groupByExpr).map(([key, value]) =>
        `${key}: ${value.describe()}`
      ).join(", ")
    }, ${this.aggregation.describe()}) AS ${this.alias}`
  }

  override toJSON(): Json {
    return {
      type: "GroupBy",
      child: this.child.toJSON(),
      groupBy: Object.fromEntries(
        Object.entries(this.groupByExpr).map((
          [key, value],
        ) => [key, value.toJSON()]),
      ),
      aggregation: this.aggregation.toJSON(),
      alias: this.alias,
    }
  }
}

/**
 * A Select node projects the results of the child node into a new set of columns.
 */
export class Select<Alias extends string, T extends UnknownRecord>
  extends AbstractQueryPlan<Record<Alias, T>> {
  /**
   * Constructs a new Select node with the given child and columns.
   * @param child
   * @param columns
   */
  constructor(
    readonly child: IQueryPlanNode,
    readonly alias: Alias,
    readonly columns: Record<string, Expr<any>>,
  ) {
    super()
  }

  override children(): IQueryPlanNode[] {
    return [this.child]
  }

  /**
   * Returns a human-readable description of the query plan
   */
  describe(): string {
    return `Select(${
      Object.entries(this.columns).map(([key, value]) =>
        `${value.describe()} AS ${key}`
      ).join(", ")
    }, ${this.child.describe()}) AS ${this.alias}`
  }
  /**
   * @ignore
   */
  override getIter(
    ctx: ExecutionContext,
  ): AsyncIterableWrapper<Record<Alias, T>> {
    return this.child.execute(ctx).map(async (row) => {
      const result = {} as T
      for (const [key, column] of Object.entries(this.columns)) {
        ;(result as UnknownRecord)[key] = await column.resolve(
          ctx.withRowData(row),
        )
      }
      return { [this.alias]: result } as Record<Alias, T>
    })
  }

  /**
   * Returns a new Select node with the given column added.
   */
  addColumn<CName extends string, ValueT>(
    name: string,
    expr: Expr<ColumnType<T>>,
  ): Select<Alias, T & { [name in CName]: ValueT }> {
    return new Select(this.child, this.alias, { ...this.columns, [name]: expr })
  }

  /**
   * Returns a JSON representation of the query plan
   */
  toJSON(): Json {
    return {
      type: "Select",
      child: this.child.toJSON(),
      columns: Object.fromEntries(
        Object.entries(this.columns).map((
          [key, value],
        ) => [key, value.toJSON()]),
      ),
      alias: this.alias,
    }
  }
}

/**
 * A Limit node limits the number of results from the child node.
 */
export class Limit<T extends RowData = RowData> extends AbstractQueryPlan<T> {
  /**
   * Constructs a new Limit node with the given child and limit.
   * @param child
   * @param limit
   */
  constructor(readonly child: IQueryPlanNode<T>, readonly limit: number) {
    super()
  }

  override children(): IQueryPlanNode[] {
    return [this.child]
  }
  /**
   * Returns a human-readable description of the query plan
   */
  describe(): string {
    return `Limit(${this.child.describe()}, ${this.limit})`
  }
  /**
   * @ignore
   */
  override getIter(ctx: ExecutionContext): AsyncIterableWrapper<T> {
    return this.child.execute(ctx).take(this.limit)
  }

  /**
   * Returns a JSON representation of the query plan
   */
  toJSON(): Json {
    return {
      type: "Limit",
      child: this.child.toJSON(),
      limit: this.limit,
    }
  }
}

/**
 * A Join node joins the results of two child nodes based on a predicate.
 */
export class Join<
  LeftT extends RowData = RowData,
  RightT extends RowData = RowData,
> extends AbstractQueryPlan<
  LeftT & RightT
> {
  /**
   * Constructs a new Join node with the given left and right child nodes
   * and predicate.
   * @param left
   * @param right
   * @param predicate
   */
  constructor(
    readonly left: IQueryPlanNode<LeftT>,
    readonly right: IQueryPlanNode<RightT>,
    readonly predicate: Expr<ColumnType<boolean>>,
  ) {
    super()
  }
  override children(): IQueryPlanNode[] {
    return [this.left, this.right]
  }
  /**
   * Returns a human-readable description of the query plan
   */
  override describe(): string {
    return `Join(${this.left.describe()}, ${this.right.describe()}, ${this.predicate.describe()})`
  }
  /**
   * @ignore
   */
  override async getIter(
    ctx: ExecutionContext,
  ): Promise<AsyncIterableWrapper<LeftT & RightT>> {
    const leftIter = await this.left.execute(ctx).toArray()
    const rightIter = await this.right.execute(ctx).toArray()
    const predicate = this.predicate
    return new AsyncIterableWrapper(async function* () {
      for (const leftRow of leftIter) {
        for (const rightRow of rightIter) {
          const row = { ...leftRow, ...rightRow }
          if (await predicate.resolve(ctx.withRowData(row))) {
            yield row
          }
        }
      }
    })
  }
  /**
   * Returns a JSON representation of the query plan
   */
  override toJSON(): Json {
    return {
      type: "Join",
      left: this.left.toJSON(),
      right: this.right.toJSON(),
      predicate: this.predicate.toJSON(),
    }
  }
}

/**
 * A Join node joins the results of two child nodes based on a predicate.
 */
export class LeftJoin<
  LeftT extends RowData = RowData,
  RightT extends RowData = RowData,
> extends AbstractQueryPlan<LeftT & Partial<RightT>> {
  /**
   * Constructs a new Join node with the given left and right child nodes
   * and predicate.
   * @param left
   * @param right
   * @param predicate
   */
  constructor(
    readonly left: IQueryPlanNode<LeftT>,
    readonly right: IQueryPlanNode<RightT>,
    readonly predicate: Expr<ColumnType<boolean>>,
  ) {
    super()
  }

  override children(): IQueryPlanNode[] {
    return [this.left, this.right]
  }

  /**
   * Returns a human-readable description of the query plan
   */
  override describe(): string {
    return `LeftJoin(${this.left.describe()}, ${this.right.describe()}, ${this.predicate.describe()})`
  }
  /**
   * @ignore
   */
  override async getIter(
    ctx: ExecutionContext,
  ): Promise<AsyncIterableWrapper<LeftT & Partial<RightT>>> {
    const leftIter = await this.left.execute(ctx).toArray()
    const rightIter = await this.right.execute(ctx).toArray()
    const predicate = this.predicate
    return new AsyncIterableWrapper(async function* () {
      for (const leftRow of leftIter) {
        let foundMatch = false
        for (const rightRow of rightIter) {
          const row = { ...leftRow, ...rightRow }
          if (await predicate.resolve(ctx.withRowData(row))) {
            foundMatch = true
            yield row
          }
        }
        if (!foundMatch) {
          yield leftRow as LeftT & Partial<RightT>
        }
      }
    })
  }
  /**
   * Returns a JSON representation of the query plan
   */
  override toJSON(): Json {
    return {
      type: "LeftJoin",
      left: this.left.toJSON(),
      right: this.right.toJSON(),
      predicate: this.predicate.toJSON(),
    }
  }
}

/**
 * An OrderBy node sorts the results of the child node by the given expressions.
 */
export class OrderBy<T extends RowData = RowData> extends AbstractQueryPlan<T> {
  /**
   * Creates a new OrderBy node with the given child and order by expressions.
   */
  constructor(
    readonly child: IQueryPlanNode<T>,
    readonly orderBy: { expr: Expr<any>; direction: "ASC" | "DESC" }[],
  ) {
    super()
  }

  override children(): IQueryPlanNode[] {
    return [this.child]
  }

  /**
   * Returns a human-readable description of the query plan
   */
  describe(): string {
    return `OrderBy(${this.child.describe()}, ${
      this.orderBy
        .map((o) => `${o.expr.describe()} ${o.direction}`)
        .join(", ")
    })`
  }

  /**
   * Returns a JSON representation of the query plan
   */
  override toJSON(): Json {
    return {
      type: "OrderBy",
      child: this.child.toJSON(),
      orderBy: this.orderBy.map((o) => ({
        expr: o.expr.toJSON(),
        direction: o.direction,
      })),
    }
  }

  /**
   * @ignore
   */
  override async getIter(
    ctx: ExecutionContext,
  ): Promise<AsyncIterableWrapper<T>> {
    const allValues = await this.child.execute(ctx).toArray()
    const valueMaps: Map<T, unknown>[] = []
    for (const { expr } of this.orderBy) {
      const resolvedMap = new Map<T, unknown>()
      for (const v of allValues) {
        resolvedMap.set(v, await expr.resolve(ctx.withRowData(v)))
      }
      valueMaps.push(resolvedMap)
    }

    allValues.sort((a, b) => {
      for (const [i, { expr, direction }] of this.orderBy.entries()) {
        const aValue = valueMaps[i].get(a)!
        const bValue = valueMaps[i].get(b)!
        if (expr.getType().compare(aValue, bValue) < 0) {
          return direction === "ASC" ? -1 : 1
        } else if (expr.getType().compare(aValue, bValue) > 0) {
          return direction === "ASC" ? 1 : -1
        }
      }
      return 0
    })
    return new AsyncIterableWrapper(allValues)
  }
}
