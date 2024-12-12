import { Promisable, UnknownRecord } from "npm:type-fest"
import { AsyncIterableWrapper } from "../async.ts"
import { Column, SomeTableSchema } from "../schema/schema.ts"
import { assertUnreachable } from "../types.ts"
import { ColumnType, ColumnTypes } from "../schema/columns/ColumnType.ts"
import { DbFile } from "../db/DbFile.ts"
import type { MultiAggregation } from "./Aggregation.ts"

export * from "./Aggregation.ts"

export class TableNotFoundError extends Error {}

export { QueryBuilder } from "./QueryBuilder.ts"

export type RowData = Record<string, UnknownRecord>

export interface IQueryPlanNode<T extends RowData = RowData> {
  describe(): string
  execute(ctx: ExecutionContext | DbFile): AsyncIterableWrapper<T>
}

abstract class AbstractQueryPlan<T extends RowData>
  implements IQueryPlanNode<T> {
  abstract describe(): string

  abstract getIter(ctx: ExecutionContext): Promisable<AsyncIterableWrapper<T>>

  execute(ctx: ExecutionContext | DbFile): AsyncIterableWrapper<T> {
    if (!(ctx instanceof ExecutionContext)) {
      ctx = new ExecutionContext(ctx)
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

export class TableScan<T extends RowData> extends AbstractQueryPlan<T> {
  readonly alias: string

  constructor(db: string, table: string)
  constructor(db: string, table: string, alias: string)
  constructor(readonly db: string, readonly table: string, alias?: string) {
    super()
    this.alias = alias ?? table
  }

  describe(): string {
    return `TableScan(${this.db}.${this.table})`
  }

  async getSchema(dbFile: DbFile): Promise<SomeTableSchema> {
    const schemas = await dbFile.getSchemasOrThrow(this.db, this.table)
    if (schemas.length === 0) {
      throw new TableNotFoundError(`Table ${this.db}.${this.table} not found`)
    }
    return schemas[0].schema
  }

  override async getIter(
    { dbFile }: ExecutionContext,
  ): Promise<AsyncIterableWrapper<T>> {
    const schema = await this.getSchema(dbFile)
    const tableInstance = await dbFile.getOrCreateTable(schema, { db: this.db })
    return tableInstance.iterate().map((row) => ({
      [this.alias]: row,
    })) as AsyncIterableWrapper<T>
  }
}

export class Aggregate<T extends UnknownRecord>
  extends AbstractQueryPlan<Record<"$0", T>> {
  constructor(
    readonly child: IQueryPlanNode,
    readonly aggregation: MultiAggregation<T>,
  ) {
    super()
  }

  describe(): string {
    return `Aggregate(${this.child.describe()}, ${this.aggregation.describe()})`
  }

  override async getIter(
    ctx: ExecutionContext,
  ): Promise<AsyncIterableWrapper<Record<"$0", T>>> {
    const aggregation = this.aggregation
    let accumulator = aggregation.init()
    for await (const row of this.child.execute(ctx)) {
      accumulator = await aggregation.update(accumulator, row)
    }
    return new AsyncIterableWrapper([
      { "$0": aggregation.result(accumulator) },
    ])
  }
}

export class ExecutionContext {
  constructor(readonly dbFile: DbFile) {}
}

export interface Expr<T> {
  resolve(row: RowData, ctx: ExecutionContext): Promisable<T>
  getType(): ColumnType<T>
  describe(): string
}

export class NotExpr implements Expr<boolean> {
  constructor(readonly expr: Expr<boolean>) {}
  async resolve(row: RowData, ctx: ExecutionContext): Promise<boolean> {
    return !(await this.expr.resolve(row, ctx))
  }
  getType(): ColumnType<boolean> {
    return ColumnTypes.boolean()
  }
  describe(): string {
    return `NOT(${this.expr.describe()})`
  }
}
export class AndOrExpr implements Expr<boolean> {
  constructor(
    readonly left: Expr<boolean>,
    readonly operator: "AND" | "OR",
    readonly right: Expr<boolean>,
  ) {}
  async resolve(row: RowData, ctx: ExecutionContext): Promise<boolean> {
    const left = await this.left.resolve(row, ctx)
    const right = await this.right.resolve(row, ctx)
    if (this.operator === "AND") {
      return left && right
    } else {
      return left || right
    }
  }
  getType(): ColumnType<boolean> {
    return ColumnTypes.boolean()
  }
  describe(): string {
    return `(${this.left.describe()} ${this.operator} ${this.right.describe()})`
  }
  static readonly operators = ["AND", "OR"] as const
  static isSupportedOperator(operator: string): operator is "AND" | "OR" {
    return AndOrExpr.operators.includes(operator as "AND" | "OR")
  }
}

export class LiteralValueExpr<T> implements Expr<T> {
  constructor(readonly value: T, readonly type: ColumnType<T>) {}
  resolve(): T {
    return this.value
  }
  getType(): ColumnType<T> {
    return this.type
  }
  describe(): string {
    return JSON.stringify(this.value)
  }
}

export class ColumnRefExpr<
  C extends Column.Any,
  TableNameT extends string = string,
> implements Expr<Column.GetOutput<C>> {
  constructor(readonly column: C, readonly tableName: TableNameT) {}

  resolve(
    row: { [Property in TableNameT]: Column.GetRecordContainingColumn<C> },
  ): Column.GetOutput<C> {
    const data: Column.GetRecordContainingColumn<C> = this.tableName != null
      ? row[this.tableName]
      : row as Column.GetRecordContainingColumn<C>

    if (this.column.kind === "stored") {
      return data[this.column.name]
    } else {
      return this.column.compute(data)
    }
  }

  getType(): C["type"] {
    return this.column.type
  }

  describe(): string {
    return this.column.name
  }
}

export type CompareOperator = typeof Compare.operators[number]

export class In<T> implements Expr<boolean> {
  constructor(readonly left: Expr<T>, readonly right: Expr<T>[]) {}

  async resolve(row: RowData, ctx: ExecutionContext): Promise<boolean> {
    const left = await this.left.resolve(row, ctx)
    for (const right of this.right) {
      if (await right.resolve(row, ctx) === left) {
        return true
      }
    }
    return false
  }

  getType(): ColumnType<boolean> {
    return ColumnTypes.boolean()
  }

  describe(): string {
    return `In(${this.left.describe()}, [${
      this.right.map((r) => r.describe()).join(", ")
    }])`
  }
}

export class SubqueryExpr<T, RowDataT extends RowData> implements Expr<T> {
  constructor(
    // TODO: passing in a factory function means we don't know
    // the plan until runtime, which makes it impossible to
    // optimize ahead of time. Need to figure out some way to capture
    // the plan at compile time.
    readonly subplanFactory: (
      rowData: RowDataT,
      ctx: ExecutionContext,
    ) => IQueryPlanNode<Record<"$0", Record<string, T>>>,
  ) {}

  getType(): ColumnType<T> {
    // TODO: we don't know the type until we run the subquery,
    // which is not ideal. So we are using the "any" type for now.
    return ColumnTypes.any()
  }

  async resolve(row: RowDataT, ctx: ExecutionContext): Promise<T> {
    const values = await this.subplanFactory(row, ctx).execute(ctx).take(2)
      .toArray()
    if (values.length === 0) {
      throw new Error("Subquery returned no rows")
    }
    if (values.length > 1) {
      throw new Error("Subquery returned more than one row")
    }
    const val = values[0].$0
    const cellValues = Object.values(val)
    if (cellValues.length !== 1) {
      throw new Error("Subquery returned more than one column")
    }
    return cellValues[0]
  }

  describe(): string {
    return "Subquery()"
  }
}
export class Compare<T> implements Expr<boolean> {
  constructor(
    readonly left: Expr<T>,
    readonly operator: CompareOperator,
    readonly right: Expr<T>,
  ) {}

  async resolve(row: RowData, ctx: ExecutionContext): Promise<boolean> {
    const leftType = this.left.getType()
    const rightType = this.right.getType()
    const left = await this.left.resolve(row, ctx)
    const right = await this.right.resolve(row, ctx)
    if (!leftType.isValid(right)) {
      throw new Error(
        `Type mismatch: ${this.left.describe()} is of type ${leftType.name}, but ${this.right.describe()} is of type ${rightType.name}, value ${right} doesn't work`,
      )
    }
    if (!rightType.isValid(right)) {
      throw new Error(
        `Type mismatch: ${this.right.describe()} is of type ${rightType.name}, but ${this.left.describe()} is of type ${leftType.name}`,
      )
    }
    switch (this.operator) {
      case "=":
        return leftType.isEqual(left, right)
      case "!=":
        return !leftType.isEqual(left, right)
      case "<":
        return leftType.compare(left, right) < 0
      case "<=":
        return leftType.compare(left, right) <= 0
      case ">":
        return leftType.compare(left, right) > 0
      case ">=":
        return leftType.compare(left, right) >= 0
      default:
        assertUnreachable(this.operator)
    }
  }

  getType(): ColumnType<boolean> {
    return ColumnTypes.boolean()
  }

  static readonly operators = ["=", "!=", "<", "<=", ">", ">="] as const
  static isSupportedOperator(operator: string): operator is CompareOperator {
    return Compare.operators.includes(operator as CompareOperator)
  }

  describe(): string {
    return `Compare(${this.left.describe()} ${this.operator} ${this.right.describe()})`
  }
}

export class Filter<T extends RowData = RowData> extends AbstractQueryPlan<T> {
  constructor(
    readonly child: IQueryPlanNode<T>,
    readonly predicate: Expr<boolean>,
  ) {
    super()
  }

  describe(): string {
    return `Filter(${this.child.describe()}, ${this.predicate.describe()})`
  }

  override getIter(ctx: ExecutionContext): AsyncIterableWrapper<T> {
    return this.child.execute(ctx).filter(
      (row) => this.predicate.resolve(row, ctx),
    )
  }
}

export class Select<T extends UnknownRecord>
  extends AbstractQueryPlan<Record<"$0", T>> {
  constructor(
    readonly child: IQueryPlanNode,
    readonly columns: Record<string, Expr<any>>,
  ) {
    super()
  }

  describe(): string {
    return `Select(${
      Object.entries(this.columns).map(([key, value]) =>
        `${value.describe()} AS ${key}`
      ).join(", ")
    }, ${this.child.describe()})`
  }

  override getIter(
    ctx: ExecutionContext,
  ): AsyncIterableWrapper<Record<"$0", T>> {
    return this.child.execute(ctx).map(async (row) => {
      const result = {} as T
      for (const [key, column] of Object.entries(this.columns)) {
        ;(result as UnknownRecord)[key] = await column.resolve(row, ctx)
      }
      return { "$0": result }
    })
  }

  addColumn<CName extends string, ValueT>(
    name: string,
    expr: Expr<T>,
  ): Select<T & { [name in CName]: ValueT }> {
    return new Select(this.child, { ...this.columns, [name]: expr })
  }
}

export class Limit<T extends RowData = RowData> extends AbstractQueryPlan<T> {
  constructor(readonly child: IQueryPlanNode<T>, readonly limit: number) {
    super()
  }

  describe(): string {
    return `Limit(${this.child.describe()}, ${this.limit})`
  }

  override getIter(ctx: ExecutionContext): AsyncIterableWrapper<T> {
    return this.child.execute(ctx).take(this.limit)
  }
}

export class Join<
  LeftT extends RowData = RowData,
  RightT extends RowData = RowData,
> extends AbstractQueryPlan<
  LeftT & RightT
> {
  constructor(
    readonly left: IQueryPlanNode<LeftT>,
    readonly right: IQueryPlanNode<RightT>,
    readonly predicate: Expr<boolean>,
  ) {
    super()
  }
  override describe(): string {
    return `Join(${this.left.describe()}, ${this.right.describe()}, ${this.predicate.describe()})`
  }
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
          if (await predicate.resolve(row, ctx)) {
            yield row
          }
        }
      }
    })
  }
}

export class OrderBy<T extends RowData = RowData> extends AbstractQueryPlan<T> {
  constructor(
    readonly child: IQueryPlanNode<T>,
    readonly orderBy: { expr: Expr<any>; direction: "ASC" | "DESC" }[],
  ) {
    super()
  }

  describe(): string {
    return `OrderBy(${this.child.describe()}, ${
      this.orderBy
        .map((o) => `${o.expr.describe()} ${o.direction}`)
        .join(", ")
    })`
  }

  override async getIter(
    ctx: ExecutionContext,
  ): Promise<AsyncIterableWrapper<T>> {
    const allValues = await this.child.execute(ctx).toArray()
    const valueMaps: Map<T, unknown>[] = []
    for (const { expr } of this.orderBy) {
      const resolvedMap = new Map<T, unknown>()
      for (const v of allValues) {
        resolvedMap.set(v, await expr.resolve(v, ctx))
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
