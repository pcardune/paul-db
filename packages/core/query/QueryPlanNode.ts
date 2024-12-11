import { Promisable, UnknownRecord } from "npm:type-fest"
import { AsyncIterableWrapper } from "../async.ts"
import { ColumnType, DbFile } from "../mod.ts"
import { Column, SomeTableSchema } from "../schema/schema.ts"
import { assertUnreachable } from "../types.ts"
import { ColumnTypes } from "../schema/columns/ColumnType.ts"

export class TableNotFoundError extends Error {}

export { QueryBuilder } from "./QueryBuilder.ts"

export interface IQueryPlanNode<T extends UnknownRecord = UnknownRecord> {
  describe(): string
  execute(dbFile: DbFile): AsyncIterableWrapper<T>
}

abstract class AbstractQueryPlan<T extends UnknownRecord>
  implements IQueryPlanNode<T> {
  abstract describe(): string
  abstract getIter(
    dbFile: DbFile,
  ): Promisable<AsyncIterableWrapper<T>>
  execute(dbFile: DbFile): AsyncIterableWrapper<T> {
    const iter = this.getIter(dbFile)
    return new AsyncIterableWrapper(async function* () {
      const wrapper = await iter
      for await (const row of wrapper) {
        yield row
      }
    })
  }
}

export class TableScan<T extends UnknownRecord> extends AbstractQueryPlan<T> {
  constructor(readonly db: string, readonly table: string) {
    super()
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
    dbFile: DbFile,
  ): Promise<AsyncIterableWrapper<T>> {
    const schema = await this.getSchema(dbFile)
    const tableInstance = await dbFile.getOrCreateTable(schema, { db: this.db })
    return tableInstance.iterate() as AsyncIterableWrapper<T>
  }
}

export interface Expr<T> {
  resolve(row: UnknownRecord): T
  getType(): ColumnType<T>
  describe(): string
}

export class NotExpr implements Expr<boolean> {
  constructor(readonly expr: Expr<boolean>) {}
  resolve(row: UnknownRecord): boolean {
    return !this.expr.resolve(row)
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
  resolve(row: UnknownRecord): boolean {
    const left = this.left.resolve(row)
    const right = this.right.resolve(row)
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
export class ColumnRefExpr<C extends Column.Any>
  implements Expr<Column.GetOutput<C>> {
  constructor(readonly column: C) {}
  resolve(row: Column.GetRecordContainingColumn<C>): Column.GetOutput<C> {
    if (this.column.kind === "stored") {
      return row[this.column.name]
    } else {
      return this.column.compute(row)
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
export class Compare<T> implements Expr<boolean> {
  constructor(
    readonly left: Expr<T>,
    readonly operator: CompareOperator,
    readonly right: Expr<T>,
  ) {}

  resolve(row: UnknownRecord): boolean {
    const leftType = this.left.getType()
    const rightType = this.right.getType()
    const left = this.left.resolve(row)
    const right = this.right.resolve(row)
    if (!leftType.isValid(right)) {
      throw new Error(
        `Type mismatch: ${this.left.describe()} is of type ${leftType.name}, but ${this.right.describe()} is of type ${rightType.name}`,
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

export class Filter<T extends UnknownRecord = UnknownRecord>
  extends AbstractQueryPlan<T> {
  constructor(
    readonly child: IQueryPlanNode<T>,
    readonly predicate: Expr<boolean>,
  ) {
    super()
  }

  describe(): string {
    return `Filter(${this.child.describe()}, ${this.predicate.describe()})`
  }

  override getIter(dbFile: DbFile): AsyncIterableWrapper<T> {
    return this.child.execute(dbFile).filter(
      this.predicate.resolve.bind(this.predicate),
    )
  }
}

export class Select<T extends UnknownRecord> extends AbstractQueryPlan<T> {
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

  override getIter(dbFile: DbFile): AsyncIterableWrapper<T> {
    return this.child.execute(dbFile).map((row) => {
      const result = {} as T
      for (const [key, column] of Object.entries(this.columns)) {
        ;(result as UnknownRecord)[key] = column.resolve(row)
      }
      return result
    })
  }

  addColumn<CName extends string, ValueT>(
    name: string,
    expr: Expr<T>,
  ): Select<T & { [name in CName]: ValueT }> {
    return new Select(this.child, { ...this.columns, [name]: expr })
  }
}

export class Limit<T extends UnknownRecord = UnknownRecord>
  extends AbstractQueryPlan<T> {
  constructor(readonly child: IQueryPlanNode<T>, readonly limit: number) {
    super()
  }

  describe(): string {
    return `Limit(${this.child.describe()}, ${this.limit})`
  }

  override getIter(dbFile: DbFile): AsyncIterableWrapper<T> {
    return this.child.execute(dbFile).take(this.limit)
  }
}

export class OrderBy<T extends UnknownRecord = UnknownRecord>
  extends AbstractQueryPlan<T> {
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
    dbFile: DbFile,
  ): Promise<AsyncIterableWrapper<T>> {
    const orderBy = this.orderBy
    const allValues = await this.child.execute(dbFile).toArray()
    allValues.sort((a, b) => {
      for (const { expr, direction } of orderBy) {
        const aValue = expr.resolve(a)
        const bValue = expr.resolve(b)
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
