import { Promisable, UnknownRecord } from "npm:type-fest"
import { AsyncIterableWrapper } from "../async.ts"
import { ColumnType, DbFile } from "../mod.ts"
import { Column, SomeTableSchema } from "../schema/schema.ts"
import { assertUnreachable } from "../types.ts"
import { ColumnTypes } from "../schema/columns/ColumnType.ts"

export class TableNotFoundError extends Error {}

export interface IQueryPlanNode {
  describe(): string
  execute(dbFile: DbFile): AsyncIterableWrapper<UnknownRecord>
}

abstract class AbstractQueryPlan implements IQueryPlanNode {
  abstract describe(): string
  abstract getIter(
    dbFile: DbFile,
  ): Promisable<AsyncIterableWrapper<UnknownRecord>>
  execute(dbFile: DbFile): AsyncIterableWrapper<UnknownRecord> {
    const iter = this.getIter(dbFile)
    return new AsyncIterableWrapper(async function* () {
      const wrapper = await iter
      for await (const row of wrapper) {
        yield row
      }
    })
  }
}

export class TableScan extends AbstractQueryPlan {
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
  ): Promise<AsyncIterableWrapper<UnknownRecord>> {
    const schema = await this.getSchema(dbFile)
    const tableInstance = await dbFile.getOrCreateTable(schema, { db: this.db })
    return tableInstance.iterate()
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

type CompareOperator = typeof Compare.operators[number]
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
    return `Predicate(${this.left.describe()} ${this.operator} ${this.right.describe()})`
  }
}

export class Filter extends AbstractQueryPlan {
  constructor(
    readonly child: IQueryPlanNode,
    readonly predicate: Expr<boolean>,
  ) {
    super()
  }

  describe(): string {
    return `Filter(${this.child.describe()}, ${this.predicate.describe()})`
  }

  override getIter(dbFile: DbFile): AsyncIterableWrapper<UnknownRecord> {
    return this.child.execute(dbFile).filter(
      this.predicate.resolve.bind(this.predicate),
    )
  }
}
