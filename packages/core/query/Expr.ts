import { Promisable } from "type-fest"
import { Column } from "../schema/TableSchema.ts"
import { assertUnreachable, Json } from "../types.ts"
import { ColumnType, ColumnTypes } from "../schema/columns/ColumnType.ts"
import { type ExecutionContext, type IQueryPlanNode } from "./QueryPlanNode.ts"

/**
 * An expression that can be evaluated to a value
 */
export interface Expr<T> {
  /**
   * Resolves the expression to a value from the
   * given execution context
   */
  resolve(ctx: ExecutionContext): Promisable<T>

  /**
   * Returns the type that the expression resolves to
   */
  getType(): ColumnType<T>

  /**
   * Returns a human-readable description of the expression
   */
  describe(): string

  /**
   * Returns a JSON representation of the expression
   */
  toJSON(): Json
}

/**
 * An expression representing a logical NOT operation
 */
export class NotExpr implements Expr<boolean> {
  /**
   * Creates a new Not expression
   */
  constructor(readonly expr: Expr<boolean>) {}
  /**
   * Resolves the expression to a value from the
   * given execution context
   */
  async resolve(ctx: ExecutionContext): Promise<boolean> {
    return !(await this.expr.resolve(ctx))
  }
  /**
   * Returns the type that the expression resolves to
   */
  getType(): ColumnType<boolean> {
    return ColumnTypes.boolean()
  }
  /**
   * Returns a human-readable description of the expression
   */
  describe(): string {
    return `NOT(${this.expr.describe()})`
  }
  /**
   * Returns a JSON representation of the expression
   */
  toJSON(): Json {
    return { type: "not", expr: this.expr.toJSON() }
  }
}

/**
 * An expression representing a logical AND or OR operation
 */
export class AndOrExpr implements Expr<boolean> {
  /**
   * Creates a new AndOr expression
   */
  constructor(
    readonly left: Expr<boolean>,
    readonly operator: "AND" | "OR",
    readonly right: Expr<boolean>,
  ) {}
  /**
   * Resolves the expression to a value from the
   * given execution context
   */
  async resolve(ctx: ExecutionContext): Promise<boolean> {
    const left = await this.left.resolve(ctx)
    const right = await this.right.resolve(ctx)
    if (this.operator === "AND") {
      return left && right
    } else {
      return left || right
    }
  }
  /**
   * Returns the type that the expression resolves to
   */
  getType(): ColumnType<boolean> {
    return ColumnTypes.boolean()
  }
  /**
   * Returns a human-readable description of the expression
   */
  describe(): string {
    return `(${this.left.describe()} ${this.operator} ${this.right.describe()})`
  }
  /**
   * A list of supported operators
   */
  static readonly operators = ["AND", "OR"] as const
  /**
   * checks if the given operator is supported
   */
  static isSupportedOperator(operator: string): operator is "AND" | "OR" {
    return AndOrExpr.operators.includes(operator as "AND" | "OR")
  }
  /**
   * Returns a JSON representation of the expression
   */
  toJSON(): Json {
    return {
      type: this.operator.toLowerCase(),
      left: this.left.toJSON(),
      right: this.right.toJSON(),
    }
  }
}

/**
 * An expression representing a literal value
 */
export class LiteralValueExpr<T> implements Expr<T> {
  /**
   * Creates a new LiteralValue expression
   */
  constructor(readonly value: T, readonly type: ColumnType<T>) {}
  /**
   * Resolves the expression to a value from the
   * given execution context
   */
  resolve(): T {
    return this.value
  }
  /**
   * Returns the type that the expression resolves to
   */
  getType(): ColumnType<T> {
    return this.type
  }
  /**
   * Returns a human-readable description of the expression
   */
  describe(): string {
    return JSON.stringify(this.value)
  }
  /**
   * Returns a JSON representation of the expression
   */
  toJSON(): Json {
    return this.type.serializer?.toJSON(this.value) ??
      "<UNSERIALIZABLE LITERAL>"
  }
}
export type { Column }
/**
 * An expression representing a reference to a column in a table
 */
export class ColumnRefExpr<
  C extends Column.Any,
  TableNameT extends string = string,
> implements Expr<Column.GetOutput<C>> {
  /**
   * Creates a new ColumnRef expression
   */
  constructor(readonly column: C, readonly tableName: TableNameT) {}

  /**
   * Resolves the expression to a value from the
   * given execution context
   */
  resolve(
    ctx: ExecutionContext<
      { [Property in TableNameT]: Column.GetRecordContainingColumn<C> }
    >,
  ): Column.GetOutput<C> {
    const data: Column.GetRecordContainingColumn<C> = this.tableName != null
      ? ctx.rowData[this.tableName]
      : ctx.rowData as Column.GetRecordContainingColumn<C>

    if (this.column.kind === "stored") {
      return data[this.column.name]
    } else {
      return this.column.compute(data)
    }
  }

  /**
   * Returns the type that the expression resolves to
   */
  getType(): C["type"] {
    return this.column.type
  }
  /**
   * Returns a human-readable description of the expression
   */
  describe(): string {
    return this.column.name
  }

  /**
   * Returns a JSON representation of the expression
   */
  toJSON(): Json {
    return { type: "column_ref", column: this.column.name }
  }
}

/**
 * A type representing a comparison operators
 */
export type CompareOperator = typeof Compare.operators[number]

/**
 * An expression that checks if the value resolved by the left
 * expression is equal to the value resolved by any of the right
 * expressions
 */
export class In<T> implements Expr<boolean> {
  /**
   * Creates a new In expression
   */
  constructor(readonly left: Expr<T>, readonly right: Expr<T>[]) {}

  /**
   * Resolves the expression to a value from the
   * given execution context
   */
  async resolve(ctx: ExecutionContext): Promise<boolean> {
    const left = await this.left.resolve(ctx)
    for (const right of this.right) {
      if (await right.resolve(ctx) === left) {
        return true
      }
    }
    return false
  }

  /**
   * Returns the type that the expression resolves to
   */
  getType(): ColumnType<boolean> {
    return ColumnTypes.boolean()
  }

  /**
   * Returns a human-readable description of the expression
   */
  describe(): string {
    return `In(${this.left.describe()}, [${
      this.right.map((r) => r.describe()).join(", ")
    }])`
  }

  /**
   * Returns a JSON representation of the expression
   */
  toJSON(): Json {
    return {
      type: "in",
      left: this.left.toJSON(),
      right: this.right.map((r) => r.toJSON()),
    }
  }
}

/**
 * An expression that resolves to the result of a subquery
 * where the subquery returns exactly one row and one column
 */
export class SubqueryExpr<T> implements Expr<T> {
  /**
   * Creates a new Subquery expression
   */
  constructor(
    readonly subplan: IQueryPlanNode<Record<"$0", Record<string, T>>>,
  ) {}

  /**
   * Returns the type that the expression resolves to
   */
  getType(): ColumnType<T> {
    // TODO: we don't know the type until we run the subquery,
    // which is not ideal. So we are using the "any" type for now.
    return new ColumnType<T>({
      name: "any",
      isValid: (_value: T): _value is T => true,
    })
  }

  /**
   * Resolves the expression to a value from the
   * given execution context
   */
  async resolve(ctx: ExecutionContext): Promise<T> {
    const values = await this.subplan.execute(ctx).take(2)
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
  /**
   * Returns a human-readable description of the expression
   */
  describe(): string {
    return `Subquery(${this.subplan.describe()})`
  }

  /**
   * Returns a JSON representation of the expression
   */
  toJSON(): Json {
    return {
      type: "subquery",
      subplan: this.subplan.toJSON(),
    }
  }
}

/**
 * An expression that compares two values using a comparison operator
 */
export class Compare<T> implements Expr<boolean> {
  /**
   * Creates a new Compare expression
   */
  constructor(
    readonly left: Expr<T>,
    readonly operator: CompareOperator,
    readonly right: Expr<T>,
  ) {}

  /**
   * Resolves the expression to a value from the
   * given execution context
   */
  async resolve(ctx: ExecutionContext): Promise<boolean> {
    const leftType = this.left.getType()
    const rightType = this.right.getType()
    const left = await this.left.resolve(ctx)
    const right = await this.right.resolve(ctx)
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

  /**
   * Returns the type that the expression resolves to
   */
  getType(): ColumnType<boolean> {
    return ColumnTypes.boolean()
  }

  /**
   * A list of supported operators
   */
  static readonly operators = ["=", "!=", "<", "<=", ">", ">="] as const

  /**
   * checks if the given operator is supported
   */
  static isSupportedOperator(operator: string): operator is CompareOperator {
    return Compare.operators.includes(operator as CompareOperator)
  }
  /**
   * Returns a human-readable description of the expression
   */
  describe(): string {
    return `Compare(${this.left.describe()} ${this.operator} ${this.right.describe()})`
  }

  /**
   * Returns a JSON representation of the expression
   */
  toJSON(): Json {
    return {
      type: "compare",
      operator: this.operator,
      left: this.left.toJSON(),
      right: this.right.toJSON(),
    }
  }
}
