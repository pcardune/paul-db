import {
  ArrayColumnType,
  ColumnType,
  ColumnTypes,
  ColValueOf,
} from "../schema/columns/ColumnType.ts"
import { Json } from "../types.ts"
import type { ExecutionContext, Expr } from "./QueryPlanNode.ts"
import type { Promisable, UnknownRecord } from "type-fest"

/**
 * An aggregation is a computation that summarizes a set of rows into a single
 * value.
 */
export interface Aggregation<
  AccT,
  Result extends ColumnType<any> = ColumnType<AccT>,
> {
  result(accumulator: AccT | undefined): ColValueOf<Result>
  /**
   * Updates the accumulator with the values from the context.
   */
  update(accumulator: undefined | AccT, ctx: ExecutionContext): Promisable<AccT>
  /**
   * Generates a human-readable description of the aggregation.
   */
  describe(): string
  /**
   * Generates a json representation of the aggregation.
   */
  toJSON(): Json

  /**
   * Get the column type of the aggregation.
   */
  getType(): Result
}

abstract class ExprAggregation<
  AccT,
  Result extends ColumnType<any> = ColumnType<AccT>,
> implements Aggregation<AccT, Result> {
  /**
   * Updates the accumulator with the values from the context.
   */
  update: (
    accumulator: AccT | undefined,
    ctx: ExecutionContext,
  ) => Promisable<AccT>

  /**
   * Creates a new ExprAggregation with the given expression.
   * @ignore
   */
  constructor(readonly type: string, readonly expr: Expr<Result>, {
    update,
  }: {
    update: (
      accumulator: AccT | undefined,
      ctx: ExecutionContext,
    ) => Promisable<AccT>
  }) {
    this.update = update
  }

  /**
   * Generates a human-readable description of the aggregation.
   */
  describe(): string {
    return `${this.type}(${this.expr.describe()})`
  }

  /**
   * Generates a json representation of the aggregation.
   */
  toJSON(): Json {
    return { type: this.type, expr: this.expr.toJSON() }
  }

  getType(): Result {
    return this.expr.getType()
  }

  result(accumulator: AccT): ColValueOf<Result> {
    return accumulator as ColValueOf<Result>
  }
}

/**
 * An aggregation that counts the number of rows
 */
export class CountAggregation implements Aggregation<number> {
  /**
   * Updates the accumulator with the values from the context.
   */
  update(accumulator: number | undefined): number {
    if (accumulator === undefined) {
      return 1
    }
    return accumulator + 1
  }

  result(accumulator: number | undefined): number {
    if (accumulator === undefined) {
      return 0
    }
    return accumulator
  }

  /**
   * Generates a human-readable description of the aggregation.
   */
  describe(): string {
    return "COUNT(*)"
  }
  /**
   * Generates a json representation of the aggregation.
   */
  toJSON(): Json {
    return { type: "count" }
  }

  getType(): ColumnType<number> {
    return ColumnTypes.int32()
  }
}

/**
 * An aggregation that computes the maximum value of an expression.
 */
export class MaxAggregation<ColT extends ColumnType>
  extends ExprAggregation<ColValueOf<ColT>, ColT> {
  /**
   * Creates a new MaxAggregation with the given expression.
   */
  constructor(expr: Expr<ColT>) {
    super("MAX", expr, {
      update: async (accumulator, ctx) => {
        if (accumulator === undefined) {
          return this.expr.resolve(ctx)
        }
        const value = await this.expr.resolve(ctx)
        return this.expr.getType().compare(value, accumulator) > 0
          ? value
          : accumulator
      },
    })
  }
}

/**
 * An aggregation that computes the minimum value of an expression.
 */
export class MinAggregation<ColT extends ColumnType>
  extends ExprAggregation<ColValueOf<ColT>, ColT> {
  /**
   * Creates a new MinAggregation with the given expression.
   */
  constructor(expr: Expr<ColT>) {
    super("MIN", expr, {
      update: async (accumulator, ctx) => {
        if (accumulator === undefined) {
          return this.expr.resolve(ctx)
        }
        const value = await this.expr.resolve(ctx)
        return this.expr.getType().compare(value, accumulator) < 0
          ? value
          : accumulator
      },
    })
  }
}

export class SumAggregation extends ExprAggregation<number> {
  /**
   * Creates a new SumAggregation with the given expression.
   */
  constructor(expr: Expr<ColumnType<number>>) {
    super("SUM", expr, {
      update: async (accumulator, ctx) => {
        if (accumulator === undefined) {
          return this.expr.resolve(ctx)
        }
        return accumulator + (await this.expr.resolve(ctx))
      },
    })
  }
}

/**
 * An ArrayAggregation is an aggregation that collects all values into an array.
 */
export class ArrayAggregation<T>
  implements Aggregation<T[], ArrayColumnType<T>> {
  /**
   * Creates a new ArrayAggregation with the given expression.
   */
  constructor(
    readonly expr: Expr<ColumnType<T>>,
    readonly filter?: Expr<ColumnType<boolean>>,
  ) {}

  /**
   * Updates the accumulator with the values from the context.
   */
  async update(
    accumulator: T[] | undefined,
    ctx: ExecutionContext,
  ): Promise<T[]> {
    if (this.filter != null) {
      const filterValue = await this.filter.resolve(ctx)
      if (!filterValue) {
        return accumulator ?? []
      }
    }
    if (accumulator === undefined) {
      return [await this.expr.resolve(ctx)]
    }
    accumulator.push(await this.expr.resolve(ctx))
    return accumulator
  }

  result(accumulator: T[] | undefined): T[] {
    if (accumulator === undefined) {
      return []
    }
    return accumulator
  }

  /**
   * Generates a human-readable description of the aggregation.
   */
  describe(): string {
    return `ARRAY_AGG(${this.expr.describe()})`
  }

  /**
   * Generates a json representation of the aggregation.
   */
  toJSON(): Json {
    return { type: "array_agg", expr: this.expr.toJSON() }
  }

  getType(): ArrayColumnType<T> {
    return this.expr.getType().array()
  }
}

/**
 * A FirstAggregation is an aggregation that returns the first value of an
 * expression.
 */
export class FirstAggregation<ColT extends ColumnType>
  extends ExprAggregation<ColValueOf<ColT>, ColT> {
  /**
   * Creates a new FirstAggregation with the given expression.
   */
  constructor(expr: Expr<ColT>) {
    super("FIRST", expr, {
      update: (accumulator, ctx) => {
        if (accumulator === undefined) {
          return this.expr.resolve(ctx)
        }
        return accumulator
      },
    })
  }
}

/**
 * A MultiAggregation is an aggregation that combines multiple aggregations
 * into an object.
 */
export class MultiAggregation<T extends UnknownRecord>
  implements Aggregation<T> {
  /**
   * Creates a new MultiAggregation with the given aggregations.
   */
  constructor(readonly aggregations: { [K in keyof T]: Aggregation<T[K]> }) {}

  getType(): ColumnType<T> {
    throw new Error("Record column types are not implemented yet")
  }
  /**
   * Updates the accumulator with the values from the context.
   */
  async update(accumulator: T | undefined, ctx: ExecutionContext): Promise<T> {
    if (accumulator === undefined) {
      accumulator = {} as T
    }
    for (const [key, value] of Object.entries(this.aggregations)) {
      ;(accumulator as UnknownRecord)[key] = await value.update(
        accumulator[key],
        ctx,
      )
    }
    return accumulator
  }

  result(accumulator: T | undefined): T {
    if (accumulator === undefined) {
      accumulator = {} as T
    }
    for (const [key, value] of Object.entries(this.aggregations)) {
      ;(accumulator as UnknownRecord)[key] = value.result(
        accumulator[key],
      )
    }
    return accumulator
  }

  /**
   * Generates a human-readable description of the aggregation.
   */
  describe(): string {
    return `MultiAggregation(${
      Object.entries(this.aggregations).map((
        [key, value],
      ) => `${key}: ${value.describe()}`).join(", ")
    })`
  }

  /**
   * Generates a json representation of the aggregation.
   */
  toJSON(): Json {
    return {
      type: "multi_agg",
      aggregations: Object.fromEntries(
        Object.entries(this.aggregations).map(([key, value]) => [
          key,
          value.toJSON(),
        ]),
      ),
    }
  }
}
