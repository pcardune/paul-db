import { ColumnType, ColumnTypes } from "../schema/columns/ColumnType.ts"
import { Json } from "../types.ts"
import type { ExecutionContext, Expr } from "./QueryPlanNode.ts"
import type { Promisable, UnknownRecord } from "type-fest"

/**
 * An aggregation is a computation that summarizes a set of rows into a single
 * value.
 */
export interface Aggregation<AccT> {
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
  getType(): ColumnType<AccT>
}

abstract class ExprAggregation<AccT> implements Aggregation<AccT> {
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
  constructor(readonly type: string, readonly expr: Expr<AccT>, {
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

  getType(): ColumnType<AccT> {
    return this.expr.getType()
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
export class MaxAggregation<T> extends ExprAggregation<T> {
  /**
   * Creates a new MaxAggregation with the given expression.
   */
  constructor(expr: Expr<T>) {
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
export class MinAggregation<T> extends ExprAggregation<T> {
  /**
   * Creates a new MinAggregation with the given expression.
   */
  constructor(expr: Expr<T>) {
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
  constructor(expr: Expr<number>) {
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
export class ArrayAggregation<T> implements Aggregation<T[]> {
  /**
   * Creates a new ArrayAggregation with the given expression.
   */
  constructor(readonly expr: Expr<T>) {}

  /**
   * Updates the accumulator with the values from the context.
   */
  async update(
    accumulator: T[] | undefined,
    ctx: ExecutionContext,
  ): Promise<T[]> {
    if (accumulator === undefined) {
      return [await this.expr.resolve(ctx)]
    }
    accumulator.push(await this.expr.resolve(ctx))
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

  getType(): ColumnType<T[]> {
    return this.expr.getType().array()
  }
}

/**
 * A FirstAggregation is an aggregation that returns the first value of an
 * expression.
 */
export class FirstAggregation<T> extends ExprAggregation<T> {
  /**
   * Creates a new FirstAggregation with the given expression.
   */
  constructor(expr: Expr<T>) {
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
