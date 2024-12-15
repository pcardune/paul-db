import { Json } from "../types.ts"
import type { ExecutionContext, Expr } from "./QueryPlanNode.ts"
import { Promisable, UnknownRecord } from "type-fest"

/**
 * An aggregation is a computation that summarizes a set of rows into a single
 * value.
 */
export interface Aggregation<T> {
  /**
   * Initializes the accumulator with the initial values of the aggregations.
   */
  init(): T
  /**
   * Updates the accumulator with the values from the context.
   */
  update(accumulator: T, ctx: ExecutionContext): Promisable<T>
  /**
   * Returns the result of the aggregation.
   */
  result(accumulator: T): T
  /**
   * Generates a human-readable description of the aggregation.
   */
  describe(): string
  /**
   * Generates a json representation of the aggregation.
   */
  toJSON(): Json
}

/**
 * An aggregation that counts the number of rows
 */
export class CountAggregation implements Aggregation<number> {
  /**
   * Initializes the accumulator with the initial values of the aggregations.
   */
  init(): number {
    return 0
  }
  /**
   * Updates the accumulator with the values from the context.
   */
  update(accumulator: number): number {
    return accumulator + 1
  }
  /**
   * Returns the result of the aggregation.
   */
  result(accumulator: number): number {
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
}

/**
 * An aggregation that computes the maximum value of an expression.
 */
export class MaxAggregation<T> implements Aggregation<T> {
  /**
   * Creates a new MultiAggregation with the given aggregations.
   */
  constructor(readonly expr: Expr<T>) {}

  /**
   * Initializes the accumulator with the initial values of the aggregations.
   */
  init(): T {
    const type = this.expr.getType()
    const minValue = type.minValue
    if (minValue === undefined) {
      throw new Error(
        `Cannot compute max of type ${type.name} because type has no minValue`,
      )
    }
    return minValue
  }

  /**
   * Updates the accumulator with the values from the context.
   */
  async update(
    accumulator: T,
    ctx: ExecutionContext,
  ): Promise<T> {
    const value = await this.expr.resolve(ctx)
    return this.expr.getType().compare(value, accumulator) > 0
      ? value
      : accumulator
  }

  /**
   * Returns the result of the aggregation.
   */
  result(accumulator: T): T {
    return accumulator
  }

  /**
   * Generates a human-readable description of the aggregation.
   */
  describe(): string {
    return `MAX(${this.expr.describe()})`
  }

  /**
   * Generates a json representation of the aggregation.
   */
  toJSON(): Json {
    return { type: "max", expr: this.expr.toJSON() }
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
   * Initializes the accumulator with the initial values of the aggregations.
   */
  init(): T[] {
    return []
  }

  /**
   * Updates the accumulator with the values from the context.
   */
  async update(
    accumulator: T[],
    ctx: ExecutionContext,
  ): Promise<T[]> {
    accumulator.push(await this.expr.resolve(ctx))
    return accumulator
  }

  /**
   * Returns the result of the aggregation.
   */
  result(accumulator: T[]): T[] {
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

  /**
   * Initializes the accumulator with the initial values of the aggregations.
   */
  init(): T {
    return Object.fromEntries(
      Object.entries(this.aggregations).map((
        [key, value],
      ) => [key, value.init()]),
    ) as T
  }

  /**
   * Updates the accumulator with the values from the context.
   */
  async update(accumulator: T, ctx: ExecutionContext): Promise<T> {
    for (const [key, value] of Object.entries(this.aggregations)) {
      ;(accumulator as UnknownRecord)[key] = await value.update(
        accumulator[key],
        ctx,
      )
    }
    return accumulator
  }

  /**
   * Returns the result of the aggregation.
   */
  result(accumulator: T): T {
    return accumulator
  }

  /**
   * Creates a new MultiAggregation with an additional aggregation.
   *
   * @param name name of the aggregation
   * @param aggr the aggregation to add
   */
  withAggregation<CName extends string, ValueT>(
    name: CName,
    aggr: Aggregation<ValueT>,
  ): MultiAggregation<T & { [name in CName]: ValueT }> {
    return new MultiAggregation({
      ...this.aggregations,
      [name]: aggr,
    }) as MultiAggregation<T & { [name in CName]: ValueT }>
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
