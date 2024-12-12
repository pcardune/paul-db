import type { ExecutionContext, Expr, RowData } from "./QueryPlanNode.ts"
import { Promisable, UnknownRecord } from "npm:type-fest"

export interface Aggregation<T> {
  init(): T
  update(accumulator: T, row: RowData, ctx: ExecutionContext): Promisable<T>
  result(accumulator: T): T
  describe(): string
}

export class CountAggregation implements Aggregation<number> {
  init(): number {
    return 0
  }
  update(accumulator: number): number {
    return accumulator + 1
  }
  result(accumulator: number): number {
    return accumulator
  }
  describe(): string {
    return "COUNT(*)"
  }
}

export class MaxAggregation<T> implements Aggregation<T> {
  constructor(readonly expr: Expr<T>) {}

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

  async update(
    accumulator: T,
    row: RowData,
    ctx: ExecutionContext,
  ): Promise<T> {
    const value = await this.expr.resolve(row, ctx)
    return this.expr.getType().compare(value, accumulator) > 0
      ? value
      : accumulator
  }

  result(accumulator: T): T {
    return accumulator
  }

  describe(): string {
    return `MAX(${this.expr.describe()})`
  }
}

export class ArrayAggregation<T> implements Aggregation<T[]> {
  constructor(readonly expr: Expr<T>) {}

  init(): T[] {
    return []
  }

  async update(
    accumulator: T[],
    row: RowData,
    ctx: ExecutionContext,
  ): Promise<T[]> {
    accumulator.push(await this.expr.resolve(row, ctx))
    return accumulator
  }

  result(accumulator: T[]): T[] {
    return accumulator
  }

  describe(): string {
    return `ARRAY_AGG(${this.expr.describe()})`
  }
}

export class MultiAggregation<T extends UnknownRecord>
  implements Aggregation<T> {
  constructor(readonly aggregations: { [K in keyof T]: Aggregation<T[K]> }) {}

  init(): T {
    return Object.fromEntries(
      Object.entries(this.aggregations).map((
        [key, value],
      ) => [key, value.init()]),
    ) as T
  }

  async update(accumulator: T, row: RowData): Promise<T> {
    for (const [key, value] of Object.entries(this.aggregations)) {
      ;(accumulator as UnknownRecord)[key] = await value.update(
        accumulator[key],
        row,
      )
    }
    return accumulator
  }

  result(accumulator: T): T {
    return accumulator
  }

  withAggregation<CName extends string, ValueT>(
    name: CName,
    aggr: Aggregation<ValueT>,
  ): MultiAggregation<T & { [name in CName]: ValueT }> {
    return new MultiAggregation({
      ...this.aggregations,
      [name]: aggr,
    }) as MultiAggregation<T & { [name in CName]: ValueT }>
  }

  describe(): string {
    return `MultiAggregation(${
      Object.entries(this.aggregations).map((
        [key, value],
      ) => `${key}: ${value.describe()}`).join(", ")
    })`
  }
}
