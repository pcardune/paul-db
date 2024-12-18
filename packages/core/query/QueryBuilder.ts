import {
  Column,
  ISchema,
  StoredRecordForTableSchema,
  TableSchemaColumnNames,
} from "../schema/TableSchema.ts"
import * as plan from "./QueryPlanNode.ts"
import {
  AndOrExpr,
  ColumnRefExpr,
  Compare,
  CompareOperator,
  Expr,
  Filter,
  In,
  IQueryPlanNode,
  Limit,
  LiteralValueExpr,
  MultiAggregation,
  NotExpr,
  OrderBy,
  TableScan,
} from "./QueryPlanNode.ts"
import type { EmptyObject, NonEmptyTuple, TupleToUnion } from "type-fest"
import { ColumnType } from "../schema/columns/ColumnType.ts"
import { Json } from "../types.ts"

interface IQueryableDBSchema<
  DBNameT extends string = string,
  SchemasT extends Record<string, ISchema> = Record<string, ISchema>,
> {
  readonly name: DBNameT
  readonly schemas: SchemasT
}

/**
 * @ignore
 */
export interface IQB<
  DBSchemaT extends IQueryableDBSchema = IQueryableDBSchema,
> {
  readonly dbSchema: DBSchemaT
}

/**
 * Provides a type safe way to build query plans from a schema.
 */
export class QueryBuilder<
  DBSchemaT extends IQueryableDBSchema = IQueryableDBSchema,
> implements IQB<DBSchemaT> {
  /**
   * Constructs a new query builder from the given schema.
   */
  constructor(readonly dbSchema: DBSchemaT) {}

  /**
   * Adds a "FROM" clause to the query.
   * @param table The table to query from
   */
  from<TName extends Extract<keyof DBSchemaT["schemas"], string>>(
    table: TName,
  ): TableQueryBuilder<this, [TName]> {
    return new TableQueryBuilder(this, [table], table) as TableQueryBuilder<
      this,
      [TName]
    >
  }
}

type QBTableNames<QB extends IQB> = Extract<
  keyof QB["dbSchema"]["schemas"],
  string
>

class NeverExpr implements Expr<never> {
  resolve(): never {
    throw new Error("This should never be called")
  }
  getType(): ColumnType<never> {
    throw new Error("This should never be called")
  }
  describe(): string {
    return "never"
  }
  toJSON(): Json {
    return { type: "never" }
  }
}

export interface IPlanBuilder<T extends plan.RowData = plan.RowData> {
  plan(): IQueryPlanNode<T>
}

interface ITQB<
  QB extends IQB = IQB,
  TableNamesT extends NonEmptyTuple<string> = NonEmptyTuple<string>,
> extends IPlanBuilder {
  readonly queryBuilder: QB
  readonly tableNames: TableNamesT
}

/**
 * @ignore
 */
export class TableQueryBuilder<
  QB extends IQB = IQB,
  TableNamesT extends NonEmptyTuple<string> = NonEmptyTuple<string>,
> implements ITQB<QB, TableNamesT> {
  constructor(
    readonly queryBuilder: QB,
    readonly tableNames: TableNamesT,
    readonly rootTable: string,
    // TODO: surey this is a nasty type to figure out.
    private joinPredicates: Array<
      [string, (e: ExprBuilder<ITQB, never>) => ExprBuilder<ITQB, boolean>]
    > = [],
  ) {}

  join<JoinTableNameT extends QBTableNames<QB>>(
    table: JoinTableNameT,
    on: (
      tqb: ExprBuilder<
        ITQB<QB, [...TableNamesT, JoinTableNameT]>,
        never
      >,
    ) => ExprBuilder<ITQB<QB, [...TableNamesT, JoinTableNameT]>, boolean>,
  ): TableQueryBuilder<QB, [...TableNamesT, JoinTableNameT]> {
    return new TableQueryBuilder(
      this.queryBuilder,
      [...this.tableNames, table] as [...TableNamesT, JoinTableNameT],
      this.rootTable,
      [...this.joinPredicates, [table, on as any]],
    )
  }

  private whereClause: ExprBuilder<ITQB<QB, TableNamesT>, boolean> | undefined

  where(
    func: (
      tqb: ExprBuilder<ITQB<QB, TableNamesT>, never>,
    ) => ExprBuilder<ITQB<QB, TableNamesT>, boolean>,
  ): this {
    this.whereClause = func(new ExprBuilder(this, new NeverExpr()))
    return this
  }

  private limitClause: number | undefined
  limit(limit: number): this {
    this.limitClause = limit
    return this
  }

  private orderByClauses: {
    expr: ExprBuilder<ITQB<QB, TableNamesT>, any>
    order: "ASC" | "DESC"
  }[] = []
  orderBy(
    func: (tqb: ExprBuilder<this, never>) => ExprBuilder<this, any>,
    order: "ASC" | "DESC",
  ): this {
    this.orderByClauses.push({
      expr: func(new ExprBuilder(this, new NeverExpr())),
      order,
    })
    return this
  }

  select<
    SelectT extends Record<string, ExprBuilder<ITQB<QB, TableNamesT>, any>>,
  >(
    selection: {
      [Property in keyof SelectT]: (
        tqb: ExprBuilder<ITQB<QB, TableNamesT>, never>,
      ) => SelectT[Property]
    },
  ): SelectBuilder<ITQB<QB, TableNamesT>, SelectT> {
    const mapped = Object.fromEntries(
      Object.entries(selection).map((
        [key, func],
      ) => [key, func(new ExprBuilder(this, new NeverExpr()))]),
    ) as SelectT
    return new SelectBuilder(this, mapped)
  }

  groupBy<
    GroupKeyT extends Record<string, ExprBuilder<ITQB<QB, TableNamesT>, any>>,
  >(
    key: {
      [Property in keyof GroupKeyT]: (
        tqb: ExprBuilder<ITQB<QB, TableNamesT>, never>,
      ) => GroupKeyT[Property]
    },
  ): GroupByBuilder<ITQB<QB, TableNamesT>, GroupKeyT, EmptyObject> {
    return new GroupByBuilder(
      this,
      Object.fromEntries(
        Object.entries(key).map((
          [key, func],
        ) => [key, func(new ExprBuilder(this, new NeverExpr()))]),
      ) as GroupKeyT,
      {},
    )
  }

  aggregate<
    AggregateT extends Record<string, plan.Aggregation<any>>, // TODO: fix this any
  >(
    aggregations: {
      [K in keyof AggregateT]: (
        aggFuncs: AggregationFuncs<this>,
        exprFuncs: ExprBuilder<this, never>,
      ) => AggregateT[K]
    },
  ): AggregateBuilder<this, AggregateT> {
    return new AggregateBuilder(
      this,
      Object.fromEntries(
        Object.entries(aggregations).map((
          [key, func],
        ) => [
          key,
          func(
            new AggregationFuncs(this),
            new ExprBuilder(this, new NeverExpr()),
          ),
        ]),
      ) as AggregateT,
    )
  }

  plan(): IQueryPlanNode<
    {
      [K in TupleToUnion<TableNamesT>]: StoredRecordForTableSchema<
        QB["dbSchema"]["schemas"][K]
      >
    }
  > {
    type T = {
      [K in TupleToUnion<TableNamesT>]: StoredRecordForTableSchema<
        QB["dbSchema"]["schemas"][K]
      >
    }

    let root: plan.IQueryPlanNode<T> = new TableScan<T>(
      this.queryBuilder.dbSchema.name,
      this.rootTable,
    )
    if (this.joinPredicates.length > 0) {
      for (let i = 0; i < this.joinPredicates.length; i++) {
        const [joinTableName, on] = this.joinPredicates[i]
        root = new plan.Join(
          root,
          new TableScan<T>(
            this.queryBuilder.dbSchema.name,
            joinTableName,
          ),
          on(new ExprBuilder(this, new NeverExpr())).expr,
        )
      }
    }

    if (this.whereClause != null) {
      root = new Filter(root, this.whereClause.expr)
    }
    if (this.orderByClauses.length > 0) {
      root = new OrderBy(
        root,
        this.orderByClauses.map((c) => ({
          expr: c.expr.expr,
          direction: c.order,
        })),
      )
    }
    if (this.limitClause != null) {
      root = new Limit(root, this.limitClause)
    }
    return root
  }
}

class SelectBuilder<
  TQB extends ITQB = ITQB,
  SelectT extends Record<string, ExprBuilder> = Record<string, ExprBuilder>,
> {
  constructor(readonly tqb: TQB, readonly select: SelectT) {}
  plan(): plan.Select<
    {
      [Property in keyof SelectT]: SelectT[Property] extends
        ExprBuilder<infer TQB, infer T> ? T : never
    }
  > {
    return new plan.Select(
      this.tqb.plan(),
      Object.fromEntries(
        Object.entries(this.select).map((
          [key, valueExpr],
        ) => [key, valueExpr.expr]),
      ),
    )
  }
}

/**
 * Helper for constructing aggregations
 * @ignore
 */
class AggregationFuncs<TQB extends ITQB = ITQB> {
  /**
   * @ignore
   */
  constructor(readonly tqb: TQB) {}

  /**
   * aggregates rows by counting them
   */
  count(): plan.Aggregation<number> {
    return new plan.CountAggregation()
  }

  /**
   * aggregates rows by finding the maximum value
   */
  max<T>(expr: ExprBuilder<TQB, T>): plan.MaxAggregation<T> {
    return new plan.MaxAggregation(expr.expr)
  }

  /**
   * aggregates rows by finding the minimum value
   */
  min<T>(expr: ExprBuilder<TQB, T>): plan.MinAggregation<T> {
    return new plan.MinAggregation(expr.expr)
  }

  /**
   * aggregates rows by summing all values
   */
  sum(expr: ExprBuilder<TQB, number>): plan.SumAggregation {
    return new plan.SumAggregation(expr.expr)
  }

  /**
   * Aggregates rows by collecting all values into an array
   */
  arrayAgg<T>(expr: ExprBuilder<TQB, T>): plan.ArrayAggregation<T> {
    return new plan.ArrayAggregation(expr.expr)
  }

  /**
   * Aggregates rows by taking the first value and ignoring the rest
   */
  first<T>(expr: ExprBuilder<TQB, T>): plan.FirstAggregation<T> {
    return new plan.FirstAggregation(expr.expr)
  }
}

class AggregateBuilder<
  TQB extends ITQB = ITQB,
  AggT extends Record<string, plan.Aggregation<any>> = Record<
    string,
    plan.Aggregation<any>
  >,
> {
  constructor(readonly tqb: TQB, readonly aggregations: AggT) {}

  plan(): IQueryPlanNode<
    Record<
      "$0",
      {
        [Key in keyof AggT]: AggT[Key] extends plan.Aggregation<infer T> ? T
          : never
      }
    >
  > {
    return new plan.Aggregate(
      this.tqb.plan(),
      new MultiAggregation(this.aggregations),
    ) as any // TODO: fix this
  }
}

class GroupByBuilder<
  TQB extends ITQB = ITQB,
  GroupKeyT extends Record<string, ExprBuilder> = Record<string, ExprBuilder>,
  AggT extends Record<string, plan.Aggregation<any>> = Record<
    string,
    plan.Aggregation<any>
  >,
> {
  constructor(
    readonly tqb: TQB,
    readonly groupKey: GroupKeyT,
    readonly aggregations: AggT,
  ) {}

  aggregate<AggregateT extends Record<string, plan.Aggregation<any>>>(
    aggregations: {
      [K in keyof AggregateT]: (
        aggFuncs: AggregationFuncs<TQB>,
        exprFuncs: ExprBuilder<TQB, never>,
      ) => AggregateT[K]
    },
  ): GroupByBuilder<TQB, GroupKeyT, AggT & AggregateT> {
    return new GroupByBuilder(
      this.tqb,
      this.groupKey,
      {
        ...this.aggregations,
        ...Object.fromEntries(
          Object.entries(aggregations).map((
            [key, func],
          ) => [
            key,
            func(
              new AggregationFuncs(this.tqb),
              new ExprBuilder(this.tqb, new NeverExpr()),
            ),
          ]),
        ) as AggregateT,
      },
    )
  }

  plan(): IQueryPlanNode<
    Record<
      "$0",
      & {
        [Property in keyof GroupKeyT]: GroupKeyT[Property] extends
          ExprBuilder<infer TQB, infer T> ? T : never
      }
      & {
        [Key in keyof AggT]: AggT[Key] extends plan.Aggregation<infer T> ? T
          : never
      }
    >
  > {
    return new plan.GroupBy(
      this.tqb.plan(),
      Object.fromEntries(
        Object.entries(this.groupKey).map((
          [key, valueExpr],
        ) => [key, valueExpr.expr]),
      ),
      new MultiAggregation(this.aggregations),
    ) as any // TODO: fix this
  }
}

export type SchemasForTQB<TQB extends ITQB> = Pick<
  TQB["queryBuilder"]["dbSchema"]["schemas"],
  TQBTableNames<TQB>
>

export type ColumnNames<
  TQB extends ITQB,
  SchemaName extends keyof SchemasForTQB<TQB>,
> = TableSchemaColumnNames<SchemasForTQB<TQB>[SchemaName]>
export type TQBTableNames<TQB extends ITQB> = TupleToUnion<TQB["tableNames"]>

export type ColumnWithName<
  TQB extends ITQB,
  SchemaName extends keyof SchemasForTQB<TQB>,
  CName extends ColumnNames<TQB, SchemaName>,
> = SchemasForTQB<TQB>[SchemaName]["columnsByName"][CName]

class ExprBuilder<TQB extends ITQB = ITQB, T = any> {
  constructor(readonly tqb: TQB, readonly expr: Expr<T>) {}

  column<
    TName extends TQBTableNames<TQB>,
    CName extends ColumnNames<TQB, TName>,
  >(
    column: `${TName}.${CName}`,
  ): ExprBuilder<TQB, Column.GetOutput<ColumnWithName<TQB, TName, CName>>>
  column<
    TName extends TQBTableNames<TQB>,
    CName extends ColumnNames<TQB, TName>,
  >(
    table: TName,
    column: CName,
  ): ExprBuilder<TQB, Column.GetOutput<ColumnWithName<TQB, TName, CName>>>
  column<
    TName extends TQBTableNames<TQB>,
    CName extends ColumnNames<TQB, TName>,
  >(
    tableOrColumn: string,
    column?: string,
  ): ExprBuilder<TQB, Column.GetOutput<ColumnWithName<TQB, TName, CName>>> {
    let table: string
    if (column == null) {
      const parts = tableOrColumn.split(".")
      table = parts[0]
      column = parts[1]
    } else {
      table = tableOrColumn
    }
    const schema = this.tqb.queryBuilder.dbSchema.schemas[
      table
    ]
    const columnSchema = schema.columnsByName[column]
    if (columnSchema == null) {
      throw new Error(`Column ${column} not found in table ${table}`)
    }
    const ref = new ColumnRefExpr(columnSchema, table)
    return new ExprBuilder(this.tqb, ref)
  }

  in(
    ...values: NonEmptyTuple<ExprBuilder<TQB, T>> | NonEmptyTuple<T>
  ): ExprBuilder<TQB, boolean> {
    if (values[0] instanceof ExprBuilder) {
      const expr = new In(
        this.expr,
        (values as NonEmptyTuple<ExprBuilder<TQB, T>>).map((v) => v.expr),
      )
      return new ExprBuilder(this.tqb, expr)
    } else {
      const expr = new In(
        this.expr,
        (values as NonEmptyTuple<T>).map((v) =>
          new LiteralValueExpr(v, this.expr.getType())
        ),
      )
      return new ExprBuilder(this.tqb, expr)
    }
  }

  private compare(
    operator: CompareOperator,
    value: ExprBuilder<TQB, T> | T,
  ): ExprBuilder<TQB, boolean> {
    const expr = new Compare(
      this.expr,
      operator,
      value instanceof ExprBuilder
        ? value.expr
        : new LiteralValueExpr(value, this.expr.getType()),
    )
    return new ExprBuilder(this.tqb, expr)
  }

  eq(value: ExprBuilder<TQB, T> | T): ExprBuilder<TQB, boolean> {
    return this.compare("=", value)
  }
  gt(value: ExprBuilder<TQB, T> | T): ExprBuilder<TQB, boolean> {
    return this.compare(">", value)
  }
  gte(value: ExprBuilder<TQB, T> | T): ExprBuilder<TQB, boolean> {
    return this.compare(">=", value)
  }
  lt(value: ExprBuilder<TQB, T> | T): ExprBuilder<TQB, boolean> {
    return this.compare("<", value)
  }
  lte(value: ExprBuilder<TQB, T> | T): ExprBuilder<TQB, boolean> {
    return this.compare("<=", value)
  }
  neq(value: ExprBuilder<TQB, T> | T): ExprBuilder<TQB, boolean> {
    return this.compare("!=", value)
  }

  not(this: ExprBuilder<TQB, boolean>): ExprBuilder<TQB, boolean>
  not(
    this: ExprBuilder<TQB, never>,
    value: ExprBuilder<TQB, boolean>,
  ): ExprBuilder<TQB, boolean>
  not(value?: ExprBuilder<TQB, boolean>): ExprBuilder<TQB, boolean> {
    if (value == null) {
      if (this.expr.getType().name !== "boolean") {
        throw new Error(
          `Expected boolean, got ${this.expr.getType().name} for expression ${this.expr.describe()}`,
        )
      }
      return new ExprBuilder(
        this.tqb,
        new NotExpr(this.expr as unknown as Expr<boolean>),
      )
    }
    return new ExprBuilder(this.tqb, new NotExpr(value.expr))
  }

  private andOr(
    operator: "AND" | "OR",
    value: ExprBuilder<TQB, boolean>,
  ): ExprBuilder<TQB, boolean> {
    if (this.expr.getType().name !== "boolean") {
      throw new Error(
        `Expected boolean, got ${this.expr.getType().name} for expression ${this.expr.describe()}`,
      )
    }
    const expr = new AndOrExpr(
      this.expr as unknown as Expr<boolean>,
      operator,
      value.expr,
    )
    return new ExprBuilder(this.tqb, expr)
  }

  and(value: ExprBuilder<TQB, boolean>): ExprBuilder<TQB, boolean> {
    return this.andOr("AND", value)
  }
  or(value: ExprBuilder<TQB, boolean>): ExprBuilder<TQB, boolean> {
    return this.andOr("OR", value)
  }

  literal<T>(value: T, type: ColumnType<T>): ExprBuilder<TQB, T> {
    const expr = new LiteralValueExpr(value, type)
    return new ExprBuilder(this.tqb, expr)
  }

  subquery<T>(
    func: (
      qb: SubQueryBuilder<TQB["queryBuilder"], TQB["tableNames"], TQB>,
    ) => IPlanBuilder<{ "$0": Record<string, T> }>,
  ): ExprBuilder<TQB, T> {
    return new ExprBuilder(
      this.tqb,
      new plan.SubqueryExpr(func(new SubQueryBuilder(this.tqb)).plan()),
    )
  }
}

export class SubQueryBuilder<
  QB extends IQB = IQB,
  TableNamesT extends NonEmptyTuple<string> = NonEmptyTuple<string>,
  TQB extends ITQB<QB, TableNamesT> = ITQB<QB, TableNamesT>,
> implements IQB<QB["dbSchema"]> {
  constructor(readonly tqb: TQB) {}

  get dbSchema(): TQB["queryBuilder"]["dbSchema"] {
    return this.tqb.queryBuilder.dbSchema
  }

  from<TName extends QBTableNames<QB>>(
    table: TName,
  ): TableQueryBuilder<IQB<QB["dbSchema"]>, [...TableNamesT, TName]> {
    return new TableQueryBuilder(
      this,
      [...this.tqb.tableNames, table],
      table,
    )
  }
}
