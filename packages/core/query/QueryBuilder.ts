import {
  Column,
  ISchema,
  NullableSchema,
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
  LeftJoin,
  Limit,
  LiteralValueExpr,
  MultiAggregation,
  NotExpr,
  OrderBy,
  TableScan,
} from "./QueryPlanNode.ts"
import type { EmptyObject, Merge, Simplify } from "type-fest"
import { ColumnType, ColumnTypes } from "../schema/columns/ColumnType.ts"
import { Json } from "../types.ts"
import { column } from "../schema/columns/ColumnBuilder.ts"
import { Aggregation } from "./Aggregation.ts"
import { pick } from "@std/collections/pick"

type MergeQBSchema<
  FromSchemasT extends Record<string, ISchema>,
  QB extends IQB,
  SName extends keyof QB["dbSchema"]["schemas"],
> = Merge<FromSchemasT, Pick<QB["dbSchema"]["schemas"], SName>>

type NullableSchemas<SchemasT extends Record<string, ISchema>> = {
  [K in keyof SchemasT]: NullableSchema<SchemasT[K]>
}

/**
 * Same as MergeQBSchema, but makes all the schema being merged nullable
 */
type MergeQBSchemaNullable<
  FromSchemasT extends Record<string, ISchema>,
  QB extends IQB,
  SName extends keyof QB["dbSchema"]["schemas"],
> = Merge<FromSchemasT, NullableSchemas<Pick<QB["dbSchema"]["schemas"], SName>>>

function singleKeyRecord<K extends string, V>(key: K, value: V): Record<K, V> {
  return { [key]: value } as Record<K, V>
}

export interface IQueryableDBSchema<
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
  constructor(
    readonly dbSchema: DBSchemaT,
    private tableExpressions: Record<string, ITQB> = {},
  ) {
  }

  /**
   * Query a particular table in the database. This is roughly
   * equivalent to the `FROM` clause in SQL.
   *
   * For example, to query the `users` table in the database:
   *
   * ```ts
   * import {dbSchema} from "../examples.ts"
   * dbSchema.query().from("users")
   * ```
   */
  from<TName extends Extract<keyof DBSchemaT["schemas"], string>>(
    table: TName,
  ): TableQueryBuilder<this, Pick<DBSchemaT["schemas"], TName>> {
    const schemas = pick(this.dbSchema.schemas, [table])
    if (this.tableExpressions[table]) {
      return new TableQueryBuilder<this, Pick<DBSchemaT["schemas"], TName>>(
        this,
        schemas,
        table,
        this.tableExpressions[table].rootPlan as plan.IQueryPlanNode<
          MultiTableRow<Record<TName, ISchema>>
        >,
        [],
      )
    }

    if (this.dbSchema.schemas[table] == null) {
      throw new Error(`Table ${table} not found in schema`)
    }

    return new TableQueryBuilder<this, Pick<DBSchemaT["schemas"], TName>>(
      this,
      schemas,
      table,
      new TableScan(
        this.dbSchema.name,
        table,
      ),
    )
  }

  with<TQB extends ITQB>(func: (qb: this) => TQB): TQB["queryBuilder"] {
    const tqb = func(this)
    return new QueryBuilder(tqb.queryBuilder.dbSchema, {
      ...this.tableExpressions,
      [tqb.rootTable]: tqb,
    })
  }
}

type QBTableNames<QB extends IQB> = Extract<
  keyof QB["dbSchema"]["schemas"],
  string
>

/**
 * @ignore
 * @internal
 */
export class _NeverExpr implements Expr<never> {
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

/**
 * @ignore
 * @internal
 */
export interface ITQB<
  QB extends IQB = IQB,
  SchemasT extends Record<string, ISchema> = Record<string, ISchema>,
> extends IPlanBuilder {
  readonly queryBuilder: QB
  readonly tableSchemas: SchemasT
  readonly rootTable: string
  readonly rootPlan: plan.IQueryPlanNode<MultiTableRow<SchemasT>>
}

type MultiTableRow<SchemasT extends Record<string, ISchema>> = {
  [K in keyof SchemasT]: StoredRecordForTableSchema<SchemasT[K]>
}

/**
 * @ignore
 */
export class TableQueryBuilder<
  QB extends IQB = IQB,
  SchemasT extends Record<string, ISchema> = Record<string, ISchema>,
> implements ITQB<QB, SchemasT> {
  constructor(
    readonly queryBuilder: QB,
    readonly tableSchemas: SchemasT,
    readonly rootTable: string,
    readonly rootPlan: plan.IQueryPlanNode<MultiTableRow<SchemasT>>,
    // TODO: surey this is a nasty type to figure out.
    private joinPredicates: Array<
      [
        string,
        "inner" | "left",
        (e: ExprBuilder<ITQB, never>) => ExprBuilder<ITQB, boolean>,
      ]
    > = [],
  ) {}

  /**
   * Joins the table with another table in the database. This is roughly
   * equivalent to the `INNER JOIN` clause in SQL. When two tables are joined,
   * you can select columns from both tables in the query.
   *
   * For example, to join the `users` table with the `posts` table wherever
   * the `users.id` column is equal to the `posts.authorId` column:
   *
   * ```ts
   * import {dbSchema} from "../examples.ts"
   * dbSchema.query()
   *   .from("users")
   *   .join("posts", (t) => t.column("users.id").eq(t.column("posts.authorId")))
   *   .select({
   *     postTitle: (t) => t.column("posts.title"),
   *     authorName: (t) => t.column("users.name"),
   *   })
   * ```
   */
  join<JoinTableNameT extends QBTableNames<QB>>(
    table: JoinTableNameT,
    on: (
      tqb: ExprBuilder<
        ITQB<QB, MergeQBSchema<SchemasT, QB, JoinTableNameT>>,
        never
      >,
    ) => ExprBuilder<
      ITQB<QB, MergeQBSchema<SchemasT, QB, JoinTableNameT>>,
      boolean
    >,
  ): TableQueryBuilder<QB, MergeQBSchema<SchemasT, QB, JoinTableNameT>> {
    return new TableQueryBuilder<
      QB,
      MergeQBSchema<SchemasT, QB, JoinTableNameT>
    >(
      this.queryBuilder,
      {
        ...this.tableSchemas,
        [table]: this.queryBuilder.dbSchema.schemas[table],
      },
      this.rootTable,
      this.rootPlan,
      [...this.joinPredicates, [table, "inner", on as any]],
    )
  }

  /**
   * Joins the table with another table in the database. This is roughly
   * equivalent to the `LEFT JOIN` clause in SQL. When two tables are joined,
   * you can select columns from both tables in the query.
   *
   * For example, to join the `users` table with the `posts` table wherever
   * the `users.id` column is equal to the `posts.authorId` column:
   *
   * ```ts
   * import {dbSchema} from "../examples.ts"
   * dbSchema.query()
   *   .from("users")
   *   .join("posts", (t) => t.column("users.id").eq(t.column("posts.authorId")))
   *   .select({
   *     postTitle: (t) => t.column("posts.title"),
   *     authorName: (t) => t.column("users.name"),
   *   })
   * ```
   */
  leftJoin<JoinTableNameT extends QBTableNames<QB>>(
    table: JoinTableNameT,
    on: (
      tqb: ExprBuilder<
        ITQB<QB, MergeQBSchemaNullable<SchemasT, QB, JoinTableNameT>>,
        never
      >,
    ) => ExprBuilder<
      ITQB<QB, MergeQBSchemaNullable<SchemasT, QB, JoinTableNameT>>,
      boolean
    >,
  ): TableQueryBuilder<
    QB,
    MergeQBSchemaNullable<SchemasT, QB, JoinTableNameT>
  > {
    const joinSchema = this.queryBuilder.dbSchema.schemas[table]
    const nullableJoinSchema = {
      name: joinSchema.name,
      columnsByName: Object.fromEntries(
        Object.entries(joinSchema.columnsByName).map(([key, value]) =>
          [key, { ...value, type: value.type.nullable() }] as const
        ),
      ),
    }
    return new TableQueryBuilder<
      QB,
      MergeQBSchemaNullable<SchemasT, QB, JoinTableNameT>
    >(
      this.queryBuilder,
      {
        ...this.tableSchemas,
        [table]: nullableJoinSchema,
      } as unknown as MergeQBSchemaNullable<SchemasT, QB, JoinTableNameT>,
      this.rootTable,
      this.rootPlan,
      [...this.joinPredicates, [table, "left", on as any]],
    )
  }

  private whereClause: ExprBuilder<ITQB<QB, SchemasT>, boolean> | undefined

  /**
   * Filters the rows in the table using the given expression.
   * This is roughly equivalent to the `WHERE` clause in SQL.
   *
   * For example, to find the user row with the username "pcardune",
   * you could write:
   *
   * ```ts
   * import {dbSchema} from "../examples.ts"
   * dbSchema.query()
   *   .from("users")
   *   .where((t) => t.column("users.username").eq("pcardune"))
   * ```
   */
  where(
    func: (
      tqb: ExprBuilder<ITQB<QB, SchemasT>, never>,
    ) => ExprBuilder<ITQB<QB, SchemasT>, boolean>,
  ): this {
    this.whereClause = func(new ExprBuilder(this, new _NeverExpr()))
    return this
  }

  private limitClause: number | undefined

  /**
   * Limits the number of rows returned by the query, similar to the
   * `LIMIT` clause in SQL. This is typically used in conjunction with
   * `orderBy`.
   *
   * For example, to get the first 10 posts by a certain author:
   *
   * ```ts
   * import {dbSchema} from "../examples.ts"
   * dbSchema.query()
   *  .from("posts")
   *  .where((t) => t.column("posts.authorId").eq(123))
   *  .orderBy((t) => t.column("posts.createdAt"), "DESC")
   *  .limit(10)
   * ```
   */
  limit(limit: number): this {
    this.limitClause = limit
    return this
  }

  private orderByClauses: {
    expr: ExprBuilder<ITQB<QB, SchemasT>, any>
    order: "ASC" | "DESC"
  }[] = []

  /**
   * Orders the rows in the table by the given expression. This can
   * be used multiple times to order by multiple columns. The `order`
   * parameter specifies whether to order in ascending or descending.
   *
   * For example, to order the posts by the `createdAt` column in
   * descending order:
   *
   * ```ts
   * import {dbSchema} from "../examples.ts"
   * dbSchema.query()
   *   .from("posts")
   *   .orderBy((t) => t.column("posts.createdAt"), "DESC")
   * ```
   */
  orderBy(
    func: (tqb: ExprBuilder<this, never>) => ExprBuilder<this, any>,
    order: "ASC" | "DESC",
  ): this {
    this.orderByClauses.push({
      expr: func(new ExprBuilder(this, new _NeverExpr())),
      order,
    })
    return this
  }

  /**
   * Select columns from the table. This is roughly equivalent to the
   * `SELECT` clause in SQL.
   *
   * For example, to select the `name` and `username` columns from the
   * `users` table:
   *
   * ```ts
   * import {dbSchema} from "../examples.ts"
   * dbSchema.query()
   *   .from("users")
   *   .select({
   *     name: (t) => t.column("users.name"),
   *     username: (t) => t.column("users.username"),
   *   })
   * ```
   */
  select<
    SelectT extends Record<string, ExprBuilder<ITQB<QB, SchemasT>, any>>,
  >(
    selection: {
      [Property in keyof SelectT]: (
        tqb: ExprBuilder<ITQB<QB, SchemasT>, never>,
      ) => SelectT[Property]
    },
  ): SelectBuilder<ITQB<QB, SchemasT>, "$0", SelectT> {
    const mapped = Object.fromEntries(
      Object.entries(selection).map((
        [key, func],
      ) => [key, func(new ExprBuilder(this, new _NeverExpr()))]),
    ) as SelectT

    return new SelectBuilder(this, "$0", mapped)
  }

  /**
   * Groups the rows in the table by the given key. This is roughly
   * equivalent to the `GROUP BY` clause in SQL. This is typically used
   * in conjunction with `aggregate`.
   *
   * For example, to count the number of posts by each author, along with
   * the author's highest post rating:
   * ```ts
   * import {dbSchema} from "../examples.ts"
   * dbSchema.query()
   *   .from("posts")
   *   .groupBy({
   *     authorId: (t) => t.column("posts.authorId"),
   *   })
   *   .aggregate({
   *     count: (agg, t) => agg.count(),
   *     highestRating: (agg, t) => agg.max(t.column("posts.rating")),
   *   })
   * ```
   * When queries, this will return rows of {authorId: string, count: number}.
   * Unlike in SQL, the group by keys are automatically included in the output.
   */
  groupBy<
    GroupKeyT extends Record<string, ExprBuilder<ITQB<QB, SchemasT>, any>>,
  >(
    key: {
      [Property in keyof GroupKeyT]: (
        tqb: ExprBuilder<ITQB<QB, SchemasT>, never>,
      ) => GroupKeyT[Property]
    },
  ): GroupByBuilder<ITQB<QB, SchemasT>, GroupKeyT, EmptyObject> {
    return new GroupByBuilder(
      this,
      Object.fromEntries(
        Object.entries(key).map((
          [key, func],
        ) => [key, func(new ExprBuilder(this, new _NeverExpr()))]),
      ) as GroupKeyT,
      {},
    )
  }

  /**
   * Aggregates the rows in the table. This is roughly equivalent to the
   * `SELECT` clause in SQL, but for aggregate functions.
   *
   * For example, to get the highest rating of all posts:
   * ```ts
   * import {dbSchema} from "../examples.ts"
   * dbSchema.query()
   *   .from("posts")
   *   .aggregate({
   *     count: (agg, t) => agg.max(t.column("posts.rating")),
   *   })
   * ```
   */
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
            new ExprBuilder(this, new _NeverExpr()),
          ),
        ]),
      ) as AggregateT,
    )
  }

  plan(): IQueryPlanNode<MultiTableRow<SchemasT>> {
    type T = MultiTableRow<SchemasT>

    let root: plan.IQueryPlanNode<T> = this.rootPlan
    if (this.joinPredicates.length > 0) {
      for (let i = 0; i < this.joinPredicates.length; i++) {
        const [joinTableName, joinType, on] = this.joinPredicates[i]
        if (joinType === "inner") {
          root = new plan.Join(
            root,
            new TableScan<T>(
              this.queryBuilder.dbSchema.name,
              joinTableName,
            ),
            on(new ExprBuilder(this, new _NeverExpr())).expr,
          )
        } else if (joinType === "left") {
          root = new LeftJoin(
            root,
            new TableScan<T>(
              this.queryBuilder.dbSchema.name,
              joinTableName,
            ),
            on(new ExprBuilder(this, new _NeverExpr())).expr,
          )
        }
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

type SelectConfig = Record<string, ExprBuilder>
type SelectColumns<SelectT extends SelectConfig> = Simplify<
  {
    [K in Extract<keyof SelectT, string>]: Column.Stored.Any<
      K,
      ExprType<SelectT[K]>
    >
  }
>
type SelectSchema<NewAlias extends string, SelectT extends SelectConfig> =
  Simplify<ISchema<NewAlias, SelectColumns<SelectT>>>

class SelectBuilder<
  TQB extends ITQB = ITQB,
  AliasT extends string = string,
  SelectT extends SelectConfig = SelectConfig,
> {
  constructor(
    readonly tqb: TQB,
    readonly alias: AliasT,
    readonly select: SelectT,
  ) {}

  asTable<NewAlias extends string>(
    newAlias: NewAlias,
  ): TableQueryBuilder<
    QueryBuilder<
      IAugmentedDBSchema<
        TQB["queryBuilder"]["dbSchema"],
        SelectSchema<NewAlias, SelectT>
      >
    >,
    Record<NewAlias, SelectSchema<NewAlias, SelectT>>
  > {
    const newSchema = {
      name: newAlias,
      columnsByName: Object.fromEntries(
        Object.entries(this.select).map((
          [key, value],
        ) => [key, column(key, value.expr.getType())]),
      ) as unknown as SelectSchema<NewAlias, SelectT>["columnsByName"],
    }

    const newDBSchema: IAugmentedDBSchema<
      TQB["queryBuilder"]["dbSchema"],
      SelectSchema<NewAlias, SelectT>
    > = augmentDbSchema(this.tqb.queryBuilder.dbSchema, newSchema)
    const newTQB = new TableQueryBuilder(
      new QueryBuilder(newDBSchema),
      singleKeyRecord(newAlias, newSchema),
      newAlias,
      new SelectBuilder(this.tqb, newAlias, this.select).plan() as any,
      [],
    )

    return newTQB
  }

  plan(): plan.Select<
    AliasT,
    {
      [Property in keyof SelectT]: SelectT[Property] extends
        ExprBuilder<infer TQB, infer T> ? T : never
    }
  > {
    return new plan.Select(
      this.tqb.plan(),
      this.alias,
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

type AggConfig = Record<string, plan.Aggregation<any>>
type AggregatedRecord<AggT extends AggConfig> = {
  [Key in keyof AggT]: AggT[Key] extends plan.Aggregation<infer T> ? T
    : never
}
type AggregatedColumns<AggT extends AggConfig> = {
  [K in Extract<keyof AggT, string>]: Column.Stored.Any<
    K,
    AggregationType<AggT[K]>
  >
}
type AggregatedSchema<Name extends string, AggT extends AggConfig> = ISchema<
  Name,
  AggregatedColumns<AggT>
>

class AggregateBuilder<
  TQB extends ITQB = ITQB,
  AggT extends AggConfig = AggConfig,
> implements IPlanBuilder<Record<"$0", AggregatedRecord<AggT>>> {
  constructor(readonly tqb: TQB, readonly aggregations: AggT) {}

  asTable<NewAlias extends string>(newAlias: NewAlias): TableQueryBuilder<
    QueryBuilder<
      IAugmentedDBSchema<
        TQB["queryBuilder"]["dbSchema"],
        AggregatedSchema<NewAlias, AggT>
      >
    >,
    Record<NewAlias, AggregatedSchema<NewAlias, AggT>>
  > {
    const newSchema = {
      name: newAlias,
      columnsByName: Object.fromEntries(
        Object.entries(this.aggregations).map((
          [key, value],
        ) => [key, column(key, value.getType())]),
      ) as unknown as AggregatedColumns<AggT>,
    }

    const newDBSchema: IAugmentedDBSchema<
      TQB["queryBuilder"]["dbSchema"],
      AggregatedSchema<NewAlias, AggT>
    > = augmentDbSchema(this.tqb.queryBuilder.dbSchema, newSchema)
    const newTQB = new TableQueryBuilder(
      new QueryBuilder(newDBSchema),
      singleKeyRecord(newAlias, newSchema),
      newAlias,
      new plan.Aggregate(
        this.tqb.plan(),
        new MultiAggregation(this.aggregations),
        newAlias,
      ) as any, // TODO: fix this
      [],
    )

    return newTQB
  }

  plan(): IQueryPlanNode<Record<"$0", AggregatedRecord<AggT>>> {
    return new plan.Aggregate(
      this.tqb.plan(),
      new MultiAggregation(this.aggregations),
      "$0",
    ) as any // TODO: fix this
  }
}

class GroupByBuilder<
  TQB extends ITQB = ITQB,
  GroupKeyT extends SelectConfig = SelectConfig,
  AggT extends AggConfig = AggConfig,
> {
  constructor(
    readonly tqb: TQB,
    readonly groupKey: GroupKeyT,
    readonly aggregations: AggT,
  ) {}

  /**
   * Aggregates across grouped rows.
   *
   * For example, to count the number of posts by each author, along with
   * the author's highest post rating:
   *
   * ```ts
   * import {type Query} from "@paul-db/core/types"
   * import {dbSchema} from "../examples.ts"
   *
   * type AuthorStats = {
   *   authorId: number,
   *   numPosts: number,
   *   highestRating: number,
   * }
   *
   * const authorStatsQuery: Query<AuthorStats> = dbSchema.query()
   *   .from("posts")
   *   .groupBy({
   *     authorId: (t) => t.column("posts.authorId"),
   *   })
   *   .aggregate({
   *     numPosts: (agg) => agg.count(),
   *     highestRating: (agg, t) => agg.max(t.column("posts.rating")),
   *   })
   * ```
   *
   * When queried, this will return rows of {authorId: string, count: number, highestRating: number}.
   * Unlike in SQL, the group by keys are automatically included in the output.
   */
  aggregate<AggregateT extends AggConfig>(
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
              new ExprBuilder(this.tqb, new _NeverExpr()),
            ),
          ]),
        ) as AggregateT,
      },
    )
  }

  asTable<NewAlias extends string>(newAlias: NewAlias): TableQueryBuilder<
    QueryBuilder<
      IAugmentedDBSchema<
        TQB["queryBuilder"]["dbSchema"],
        ISchema<NewAlias, SelectColumns<GroupKeyT> & AggregatedColumns<AggT>>
      >
    >,
    Record<
      NewAlias,
      ISchema<NewAlias, SelectColumns<GroupKeyT> & AggregatedColumns<AggT>>
    >
  > {
    const selectSchema = {
      name: newAlias,
      columnsByName: Object.fromEntries(
        Object.entries(this.groupKey).map((
          [key, value],
        ) => [key, column(key, value.expr.getType())]),
      ) as unknown as SelectColumns<GroupKeyT>,
    }

    const aggSchema = {
      name: newAlias,
      columnsByName: Object.fromEntries(
        Object.entries(this.aggregations).map((
          [key, value],
        ) => [key, column(key, value.getType())]),
      ) as unknown as AggregatedColumns<AggT>,
    }

    const newSchema = {
      name: newAlias,
      columnsByName: {
        ...selectSchema.columnsByName,
        ...aggSchema.columnsByName,
      } as SelectColumns<GroupKeyT> & AggregatedColumns<AggT>,
    }

    const newDBSchema: IAugmentedDBSchema<
      TQB["queryBuilder"]["dbSchema"],
      ISchema<NewAlias, SelectColumns<GroupKeyT> & AggregatedColumns<AggT>>
    > = augmentDbSchema(this.tqb.queryBuilder.dbSchema, newSchema)
    const newTQB = new TableQueryBuilder(
      new QueryBuilder(newDBSchema),
      singleKeyRecord(newAlias, newSchema),
      newAlias,
      new plan.GroupBy(
        this.tqb.plan(),
        Object.fromEntries(
          Object.entries(this.groupKey).map((
            [key, valueExpr],
          ) => [key, valueExpr.expr]),
        ),
        new MultiAggregation(this.aggregations),
        newAlias,
      ) as any,
      [],
    )

    return newTQB
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
      "$0",
    ) as any // TODO: fix this
  }
}

export type SchemasForTQB<TQB extends ITQB> = TQB["tableSchemas"]

export type ColumnNames<
  TQB extends ITQB,
  SchemaName extends keyof SchemasForTQB<TQB>,
> = TableSchemaColumnNames<SchemasForTQB<TQB>[SchemaName]>
export type TQBTableNames<TQB extends ITQB> = Extract<
  keyof TQB["tableSchemas"],
  string
>

export type ColumnWithName<
  TQB extends ITQB,
  SchemaName extends keyof SchemasForTQB<TQB>,
  CName extends ColumnNames<TQB, SchemaName>,
> = SchemasForTQB<TQB>[SchemaName]["columnsByName"][CName]

type AggregationType<T extends Aggregation<any>> = T extends
  Aggregation<infer T> ? T : never

type ExprType<T extends ExprBuilder> = T extends ExprBuilder<infer TQB, infer T>
  ? T
  : never

/**
 * @ignore
 * @internal
 */
export class ExprBuilder<TQB extends ITQB = ITQB, T = any> {
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
    const schema = this.tqb.tableSchemas[table]
    const columnSchema = schema.columnsByName[column]
    if (columnSchema == null) {
      throw new Error(`Column ${column} not found in table ${table}`)
    }
    const ref = new ColumnRefExpr(columnSchema, table)
    return new ExprBuilder(this.tqb, ref)
  }

  in(
    ...values: Array<ExprBuilder<TQB, T> | T>
  ): ExprBuilder<TQB, boolean> {
    const expr = new In(
      this.expr,
      values.map((v) =>
        v instanceof ExprBuilder
          ? v.expr
          : new LiteralValueExpr(v, this.expr.getType())
      ),
    )
    return new ExprBuilder(this.tqb, expr)
  }

  private compare(
    operator: CompareOperator,
    value: ExprBuilder<TQB, T> | ExprBuilder<TQB, T | null> | T,
  ): ExprBuilder<TQB, boolean> {
    const expr = new Compare(
      this.expr as Expr<T | null>,
      operator,
      (value instanceof ExprBuilder
        ? value.expr
        : new LiteralValueExpr(value, this.expr.getType())) as Expr<T | null>,
    )
    return new ExprBuilder(this.tqb, expr)
  }

  eq(
    value: ExprBuilder<TQB, T> | ExprBuilder<TQB, T | null> | T,
  ): ExprBuilder<TQB, boolean> {
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

  literal(value: boolean): ExprBuilder<TQB, boolean>
  literal(value: string): ExprBuilder<TQB, string>
  literal(value: number): ExprBuilder<TQB, number>
  literal<T>(value: T, type: ColumnType<T>): ExprBuilder<TQB, T>
  literal<T>(value: T, type?: ColumnType<T>): ExprBuilder<TQB, T> {
    if (type == null) {
      if (typeof value === "boolean") {
        type = ColumnTypes.boolean() as unknown as ColumnType<T>
      } else if (typeof value === "string") {
        type = ColumnTypes.string() as unknown as ColumnType<T>
      } else if (typeof value === "number") {
        if (Number.isInteger(value)) {
          type = ColumnTypes.int32() as unknown as ColumnType<T>
        } else {
          type = ColumnTypes.float() as unknown as ColumnType<T>
        }
      }
    }
    if (type == null) {
      throw new Error(
        `Type must be provided for literal ${JSON.stringify(value)}`,
      )
    }
    if (!type.isValid(value)) {
      throw new Error(`Value ${value} is not valid for type ${type.name}`)
    }
    const expr = new LiteralValueExpr(value, type)
    return new ExprBuilder(this.tqb, expr)
  }

  subquery<T>(
    func: (
      qb: SubQueryBuilder<TQB["queryBuilder"], TQB["tableSchemas"], TQB>,
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
  TableNamesT extends Record<string, ISchema> = Record<string, ISchema>,
  TQB extends ITQB<QB, TableNamesT> = ITQB<QB, TableNamesT>,
> implements IQB<QB["dbSchema"]> {
  constructor(readonly tqb: TQB) {}

  get dbSchema(): TQB["queryBuilder"]["dbSchema"] {
    return this.tqb.queryBuilder.dbSchema
  }

  from<TName extends QBTableNames<QB>>(
    table: TName,
  ): TableQueryBuilder<
    IQB<QB["dbSchema"]>,
    MergeQBSchema<TableNamesT, QB, TName>
  > {
    return new TableQueryBuilder<
      IQB<QB["dbSchema"]>,
      MergeQBSchema<TableNamesT, QB, TName>
    >(
      this,
      {
        ...this.tqb.tableSchemas,
        [table]: this.dbSchema.schemas[table],
      },
      table,
      new TableScan(
        this.dbSchema.name,
        table,
      ),
    )
  }
}

type IAugmentedDBSchema<
  DBSchemaT extends IQueryableDBSchema,
  SchemasT extends ISchema,
> = IQueryableDBSchema<
  DBSchemaT["name"],
  DBSchemaT["schemas"] & { [K in SchemasT["name"]]: SchemasT }
>

function augmentDbSchema<
  DBSchemaT extends IQueryableDBSchema,
  SchemaT extends ISchema,
>(
  dbSchema: DBSchemaT,
  schema: ISchema,
): IAugmentedDBSchema<DBSchemaT, SchemaT> {
  return {
    name: dbSchema.name,
    schemas: {
      ...dbSchema.schemas,
      [schema.name]: schema,
    },
  } as IAugmentedDBSchema<DBSchemaT, SchemaT>
}
