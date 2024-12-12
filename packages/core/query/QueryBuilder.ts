import { ColumnType, plan } from "../mod.ts"
import {
  Column,
  StoredRecordForTableSchema,
  TableSchemaColumns,
} from "../schema/schema.ts"
import { DBSchema } from "../schema/DBSchema.ts"
import {
  AndOrExpr,
  ColumnRefExpr,
  Compare,
  CompareOperator,
  Expr,
  Filter,
  IQueryPlanNode,
  Limit,
  LiteralValueExpr,
  OrderBy,
  TableScan,
} from "./QueryPlanNode.ts"
import { NonEmptyTuple, TupleToUnion } from "npm:type-fest"

export class QueryBuilder<DBSchemaT extends DBSchema = DBSchema> {
  constructor(readonly dbSchema: DBSchemaT) {}

  scan<TName extends Extract<keyof DBSchemaT["schemas"], string>>(
    table: TName,
  ): TableQueryBuilder<this, [TName]> {
    return new TableQueryBuilder(this, [table]) as TableQueryBuilder<
      this,
      [TName]
    >
  }
}

type TableNames<QB extends QueryBuilder> = Extract<
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
}

interface ITQB<
  QB extends QueryBuilder = QueryBuilder,
  TableNamesT extends NonEmptyTuple<TableNames<QB>> = NonEmptyTuple<
    TableNames<QB>
  >,
> {
  readonly queryBuilder: QB
  readonly tableNames: TableNamesT
  plan(): IQueryPlanNode
}

class TableQueryBuilder<
  QB extends QueryBuilder = QueryBuilder,
  TableNamesT extends NonEmptyTuple<TableNames<QB>> = NonEmptyTuple<
    TableNames<QB>
  >,
> implements ITQB<QB, TableNamesT> {
  constructor(
    readonly queryBuilder: QB,
    readonly tableNames: TableNamesT,
    // TODO: surey this is a nasty type to figure out.
    private joinPredicates: Array<
      (e: ExprBuilder<ITQB, never>) => ExprBuilder<ITQB, boolean>
    > = [],
  ) {}

  join<JoinTableNameT extends TableNames<QB>>(
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
      [...this.joinPredicates, on as any],
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

  // private selectClause: Record<string, ExprBuilder<any>>
  select<SelectT extends Record<string, ExprBuilder<this, any>>>(
    selection: {
      [Property in keyof SelectT]: (
        tqb: ExprBuilder<this, never>,
      ) => SelectT[Property]
    },
  ): SelectBuilder<this, SelectT> {
    return new SelectBuilder(
      this,
      Object.fromEntries(
        Object.entries(selection).map((
          [key, func],
        ) => [key, func(new ExprBuilder(this, new NeverExpr()))]),
      ) as SelectT,
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
      this.tableNames[0],
    )
    if (this.tableNames.length > 1) {
      for (let i = 1; i < this.tableNames.length; i++) {
        root = new plan.Join(
          root,
          new TableScan<T>(
            this.queryBuilder.dbSchema.name,
            this.tableNames[i],
          ),
          this.joinPredicates[i - 1](new ExprBuilder(this, new NeverExpr()))
            .expr,
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
  plan(): IQueryPlanNode<
    Record<
      "0",
      {
        [Property in keyof SelectT]: SelectT[Property] extends
          ExprBuilder<infer TQB, infer T> ? T : never
      }
    >
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

export type SchemasForTQB<TQB extends ITQB> =
  TQB["queryBuilder"]["dbSchema"]["schemas"][TQBTableNames<TQB>]
type SchemaWithName<TQB extends ITQB, SchemaName extends string> = Extract<
  SchemasForTQB<TQB>,
  { name: SchemaName }
>
export type ColumnNames<
  TQB extends ITQB,
  SchemaName extends string,
> = TableSchemaColumns<SchemaWithName<TQB, SchemaName>>["name"]
export type TQBTableNames<TQB extends ITQB> = TupleToUnion<
  TQB["tableNames"]
>

export type ColumnWithName<
  TQB extends ITQB,
  SchemaName extends SchemasForTQB<TQB>["name"],
  CName extends ColumnNames<TQB, SchemaName>,
> = Column.FindWithName<SchemasForTQB<TQB>["columns"], CName>

class ExprBuilder<
  TQB extends ITQB = ITQB,
  T = any,
> {
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
    const columnSchema = schema.getColumnByNameOrThrow(column)
    const ref = new ColumnRefExpr(columnSchema, table)
    return new ExprBuilder(this.tqb, ref)
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
}
