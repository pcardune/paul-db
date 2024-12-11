import { ColumnType } from "../mod.ts"
import { Column, TableSchemaColumns } from "../schema/schema.ts"
import { DBSchema } from "../schema/DBSchema.ts"
import {
  AndOrExpr,
  ColumnRefExpr,
  Compare,
  CompareOperator,
  Expr,
  Filter,
  IQueryPlanNode,
  LiteralValueExpr,
  TableScan,
} from "./QueryPlanNode.ts"

export class QueryBuilder<DBSchemaT extends DBSchema = DBSchema> {
  constructor(readonly dbSchema: DBSchemaT) {}

  scan<TName extends Extract<keyof DBSchemaT["schemas"], string>>(
    table: TName,
  ): TableQueryBuilder<this, TName> {
    return new TableQueryBuilder(this, table)
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

class TableQueryBuilder<
  QB extends QueryBuilder = QueryBuilder,
  TableName extends TableNames<QB> = TableNames<QB>,
> {
  constructor(readonly queryBuilder: QB, readonly tableName: TableName) {}

  private whereClause: ExprBuilder<this, boolean> | undefined
  where(
    func: (tqb: ExprBuilder<this, never>) => ExprBuilder<this, boolean>,
  ): this {
    this.whereClause = func(new ExprBuilder(this, new NeverExpr()))
    return this
  }

  plan(): IQueryPlanNode {
    const tableScan = new TableScan("default", this.tableName)
    if (this.whereClause == null) {
      return tableScan
    }
    return new Filter(tableScan, this.whereClause.expr)
  }
}

type SchemaForTQB<TQB extends TableQueryBuilder> =
  TQB["queryBuilder"]["dbSchema"]["schemas"][TQB["tableName"]]
type ColumnNames<TQB extends TableQueryBuilder> = TableSchemaColumns<
  SchemaForTQB<TQB>
>["name"]
type ColumnWithName<
  TQB extends TableQueryBuilder,
  CName extends ColumnNames<TQB>,
> = Column.FindWithName<SchemaForTQB<TQB>["columns"], CName>

class ExprBuilder<
  TQB extends TableQueryBuilder = TableQueryBuilder,
  T = unknown,
> {
  constructor(readonly tqb: TQB, readonly expr: Expr<T>) {}

  column<CName extends ColumnNames<TQB>>(
    column: CName,
  ): ExprBuilder<TQB, Column.GetOutput<ColumnWithName<TQB, CName>>> {
    const ref = new ColumnRefExpr(
      this.tqb.queryBuilder.dbSchema
        .schemas[this.tqb.tableName].getColumnByNameOrThrow(
          column,
        ),
    ) as ColumnRefExpr<ColumnWithName<TQB, CName>>
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