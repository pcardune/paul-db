import { plan } from "../core/mod.ts"
import { Column, SomeTableSchema } from "../core/schema/schema.ts"
import { ColumnNotFoundError, NotImplementedError } from "./errors.ts"
import SQLParser from "npm:node-sql-parser"
import {
  isBinary,
  isColumnRefItem,
  isExpressionValue,
  isValue,
} from "./parser.ts"
import { ColumnTypes } from "../core/schema/columns/ColumnType.ts"

function columnRef(
  schema: SomeTableSchema,
  column: string,
): plan.ColumnRefExpr<Column.Any> {
  const columnSchema = schema.getColumnByName(column)
  if (columnSchema == null) {
    throw new ColumnNotFoundError(
      `Column ${column} not found in table ${schema.name}`,
    )
  }
  return new plan.ColumnRefExpr(columnSchema)
}

export function parseExpr(
  schema: SomeTableSchema,
  expr: SQLParser.ExpressionValue | SQLParser.ExprList,
): plan.Expr<boolean> {
  if (!isExpressionValue(expr)) {
    throw new NotImplementedError(
      `Only single expressions are supported in WHERE clause`,
    )
  }

  if (isColumnRefItem(expr)) {
    if (expr.table != null) {
      throw new NotImplementedError(
        `Only column references without table are supported in WHERE clause`,
      )
    }
    if (typeof expr.column !== "string") {
      throw new NotImplementedError(
        `Only string column names are supported in WHERE clause`,
      )
    }
    return columnRef(schema, expr.column)
  }

  if (isValue(expr)) {
    if (expr.type === "number") {
      return new plan.LiteralValueExpr(
        expr.value,
        ColumnTypes.float(),
      )
    } else if (expr.type === "single_quote_string") {
      return new plan.LiteralValueExpr(
        expr.value,
        ColumnTypes.string(),
      )
    } else {
      throw new NotImplementedError(
        `Type ${expr.type} not supported in WHERE clause`,
      )
    }
  }

  if (isBinary(expr)) {
    const leftExpr = parseExpr(schema, expr.left)
    const rightExpr = parseExpr(schema, expr.right)

    // parse operator
    if (plan.Compare.isSupportedOperator(expr.operator)) {
      return new plan.Compare(
        leftExpr,
        expr.operator,
        rightExpr,
      )
    } else if (plan.AndOrExpr.isSupportedOperator(expr.operator)) {
      return new plan.AndOrExpr(
        leftExpr,
        expr.operator,
        rightExpr,
      )
    } else {
      throw new NotImplementedError(
        `Operator ${expr.operator} not supported in WHERE clause`,
      )
    }
  }

  throw new NotImplementedError(
    `Unrecognized expresion. Found ${JSON.stringify(expr)}`,
  )
}
