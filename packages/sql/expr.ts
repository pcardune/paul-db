import { plan } from "../core/mod.ts"
import { Column, SomeTableSchema } from "../core/schema/schema.ts"
import {
  AmbiguousError,
  ColumnNotFoundError,
  NotImplementedError,
} from "./errors.ts"
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
  return new plan.ColumnRefExpr(columnSchema, schema.name)
}

export function parseExpr(
  schemas: Record<string, SomeTableSchema>,
  expr: SQLParser.ExpressionValue | SQLParser.ExprList,
): plan.Expr<any> {
  if (!isExpressionValue(expr)) {
    throw new NotImplementedError(
      `Only single expressions are supported in WHERE clause`,
    )
  }

  if (isColumnRefItem(expr)) {
    const columnName = typeof expr.column === "string"
      ? expr.column
      : typeof expr.column.expr.value === "string"
      ? expr.column.expr.value
      : null
    if (columnName == null) {
      throw new NotImplementedError(
        `Only string column names are supported in WHERE clause`,
      )
    }
    let schema: SomeTableSchema
    if (expr.table == null) {
      // find all the schemas with a matching column name
      const matchingSchemas = Object.values(schemas).filter((schema) =>
        schema.getColumnByName(columnName) != null
      )
      if (matchingSchemas.length === 0) {
        throw new ColumnNotFoundError(
          `Column ${columnName} not found in tables ${
            Object.keys(schemas).join(", ")
          }`,
        )
      }
      if (matchingSchemas.length > 1) {
        throw new AmbiguousError(
          `Column ${columnName} is ambiguous. Found in multiple tables: ${
            matchingSchemas.map((s) => s.name).join(", ")
          }`,
        )
      }
      schema = matchingSchemas[0]
    } else {
      schema = schemas[expr.table]
      if (schema == null) {
        throw new ColumnNotFoundError(`Table ${expr.table} not available here`)
      }
    }
    return columnRef(schema, columnName)
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
    const leftExpr = parseExpr(schemas, expr.left)
    const rightExpr = parseExpr(schemas, expr.right)

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
