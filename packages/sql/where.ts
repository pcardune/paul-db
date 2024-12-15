import { plan } from "../core/mod.ts"
import { SomeTableSchema } from "../core/schema/TableSchema.ts"
import { NotImplementedError } from "./errors.ts"
import SQLParser from "npm:node-sql-parser"
import { parseExpr } from "./expr.ts"

export function handleWhere(
  rootPlan: plan.IQueryPlanNode,
  schemas: Record<string, SomeTableSchema>,
  where: SQLParser.Binary | SQLParser.Function,
): plan.Filter {
  const ast = { where }
  if (ast.where.type != "binary_expr") {
    throw new NotImplementedError(
      `Only binary expressions are supported in WHERE clause`,
    )
  }

  const predicate = parseExpr(schemas, ast.where)
  return new plan.Filter(
    rootPlan,
    predicate,
  )
}
