import { plan } from "../core/mod.ts"
import { SomeTableSchema } from "../core/schema/schema.ts"
import { NotImplementedError } from "./errors.ts"
import SQLParser from "npm:node-sql-parser"
import { parseExpr } from "./expr.ts"

export function handleWhere(
  rootPlan: plan.IQueryPlanNode,
  schema: SomeTableSchema,
  where: SQLParser.Binary | SQLParser.Function,
): plan.Filter {
  const ast = { where }
  if (ast.where.type != "binary_expr") {
    throw new NotImplementedError(
      `Only binary expressions are supported in WHERE clause`,
    )
  }

  const predicate = parseExpr(schema, ast.where)
  return new plan.Filter(
    rootPlan,
    predicate,
  )
}
