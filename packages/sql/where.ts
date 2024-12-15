import { Filter, IQueryPlanNode } from "@paul-db/core/planner"
import { schema } from "@paul-db/core"
import { NotImplementedError } from "./errors.ts"
import SQLParser from "node-sql-parser"
import { parseExpr } from "./expr.ts"

export function handleWhere(
  rootPlan: IQueryPlanNode,
  schemas: Record<string, schema.SomeTableSchema>,
  where: SQLParser.Binary | SQLParser.Function,
): Filter {
  const ast = { where }
  if (ast.where.type != "binary_expr") {
    throw new NotImplementedError(
      `Only binary expressions are supported in WHERE clause`,
    )
  }

  const predicate = parseExpr(schemas, ast.where)
  return new Filter(
    rootPlan,
    predicate,
  )
}
