import SQLParser from "npm:node-sql-parser"
import { plan } from "../core/mod.ts"

export function handleLimit(
  rootPlan: plan.IQueryPlanNode,
  limitAst: SQLParser.Limit,
): plan.Limit {
  if (limitAst.value.length !== 1) {
    throw new Error("Only single limit value is supported")
  }

  const limit = limitAst.value[0]

  if (limit.type !== "number") {
    throw new Error("Only number limit value is supported")
  }

  return new plan.Limit(rootPlan, limit.value)
}
