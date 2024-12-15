import SQLParser from "node-sql-parser"
import { IQueryPlanNode, Limit } from "@paul-db/core/planner"

export function handleLimit(
  rootPlan: IQueryPlanNode,
  limitAst: SQLParser.Limit,
): IQueryPlanNode {
  if (limitAst.value.length == 0) {
    // no-op
    return rootPlan
  }
  if (limitAst.value.length !== 1) {
    throw new Error(
      `Only single limit value is supported: ${
        JSON.stringify(limitAst, null, 2)
      }`,
    )
  }

  const limit = limitAst.value[0]

  if (limit.type !== "number") {
    throw new Error("Only number limit value is supported")
  }

  return new Limit(rootPlan, limit.value)
}
