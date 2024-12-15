import SQLParser from "node-sql-parser"
import { NotImplementedError } from "./errors.ts"
import * as plan from "@paul-db/core/planner"
import { isAggrFunc, isExprList, isFunction } from "./parser.ts"
import { parseExpr } from "./expr.ts"
import { schema } from "@paul-db/core"

export function parseAggregationColumns(
  astColumns: SQLParser.Column[],
  schemas: Record<string, schema.SomeTableSchema>,
) {
  let multiAgg = new plan.MultiAggregation({})
  for (const astColumn of astColumns) {
    let colName: string | null = null
    if (astColumn.as != null) {
      if (typeof astColumn.as !== "string") {
        throw new NotImplementedError(
          `Only string column names are supported`,
        )
      }
      colName = astColumn.as
    }

    const exprAst = astColumn.expr
    if (isAggrFunc(exprAst)) {
      if (exprAst.name === "MAX") {
        const expr = parseExpr(schemas, exprAst.args.expr)
        const aggregation = new plan.MaxAggregation(expr)
        multiAgg = multiAgg.withAggregation(
          colName ?? aggregation.describe(),
          aggregation,
        )
      } else if (exprAst.name === "COUNT") {
        const aggregation = new plan.CountAggregation()
        multiAgg = multiAgg.withAggregation(
          colName ?? aggregation.describe(),
          aggregation,
        )
      } else if (exprAst.name === "ARRAY_AGG") {
        const expr = parseExpr(schemas, exprAst.args.expr)
        const aggregation = new plan.ArrayAggregation(expr)
        multiAgg = multiAgg.withAggregation(
          colName ?? aggregation.describe(),
          aggregation,
        )
      } else {
        throw new NotImplementedError(
          `Aggregate function ${exprAst.name} not supported: ${
            JSON.stringify(exprAst)
          }`,
        )
      }
    } else if (isFunction(exprAst)) {
      if (exprAst.name.name.length != 1) {
        throw new NotImplementedError(
          `Only one function name allowed, found: ${
            JSON.stringify(exprAst.name.name)
          }`,
        )
      }
      const funcName = exprAst.name.name[0]
      if (funcName.type !== "default") {
        throw new NotImplementedError(
          `Only default function names allowed, found: ${
            JSON.stringify(funcName)
          }`,
        )
      }
      if (funcName.value === "ARRAY_AGG") {
        if (exprAst.args == null) {
          throw new NotImplementedError(
            `ARRAY_AGG must have arguments, found: ${JSON.stringify(exprAst)}`,
          )
        }
        if (!isExprList(exprAst.args)) {
          throw new NotImplementedError(
            `ARRAY_AGG arguments must be an expr_list, found: ${
              JSON.stringify(exprAst.args)
            }`,
          )
        }
        if (exprAst.args.value.length != 1) {
          throw new NotImplementedError(
            `ARRAY_AGG must have exactly one argument, found: ${
              JSON.stringify(exprAst.args.value)
            }`,
          )
        }
        const aggregation = new plan.ArrayAggregation(
          parseExpr(schemas, exprAst.args.value[0]),
        )
        multiAgg = multiAgg.withAggregation(
          colName ?? aggregation.describe(),
          aggregation,
        )
      }
    } else {
      throw new NotImplementedError(
        `Only aggregate functions are supported in aggregation queries found ${
          JSON.stringify(exprAst)
        }`,
      )
    }
  }
  return multiAgg
}
