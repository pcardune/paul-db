import { Column } from "node-sql-parser"
import { PaulDB, schema } from "@paul-db/core"
import * as plan from "@paul-db/core/planner"
import { parseAggregationColumns } from "./aggregation.ts"
import { NotImplementedError } from "./errors.ts"
import { parseExpr } from "./expr.ts"
import { handleLimit } from "./limit.ts"
import { isColumnRefItem, Select } from "./parser.ts"
import { handleWhere } from "./where.ts"

export async function parseSelect(ast: Select, { dbFile }: PaulDB) {
  if (ast.groupby != null) {
    throw new NotImplementedError(`GROUP BY clause not supported yet`)
  }
  if (ast.having != null) {
    throw new NotImplementedError(`HAVING clause not supported yet`)
  }
  if (ast.distinct == "DISTINCT") {
    throw new NotImplementedError(
      `DISTINCT clause not supported yet: ${
        JSON.stringify(ast.distinct, null, 2)
      }`,
    )
  }
  if (ast.with != null) {
    throw new NotImplementedError(`WITH clause not supported yet`)
  }
  if (ast.window != null) {
    throw new NotImplementedError(`WINDOW clause not supported yet`)
  }
  if (!Array.isArray(ast.from)) {
    throw new NotImplementedError(
      `Only FROM lists are supported. Found ${JSON.stringify(ast.from)}`,
    )
  }
  if (ast.from.length < 1) {
    throw new NotImplementedError(`Must speficy FROM for now`)
  }
  const astFrom = ast.from[0]
  if (!("table" in astFrom)) {
    throw new NotImplementedError(
      `Only table names are supported in FROM lists`,
    )
  }
  const tableScan = new plan.TableScan(
    astFrom.db ? astFrom.db : "default",
    astFrom.table,
  )
  let rootPlan: plan.IQueryPlanNode = tableScan
  const schemas: Record<string, schema.SomeTableSchema> = {}
  schemas[astFrom.table] = await tableScan.getSchema(dbFile)
  if (ast.from.length > 1) {
    // we're doing joins!
    for (let i = 1; i < ast.from.length; i++) {
      const joinAst = ast.from[i]
      if (!("table" in joinAst)) {
        throw new NotImplementedError(
          `Only table names are supported in JOIN lists`,
        )
      }
      if ("join" in joinAst) {
        if (joinAst.on == null) {
          throw new NotImplementedError(
            `JOIN without ON clause not supported`,
          )
        }
        const joinTableScan = new plan.TableScan(
          joinAst.db ? joinAst.db : "default",
          joinAst.table,
        )
        schemas[joinAst.table] = await joinTableScan.getSchema(dbFile)
        rootPlan = new plan.Join(
          rootPlan,
          joinTableScan,
          parseExpr(schemas, joinAst.on),
        )
      } else {
        throw new NotImplementedError(`Only JOINs are supported`)
      }
    }
  }

  if (ast.where != null) {
    rootPlan = handleWhere(rootPlan, schemas, ast.where)
  }
  if (ast.orderby != null) {
    rootPlan = new plan.OrderBy(
      rootPlan,
      ast.orderby.map((ordering) => {
        const expr = parseExpr(schemas, ordering.expr)
        const direction = ordering.type
        return { expr, direction }
      }),
    )
  }
  if (ast.limit != null) {
    rootPlan = handleLimit(rootPlan, ast.limit)
  }

  const astColumns = ast.columns as Column[]
  if (
    astColumns.some((col) =>
      col.expr.type === "aggr_func" || col.expr.type === "function"
    )
  ) {
    // this is an aggregation query
    rootPlan = new plan.Aggregate(
      rootPlan,
      parseAggregationColumns(astColumns, schemas),
    )
  } else {
    let select = new plan.Select(rootPlan, "$0", {})
    for (const astColumn of astColumns) {
      if (isColumnRefItem(astColumn.expr) && astColumn.expr.column === "*") {
        const schemaEntries = Object.entries(schemas)
        for (const [tableName, schema] of schemaEntries) {
          for (const column of schema.columns) {
            select = select.addColumn(
              schemaEntries.length === 1
                ? column.name
                : `${tableName}_${column.name}`,
              new plan.ColumnRefExpr(column, schema.name),
            )
          }
        }
      } else {
        let planExpr: plan.Expr<any>
        try {
          planExpr = parseExpr(schemas, astColumn.expr)
        } catch (e) {
          throw new Error(
            `Error parsing column ${JSON.stringify(astColumn, null, 2)}: ${
              String(e)
            }`,
          )
        }
        let columnName = planExpr.describe()
          .replace(/\s/g, "")
          .replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase()
        if (astColumn.as != null) {
          if (typeof astColumn.as !== "string") {
            throw new NotImplementedError(
              `Only string column names are supported`,
            )
          }
          columnName = astColumn.as
        }
        select = select.addColumn(columnName, planExpr)
      }
    }
    rootPlan = select
  }
  return rootPlan
}
