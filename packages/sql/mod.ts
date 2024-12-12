import { type DbFile, plan, s } from "@paul-db/core"
import SQLParser, { Column } from "npm:node-sql-parser"
import { Create } from "npm:node-sql-parser/types"
import { SomeTableSchema } from "../core/schema/schema.ts"
import { getColumnTypeFromSQLType } from "../core/schema/columns/ColumnType.ts"
import { UnknownRecord } from "npm:type-fest"
import { NotImplementedError, TableNotFoundError } from "./errors.ts"
import {
  isAggrFunc,
  isColumnRefItem,
  isExprList,
  isFunction,
} from "./parser.ts"
import { handleWhere } from "./where.ts"
import { parseExpr } from "./expr.ts"
import { handleLimit } from "./limit.ts"
type CreateDefinition = Exclude<
  Create["create_definitions"],
  null | undefined
>[number]
type CreateColumnDefinition = Extract<CreateDefinition, { resource: "column" }>

export class SQLParseError extends Error {}
export class SQLExecutor {
  private parser: SQLParser.Parser

  constructor(private dbFile: DbFile) {
    this.parser = new SQLParser.Parser()
  }

  async execute<T>(sql: string): Promise<T> {
    let ast: SQLParser.TableColumnAst
    try {
      ast = this.parser.parse(sql // {database: "Postgresql"} // TODO: Turn this on.
      )
    } catch (e) {
      if (e instanceof Error) {
        throw new SQLParseError(e.message)
      }
      throw e
    }
    return await this.handleAST(ast) as T
  }

  async handleAST(ast: SQLParser.TableColumnAst) {
    const commands = Array.isArray(ast.ast) ? ast.ast : [ast.ast]

    const results = []
    for (const command of commands) {
      if (command.type === "create") {
        results.push(await this.handleCreate(command))
      } else if (command.type === "insert") {
        results.push(await this.handleInsert(command))
      } else if (command.type === "select") {
        results.push(await this.handleSelect(command))
      } else {
        throw new NotImplementedError(
          `Command type ${command.type} not implemented`,
        )
      }
    }
    if (results.length === 1) return results[0]
    return results
  }

  async handleSelect(ast: SQLParser.Select): Promise<UnknownRecord[]> {
    if (ast.groupby != null) {
      throw new NotImplementedError(`GROUP BY clause not supported yet`)
    }
    if (ast.having != null) {
      throw new NotImplementedError(`HAVING clause not supported yet`)
    }
    if (ast.distinct != null) {
      throw new NotImplementedError(`DISTINCT clause not supported yet`)
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
    const schemas: Record<string, SomeTableSchema> = {}
    schemas[astFrom.table] = await tableScan.getSchema(this.dbFile)
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
          schemas[joinAst.table] = await joinTableScan.getSchema(this.dbFile)
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
            console.log("HERE YOU GO", exprAst)
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
                `ARRAY_AGG must have arguments, found: ${
                  JSON.stringify(exprAst)
                }`,
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
      rootPlan = new plan.Aggregate(rootPlan, multiAgg)
    } else {
      let select = new plan.Select(rootPlan, {})
      for (const astColumn of astColumns) {
        const expr = astColumn.expr as SQLParser.ColumnRefItem
        if (isColumnRefItem(expr) && expr.column === "*") {
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
          const planExpr = parseExpr(schemas, astColumn.expr)
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

    return await rootPlan.execute(this.dbFile).map((rowData) => rowData["0"])
      .toArray()
  }

  async handleInsert(ast: SQLParser.Insert_Replace): Promise<void> {
    const astTables: unknown = ast.table
    if (!Array.isArray(astTables)) {
      throw new NotImplementedError(`Do not understand this AST format`)
    }
    if (astTables.length != 1) {
      throw new NotImplementedError(
        `Only one table name allowed, found: ${astTables.length}`,
      )
    }
    const astTable: unknown = astTables[0]
    if (typeof astTable !== "object" || astTable == null) {
      throw new NotImplementedError(`Do not understand this AST format`)
    }
    if (
      !("db" in astTable &&
        (typeof astTable.db === "string" || astTable.db === null)) ||
      !("table" in astTable && (typeof astTable.table === "string")) ||
      !("as" in astTable &&
        (typeof astTable.as === "string" || astTable.as === null))
    ) {
      throw new NotImplementedError(`Do not understand this AST format`)
    }
    const db = astTable.db ? astTable.db : "default"
    const tableName = astTable.table

    const schemas = await this.dbFile.getSchemasOrThrow(db, tableName)
    if (schemas.length === 0) {
      throw new TableNotFoundError(`Table ${db}.${tableName} not found`)
    }
    // TODO: support correct schema version
    const tableInstance = await this.dbFile.getOrCreateTable(
      schemas[0].schema,
      { db },
    )

    if (!Array.isArray(ast.values)) {
      throw new NotImplementedError(`only VALUES lists are supported`)
    }
    if (ast.values.length != 1) {
      throw new NotImplementedError(`Only one VALUES list item supported`)
    }
    const values = ast.values[0]
    if (values.type !== "expr_list") {
      throw new NotImplementedError(`Only expr_list values supported`)
    }
    if (!Array.isArray(ast.columns)) {
      throw new NotImplementedError(`list of columns must be specified`)
    }
    if (!Array.isArray(values.value)) {
      throw new NotImplementedError(`Do not understand this AST format`)
    }
    const insertObject: Record<string, unknown> = {}
    for (const [i, columnName] of ast.columns.entries()) {
      const insertValue: unknown = values.value[i]
      if (typeof insertValue != "object" || insertValue == null) {
        throw new NotImplementedError(`Do not understand this AST format`)
      }
      if (
        !("type" in insertValue && typeof insertValue.type === "string") ||
        !("value" in insertValue)
      ) {
        throw new NotImplementedError(
          `Do not understand this AST format: ${JSON.stringify(insertValue)}`,
        )
      }
      if (
        insertValue.type !== "single_quote_string" &&
        insertValue.type !== "number"
      ) {
        throw new NotImplementedError(
          `Only string and number values are supported, found ${insertValue.type}`,
        )
      }
      insertObject[columnName] = insertValue.value
    }
    await tableInstance.insert(insertObject)
  }

  async handleCreate(ast: SQLParser.Create): Promise<void> {
    if (ast.table == null) {
      throw new NotImplementedError(
        "Can't handle CREATE without table name provided",
      )
    }
    if (ast.table.length != 1) {
      throw new NotImplementedError(
        `Only one table name allowed, found: ${ast.table.length}`,
      )
    }
    const table = ast.table[0]

    const columnsToCreate: CreateColumnDefinition[] =
      (ast.create_definitions ?? []).filter((c) => c.resource === "column")
    let schema: SomeTableSchema = s.table(table.table)
    if (columnsToCreate.length > 0) {
      for (const columnDef of columnsToCreate) {
        if (!isColumnRefItem(columnDef.column)) {
          throw new NotImplementedError("Only column_ref columns are supported")
        }
        if (typeof columnDef.column.column != "string") {
          throw new NotImplementedError(
            `Column name must be a string, column expressions not supported`,
          )
        }
        const columnType = getColumnTypeFromSQLType(
          columnDef.definition.dataType,
        )
        const col = s.column(columnDef.column.column, columnType)
        schema = schema.with(col)
      }
    }

    await this.dbFile.getOrCreateTable(schema, {
      db: table.db ? table.db : "default",
    })
  }
}
