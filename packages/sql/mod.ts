import { type DbFile, plan, s } from "@paul-db/core"
import SQLParser, { Column } from "npm:node-sql-parser"
import { Create } from "npm:node-sql-parser/types"
import { SomeTableSchema } from "../core/schema/schema.ts"
import { getColumnTypeFromSQLType } from "../core/schema/columns/ColumnType.ts"
import { UnknownRecord } from "npm:type-fest"
import { NotImplementedError, TableNotFoundError } from "./errors.ts"
import { isColumnRefItem } from "./parser.ts"
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
      ast = this.parser.parse(sql)
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
    if (ast.orderby != null) {
      throw new NotImplementedError(`ORDER BY clause not supported yet`)
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
    if (ast.from.length != 1) {
      throw new NotImplementedError(
        `Only one table in FROM list supported. Found ${ast.from.length}`,
      )
    }
    const astFrom = ast.from[0]
    if (!("table" in astFrom)) {
      throw new NotImplementedError(
        `Only table names are supported in FROM lists`,
      )
    }
    const tableName = astFrom.table
    const db = astFrom.db ? astFrom.db : "default"

    const tableScan = new plan.TableScan(db, tableName)
    let rootPlan: plan.IQueryPlanNode = tableScan
    if (ast.where != null) {
      const schema = await tableScan.getSchema(this.dbFile)
      rootPlan = handleWhere(rootPlan, schema, ast.where)
    }
    const astColumns = ast.columns as Column[]
    const schema = await tableScan.getSchema(this.dbFile)
    let select = new plan.Select(rootPlan, {})
    for (const astColumn of astColumns) {
      const expr = astColumn.expr as SQLParser.ColumnRefItem
      if (isColumnRefItem(expr) && expr.column === "*") {
        for (const column of schema.columns) {
          select = select.addColumn(column.name, new plan.ColumnRefExpr(column))
        }
      } else {
        const planExpr = parseExpr(schema, astColumn.expr)
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
        select = select.addColumn(
          columnName,
          planExpr,
        )
      }
    }
    rootPlan = select
    if (ast.limit != null) {
      rootPlan = handleLimit(rootPlan, ast.limit)
    }

    return await rootPlan.execute(this.dbFile).toArray()
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
