import { PaulDB, schema as s } from "@paul-db/core"
import SQLParser from "node-sql-parser"
import { Create } from "node-sql-parser/types"
import {
  NotImplementedError,
  SQLParseError,
  TableNotFoundError,
} from "./errors.ts"
import { Insert_Replace, isColumnRefItem, isInsertReplace } from "./parser.ts"
import { parseSelect } from "./select.ts"
import { getColumnTypeFromSQLType } from "./sqlColumnTypes.ts"

type CreateDefinition = Exclude<
  Create["create_definitions"],
  null | undefined
>[number]

type CreateColumnDefinition = Extract<CreateDefinition, { resource: "column" }>

/**
 * SQLExecutor is a class that can execute SQL commands on a PaulDB instance.
 */
export class SQLExecutor {
  /**
   * @ignore
   */
  private parser: SQLParser.Parser

  /**
   * Construct a new SQLExecutor
   * @param db The PaulDB instance to execute SQL commands on
   */
  constructor(private db: PaulDB) {
    this.parser = new SQLParser.Parser()
  }

  /**
   * Execute a SQL command
   *
   * @param sql The SQL command to execute
   *
   * @returns The result of the SQL command
   *
   * ```ts
   * import {PaulDB} from "@paul-db/core"
   * const db = await PaulDB.inMemory()
   * const executor = new SQLExecutor(db)
   * await executor.execute("CREATE TABLE test (id INT, name TEXT)")
   * await executor.execute("INSERT INTO test (id, name) VALUES (1, 'Alice')")
   * const result = await executor.execute("SELECT * FROM test")
   * console.log(result)
   * ```
   */
  async execute<T>(sql: string): Promise<T> {
    let ast: SQLParser.TableColumnAst
    try {
      ast = this.parser.parse(sql, { database: "Postgresql" })
    } catch (e) {
      if (e instanceof Error) {
        throw new SQLParseError(e.message)
      }
      throw e
    }
    return await this.handleAST(ast) as T
  }

  /**
   * @ignore
   */
  async handleAST(ast: SQLParser.TableColumnAst) {
    const commands = Array.isArray(ast.ast) ? ast.ast : [ast.ast]

    const results = []
    for (const command of commands) {
      if (command.type === "create") {
        results.push(await this.handleCreate(command))
      } else if (isInsertReplace(command)) {
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

  /**
   * @ignore
   */
  private async handleSelect(
    ast: SQLParser.Select,
  ): Promise<Record<string, unknown>[]> {
    const rootPlan = await parseSelect(ast, this.db)
    return await rootPlan.execute(this.db).map((rowData) => rowData.$0)
      .toArray()
  }

  /**
   * @ignore
   */
  private async handleInsert(ast: Insert_Replace): Promise<void> {
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

    const schemas = await this.db.dbFile.getSchemasOrThrow(db, tableName)
    if (schemas.length === 0) {
      throw new TableNotFoundError(`Table ${db}.${tableName} not found`)
    }
    // TODO: support correct schema version
    const tableInstance = await this.db.dbFile.getOrCreateTable(
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
    for (const [i, column] of ast.columns.entries()) {
      const columnName = typeof column === "string" ? column : column.value
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

  /**
   * @ignore
   */
  private async handleCreate(ast: SQLParser.Create): Promise<void> {
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
    let schema: s.SomeTableSchema = s.table(table.table)
    if (columnsToCreate.length > 0) {
      for (const columnDef of columnsToCreate) {
        if (!isColumnRefItem(columnDef.column)) {
          throw new NotImplementedError("Only column_ref columns are supported")
        }
        const columnType = getColumnTypeFromSQLType(
          columnDef.definition.dataType,
        )
        let columnName: string
        if (typeof columnDef.column.column === "string") {
          columnName = columnDef.column.column
        } else {
          const expr = columnDef.column.column.expr
          if (typeof expr.value === "string") {
            columnName = expr.value
          } else {
            throw new NotImplementedError(
              "Only string column names are supported",
            )
          }
        }
        const col = s.column(columnName, columnType)
        schema = schema.with(col)
      }
    }

    await this.db.dbFile.getOrCreateTable(schema, {
      db: table.db ? table.db : "default",
    })
  }
}
