import { column, type DbFile, Table, TableSchema } from "@paul-db/core"
import SQLParser from "node-sql-parser"
import { Create } from "node-sql-parser/types"
import { SomeTableSchema } from "../core/schema/schema.ts"
import { getColumnTypeFromSQLType } from "../core/schema/ColumnType.ts"

type CreateDefinition = Exclude<
  Create["create_definitions"],
  null | undefined
>[number]
type CreateColumnDefinition = Extract<CreateDefinition, { resource: "column" }>

export class SQLParseError extends Error {}
export class NotImplementedError extends Error {}
export class TableNotFoundError extends Error {}
export class SQLExecutor {
  private parser: SQLParser.Parser

  constructor(private dbFile: DbFile) {
    this.parser = new SQLParser.Parser()
  }

  async execute(sql: string) {
    let ast: SQLParser.TableColumnAst
    try {
      ast = this.parser.parse(sql)
    } catch (e) {
      if (e instanceof Error) {
        throw new SQLParseError(e.message)
      }
      throw e
    }
    console.log("SQL:", sql)
    console.log("AST:", JSON.stringify(ast, null, 2))
    await this.handleAST(ast)
  }

  handleAST(ast: SQLParser.TableColumnAst) {
    const commands = Array.isArray(ast.ast) ? ast.ast : [ast.ast]
    if (commands.length > 1) {
      console.log("Only one command at a time for now...")
      return
    }
    const command = commands[0]
    if (command.type === "create") {
      return this.handleCreate(command)
    }
    if (command.type === "insert") {
      return this.handleInsert(command)
    }
    console.log("Got AST:", JSON.stringify(ast, null, 2))
    throw new NotImplementedError(
      `Command type ${command.type} not implemented`,
    )
  }

  async handleInsert(ast: SQLParser.Insert_Replace) {
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

    const schemas = await this.dbFile.getSchemas(db, tableName)
    if (schemas.length === 0) {
      throw new TableNotFoundError(`Table ${db}.${tableName} not found`)
    }
    // TODO: support correct schema version
    const tableInstance = new Table(
      await this.dbFile.getTableStorage(schemas[0].schema, db),
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
    const rowId = await tableInstance.insert(insertObject)
    console.log("Inserted row:", rowId, insertObject)
  }

  async handleCreate(ast: SQLParser.Create) {
    console.log("CREATE TABLE command")
    console.log("Table name:", ast.table)
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
    let schema: SomeTableSchema = TableSchema.create(table.table)
    if (columnsToCreate.length > 0) {
      for (const columnDef of columnsToCreate) {
        if (columnDef.column.type != "column_ref") {
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
        const col = column(columnDef.column.column, columnType)
        schema = schema.withColumn(col)
      }
    }

    await this.dbFile.createTable(schema, table.db ? table.db : "default")

    console.log(JSON.stringify(ast, null, 2))
  }
}
