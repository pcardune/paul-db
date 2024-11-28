import { PaulDB } from "@paul-db/core"
import SQLParser from "npm:node-sql-parser"
import { Input } from "@cliffy/prompt"
import { SQLExecutor } from "@paul-db/sql"

export async function startRepl(db: PaulDB) {
  Deno.stdout.write(new TextEncoder().encode("Welcome to the PaulDB REPL\n"))
  const parser = new SQLParser.Parser()

  while (true) {
    const msg = await Input.prompt({
      message: "",
      suggestions: (msg) => {
        if (msg.startsWith("/")) {
          return [
            "/exit",
            "/help",
            "/dt",
          ]
        }
        return []
      },
    })

    const matches = msg.match(/^\/(\w+)(.*)/)
    if (matches) {
      const cmd = matches[1]
      const rest = matches[2].trim()
      if (cmd === "exit") {
        break
      }
      if (cmd === "help") {
        console.log("Commands:")
        console.log("  /exit - Exit the REPL")
        console.log("  /help - Show this help message")
        console.log("  /dt - Show all tables")
        continue
      }
      if (cmd === "dt") {
        console.log("Tables:")
        const schemas = await db.dbFile.getSchemasTable()
        for await (const table of db.dbFile.tablesTable.iterate()) {
          console.log(` ${table.db}:${table.name}`)
          for await (
            const schemaVersion of schemas.schemaTable.scanIter(
              "tableId",
              table.id,
            )
          ) {
            console.log(`   Version ${schemaVersion.version}`)
            for (
              const column of await schemas.columnsTable.lookup(
                "schemaId",
                schemaVersion.id,
              )
            ) {
              console.log(
                `     ${column.name} ${column.type} ${
                  column.unique ? "UNIQUE" : ""
                } ${column.indexed ? "INDEXED" : ""}`,
              )
            }
          }
        }
        continue
      }
      if (cmd === "parse") {
        try {
          console.log(parser.parse(rest))
        } catch (e) {
          if (e instanceof Error) {
            console.error("Error parsing SQL:", rest, e.message)
          }
          continue
        }
      }
      console.log(`Unknown command: ${cmd} - type /help for help`)
      continue
    }

    try {
      const ast = parser.parse(msg)
      const result = await new SQLExecutor(db.dbFile).handleAST(ast)
      console.log(result)
    } catch (e) {
      if (e instanceof Error) {
        console.error("Error parsing SQL:", e.message)
      }
      continue
    }
  }
}
