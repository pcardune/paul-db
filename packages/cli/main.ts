#!/usr/bin/env -S deno run --allow-read --allow-write --allow-env
import { Command, program } from "@commander-js/extra-typings"
import { exists } from "@std/fs"

import { PaulDB } from "@paul-db/core"
import { startRepl } from "./repl.ts"
import { Confirm } from "@cliffy/prompt"

new Command().name("paul-db").version("0.0.1")
  .description("A simple key-value store")
  .addCommand(
    program.command("open")
      .argument("<dir>", "The directory to open")
      .option("--truncate", "Truncate the file")
      .option("--noPrompts", "Disable prompts")
      .action(
        async (dir, { truncate, noPrompts }) => {
          if (truncate && await exists(dir)) {
            const confirm = noPrompts || await Confirm.prompt(
              "Are you sure you want to truncate the file?",
            )
            if (!confirm) {
              console.log("Aborting")
              return
            }
            Deno.remove(dir, { recursive: true })
          }

          const db = await PaulDB.open(dir, { create: true })
          await startRepl(db)
        },
      ),
  )
  .addCommand(
    program
      .command("export")
      .argument("<dir>", "The directory to export")
      .argument("[output]", "The output file")
      .option("--table <table>", "The table to export")
      .option("--dbName <dbName>", "The db to export", "default")
      .option("--recordsOnly", "Don't export table metadata")
      .action(async (dir, output, { table, dbName, recordsOnly }) => {
        using db = await PaulDB.open(dir)
        if (output) {
          await Deno.open(output, { create: true, write: true, truncate: true })
        }
        for await (
          const record of db.dbFile.exportRecords({ table, db: dbName })
        ) {
          const json = JSON.stringify(recordsOnly ? record.record : record)

          if (output) {
            await Deno.writeTextFile(output, json + "\n", {
              append: true,
            })
          } else {
            console.log(json)
          }
        }
      }),
  )
  .parse(["deno", "main.ts", ...Deno.args])
