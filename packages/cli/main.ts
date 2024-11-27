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
  ).parse(["deno", "main.ts", ...Deno.args])
