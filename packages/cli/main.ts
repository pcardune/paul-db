#!/usr/bin/env -S deno run --allow-read --allow-write
import { Command, program } from "@commander-js/extra-typings"

import { PaulDB } from "@paul-db/core"
new Command().name("paul-db").version("0.0.1")
  .description("A simple key-value store")
  .addCommand(
    program.command("insert")
      .argument("<key>", "The key to insert")
      .argument("<value>", "The value to insert")
      .action(async (key, value) => {
        const db = await PaulDB.create()
        await db.insert(key, value)
        console.log(`Inserted ${key} ${value}`)
      }),
  ).parse(["deno", "main.ts", ...Deno.args])
