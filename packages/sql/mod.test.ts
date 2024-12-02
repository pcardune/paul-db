import { DbFile } from "@paul-db/core"
import { SQLExecutor } from "./mod.ts"
import { expect } from "jsr:@std/expect"
import { omit, pick } from "jsr:@std/collections"

async function getExecutor() {
  const dbFile = await DbFile.open("/tmp/paul-db-test", {
    create: true,
    truncate: true,
  })
  const sql = new SQLExecutor(dbFile)
  return { sql, dbFile, [Symbol.dispose]: () => dbFile.close() }
}

Deno.test("CREATE TABLE", async (t) => {
  await t.step("CREATE TABLE test", async () => {
    using e = await getExecutor()
    await e.sql.execute("CREATE TABLE test")
    const tables = await e.dbFile.tablesTable.scan("name", "test")
    expect(tables).toHaveLength(1)
    expect(tables[0].db).toEqual("default")
  })

  await t.step(
    "CREATE TABLE points (x float, y float, color TEXT)",
    async () => {
      using e = await getExecutor()
      await e.sql.execute("CREATE TABLE points (x float, y float, color TEXT)")
      const tables = await e.dbFile.tablesTable.scan("name", "points")
      expect(tables).toHaveLength(1)
      expect(tables[0].db).toEqual("default")

      const schemas = await e.dbFile.getSchemasOrThrow("default", "points")
      expect(schemas.length).toEqual(1)
      const colRecs = schemas[0].columnRecords.map((c) =>
        pick(c, ["name", "type"])
      )
      expect(colRecs).toContainEqual({ name: "x", type: "float" })
      expect(colRecs).toContainEqual({ name: "y", type: "float" })
    },
  )
})

async function getPointsTable() {
  const e = await getExecutor()
  await e.sql.execute("CREATE TABLE points (x float, y float, color TEXT)")
  const schemas = await e.dbFile.getSchemasOrThrow("default", "points")
  const table = await e.dbFile.getOrCreateTable(schemas[0].schema)
  return { ...e, table }
}

Deno.test("INSERT INTO", async (t) => {
  await t.step("INSERT INTO points (x, y) VALUES (1.0, 2.0)", async () => {
    using e = await getPointsTable()
    await e.sql.execute(
      "INSERT INTO points (x, y, color) VALUES (1.0, 2.0, 'green')",
    )

    expect(
      await e.table.iterate().map((row) => omit(row, ["id"]))
        .toArray(),
    ).toEqual([
      { x: 1.0, y: 2.0, color: "green" },
    ])
  })
})

Deno.test("SELECT", async (t) => {
  await t.step("SELECT * FROM points", async () => {
    using e = await getPointsTable()
    await e.sql.execute(
      "INSERT INTO points (x, y, color) VALUES (1.0, 2.0, 'green')",
    )

    const result = await e.sql.execute<{ id: string }[]>("SELECT * FROM points")
    expect(result?.map((r) => omit(r, ["id"]))).toEqual([
      { x: 1.0, y: 2.0, color: "green" },
    ])
  })
})
