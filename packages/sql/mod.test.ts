import { DbFile } from "@paul-db/core"
import { SQLExecutor } from "./mod.ts"
import { expect } from "jsr:@std/expect"
import { pick } from "jsr:@std/collections"

async function getExecutor() {
  const dbFile = await DbFile.open({ type: "memory" })
  const sql = new SQLExecutor(dbFile)
  return { sql, dbFile, [Symbol.dispose]: () => dbFile.close() }
}

Deno.test("CREATE TABLE", async (t) => {
  await t.step("CREATE TABLE test", async () => {
    using e = await getExecutor()
    await e.sql.execute("CREATE TABLE test")
    const tables = await e.dbFile.tableManager.tablesTable.scan("name", "test")
    expect(tables).toHaveLength(1)
    expect(tables[0].db).toEqual("default")
  })

  await t.step(
    "CREATE TABLE points (x float, y float, color TEXT)",
    async () => {
      using e = await getExecutor()
      await e.sql.execute("CREATE TABLE points (x float, y float, color TEXT)")
      const tables = await e.dbFile.tableManager.tablesTable.scan(
        "name",
        "points",
      )
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
      await e.table.iterate().toArray(),
    ).toEqual([
      { x: 1.0, y: 2.0, color: "green" },
    ])
  })
})

type Suite = {
  setup: string[]
  cases: [sql: string, result: unknown][]
}

function testSuite(name: string, suite: Suite) {
  Deno.test(name, async (t) => {
    using e = await getExecutor()
    for (const setup of suite.setup) {
      await e.sql.execute(setup)
    }
    for (const [sql, result] of suite.cases) {
      await t.step(sql, async () => {
        expect(await e.sql.execute(sql)).toEqual(result)
      })
    }
  })
}

testSuite("SELECT", {
  setup: [
    `
    CREATE TABLE points (x float, y float, color TEXT);
    INSERT INTO points (x, y, color) VALUES (1.0, 2.0, 'green');
    INSERT INTO points (x, y, color) VALUES (3.0, 4.0, 'blue');
    INSERT INTO points (x, y, color) VALUES (5.0, 6.0, 'red');
    `,
  ],
  cases: [
    ["SELECT * FROM points", [
      { x: 1.0, y: 2.0, color: "green" },
      { x: 3.0, y: 4.0, color: "blue" },
      { x: 5.0, y: 6.0, color: "red" },
    ]],
    ["SELECT * FROM points WHERE color = 'green'", [
      { x: 1.0, y: 2.0, color: "green" },
    ]],
    ["SELECT * FROM points WHERE color != 'green'", [
      { x: 3.0, y: 4.0, color: "blue" },
      { x: 5.0, y: 6.0, color: "red" },
    ]],
    ["SELECT * FROM points WHERE color >= 'green'", [
      { x: 1.0, y: 2.0, color: "green" },
      { x: 5.0, y: 6.0, color: "red" },
    ]],
    ["SELECT * FROM points WHERE color > 'green'", [
      { x: 5.0, y: 6.0, color: "red" },
    ]],
    ["SELECT * FROM points WHERE color < 'green'", [
      { x: 3.0, y: 4.0, color: "blue" },
    ]],
    ["SELECT * FROM points WHERE color <= 'green'", [
      { x: 1.0, y: 2.0, color: "green" },
      { x: 3.0, y: 4.0, color: "blue" },
    ]],
    ["SELECT * FROM points WHERE x <= 3.5", [
      { x: 1.0, y: 2.0, color: "green" },
      { x: 3.0, y: 4.0, color: "blue" },
    ]],
    ["SELECT * FROM points WHERE x <= 3.5 AND color < 'green'", [
      { x: 3.0, y: 4.0, color: "blue" },
    ]],
    ["SELECT * FROM points WHERE x <= 3.5 AND color < 'green' OR y < 3.0", [
      { x: 1.0, y: 2.0, color: "green" },
      { x: 3.0, y: 4.0, color: "blue" },
    ]],
    ["SELECT color FROM points WHERE x <= 3.5 AND color < 'green' OR y < 3.0", [
      { color: "green" },
      { color: "blue" },
    ]],
    ["SELECT x, y FROM points WHERE x <= 3.5 AND color < 'green' OR y < 3.0", [
      { x: 1.0, y: 2.0 },
      { x: 3.0, y: 4.0 },
    ]],
    [
      "SELECT x as pointx FROM points WHERE x <= 3.5 AND color < 'green' OR y < 3.0",
      [
        { pointx: 1.0 },
        { pointx: 3.0 },
      ],
    ],
    [
      "SELECT color, x > 1 as x_gt_1 FROM points WHERE x <= 3.5 AND color < 'green' OR y < 3.0",
      [
        { color: "green", x_gt_1: false },
        { color: "blue", x_gt_1: true },
      ],
    ],
    [
      "SELECT color, x > 1 FROM points",
      [
        { color: "green", compare_x_1_: false },
        { color: "blue", compare_x_1_: true },
        { color: "red", compare_x_1_: true },
      ],
    ],
  ],
})
