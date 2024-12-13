import { PaulDB } from "@paul-db/core"
import { SQLExecutor } from "./mod.ts"
import { expect } from "jsr:@std/expect"
import { pick } from "jsr:@std/collections"

async function getExecutor() {
  const db = await PaulDB.inMemory()
  const sql = new SQLExecutor(db)
  return { sql, db, [Symbol.dispose]: () => db[Symbol.dispose]() }
}

Deno.test("CREATE TABLE", async (t) => {
  await t.step("CREATE TABLE test", async () => {
    using e = await getExecutor()
    await e.sql.execute("CREATE TABLE test")
    const tables = await e.db.dbFile.tableManager.tablesTable.scan(
      "name",
      "test",
    )
    expect(tables).toHaveLength(1)
    expect(tables[0].db).toEqual("default")
  })

  await t.step(
    "CREATE TABLE points (x float, y float, color TEXT)",
    async () => {
      using e = await getExecutor()
      await e.sql.execute("CREATE TABLE points (x float, y float, color TEXT)")
      const tables = await e.db.dbFile.tableManager.tablesTable.scan(
        "name",
        "points",
      )
      expect(tables).toHaveLength(1)
      expect(tables[0].db).toEqual("default")

      const schemas = await e.db.dbFile.getSchemasOrThrow("default", "points")
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
  const schemas = await e.db.dbFile.getSchemasOrThrow("default", "points")
  const table = await e.db.dbFile.getOrCreateTable(schemas[0].schema)
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
  include?: "ignore" | "only"
  setup: string[]
  cases: [sql: string, result: unknown][]
}

function testSuite(name: string, suite: Suite) {
  const run = async (t: Deno.TestContext) => {
    using e = await getExecutor()
    for (const setup of suite.setup) {
      await e.sql.execute(setup)
    }
    for (const [sql, result] of suite.cases) {
      await t.step(sql, async () => {
        expect(await e.sql.execute(sql)).toEqual(result)
      })
    }
  }
  if (suite.include === "ignore") {
    Deno.test.ignore(name, run)
    return
  } else if (suite.include === "only") {
    Deno.test.only(name, run)
    return
  }
  Deno.test(name, run)
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
    [
      "SELECT color FROM points WHERE x > 1 LIMIT 1",
      [
        { color: "blue" },
      ],
    ],
    [
      "SELECT color FROM points WHERE x > 1 ORDER BY x DESC LIMIT 1",
      [
        { color: "red" },
      ],
    ],
    [
      "SELECT color FROM points WHERE x > 1 ORDER BY x ASC LIMIT 1",
      [
        { color: "blue" },
      ],
    ],
  ],
})

testSuite("JOINS", {
  setup: [
    `CREATE TABLE cats (id INT, name TEXT, age INT)`,
    `CREATE TABLE humans (id INT, name TEXT)`,
    `CREATE TABLE cat_owners (cat_id INT, human_id INT)`,
    `INSERT INTO cats (id, name, age) VALUES (1, 'fluffy', 3)`,
    `INSERT INTO cats (id, name, age) VALUES (2, 'mittens', 5)`,
    `INSERT INTO humans (id, name) VALUES (1, 'alice')`,
    `INSERT INTO humans (id, name) VALUES (2, 'bob')`,
    `INSERT INTO cat_owners (cat_id, human_id) VALUES (1, 1)`,
    `INSERT INTO cat_owners (cat_id, human_id) VALUES (2, 2)`,
    `INSERT INTO cat_owners (cat_id, human_id) VALUES (2, 1)`,
  ],
  cases: [
    [
      `SELECT cats.name as cat, humans.name as owner
       FROM cats
       JOIN cat_owners ON cats.id = cat_owners.cat_id
       JOIN humans ON humans.id = cat_owners.human_id`,
      [
        { cat: "fluffy", owner: "alice" },
        { cat: "mittens", owner: "bob" },
        { cat: "mittens", owner: "alice" },
      ],
    ],
  ],
})

testSuite("Aggregations", {
  setup: [
    `CREATE TABLE cats (id INT, name TEXT, age INT)`,
    `INSERT INTO cats (id, name, age) VALUES (1, 'fluffy', 3)`,
    `INSERT INTO cats (id, name, age) VALUES (2, 'mittens', 5)`,
  ],
  cases: [
    [
      `SELECT MAX(age) as max_age FROM cats`,
      [
        { max_age: 5 },
      ],
    ],
    [
      `SELECT COUNT(*) as num_cats FROM cats`,
      [{ num_cats: 2 }],
    ],
    [
      `SELECT ARRAY_AGG(name) as names FROM cats`,
      [{ names: ["fluffy", "mittens"] }],
    ],
  ],
})

testSuite("Subqueries", {
  include: "ignore",
  setup: [
    `CREATE TABLE cats (id INT, name TEXT, age INT, ownerId INT)`,
    `CREATE TABLE humans (id INT, name TEXT)`,
    `INSERT INTO humans (id, name) VALUES (1, 'alice')`,
    `INSERT INTO humans (id, name) VALUES (2, 'bob')`,
    `INSERT INTO cats (id, name, age, ownerId) VALUES (1, 'fluffy', 3, 1)`,
    `INSERT INTO cats (id, name, age, ownerId) VALUES (2, 'mittens', 5, 2)`,
  ],
  cases: [
    [
      `SELECT
        name,
        (SELECT name
         FROM humans
         WHERE humans.id = cats.ownerId) as owner
      FROM cats`,
      [{ name: "mittens" }],
    ],
  ],
})
