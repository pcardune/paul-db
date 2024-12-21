import { expect } from "@std/expect"
import { PaulDB, schema as s } from "../exports/mod.ts"

Deno.test("ExprBuilder .in()", async () => {
  const dbSchema = s.db().withTables(
    s.table("cats").with(
      s.column("name", s.type.string()),
    ),
  )
  const db = await PaulDB.inMemory()
  const model = await db.dbFile.getDBModel(dbSchema)
  await model.cats.insertMany([
    { name: "fluffy" },
    { name: "mittens" },
    { name: "Mr. Blue" },
  ])

  const results = await db.query(
    dbSchema.query()
      .from("cats")
      .where((t) => t.column("cats", "name").in("fluffy", "Mr. Blue"))
      .select({ name: (t) => t.column("cats", "name") }),
  ).toArray()
  expect(results).toEqual([
    { name: "fluffy" },
    { name: "Mr. Blue" },
  ])
})

Deno.test("ExprBuilder .in() with expressions", async () => {
  const dbSchema = s.db().withTables(
    s.table("names").with(
      s.column("name", s.type.string()),
      s.column("nickname", s.type.string()),
    ),
  )
  const db = await PaulDB.inMemory()
  const model = await db.dbFile.getDBModel(dbSchema)
  await model.names.insertMany([
    { name: "Alice", nickname: "Ali" },
    { name: "Bob", nickname: "Bob" },
    { name: "Charlie", nickname: "Chuck" },
  ])

  const rows = await db.query(
    dbSchema.query()
      .from("names")
      .where((t) =>
        // this is a contrived example, but it shows that you can use expressions
        // in the `in` clause. This finds all rows where the `name` is equal to
        // the `nickname`, or where the `name` is equal to "Chuck"
        t.column("names", "nickname").in("Chuck", t.column("names.name"))
      )
      .select({
        name: (t) => t.column("names.name"),
        nickname: (t) => t.column("names.nickname"),
      }),
  ).toArray()
  expect(rows).toEqual([
    { name: "Bob", nickname: "Bob" },
    { name: "Charlie", nickname: "Chuck" },
  ])
})

Deno.test("ExprBuilder .not()", async (test) => {
  const dbSchema = s.db().withTables(
    s.table("cats").with(
      s.column("name", s.type.string()),
    ),
  )
  const db = await PaulDB.inMemory()
  const model = await db.dbFile.getDBModel(dbSchema)
  await model.cats.insertMany([
    { name: "fluffy" },
    { name: "mittens" },
    { name: "Mr. Blue" },
  ])
  await test.step("used at the end of an expression", async () => {
    const query = dbSchema.query()
      .from("cats")
      .where((t) => t.column("cats", "name").eq("fluffy").not())
      .select({ name: (t) => t.column("cats", "name") })
    expect(query.plan().describe()).toEqual(
      `Select(name AS name, Filter(TableScan(default.cats), NOT(Compare(name = "fluffy")))) AS $0`,
    )
    expect(await db.query(query).toArray()).toEqual([
      { name: "mittens" },
      { name: "Mr. Blue" },
    ])
  })

  await test.step("When passed an expression", async () => {
    const query = dbSchema.query()
      .from("cats")
      .where((t) => t.not(t.column("cats", "name").eq("fluffy")))
      .select({ name: (t) => t.column("cats", "name") })
    expect(query.plan().describe()).toEqual(
      `Select(name AS name, Filter(TableScan(default.cats), NOT(Compare(name = "fluffy")))) AS $0`,
    )
    expect(await db.query(query).toArray()).toEqual([
      { name: "mittens" },
      { name: "Mr. Blue" },
    ])
  })
})
