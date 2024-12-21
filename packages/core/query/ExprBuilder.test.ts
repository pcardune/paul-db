import { expect } from "@std/expect"
import { PaulDB, schema as s } from "../exports/mod.ts"
import { _NeverExpr, ExprBuilder, ITQB } from "./QueryBuilder.ts"

function exprBuilder<TQB extends ITQB>(tqb: TQB) {
  return new ExprBuilder(tqb, new _NeverExpr())
}

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

Deno.test("ExprBuilder .literal()", async (test) => {
  const dbSchema = s.db().withTables(
    s.table("cats").with(
      s.column("name", s.type.string()),
    ),
  )
  const db = await PaulDB.inMemory()
  const model = await db.dbFile.getDBModel(dbSchema)
  await model.cats.insertMany([{ name: "fluffy" }])

  await test.step("When column types are not passed", async () => {
    const t = exprBuilder(dbSchema.query().from("cats"))
    const boolExpr = t.literal(true).expr
    expect(boolExpr.describe()).toEqual("true")
    expect(boolExpr.getType().name).toEqual(s.type.boolean().name)

    const stringExpr = t.literal("hello").expr
    expect(stringExpr.describe()).toEqual(`"hello"`)
    expect(stringExpr.getType().name).toEqual(s.type.string().name)

    const numberExpr = t.literal(123).expr
    expect(numberExpr.describe()).toEqual("123")
    expect(numberExpr.getType().name).toEqual(s.type.int32().name)

    const floatExpr = t.literal(123.456).expr
    expect(floatExpr.describe()).toEqual("123.456")
    expect(floatExpr.getType().name).toEqual(s.type.float().name)

    // @ts-expect-error can't pass arrays as literals
    expect(() => t.literal(["foo"])).toThrow(
      'Type must be provided for literal ["foo"]',
    )

    const tqb = dbSchema.query().from("cats")
    new ExprBuilder(tqb, new _NeverExpr())
    const results = await db.query(
      dbSchema.query()
        .from("cats")
        .select({
          someBool: (t) => t.literal(true),
          someString: (t) => t.literal("hello"),
          someNumber: (t) => t.literal(123),
          someFloat: (t) => t.literal(123.456),
        }),
    ).toArray()
    expect(results).toEqual([{
      someBool: true,
      someString: "hello",
      someNumber: 123,
      someFloat: 123.456,
    }])
  })

  await test.step("When column types are passed", async () => {
    const t = exprBuilder(dbSchema.query().from("cats"))
    // @ts-expect-error true is not a string
    expect(() => t.literal(true, s.type.string())).toThrow(
      "Value true is not valid for type string",
    )

    const uintExpr = t.literal(123, s.type.uint32())
    expect(uintExpr.expr.describe()).toEqual("123")
    expect(uintExpr.expr.getType().name).toEqual(s.type.uint32().name)

    const results = await db.query(
      dbSchema.query()
        .from("cats")
        .select({
          someBool: (t) => t.literal(true, s.type.boolean()),
          someString: (t) => t.literal("hello", s.type.string()),
          someNumber: (t) => t.literal(123, s.type.uint32()),
          someFloat: (t) => t.literal(123.456, s.type.float()),
        }),
    ).toArray()
    expect(results).toEqual([{
      someBool: true,
      someString: "hello",
      someNumber: 123,
      someFloat: 123.456,
    }])
  })
})
