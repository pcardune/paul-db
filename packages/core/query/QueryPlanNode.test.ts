import { expect } from "jsr:@std/expect"
import {
  Aggregate,
  ColumnRefExpr,
  Compare,
  CountAggregation,
  Filter,
  Join,
  Limit,
  LiteralValueExpr,
  MaxAggregation,
  MultiAggregation,
  Select,
  TableScan,
} from "./QueryPlanNode.ts"
import { DbFile, s } from "../mod.ts"
import { ColumnTypes } from "../schema/columns/ColumnType.ts"
import { assertTrue, TypeEquals } from "../testing.ts"

const dbSchema = s.db().withTables(
  s.table("humans").with(
    s.column("id", "serial"),
    s.column("firstName", s.type.string()),
    s.column("lastName", s.type.string()),
  ),
  s.table("cats").with(
    s.column("id", "serial"),
    s.column("name", s.type.string()),
    s.column("age", s.type.uint32()),
    s.column("likesTreats", s.type.boolean()).defaultTo(() => true),
  ),
  s.table("catOwners").with(
    s.column("petId", s.type.uint32()),
    s.column("ownerId", s.type.uint32()),
  ),
)

async function init() {
  const dbFile = await DbFile.open({ type: "memory" })
  const model = await dbFile.getDBModel(dbSchema)
  await model.cats.insert({ name: "fluffy", age: 3, id: 1 })
  await model.cats.insert({ name: "mittens", age: 5, id: 2 })
  await model.humans.insertMany([
    { firstName: "Alice", lastName: "Smith", id: 1 },
    { firstName: "Bob", lastName: "Jones", id: 2 },
  ])
  await model.catOwners.insertMany([
    { petId: 1, ownerId: 1 },
    { petId: 1, ownerId: 2 },
    { petId: 2, ownerId: 2 },
  ])
  return { model, dbFile }
}

Deno.test("QueryPlanNode", async () => {
  const { model, dbFile } = await init()

  const nameCol = model.cats.schema.getColumnByNameOrThrow("name")
  const ageCol = model.cats.schema.getColumnByNameOrThrow("age")
  const plan = new Limit(
    new Select(
      new Filter(
        new TableScan("default", "cats"),
        new Compare(
          new ColumnRefExpr(nameCol, "cats"),
          "=",
          new LiteralValueExpr("fluffy", ColumnTypes.string()),
        ),
      ),
      {
        name: new ColumnRefExpr(nameCol, "cats"),
        age: new ColumnRefExpr(ageCol, "cats"),
        isOld: new Compare(
          new ColumnRefExpr(ageCol, "cats"),
          ">",
          new LiteralValueExpr(3, ColumnTypes.uint32()),
        ),
      },
    ),
    1,
  )

  expect(plan.describe()).toEqual(
    'Limit(Select(name AS name, age AS age, Compare(age > 3) AS isOld, Filter(TableScan(default.cats), Compare(name = "fluffy"))), 1)',
  )

  expect(await plan.execute(dbFile).toArray()).toEqual([{
    "0": {
      name: "fluffy",
      age: 3,
      isOld: false,
    },
  }])

  // via the builder API:
  const oldAndFluffyQuery = dbSchema.query()
    .from("cats")
    .where((t) =>
      t.column("cats", "name").eq("fluffy")
        .or(t.column("cats", "age").gt(3))
    )
  const plan2 = oldAndFluffyQuery.plan()

  expect(plan2.describe()).toEqual(
    'Filter(TableScan(default.cats), (Compare(name = "fluffy") OR Compare(age > 3)))',
  )
  const oldOrFluffyCats = await plan2.execute(dbFile).toArray()
  expect(oldOrFluffyCats).toEqual([
    { cats: { id: 1, name: "fluffy", age: 3, likesTreats: true } },
    { cats: { id: 2, name: "mittens", age: 5, likesTreats: true } },
  ])

  // types flow through the query builder API:
  assertTrue<
    TypeEquals<
      typeof oldOrFluffyCats,
      Array<
        {
          cats: { id: number; name: string; age: number; likesTreats: boolean }
        }
      >
    >
  >()

  const limitedOldAndFluffy = oldAndFluffyQuery.limit(1)
  const plan3 = limitedOldAndFluffy.plan()
  expect(plan3.describe()).toEqual(
    'Limit(Filter(TableScan(default.cats), (Compare(name = "fluffy") OR Compare(age > 3))), 1)',
  )
  expect(await plan3.execute(dbFile).toArray()).toEqual([
    { "cats": { id: 1, name: "fluffy", age: 3, likesTreats: true } },
  ])

  const limitedAndOrderedOldAndFluffy = oldAndFluffyQuery.limit(1).orderBy(
    (t) => t.column("cats.name"),
    "DESC",
  )
  const plan4 = limitedAndOrderedOldAndFluffy.plan()
  expect(plan4.describe()).toEqual(
    'Limit(OrderBy(Filter(TableScan(default.cats), (Compare(name = "fluffy") OR Compare(age > 3))), name DESC), 1)',
  )
  expect(await plan4.execute(dbFile).toArray()).toEqual([
    { "cats": { id: 2, name: "mittens", age: 5, likesTreats: true } },
  ])

  const namesOnly = await limitedAndOrderedOldAndFluffy.select({
    catName: (t) => t.column("cats.name"),
    isOld: (t) => t.column("cats.age").gt(3),
  }).plan().execute(dbFile).toArray()
  expect(namesOnly).toEqual([
    { "0": { catName: "mittens", isOld: true } },
  ])
  assertTrue<
    TypeEquals<
      typeof namesOnly,
      Array<{ "0": { catName: string; isOld: boolean } }>
    >
  >()
})

Deno.test("QueryPlanNode Aggregates", async () => {
  const { model, dbFile } = await init()

  const ageCol = model.cats.schema.getColumnByNameOrThrow("age")
  const plan = new Aggregate(
    new TableScan("default", "cats"),
    new MultiAggregation({
      count: new CountAggregation(),
      max: new MaxAggregation(new ColumnRefExpr(ageCol, "cats")),
    }),
  )
  expect(await plan.execute(dbFile).toArray()).toEqual([
    { "0": { count: 2, max: 5 } },
  ])
})

Deno.test("QueryPlanNode JOINS", async () => {
  const { model, dbFile } = await init()

  const catNameCol = model.cats.schema.getColumnByNameOrThrow("name")
  const ownerNameCol = model.humans.schema.getColumnByNameOrThrow("firstName")
  const ownerIdCol = model.catOwners.schema.getColumnByNameOrThrow("ownerId")
  const plan = new Select(
    new Join(
      new Join(
        new TableScan("default", "cats"),
        new TableScan("default", "catOwners"),
        new Compare(
          new ColumnRefExpr(
            model.cats.schema.getColumnByNameOrThrow("id"),
            "cats",
          ),
          "=",
          new ColumnRefExpr(
            model.catOwners.schema.getColumnByNameOrThrow("petId"),
            "catOwners",
          ),
        ),
      ),
      new TableScan("default", "humans"),
      new Compare(
        new ColumnRefExpr(ownerIdCol, "catOwners"),
        "=",
        new ColumnRefExpr(
          model.humans.schema.getColumnByNameOrThrow("id"),
          "humans",
        ),
      ),
    ),
    {
      name: new ColumnRefExpr(catNameCol, "cats"),
      ownerId: new ColumnRefExpr(ownerIdCol, "catOwners"),
      ownerName: new ColumnRefExpr(ownerNameCol, "humans"),
    },
  )
  expect(await plan.execute(dbFile).toArray()).toEqual([
    { "0": { name: "fluffy", ownerId: 1, ownerName: "Alice" } },
    { "0": { name: "fluffy", ownerId: 2, ownerName: "Bob" } },
    { "0": { name: "mittens", ownerId: 2, ownerName: "Bob" } },
  ])
})

Deno.test("QueryBuilder JOINS", async () => {
  const { dbFile } = await init()

  const cats = await dbSchema.query()
    .from("cats")
    .join(
      "catOwners",
      (t) => t.column("catOwners", "petId").eq(t.column("cats", "id")),
    )
    .join(
      "humans",
      (t) => t.column("humans.id").eq(t.column("catOwners.ownerId")),
    )
    .select({
      catName: (t) => t.column("cats.name"),
      owner: (t) => t.column("humans.firstName"),
    }).plan().execute(dbFile).toArray()

  expect(cats).toEqual([
    { "0": { catName: "fluffy", owner: "Alice" } },
    { "0": { catName: "fluffy", owner: "Bob" } },
    { "0": { catName: "mittens", owner: "Bob" } },
  ])

  const plan = dbSchema.query()
    .from("cats")
    .join(
      "catOwners",
      (t) => t.column("cats.id").eq(t.column("catOwners.petId")),
    )
    .join(
      "humans",
      (t) => t.column("catOwners.ownerId").eq(t.column("humans.id")),
    )
    .select({
      name: (t) => t.column("cats.name"),
      ownerId: (t) => t.column("catOwners.ownerId"),
      ownerName: (t) => t.column("humans.firstName"),
    })
    .plan()

  expect(await plan.execute(dbFile).toArray()).toEqual([
    { "0": { name: "fluffy", ownerId: 1, ownerName: "Alice" } },
    { "0": { name: "fluffy", ownerId: 2, ownerName: "Bob" } },
    { "0": { name: "mittens", ownerId: 2, ownerName: "Bob" } },
  ])
})

Deno.test("QueryBuilder .in()", async () => {
  const { dbFile, model } = await init()
  await model.cats.insert({ name: "Mr. Blue", age: 3, id: 3 })
  const plan = dbSchema.query()
    .from("cats")
    .where((t) => t.column("cats", "name").in("fluffy", "Mr. Blue"))
    .select({ name: (t) => t.column("cats", "name") })
    .plan()
  expect(plan.describe()).toEqual(
    `Select(name AS name, Filter(TableScan(default.cats), In(name, ["fluffy", "Mr. Blue"])))`,
  )
  expect(await plan.execute(dbFile).toArray()).toEqual([
    { "0": { name: "fluffy" } },
    { "0": { name: "Mr. Blue" } },
  ])
})

Deno.test("QueryBuilder .not()", async () => {
  const { dbFile, model } = await init()
  await model.cats.insert({ name: "Mr. Blue", age: 3, id: 3 })
  const plan = dbSchema.query()
    .from("cats")
    .where((t) => t.column("cats", "name").eq("fluffy").not())
    .select({ name: (t) => t.column("cats", "name") })
    .plan()
  expect(plan.describe()).toEqual(
    `Select(name AS name, Filter(TableScan(default.cats), NOT(Compare(name = "fluffy"))))`,
  )
  expect(await plan.execute(dbFile).toArray()).toEqual([
    { "0": { name: "mittens" } },
    { "0": { name: "Mr. Blue" } },
  ])

  const plan2 = dbSchema.query()
    .from("cats")
    .where((t) => t.not(t.column("cats", "name").eq("fluffy")))
    .select({ name: (t) => t.column("cats", "name") })
    .plan()
  expect(plan2.describe()).toEqual(plan.describe())
})

Deno.test("QueryBuilder .aggregate()", async () => {
  const { dbFile } = await init()
  const plan = dbSchema.query()
    .from("cats")
    .aggregate({
      count: (agg) => agg.count(),
      maxAge: (agg, t) => agg.max(t.column("cats", "age")),
      maxName: (agg, t) => agg.max(t.column("cats", "name")),
    })
    .plan()

  expect(plan.describe()).toEqual(
    "Aggregate(TableScan(default.cats), MultiAggregation(count: COUNT(*), maxAge: MAX(age), maxName: MAX(name)))",
  )
  const data = await plan.execute(dbFile).toArray()
  expect(data).toEqual([
    { "0": { count: 2, maxAge: 5, maxName: "mittens" } },
  ])
  assertTrue<
    TypeEquals<
      typeof data,
      Array<{ "0": { count: number; maxAge: number; maxName: string } }>
    >
  >()
})
