import { expect } from "@std/expect"
import {
  Aggregate,
  ColumnRefExpr,
  Compare,
  CountAggregation,
  Filter,
  GroupBy,
  Join,
  Limit,
  LiteralValueExpr,
  MaxAggregation,
  MultiAggregation,
  Select,
  TableScan,
} from "./QueryPlanNode.ts"
import { PaulDB, s } from "../mod.ts"
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
  const db = await PaulDB.inMemory()
  const model = await db.dbFile.getDBModel(dbSchema)
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
  return { model, db }
}

Deno.test("QueryPlanNode", async () => {
  const { model, db } = await init()

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

  expect(await plan.execute(db).toArray()).toEqual([{
    "$0": {
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
  const oldOrFluffyCats = await plan2.execute(db).toArray()
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
  expect(await plan3.execute(db).toArray()).toEqual([
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
  expect(await plan4.execute(db).toArray()).toEqual([
    { "cats": { id: 2, name: "mittens", age: 5, likesTreats: true } },
  ])

  const namesOnly = await limitedAndOrderedOldAndFluffy.select({
    catName: (t) => t.column("cats.name"),
    isOld: (t) => t.column("cats.age").gt(3),
  }).plan().execute(db).toArray()
  expect(namesOnly).toEqual([
    { "$0": { catName: "mittens", isOld: true } },
  ])
  assertTrue<
    TypeEquals<
      typeof namesOnly,
      Array<{ "$0": { catName: string; isOld: boolean } }>
    >
  >()
})

Deno.test("QueryPlanNode Aggregates", async () => {
  const { model, db } = await init()

  const ageCol = model.cats.schema.getColumnByNameOrThrow("age")
  const plan = new Aggregate(
    new TableScan("default", "cats"),
    new MultiAggregation({
      count: new CountAggregation(),
      max: new MaxAggregation(new ColumnRefExpr(ageCol, "cats")),
    }),
  )
  expect(await plan.execute(db).toArray()).toEqual([
    { "$0": { count: 2, max: 5 } },
  ])
})

Deno.test("QueryPlanNode JOINS", async () => {
  const { model, db } = await init()

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
  expect(await plan.execute(db).toArray()).toEqual([
    { "$0": { name: "fluffy", ownerId: 1, ownerName: "Alice" } },
    { "$0": { name: "fluffy", ownerId: 2, ownerName: "Bob" } },
    { "$0": { name: "mittens", ownerId: 2, ownerName: "Bob" } },
  ])
})

Deno.test("QueryPlanNode GroupBy", async () => {
  const db = await PaulDB.inMemory()
  const model = await db.dbFile.getDBModel(
    s.db().withTables(
      s.table("products").with(
        s.column("id", "serial"),
        s.column("name", s.type.string()),
        s.column("category", s.type.string()),
        s.column("color", s.type.string()),
        s.column("price", s.type.float()),
      ),
    ),
  )

  await model.products.insertMany([
    { name: "apple", category: "fruit", color: "red", price: 1.0 },
    { name: "cherry", category: "fruit", color: "red", price: 0.5 },
    { name: "banana", category: "fruit", color: "yellow", price: 0.5 },
    { name: "carrot", category: "veg", color: "orange", price: 0.25 },
    { name: "lettuce", category: "veg", color: "green", price: 0.75 },
    { name: "tomato", category: "fruit", color: "red", price: 0.75 },
    { name: "cucumber", category: "veg", color: "green", price: 0.5 },
    { name: "potato", category: "veg", color: "brown", price: 0.25 },
  ])

  const plan = new GroupBy(
    new TableScan("default", model.products.schema.name),
    {
      category: new ColumnRefExpr(
        model.products.schema.getColumnByNameOrThrow("category"),
        "products",
      ),
      color: new ColumnRefExpr(
        model.products.schema.getColumnByNameOrThrow("color"),
        "products",
      ),
    },
    new MultiAggregation({
      count: new CountAggregation(),
      maxPrice: new MaxAggregation(
        new ColumnRefExpr(
          model.products.schema.getColumnByNameOrThrow("price"),
          "products",
        ),
      ),
    }),
  )
  expect(plan.describe()).toEqual(
    "GroupBy(TableScan(default.products), category: category, color: color, MultiAggregation(count: COUNT(*), maxPrice: MAX(price)))",
  )
  const results = await plan.execute(db).toArray()
  expect(results).toEqual(
    [
      { "$0": { category: "fruit", color: "red", count: 3, maxPrice: 1 } },
      { "$0": { category: "fruit", color: "yellow", count: 1, maxPrice: 0.5 } },
      { "$0": { category: "veg", color: "orange", count: 1, maxPrice: 0.25 } },
      { "$0": { category: "veg", color: "green", count: 2, maxPrice: 0.75 } },
      { "$0": { category: "veg", color: "brown", count: 1, maxPrice: 0.25 } },
    ],
  )
})

// Deno.test("QueryPlanNode Subqueries", async () => {
//   const { model, db } = await init()
//   const catNameCol = model.cats.schema.getColumnByNameOrThrow("name")
//   const ownerNameCol = model.humans.schema.getColumnByNameOrThrow("firstName")
//   const ownerIdCol = model.catOwners.schema.getColumnByNameOrThrow("ownerId")

//   const plan = new Select(
//     new TableScan("default", "catOwners"),
//     {
//       ownerId: new ColumnRefExpr(ownerIdCol, "catOwners"),
//       numCats: new QueryExpr
//     },
//   )
//   expect(await plan.execute(db).toArray()).toEqual([
//     { "$0": { ownerId: 1 } },
//     { "$0": { ownerId: 2 } },
//     { "$0": { ownerId: 2 } },
//   ])
// })

Deno.test("QueryBuilder JOINS", async () => {
  const { db } = await init()

  const cats = await db.query(
    dbSchema.query()
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
      }),
  ).toArray()

  expect(cats).toEqual([
    { catName: "fluffy", owner: "Alice" },
    { catName: "fluffy", owner: "Bob" },
    { catName: "mittens", owner: "Bob" },
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

  expect(await plan.execute(db).toArray()).toEqual([
    { "$0": { name: "fluffy", ownerId: 1, ownerName: "Alice" } },
    { "$0": { name: "fluffy", ownerId: 2, ownerName: "Bob" } },
    { "$0": { name: "mittens", ownerId: 2, ownerName: "Bob" } },
  ])
})

Deno.test("QueryBuilder .in()", async () => {
  const { db, model } = await init()
  await model.cats.insert({ name: "Mr. Blue", age: 3, id: 3 })
  const plan = dbSchema.query()
    .from("cats")
    .where((t) => t.column("cats", "name").in("fluffy", "Mr. Blue"))
    .select({ name: (t) => t.column("cats", "name") })
    .plan()
  expect(plan.describe()).toEqual(
    `Select(name AS name, Filter(TableScan(default.cats), In(name, ["fluffy", "Mr. Blue"])))`,
  )
  expect(await plan.execute(db).toArray()).toEqual([
    { "$0": { name: "fluffy" } },
    { "$0": { name: "Mr. Blue" } },
  ])
})

Deno.test("QueryBuilder .not()", async () => {
  const { db, model } = await init()
  await model.cats.insert({ name: "Mr. Blue", age: 3, id: 3 })
  const plan = dbSchema.query()
    .from("cats")
    .where((t) => t.column("cats", "name").eq("fluffy").not())
    .select({ name: (t) => t.column("cats", "name") })
    .plan()
  expect(plan.describe()).toEqual(
    `Select(name AS name, Filter(TableScan(default.cats), NOT(Compare(name = "fluffy"))))`,
  )
  expect(await plan.execute(db).toArray()).toEqual([
    { "$0": { name: "mittens" } },
    { "$0": { name: "Mr. Blue" } },
  ])

  const plan2 = dbSchema.query()
    .from("cats")
    .where((t) => t.not(t.column("cats", "name").eq("fluffy")))
    .select({ name: (t) => t.column("cats", "name") })
    .plan()
  expect(plan2.describe()).toEqual(plan.describe())
})

Deno.test("QueryBuilder .aggregate()", async () => {
  const { db } = await init()
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
  const data = await plan.execute(db).toArray()
  expect(data).toEqual([
    { "$0": { count: 2, maxAge: 5, maxName: "mittens" } },
  ])
  assertTrue<
    TypeEquals<
      typeof data,
      Array<{ "$0": { count: number; maxAge: number; maxName: string } }>
    >
  >()
})

Deno.test("QueryBuilder subqueries", async () => {
  const { db } = await init()
  const query = dbSchema.query()
    .from("humans")
    .select({
      name: (t) => t.column("humans.firstName"),
      numCats: (t) =>
        t.subquery((qb) =>
          qb.from("catOwners")
            .where((st) =>
              st.column("catOwners.ownerId").eq(st.column("humans.id"))
            )
            .aggregate({ count: (agg) => agg.count() })
        ),
    })
  const plan = query.plan()
  expect(plan.describe()).toEqual(
    "Select(firstName AS name, Subquery(Aggregate(Filter(TableScan(default.catOwners), Compare(ownerId = id)), MultiAggregation(count: COUNT(*)))) AS numCats, TableScan(default.humans))",
  )
  const data = await plan.execute(db).toArray()
  expect(data).toEqual([
    { "$0": { name: "Alice", numCats: 1 } },
    { "$0": { name: "Bob", numCats: 2 } },
  ])
  assertTrue<
    TypeEquals<
      typeof data,
      Array<{ "$0": { name: string; numCats: number } }>
    >
  >()
  expect(plan.toJSON()).toEqual({
    type: "Select",
    child: {
      alias: "humans",
      db: "default",
      table: "humans",
      type: "TableScan",
    },
    columns: {
      name: {
        type: "column_ref",
        column: "firstName",
      },
      numCats: {
        type: "subquery",
        subplan: {
          type: "Aggregate",
          aggregation: {
            type: "multi_agg",
            aggregations: {
              count: {
                type: "count",
              },
            },
          },
          child: {
            type: "Filter",
            child: {
              type: "TableScan",
              alias: "catOwners",
              db: "default",
              table: "catOwners",
            },
            predicate: {
              type: "compare",
              left: {
                type: "column_ref",
                column: "ownerId",
              },
              operator: "=",
              right: {
                type: "column_ref",
                column: "id",
              },
            },
          },
        },
      },
    },
  })
})

Deno.test("QueryBuilder .groupBy", async () => {
  const db = await PaulDB.inMemory()
  const dbSchema = s.db().withTables(
    s.table("products").with(
      s.column("id", "serial"),
      s.column("name", s.type.string()),
      s.column("category", s.type.string()),
      s.column("color", s.type.string()),
      s.column("price", s.type.float()),
    ),
  )
  const model = await db.dbFile.getDBModel(dbSchema)

  await model.products.insertMany([
    { name: "apple", category: "fruit", color: "red", price: 1.0 },
    { name: "cherry", category: "fruit", color: "red", price: 0.5 },
    { name: "banana", category: "fruit", color: "yellow", price: 0.5 },
    { name: "carrot", category: "veg", color: "orange", price: 0.25 },
    { name: "lettuce", category: "veg", color: "green", price: 0.75 },
    { name: "tomato", category: "fruit", color: "red", price: 0.75 },
    { name: "cucumber", category: "veg", color: "green", price: 0.5 },
    { name: "potato", category: "veg", color: "brown", price: 0.25 },
  ])

  const query = dbSchema.query()
    .from("products")
    .groupBy({
      category: (t) => t.column("products.category"),
      color: (t) => t.column("products.color"),
    })
    .aggregate({
      count: (agg) => agg.count(),
      maxPrice: (agg, t) => agg.max(t.column("products.price")),
    })

  expect(query.plan().describe()).toEqual(
    "GroupBy(TableScan(default.products), category: category, color: color, MultiAggregation(count: COUNT(*), maxPrice: MAX(price)))",
  )

  const results = await db.query(query).toArray()
  expect(results).toEqual([
    { category: "fruit", color: "red", count: 3, maxPrice: 1 },
    { category: "fruit", color: "yellow", count: 1, maxPrice: 0.5 },
    { category: "veg", color: "orange", count: 1, maxPrice: 0.25 },
    { category: "veg", color: "green", count: 2, maxPrice: 0.75 },
    { category: "veg", color: "brown", count: 1, maxPrice: 0.25 },
  ])
})
