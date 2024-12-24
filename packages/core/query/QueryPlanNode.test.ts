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
import { PaulDB, schema as s } from "../exports/mod.ts"
import { ArrayColumnType, ColumnTypes } from "../schema/columns/ColumnType.ts"
import { assertTrue, TypeEquals } from "../testing.ts"
import { ColumnNames, SchemasForTQB, TQBTableNames } from "./QueryBuilder.ts"
import type { IsEqual } from "type-fest"

async function init() {
  const db = await PaulDB.inMemory()
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
  const model = await db.getModelForSchema(dbSchema)
  const [fluffy, mittens, _mrBlue] = await model.cats.insertManyAndReturn([
    { name: "fluffy", age: 3 },
    { name: "mittens", age: 5 },
    { name: "Mr. Blue", age: 16 },
  ])
  const [alice, bob, _charlie] = await model.humans.insertManyAndReturn([
    { firstName: "Alice", lastName: "Smith" },
    { firstName: "Bob", lastName: "Jones" },
    { firstName: "Charlie", lastName: "Brown" },
  ])
  await model.catOwners.insertMany([
    { petId: fluffy.id, ownerId: alice.id },
    { petId: fluffy.id, ownerId: bob.id },
    { petId: mittens.id, ownerId: bob.id },
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
      "$0",
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
    'Limit(Select(name AS name, age AS age, Compare(age > 3) AS isOld, Filter(TableScan(default.cats), Compare(name = "fluffy"))) AS $0, 1)',
  )

  expect(await plan.execute(db).toArray()).toEqual([{
    "$0": {
      name: "fluffy",
      age: 3,
      isOld: false,
    },
  }])

  // via the builder API:
  const oldAndFluffyQuery = model.$schema.query()
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
    { cats: { id: 3, name: "Mr. Blue", age: 16, likesTreats: true } },
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
    "$0",
  )
  expect(await plan.execute(db).toArray()).toEqual([
    { "$0": { count: 3, max: 16 } },
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
    "$0",
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
    "$0",
  )
  expect(plan.describe()).toEqual(
    "GroupBy(TableScan(default.products), category: category, color: color, MultiAggregation(count: COUNT(*), maxPrice: MAX(price))) AS $0",
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

Deno.test("TypeTools", async () => {
  const { model } = await init()
  const query = model.$schema.query().from("cats")
  type QueryColumns = ColumnNames<typeof query, "cats">
  assertTrue<TypeEquals<TQBTableNames<typeof query>, "cats">>()
  assertTrue<TypeEquals<QueryColumns, "id" | "name" | "age" | "likesTreats">>()

  assertTrue<
    TypeEquals<
      SchemasForTQB<typeof query>,
      Pick<typeof model["$schema"]["schemas"], "cats">
    >
  >()
  const withJoin = query.join(
    "catOwners",
    (t) => t.literal(true, s.type.boolean()),
  )
  type QueryWithJoinColumns = ColumnNames<typeof withJoin, "catOwners">
  assertTrue<TypeEquals<QueryWithJoinColumns, "petId" | "ownerId">>()
  assertTrue<TypeEquals<TQBTableNames<typeof withJoin>, "cats" | "catOwners">>()
  assertTrue<
    TypeEquals<
      SchemasForTQB<typeof withJoin>,
      Pick<typeof model["$schema"]["schemas"], "cats" | "catOwners">
    >
  >()
})

Deno.test("TableQueryBuilder .select()", async () => {
  const { model } = await init()

  const cats = await model.$query((q) =>
    q.from("cats")
      .select({
        catName: (t) => t.column("cats.name"),
        age: (t) => t.column("cats.age"),
      })
  ).toArray()

  expect(cats).toEqual([
    { catName: "fluffy", age: 3 },
    { catName: "mittens", age: 5 },
    { catName: "Mr. Blue", age: 16 },
  ])

  {
    const allCatCols = await model.$query((q) => q.from("cats").select("cats"))
      .toArray()
    expect(allCatCols).toEqual([
      { name: "fluffy", age: 3, id: 1, likesTreats: true },
      { name: "mittens", age: 5, id: 2, likesTreats: true },
      { name: "Mr. Blue", age: 16, id: 3, likesTreats: true },
    ])
    assertTrue<
      IsEqual<
        typeof allCatCols,
        Array<{ name: string; age: number; id: number; likesTreats: boolean }>
      >
    >()
  }
})

Deno.test("QueryBuilder (INNER) JOINS", async () => {
  const { model } = await init()

  const cats = await model.$query(
    (q) =>
      q.from("cats")
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
})

Deno.test("QueryBuilder LEFT JOINS", async () => {
  const { model } = await init()

  const cats = await model.$query((q) =>
    q.from("cats")
      .leftJoin(
        "catOwners",
        (t) => t.column("cats", "id").eq(t.column("catOwners", "petId")),
      )
      .leftJoin(
        "humans",
        (t) => t.column("humans.id").eq(t.column("catOwners.ownerId")),
      )
      .select({
        catName: (t) => t.column("cats.name"),
        owner: (t) => t.column("humans.firstName"),
      })
  ).toArray()

  expect(cats).toEqual([
    { catName: "fluffy", owner: "Alice" },
    { catName: "fluffy", owner: "Bob" },
    { catName: "mittens", owner: "Bob" },
    { catName: "Mr. Blue", owner: null },
  ])

  assertTrue<
    TypeEquals<Array<{ catName: string; owner: string | null }>, typeof cats>
  >()
})

Deno.test("QueryBuilder .aggregate()", async (test) => {
  const { db, model } = await init()
  const plan = model.$schema.query()
    .from("cats")
    .aggregate({
      count: (agg) => agg.count(),
      maxAge: (agg, t) => agg.max(t.column("cats", "age")),
      maxName: (agg, t) => agg.max(t.column("cats", "name")),
      minName: (agg, t) => agg.min(t.column("cats", "name")),
      totalAge: (agg, t) => agg.sum(t.column("cats", "age")),
      firstAge: (agg, t) => agg.first(t.column("cats", "age")),
      names: (agg, t) => agg.arrayAgg(t.column("cats", "name")),
    })
    .plan()

  expect(plan.describe()).toEqual(
    "Aggregate(TableScan(default.cats), MultiAggregation(count: COUNT(*), maxAge: MAX(age), maxName: MAX(name), minName: MIN(name), totalAge: SUM(age), firstAge: FIRST(age), names: ARRAY_AGG(name))) AS $0",
  )
  const data = await plan.execute(db).toArray()
  expect(data).toEqual([
    {
      "$0": {
        count: 3,
        maxAge: 16,
        maxName: "mittens",
        minName: "Mr. Blue",
        totalAge: 24,
        firstAge: 3,
        names: ["fluffy", "mittens", "Mr. Blue"],
      },
    },
  ])
  assertTrue<
    TypeEquals<
      Array<
        {
          "$0": {
            count: number
            maxAge: number
            maxName: string
            minName: string
            totalAge: number
            firstAge: number
            names: string[]
          }
        }
      >,
      typeof data
    >
  >()

  await test.step(".arrayAgg().filter()", async (test) => {
    const query = model.$schema.query()
      .from("cats")
      .aggregate({
        names: (agg, t) =>
          agg.arrayAgg(t.column("cats", "name")).filter(
            (name) => name.neq("fluffy"),
          ),
      })
    const result = await model.$query(query).toArray()
    expect(result).toEqual([{ names: ["mittens", "Mr. Blue"] }])

    await test.step("with .asTable()", () => {
      const tqb = query.asTable("catNames")
      assertTrue<
        TypeEquals<
          typeof tqb.tableSchemas.catNames.columnsByName.names.type,
          ArrayColumnType<string>
        >
      >()
    })
  })

  await test.step(".arrayAgg().filterNonNull()", async () => {
    const dbSchema = s.db().withTables(
      s.table("products").with(
        s.column("category", s.type.string()),
        s.column("tagline", s.type.string().nullable()),
      ),
    )
    const model = await db.getModelForSchema(dbSchema)
    await model.products.insertMany([
      { category: "fruit", tagline: "juicy" },
      { category: "fruit", tagline: null },
      { category: "fruit", tagline: "sweet" },
      { category: "veg", tagline: "crunchy" },
      { category: "veg", tagline: null },
    ])
    const result = await model.$query((q) =>
      q.from("products")
        .groupBy({
          category: (t) => t.column("products.category"),
        })
        .aggregate({
          taglines: (agg, t) =>
            agg.arrayAgg(t.column("products.tagline")).filterNonNull(),
        })
    ).toArray()
    expect(result).toEqual([
      { category: "fruit", taglines: ["juicy", "sweet"] },
      { category: "veg", taglines: ["crunchy"] },
    ])
    assertTrue<
      TypeEquals<typeof result, Array<{ category: string; taglines: string[] }>>
    >()
  })
})

Deno.test("QueryBuilder subqueries", async () => {
  const { model, db } = await init()
  const query = model.$schema.query()
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
    "Select(firstName AS name, Subquery(Aggregate(Filter(TableScan(default.catOwners), Compare(ownerId = id)), MultiAggregation(count: COUNT(*))) AS $0) AS numCats, TableScan(default.humans)) AS $0",
  )
  const data = await plan.execute(db).toArray()
  expect(data).toEqual([
    { "$0": { name: "Alice", numCats: 1 } },
    { "$0": { name: "Bob", numCats: 2 } },
    { "$0": { name: "Charlie", numCats: 0 } },
  ])
  assertTrue<
    TypeEquals<
      typeof data,
      Array<{ "$0": { name: string; numCats: number } }>
    >
  >()
  expect(plan.toJSON()).toEqual({
    type: "Select",
    alias: "$0",
    child: {
      alias: "humans",
      db: "default",
      table: "humans",
      type: "TableScan",
    },
    columns: {
      name: {
        type: "column_ref",
        table: "humans",
        column: "firstName",
      },
      numCats: {
        type: "subquery",
        subplan: {
          type: "Aggregate",
          alias: "$0",
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
                table: "catOwners",
                column: "ownerId",
              },
              operator: "=",
              right: {
                type: "column_ref",
                table: "humans",
                column: "id",
              },
            },
          },
        },
      },
    },
  })
})

Deno.test("QueryBuilder .groupBy", async (test) => {
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
  const model = await db.getModelForSchema(dbSchema)

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
    "GroupBy(TableScan(default.products), category: category, color: color, MultiAggregation(count: COUNT(*), maxPrice: MAX(price))) AS $0",
  )

  const results = await model.$query(query).toArray()
  expect(results).toEqual([
    { category: "fruit", color: "red", count: 3, maxPrice: 1 },
    { category: "fruit", color: "yellow", count: 1, maxPrice: 0.5 },
    { category: "veg", color: "orange", count: 1, maxPrice: 0.25 },
    { category: "veg", color: "green", count: 2, maxPrice: 0.75 },
    { category: "veg", color: "brown", count: 1, maxPrice: 0.25 },
  ])

  assertTrue<
    TypeEquals<
      typeof results,
      Array<{
        category: string
        color: string
        count: number
        maxPrice: number
      }>
    >
  >()

  await test.step("with .arrayAgg()", async (test) => {
    const query = dbSchema.query()
      .from("products")
      .groupBy({
        category: (t) => t.column("products.category"),
      })
      .aggregate({
        names: (agg, t) => agg.arrayAgg(t.tables.products.name),
      })

    const results = await model.$query(query).toArray()
    expect(results).toEqual([
      { category: "fruit", names: ["apple", "cherry", "banana", "tomato"] },
      { category: "veg", names: ["carrot", "lettuce", "cucumber", "potato"] },
    ])

    await test.step("with .asTable()", () => {
      const tqb = query.asTable("categories")
      assertTrue<
        TypeEquals<
          typeof tqb.tableSchemas.categories.columnsByName.names.type,
          ArrayColumnType<string>
        >
      >()
    })
  })
})

Deno.test("QueryBuilder.with()", async () => {
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
  const model = await db.getModelForSchema(dbSchema)

  await model.products.insertMany([
    { name: "apple", category: "fruit", color: "red", price: 1.0 },
    { name: "tomato", category: "fruit", color: "red", price: 0.75 },
    { name: "lettuce", category: "veg", color: "green", price: 0.75 },
    // the below are all have price less than or equal to 0.5, the above not.
    { name: "cherry", category: "fruit", color: "red", price: 0.5 },
    { name: "banana", category: "fruit", color: "yellow", price: 0.5 },
    { name: "carrot", category: "veg", color: "orange", price: 0.25 },
    { name: "cucumber", category: "veg", color: "green", price: 0.5 },
    { name: "potato", category: "veg", color: "brown", price: 0.25 },
  ])

  const priceData = await model.$query((q) =>
    q.with(
      (q) =>
        q.from("products")
          .where((t) => t.column("products.price").gt(0.5))
          .select({
            category: (t) => t.column("products.category"),
            price: (t) => t.column("products.price"),
          })
          .asTable("selected"),
    )
      .from("selected")
      .aggregate({
        totalPrice: (agg, t) => agg.sum(t.column("selected.price")),
        prices: (agg, t) => {
          const prices = agg.arrayAgg(t.tables.selected.price)
          return prices
        },
      })
  ).toArray()
  expect(priceData).toEqual([{ totalPrice: 2.50, prices: [1, 0.75, 0.75] }])

  const selectedQuery = dbSchema.query()
    .from("products")
    .where((t) => t.column("products.price").gt(0.5))
    .select({
      category: (t) => t.column("products.category"),
      price: (t) => t.column("products.price"),
    })

  const aggregatedQuery = selectedQuery.asTable("selected").groupBy({
    category: (t) => t.column("selected.category"),
  }).aggregate({
    totalPrice: (agg, t) => agg.sum(t.column("selected.price")),
  })

  expect(await model.$query(aggregatedQuery).toArray()).toEqual([
    { category: "fruit", totalPrice: 1.75 },
    { category: "veg", totalPrice: 0.75 },
  ])
})
