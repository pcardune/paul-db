import { expect } from "jsr:@std/expect"
import {
  ColumnRefExpr,
  Compare,
  Filter,
  LiteralValueExpr,
  Select,
  TableScan,
} from "./QueryPlanNode.ts"
import { DbFile, s } from "../mod.ts"
import { ColumnTypes } from "../schema/columns/ColumnType.ts"
import { QueryBuilder } from "./QueryBuilder.ts"

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

Deno.test("QueryPlanNode", async () => {
  const dbFile = await DbFile.open({ type: "memory" })
  const model = await dbFile.getDBModel(dbSchema)
  await model.cats.insert({ name: "fluffy", age: 3 })
  await model.cats.insert({ name: "mittens", age: 5 })

  const nameCol = model.cats.schema.getColumnByNameOrThrow("name")
  const ageCol = model.cats.schema.getColumnByNameOrThrow("age")
  const plan = new Select(
    new Filter(
      new TableScan("default", "cats"),
      new Compare(
        new ColumnRefExpr(nameCol),
        "=",
        new LiteralValueExpr("fluffy", ColumnTypes.string()),
      ),
    ),
    {
      name: new ColumnRefExpr(nameCol),
      age: new ColumnRefExpr(ageCol),
      isOld: new Compare(
        new ColumnRefExpr(ageCol),
        ">",
        new LiteralValueExpr(3, ColumnTypes.uint32()),
      ),
    },
  )

  expect(plan.describe()).toEqual(
    'Select(name AS name, age AS age, Compare(age > 3) AS isOld, Filter(TableScan(default.cats), Compare(name = "fluffy")))',
  )

  expect(await plan.execute(dbFile).toArray()).toEqual([{
    name: "fluffy",
    age: 3,
    isOld: false,
  }])

  // via the builder API:
  const plan2 = new QueryBuilder(dbSchema)
    .scan("cats")
    .where((t) =>
      t.column("name").eq("fluffy")
        .or(t.column("age").gt(3))
    )
    .plan()

  expect(plan2.describe()).toEqual(
    'Filter(TableScan(default.cats), (Compare(name = "fluffy") OR Compare(age > 3)))',
  )
  expect(await plan2.execute(dbFile).toArray()).toEqual([
    { id: 1, name: "fluffy", age: 3, likesTreats: true },
    { id: 2, name: "mittens", age: 5, likesTreats: true },
  ])
})
