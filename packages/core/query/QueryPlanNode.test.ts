import { expect } from "jsr:@std/expect"
import {
  ColumnRefExpr,
  Compare,
  Filter,
  LiteralValueExpr,
  TableScan,
} from "./QueryPlanNode.ts"
import { DbFile, s } from "../mod.ts"
import { ColumnTypes } from "../schema/columns/ColumnType.ts"

Deno.test("QueryPlanNode", async () => {
  const dbFile = await DbFile.open({ type: "memory" })
  const model = await dbFile.getDBModel(
    s.db("default").withTables(
      s.table("cats").with(
        s.column("name", s.type.string()),
        s.column("age", s.type.uint32()),
      ),
    ),
  )
  await model.cats.insert({ name: "fluffy", age: 3 })
  await model.cats.insert({ name: "mittens", age: 5 })

  const plan = new Filter(
    new TableScan("default", "cats"),
    new Compare(
      new ColumnRefExpr(
        model.cats.schema.getColumnByNameOrThrow("name"),
      ),
      "=",
      new LiteralValueExpr("fluffy", ColumnTypes.string()),
    ),
  )

  expect(plan.describe()).toEqual(
    'Filter(TableScan(default.cats), Predicate(name = "fluffy"))',
  )

  const rows = await plan.execute(dbFile).toArray()
  expect(rows).toEqual([{ name: "fluffy", age: 3 }])
})
