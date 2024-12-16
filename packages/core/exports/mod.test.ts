import { expect } from "@std/expect/expect"
import { PaulDB, schema as s } from "./mod.ts"

Deno.test("PaulDB", async () => {
  const dbSchema = s.db().withTables(
    s.table("projects").with(
      s.column("id", "serial").unique(),
      s.column("name", s.type.string()),
    ),
    s.table("todos").with(
      s.column("id", "serial").unique(), // unique columns are automatically indexed
      s.column("projectId", s.type.uint32()).index(),
      s.column("description", s.type.string()),
      s
        .column("createdAt", s.type.date())
        .defaultTo(() => new Date())
        .index(),
      s.column("completedAt", s.type.date().nullable()),
    ),
  )

  const db = await PaulDB.inMemory()

  const model = await db.getModelForSchema(dbSchema)

  const project = await model.projects.insertAndReturn({
    name: "Paul's Database",
  })
  const todos = await model.todos.insertManyAndReturn([
    {
      projectId: project.id,
      completedAt: null,
      description: "Write to the database",
    },
    {
      projectId: project.id,
      completedAt: null,
      description: "Query the database with sql",
    },
    {
      projectId: project.id,
      completedAt: null,
      description: "Query the database with typescript",
    },
  ])

  await model.todos.updateWhere("id", todos[0].id, { completedAt: new Date() })

  const allIncompleteTodosQuery = dbSchema
    .query()
    .from("todos")
    .join(
      "projects",
      (t) => t.column("todos.projectId").eq(t.column("projects.id")),
    )
    .where((t) => t.column("todos.completedAt").eq(null))
    .orderBy((t) => t.column("todos.createdAt"), "ASC")
    .select({
      taskDescription: (t) => t.column("todos.description"),
      taskCreatedOn: (t) => t.column("todos.createdAt"),
      projectName: (t) => t.column("projects.name"),
    })

  const projectSummaryQuery = dbSchema
    .query()
    .from("todos")
    .join(
      "projects",
      (t) => t.column("todos.projectId").eq(t.column("projects.id")),
    )
    .where((t) => t.column("todos.completedAt").eq(null))
    .groupBy({ projectId: (t) => t.column("todos.projectId") })
    .aggregate({
      numTodosRemaining: (agg) => agg.count(),
      projectName: (agg, t) => agg.first(t.column("projects.name")),
    })

  /**
   * Execute queries against the database. Query results are asynchronously
   * streamed as the query executes using async iterators.
   */
  const summary = await db.query(projectSummaryQuery).toArray()
  expect(summary).toEqual([
    {
      projectId: 1,
      projectName: "Paul's Database",
      numTodosRemaining: 2,
    },
  ])
  const allIncompleteTodos = await db.query(allIncompleteTodosQuery).toArray()
  expect(allIncompleteTodos).toEqual([
    {
      projectName: "Paul's Database",
      taskCreatedOn: todos[1].createdAt,
      taskDescription: "Query the database with sql",
    },
    {
      projectName: "Paul's Database",
      taskCreatedOn: todos[2].createdAt,
      taskDescription: "Query the database with typescript",
    },
  ])
})
