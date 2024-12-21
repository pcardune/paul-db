# @paul-db/core

PaulDB is an _experimental_, typescript native relational database that can run
anywhere javascript can run.

## Installation

PaulDB is built with deno but can be used in any javascript environment.

Deno:

```bash
deno add jsr:@paul-db/core
```

npm:

```bash
npx jsr add @paul-db/core
```

Yarn:

```bash
yarn dlx jsr add @paul-db/core
```

pnpm:

```bash
pnpm dlx jsr add @paul-db/core
```

Bun:

```bash
bunx jsr add @paul-db/core
```

## Feature Overview

A database "connection" can be established with a variety of storage backends
including in-memory, local storage, and file storage.

```typescript
import { PaulDB } from "@paul-db/core"
const db = await PaulDB.inMemory()
// const db = await PaulDB.localStorage();
// const db = await PaulDB.open("/tmp/db", { create: true });
```

Database table schemas can be defined directly in typescript:

```typescript
import { schema as s } from "@paul-db/core"
const dbSchema = s.db().withTables(
  s.table("projects").with(
    s.column("id", "serial").unique(), // serial column will generate sequential ids for you.
    s.column("name", s.type.string()),
  ),
  s.table("todos").with(
    s.column("id", "serial").unique(), // unique columns are automatically indexed
    s.column("projectId", s.type.uint32()).index(),
    s.column("description", s.type.string()),
    s.column("createdAt", s.type.date())
      .defaultTo(() => new Date())
      .index(),
    s.column("completedAt", s.type.date().nullable()),
  ),
)
```

You can then read and write to the database using a model generated from the
schema:

```typescript ignore
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
```

While the model allows querying individual tables, you can also do more complex
queries across multiple tables using a SQL-like query builder that supports
filtering, sorting, table joins, aggregation, and more.

Start by constructing your query:

```typescript ignore
const allIncompleteTodosQuery = dbSchema.query()
  .from("todos")
  .join(
    "projects",
    (t) => t.tables.todos.projectId.eq(t.tables.projects.id),
  )
  .where((t) => t.tables.todos.completedAt.eq(null))
  .orderBy((t) => t.tables.todos.createdAt, "ASC")
  .select({
    taskDescription: (t) => t.tables.todos.description,
    taskCreatedOn: (t) => t.tables.todos.createdAt,
    projectName: (t) => t.tables.projects.name,
  })
```

Next, you can execute the query to get the results in the form of an async
iterator:

```typescript ignore
for await (const row of db.query(allIncompleteTodosQuery)) {
  console.log(
    `  - ${row.projectName}: [${row.taskCreatedOn.toLocaleDateString()}] ${row.taskDescription}`,
  )
}
```

Or just simply get all the results at once:

```typescript ignore
console.log(await db.query(allIncompleteTodosQuery).toArray())
```
