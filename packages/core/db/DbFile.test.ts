import { expect } from "@std/expect"
import { s } from "../mod.ts"
import { DbFile, DBModel } from "./DbFile.ts"
import {
  assertTrue,
  assertType,
  generateTestFilePath,
  TypeEquals,
} from "../testing.ts"
import { HeapFileTableInfer } from "../tables/TableStorage.ts"
import { AsyncIterableWrapper } from "../async.ts"
import { Json } from "../types.ts"

Deno.test("DbFile initialization", async (t) => {
  using tempFile = generateTestFilePath("DbFile.db")
  using db = await DbFile.open({
    type: "file",
    path: tempFile.filePath,
    create: true,
    truncate: true,
  })
  async function dumpState(db: DbFile) {
    const pages = await db.dbPageIdsTable.iterate().toArray()
    const tables = await db.tableManager.tablesTable.iterate().toArray()
    const indexes = await db.indexManager.indexesTable.iterate().toArray()
    const { schemaTable } = await db.getSchemasTable()
    const schemas = await schemaTable.iterate().toArray()
    return { pages, tables, indexes, schemas }
  }

  async function getColumns(db: DbFile) {
    const { columnsTable } = await db.getSchemasTable()
    for (const schema of initialState.schemas) {
      const columns = await columnsTable.lookup("schemaId", schema.id)
      expect(columns.length).toBeGreaterThan(0)
    }
  }

  const initialState = await dumpState(db)

  await t.step("Initial state", () => {
    const { pages, tables, indexes, schemas } = initialState
    expect(pages).toEqual(
      [
        {
          pageId: 4116n,
          pageType: "indexesTable",
        },
        {
          pageId: 12308n,
          pageType: "dbsTable",
        },
        {
          pageId: 16404n,
          pageType: "tablesTable",
        },
      ],
    )
    expect(tables.map(({ id: _id, ...t }) => t)).toEqual([
      {
        db: "system",
        heapPageId: 20n,
        name: "__dbPageIds",
      },
      {
        db: "system",
        heapPageId: 24596n,
        name: "__dbSchemas",
      },
      {
        db: "system",
        heapPageId: 28692n,
        name: "__dbTableColumns",
      },
    ])
    expect(indexes).toEqual([])

    expect(schemas).toEqual(
      [
        {
          id: 0,
          tableId: tables.find((t) => t.name === "__dbTableColumns")?.id,
          version: 0,
        },
      ],
    )
  })

  await t.step("Reading columns works", async () => {
    await getColumns(db)
  })

  await t.step("Reopening the db yields the same initial state", async () => {
    using db = await DbFile.open({ type: "file", path: tempFile.filePath })
    const newState = await dumpState(db)
    expect(newState).toEqual(initialState)
    await getColumns(db)
  })
})

Deno.test("DbFile.createTable()", async () => {
  using tempFile = generateTestFilePath("DbFile.db")
  using db = await DbFile.open({
    type: "file",
    path: tempFile.filePath,
    create: true,
    truncate: true,
  })

  const usersSchema = s.table("users")
    .with(s.column("id", s.type.uint32()).unique())
    .with(s.column("name", s.type.string()))

  const table = await db.getOrCreateTable(usersSchema)
  const tableRecord = await db.tableManager.getTableRecord("default", "users")

  const indexId = {
    tableId: tableRecord!.id,
    indexName: "id",
  }
  expect(
    await db.indexManager.getIndexStoragePageId(indexId),
    "An index page won't be allocated until something is inserted",
  ).toBeNull()

  await table.insert({ id: 1, name: "Mr. Blue" })

  expect(
    await db.indexManager.getIndexStoragePageId(indexId),
    "An index page won't be allocated until something is inserted",
  ).not.toBeNull()
})

Deno.test("DbFile.createTable() and schema changes", async (t) => {
  using tempFile = generateTestFilePath("DbFile.db")
  const usersSchema = s.table("users")
    .with(s.column("id", s.type.uint32()).unique())
    .with(s.column("name", s.type.string()))
  const dbSchema = s.db().withTables(usersSchema)

  async function init() {
    const db = await DbFile.open({
      type: "file",
      path: tempFile.filePath,
      create: true,
      truncate: true,
    })
    const model = await db.getDBModel(dbSchema)
    await model.users.insert({ id: 1, name: "Mr. Blue" })
    return {
      db,
      users: model.users,
      model,
      [Symbol.dispose]: () => {
        db[Symbol.dispose]()
      },
    }
  }

  await t.step("Adding a table", async () => {
    using t = await init()
    const schema2 = t.model.$schema.withTables(
      s.table("todos").with(
        s.column("id", s.type.uint32()).unique(),
        s.column("text", s.type.string()),
      ),
    )
    await expect(t.db.getDBModel(schema2)).rejects.toThrow(
      "Table default.todos not found in db. Did you forget to run a migration?",
    )

    const model = await t.db.getDBModel(schema2, 2, async (helper) => {
      expect(helper.currentVersion).toBe(1)
      await helper.addMissingTables()
    })
    await model.todos.insert({ id: 1, text: "Buy milk" })
  })

  await t.step("Adding a column", async () => {
    using t = await init()
    expect(await t.users.lookupUniqueOrThrow("id", 1)).toEqual({
      id: 1,
      name: "Mr. Blue",
    })

    const schema2 = s.db().withTables(
      t.model.$schema.schemas.users.with(
        s.column("age", s.type.uint16()).defaultTo(() => 0),
      ),
    )
    await expect(t.db.getDBModel(schema2)).rejects.toThrow(
      "Column default.users.age not found in db. Did you forget to run a migration?",
    )

    const model = await t.db.getDBModel(schema2, 2, async (helper) => {
      expect(helper.currentVersion).toBe(1)
      await helper.addMissingColumn("users", "age")
    })

    expect(await model.users.lookupUniqueOrThrow("id", 1)).toEqual({
      id: 1,
      name: "Mr. Blue",
      age: 0,
    })
  })
})

Deno.test("DbFile.getDBModel()", async () => {
  using tempFile = generateTestFilePath("DbFile.db")
  const dbSchema = s.db().withTables(
    s.table("users")
      .with(
        s.column("id", s.type.uint32()).unique(),
        s.column("name", s.type.string()),
      ),
    s.table("todos")
      .with(
        s.column("id", s.type.uint32()).unique(),
        s.column("text", s.type.string()),
      ),
  )

  using dbFile = await DbFile.open({
    type: "file",
    path: tempFile.filePath,
    create: true,
    truncate: true,
  })

  const model = await dbFile.getDBModel(dbSchema)

  await model.users.insert({ id: 1, name: "Mr. Blue" })
  await model.todos.insert({ id: 1, text: "Buy milk" })

  assertTrue<
    TypeEquals<DBModel<typeof dbSchema>, {
      users: HeapFileTableInfer<typeof dbSchema.schemas.users>
      todos: HeapFileTableInfer<typeof dbSchema.schemas.todos>
    }>
  >()
  assertType<DBModel<typeof dbSchema>>(model)
})

Deno.test("DbFile.export() and DbFile.importRecords()", async () => {
  const rows: { db: string; table: string; record: Json }[] = []
  const dbSchema = s.db().withTables(
    s.table("users")
      .with(
        s.column("id", s.type.uint32()).unique(),
        s.column("name", s.type.string()),
      ),
    s.table("todos")
      .with(
        s.column("id", s.type.uint32()).unique(),
        s.column("text", s.type.string()),
      ),
  )

  {
    using tempFile = generateTestFilePath("DbFile.db")
    using dbFile = await DbFile.open({
      type: "file",
      path: tempFile.filePath,
      create: true,
      truncate: true,
    })
    const model = await dbFile.getDBModel(dbSchema)
    await model.users.insert({ id: 1, name: "Mr. Blue" })
    await model.todos.insert({ id: 1, text: "Buy milk" })
    for await (const row of dbFile.exportRecords({ db: "default" })) {
      rows.push(row)
    }
  }
  expect(rows).toEqual([
    {
      db: "default",
      table: "users",
      record: { id: 1, name: "Mr. Blue" },
    },
    {
      db: "default",
      table: "todos",
      record: { id: 1, text: "Buy milk" },
    },
  ])

  {
    using tempFile = generateTestFilePath("DbFile.db")
    using dbFile = await DbFile.open({
      type: "file",
      path: tempFile.filePath,
      create: true,
      truncate: true,
    })
    const model = await dbFile.getDBModel(dbSchema)
    await dbFile.importRecords(new AsyncIterableWrapper(rows))

    expect(await model.users.iterate().toArray()).toEqual([
      { id: 1, name: "Mr. Blue" },
    ])
    expect(await model.todos.iterate().toArray()).toEqual([
      { id: 1, text: "Buy milk" },
    ])
    expect(await model.users.lookupUniqueOrThrow("id", 1)).toEqual(
      { id: 1, name: "Mr. Blue" },
    )
  }
})

Deno.test("local storage buffer pool", async () => {
  localStorage.clear()
  {
    using db = await DbFile.open({ type: "localstorage", prefix: "test" })
    const dbSchema = s.db().withTables(
      s.table("users").with(
        s.column("id", s.type.uint32()).unique(),
        s.column("name", s.type.string()),
      ),
    )
    const model = await db.getDBModel(dbSchema)
    await model.users.insert({ id: 1, name: "Alice" })
    expect(await model.users.iterate().toArray()).toEqual([
      { id: 1, name: "Alice" },
    ])

    expect(localStorage.getItem("test-header")).toEqual("1")
    expect(localStorage.getItem("test-root")).toEqual("18")
  }

  {
    using db = await DbFile.open({ type: "localstorage", prefix: "test" })
    const dbSchema = s.db().withTables(
      s.table("users").with(
        s.column("id", s.type.uint32()).unique(),
        s.column("name", s.type.string()),
      ),
    )
    const model = await db.getDBModel(dbSchema)
    expect(await model.users.iterate().toArray()).toEqual([
      { id: 1, name: "Alice" },
    ])
  }
})

import indexedDB from "npm:fake-indexeddb@6.2.2"

Deno.test("IndexedDB buffer pool", async () => {
  const dbName = "test" + Math.random()
  const req = indexedDB.deleteDatabase(dbName)
  await new Promise((resolve, reject) => {
    req.onsuccess = resolve
    req.onerror = reject
  })
  const dbSchema = s.db().withTables(
    s.table("users").with(
      s.column("id", s.type.uint32()).unique(),
      s.column("name", s.type.string()),
    ),
  )
  using db = await DbFile.open({
    type: "indexeddb",
    name: dbName,
    indexedDB,
  })
  {
    const model = await db.getDBModel(dbSchema)
    await model.users.insert({ id: 1, name: "Alice" })
    expect(await model.users.iterate().toArray()).toEqual([
      { id: 1, name: "Alice" },
    ])
  }

  using db2 = await DbFile.open({ type: "indexeddb", name: dbName, indexedDB })
  {
    const model = await db2.getDBModel(dbSchema)
    expect(await model.users.iterate().toArray()).toEqual([
      { id: 1, name: "Alice" },
    ])
  }

  // This seems to be necessary due to timers that are not cleared in
  // the fake indexedDB implementation
  await new Promise((resolve) => setTimeout(resolve, 10))
})
