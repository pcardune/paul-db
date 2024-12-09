import { expect } from "jsr:@std/expect"
import { s } from "../mod.ts"
import { DbFile, DBModel } from "./DbFile.ts"
import {
  assertTrue,
  assertType,
  generateTestFilePath,
  TypeEquals,
} from "../testing.ts"
import { tableSchemaMigration } from "./migrations.ts"
import { HeapFileTableInfer } from "../tables/TableStorage.ts"
import { AsyncIterableWrapper } from "../async.ts"
import { Json } from "../types.ts"

Deno.test("DbFile initialization", async (t) => {
  using tempFile = generateTestFilePath("DbFile.db")
  using db = await DbFile.open(tempFile.filePath, {
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
        heapPageId: 20500n,
        name: "__dbSchemas",
      },
      {
        db: "system",
        heapPageId: 24596n,
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
    using db = await DbFile.open(tempFile.filePath)
    const newState = await dumpState(db)
    expect(newState).toEqual(initialState)
    await getColumns(db)
  })
})

Deno.test("DbFile.createTable()", async () => {
  using tempFile = generateTestFilePath("DbFile.db")
  using db = await DbFile.open(tempFile.filePath, {
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

  async function init() {
    const db = await DbFile.open(tempFile.filePath, {
      create: true,
      truncate: true,
    })

    const users = await db.getOrCreateTable(usersSchema)
    await users.insert({ id: 1, name: "Mr. Blue" })
    return {
      db,
      users,
      [Symbol.dispose]: db[Symbol.dispose].bind(db),
    }
  }

  await t.step("Adding a column", async () => {
    using t = await init()
    expect(await t.users.lookupUniqueOrThrow("id", 1)).toEqual({
      id: 1,
      name: "Mr. Blue",
    })

    expect(
      await t.db.tableManager.tablesTable.scanIter("db", "default").map((t) =>
        t.name
      ).toArray(),
    ).toEqual(["users"])

    const { newSchema } = await t.db.migrate(tableSchemaMigration(
      "add-age-column",
      t.users.schema,
      (oldSchema) => oldSchema.with(s.column("age", s.type.uint16())),
      (row) => ({ ...row, age: 42 }),
    ))

    const newTable = await t.db.getOrCreateTable(newSchema)
    expect(await newTable.lookupUniqueOrThrow("id", 1)).toEqual({
      id: 1,
      name: "Mr. Blue",
      age: 42,
    })

    expect(t.users.lookupUnique("id", 1)).rejects.toThrow(
      "Table has been dropped",
    )

    expect(
      await t.db.tableManager.tablesTable.scanIter("db", "default").map((t) =>
        t.name
      ).toArray(),
    ).toEqual(["users"])
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

  using dbFile = await DbFile.open(tempFile.filePath, {
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
    using dbFile = await DbFile.open(tempFile.filePath, {
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
    using dbFile = await DbFile.open(tempFile.filePath, {
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
  }
})
