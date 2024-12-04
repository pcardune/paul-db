import { expect } from "jsr:@std/expect"
import { ColumnTypes, DbFile, s } from "../mod.ts"
import { generateTestFilePath } from "../testing.ts"

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
    expect(tables).toEqual([
      {
        db: "system",
        heapPageId: 20n,
        id: "system.__dbPageIds",
        name: "__dbPageIds",
      },
      {
        db: "system",
        heapPageId: 20500n,
        id: "system.__dbSchemas",
        name: "__dbSchemas",
      },
      {
        db: "system",
        heapPageId: 24596n,
        id: "system.__dbTableColumns",
        name: "__dbTableColumns",
      },
    ])
    expect(indexes).toEqual([])

    expect(schemas).toEqual(
      [
        {
          id: 0,
          tableId: "system.__dbTableColumns",
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
    .with(s.column("id", ColumnTypes.uint32()).unique())
    .with(s.column("name", ColumnTypes.string()))

  const table = await db.getOrCreateTable(usersSchema)

  expect(
    await db.indexManager.getIndexStoragePageId({
      db: "default",
      table: "users",
      column: "id",
    }),
    "An index page won't be allocated until something is inserted",
  ).toBeNull()

  await table.insert({ id: 1, name: "Mr. Blue" })

  expect(
    await db.indexManager.getIndexStoragePageId({
      db: "default",
      table: "users",
      column: "id",
    }),
    "An index page won't be allocated until something is inserted",
  ).not.toBeNull()
})

Deno.test.ignore("DbFile.createTable() and schema changes", async (t) => {
  using tempFile = generateTestFilePath("DbFile.db")

  async function init() {
    const db = await DbFile.open(tempFile.filePath, {
      create: true,
      truncate: true,
    })

    const usersSchema = s.table("users")
      .with(s.column("id", ColumnTypes.uint32()).unique())
      .with(s.column("name", ColumnTypes.string()))

    const table = await db.getOrCreateTable(usersSchema)
    await table.insert({ id: 1, name: "Mr. Blue" })
    return {
      db,
      table,
      usersSchema,
      [Symbol.dispose]: db[Symbol.dispose].bind(db),
    }
  }

  await t.step("Adding a column", async () => {
    using t = await init()
    expect(await t.table.lookupUniqueOrThrow("id", 1)).toEqual({
      id: 1,
      name: "Mr. Blue",
    })
    const updatedSchema = t.usersSchema.with(
      s.column("age", ColumnTypes.uint16()),
    )
    await expect(t.db.getOrCreateTable(updatedSchema)).rejects.toThrow(
      'Column length mismatch. Found new column(s) "age"',
    )
  })
})
