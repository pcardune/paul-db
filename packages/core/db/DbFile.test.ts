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
    const tables = await db.tablesTable.iterate().toArray()
    const indexes = await db.indexesTable.iterate().toArray()
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
          pageId: 28724n,
          pageType: "indexesTable",
        },
        {
          pageId: 45108n,
          pageType: "tablesTable",
        },
      ],
    )
    expect(tables).toEqual([
      {
        db: "system",
        heapPageId: 52n,
        id: "system.__dbPageIds",
        name: "__dbPageIds",
      },
      {
        db: "system",
        heapPageId: 16436n,
        id: "system.__dbIndexes",
        name: "__dbIndexes",
      },
      {
        db: "system",
        heapPageId: 8244n,
        id: "system.__dbTables",
        name: "__dbTables",
      },
      {
        db: "system",
        heapPageId: 69684n,
        id: "system.__dbSchemas",
        name: "__dbSchemas",
      },
      {
        db: "system",
        heapPageId: 102452n,
        id: "system.__dbTableColumns",
        name: "__dbTableColumns",
      },
    ])
    expect(indexes).toEqual([
      {
        heapPageId: 73780n,
        indexName: "__dbSchemas_id",
      },
      {
        heapPageId: 81972n,
        indexName: "__dbSchemas_tableId_version",
      },
      {
        heapPageId: 106548n,
        indexName: "__dbTableColumns_id",
      },
      {
        heapPageId: 110644n,
        indexName: "__dbTableColumns_schemaId",
      },
      {
        heapPageId: 114740n,
        indexName: "__dbTableColumns_schemaId_name",
      },
    ])

    expect(schemas).toEqual(
      [
        {
          id: 0,
          tableId: "system.__dbSchemas",
          version: 0,
        },
        {
          id: 1,
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

Deno.test("DbFile.createTable() and schema changes", async (t) => {
  using tempFile = generateTestFilePath("DbFile.db")

  async function init() {
    const db = await DbFile.open(tempFile.filePath, {
      create: true,
      truncate: true,
    })

    const usersSchema = s.table("users")
      .with(s.column("id", ColumnTypes.uint32()).unique())
      .with(s.column("name", ColumnTypes.string()))

    const table = await db.createTable(usersSchema)
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
    await expect(t.db.createTable(updatedSchema)).rejects.toThrow(
      'Column length mismatch. Found new column(s) "age"',
    )
  })
})
