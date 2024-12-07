import { Promisable } from "npm:type-fest"
import { StoredRecordForTableSchema, TableSchema } from "./schema/schema.ts"
import { ColumnTypes } from "./schema/columns/ColumnType.ts"
import { column } from "./schema/columns/ColumnBuilder.ts"
import { Mutex } from "./async.ts"
import { HeapFileRowId } from "./tables/TableStorage.ts"
import { TableManager } from "./db/TableManager.ts"

export interface SerialIdGenerator {
  next(columnName: string): Promisable<number>
}

const dbSequenceTableSchema = TableSchema.create("__dbSequences")
  .with(column("name", ColumnTypes.string()).unique())
  .with(column("value", ColumnTypes.uint32()).defaultTo(() => 0))

export class DBFileSerialIdGenerator implements SerialIdGenerator {
  private locks = new Map<string, Mutex>()
  constructor(
    private tableManager: TableManager,
    private sequencePrefix: string,
  ) {
  }

  private async acquireLock(columnName: string) {
    let lock = this.locks.get(columnName)
    if (!lock) {
      lock = new Mutex()
      this.locks.set(columnName, lock)
    }
    await lock.acquire()
    return { [Symbol.dispose]: () => lock.release() }
  }

  _cache = new Map<
    string,
    {
      rowId: HeapFileRowId
      sequenceRecord: StoredRecordForTableSchema<typeof dbSequenceTableSchema>
    }
  >()

  async next(columnName: string): Promise<number> {
    const sequenceTable = await this.tableManager.getOrCreateTable(
      "system",
      dbSequenceTableSchema,
    )

    const cached = this._cache.get(columnName)
    if (cached) {
      const { rowId, sequenceRecord } = cached
      const updated = { ...sequenceRecord, value: sequenceRecord.value + 1 }
      this._cache.set(columnName, { rowId, sequenceRecord: updated })
      await sequenceTable.set(rowId, updated)
      return updated.value
    }

    const sequenceName = `${this.sequencePrefix}.${columnName}`
    const sequenceRecordIds = await sequenceTable.lookupInIndex(
      "name",
      sequenceName,
    )
    if (sequenceRecordIds.length === 0) {
      await sequenceTable.insert({ name: sequenceName, value: 1 })
      return 1
    } else if (sequenceRecordIds.length > 1) {
      throw new Error(
        `Multiple sequence records found for ${sequenceName}`,
      )
    }
    using _lock = await this.acquireLock(columnName)
    const sequenceRecord = await sequenceTable.get(sequenceRecordIds[0])
    if (sequenceRecord == null) {
      throw new Error(
        `Sequence record ${sequenceRecordIds[0]} not found for ${sequenceName}`,
      )
    }
    await sequenceTable.set(sequenceRecordIds[0], {
      ...sequenceRecord,
      value: sequenceRecord.value + 1,
    })
    return sequenceRecord.value + 1
  }
}
