import { schema as s } from "@paul-db/core"

/**
 * Converts a SQL type to a ColumnType instance
 */
export function getColumnTypeFromSQLType(sqlType: string): s.ColumnType<any> {
  if (sqlType.endsWith("[]")) {
    return getColumnTypeFromSQLType(sqlType.slice(0, -2)).array()
  }
  switch (sqlType) {
    case "TEXT":
    case "VARCHAR":
    case "CHAR":
      return s.type.string()
    case "SMALLINT":
      return s.type.int16()
    case "INT":
    case "INTEGER":
      return s.type.int32()
    case "FLOAT":
    case "REAL":
    case "DOUBLE":
      return s.type.float()
    case "BOOLEAN":
      return s.type.boolean()
    case "UUID":
      return s.type.uuid()
    case "JSON":
    case "JSONB":
      return s.type.json()
    case "DATE":
      return s.type.date()
    case "TIMESTAMP":
      return s.type.timestamp()
    case "BLOB":
      return s.type.blob()
    case "SERIAL":
      return s.type.serial()
    default:
      throw new Error(`Unknown SQL type: ${sqlType}`)
  }
}
