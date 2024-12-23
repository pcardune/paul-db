import type { UnknownRecord } from "type-fest"
import { IPlanBuilder } from "../query/QueryBuilder.ts"

/**
 * A query is a computation that returns a set of rows.
 * You can use this type to define a query that returns a particular
 * type of row. For example:
 *
 * ```ts
 * type User = { id: string, name: string }
 * type UserQuery = Query<User>
 * ```
 */
export type Query<T extends UnknownRecord> = IPlanBuilder<Record<"$0", T>>
