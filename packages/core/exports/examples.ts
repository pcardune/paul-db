import { schema as s } from "@paul-db/core"

export const dbSchema = s.db().withTables(
  s.table("users").with(
    s.column("id", s.type.serial()).unique(),
    s.column("name", s.type.string()),
    s.column("username", s.type.string()).unique(),
    s.column("lastLogin", s.type.timestamp()),
  ),
  s.table("posts").with(
    s.column("id", s.type.serial()).unique(),
    s.column("createdAt", s.type.timestamp()).defaultTo(() => new Date()),
    s.column("title", s.type.string()),
    s.column("content", s.type.string()),
    s.column("authorId", s.type.uint32()),
    s.column("rating", s.type.uint16()), // scale of 1-10
  ),
)
