# @paul-db/sql

The `@paul-db/sql` provides support for querying a PaulDB database using SQL.

## Example

```typescript
import { PaulDB } from "@paul-db/core"
import { SQLExecutor } from "@paul-db/sql"

const db = await PaulDB.inMemory()
const db = PaulDB.inMemory()
const executor = new SQLExecutor(db)
await executor.execute("CREATE TABLE test (id INT, name TEXT)")
await executor.execute("INSERT INTO test (id, name) VALUES (1, 'Alice')")
const result = await executor.execute("SELECT * FROM test")
console.log(result)
```
