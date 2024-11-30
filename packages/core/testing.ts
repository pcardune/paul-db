import { ensureDirSync } from "@std/fs"
import * as path from "@std/path"

const subdir = `${Date.now()}`

export function generateTestFilePath(
  filename: string = "",
) {
  const dir = path.join("test_output", subdir)
  ensureDirSync(dir)
  const parts = filename.split(".")
  const prefix = parts.slice(0, -1).join(".")
  const extension = parts[parts.length - 1]
  const filePath = path.join(dir, `${prefix}-test-${Date.now()}.${extension}`)
  return {
    filePath: filePath,
    [Symbol.dispose]: () => {
      try {
        Deno.removeSync(filePath)
      } catch (e) {
        if (!(e instanceof Deno.errors.NotFound)) {
          throw e
        }
      }
    },
  }
}
