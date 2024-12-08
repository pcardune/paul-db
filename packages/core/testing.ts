import { ensureDirSync } from "@std/fs"
import * as path from "@std/path"
import { spy, stub } from "jsr:@std/testing/mock"
import { IBufferPool } from "./pages/BufferPool.ts"

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

export function spyOnBufferPool(bufferPool: IBufferPool, trace = false) {
  const alllocatePage = bufferPool.allocatePage.bind(bufferPool)
  const allocatePage = stub(bufferPool, "allocatePage", async () => {
    const pageId = await alllocatePage()
    if (trace) {
      console.trace("allocatePage", pageId)
    }
    return pageId
  })
  const freePages = spy(bufferPool, "freePages")
  return {
    allocatePage,
    freePages,
    async getAllocatedPages() {
      const allocatedPageIds = await Promise.all(
        allocatePage.calls.flatMap((c) =>
          c.returned == null ? [] : [c.returned]
        ),
      )
      return allocatedPageIds.sort((a, b) => Number(a - b))
    },
    getFreedPages() {
      return freePages.calls.flatMap((c) => c.args[0]).sort((a, b) =>
        Number(a - b)
      )
    },
  }
}

export function assertType<T>(_value: T) {}
