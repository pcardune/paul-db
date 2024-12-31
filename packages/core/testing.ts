import { ensureDirSync } from "@std/fs"
import * as path from "@std/path"
import { spy, stub } from "@std/testing/mock"
import { IBufferPool } from "./pages/BufferPool.ts"

const subdir = `${Date.now()}-${Math.floor(Math.random() * 1000)}`

let subdirCounter = 0
export type TestFilePath = {
  [Symbol.dispose]: () => void
  filePath: string
}

export function generateTestFilePath(
  filename: string = "",
): TestFilePath {
  const dir = path.join("test_output", subdir)
  ensureDirSync(dir)
  const parts = filename.split(".")
  const prefix = parts.slice(0, -1).join(".")
  const extension = parts[parts.length - 1]
  const filePath = path.join(dir, `${prefix}-test-${Date.now()}.${extension}`)
  subdirCounter++
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
      subdirCounter--
      if (subdirCounter === 0) {
        Deno.removeSync(dir)
      }
    },
  }
}

export function spyOnBufferPool(bufferPool: IBufferPool, trace = false) {
  const originalAllocatePage = bufferPool.allocatePage.bind(bufferPool)
  const myAllocate = async () => {
    const pageId = await originalAllocatePage()
    if (trace) {
      console.trace("allocatePage", pageId)
    }
    return pageId
  }
  let allocatePage = stub(bufferPool, "allocatePage", myAllocate)
  let freePages = spy(bufferPool, "freePages")
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
    restore() {
      allocatePage.restore()
      freePages.restore()
    },
    reset() {
      allocatePage.restore()
      freePages.restore()
      allocatePage = stub(bufferPool, "allocatePage", myAllocate)
      freePages = spy(bufferPool, "freePages")
    },
  }
}

export function assertType<T>(_value: T) {}
export type TypeEquals<Actual, Expected> = Actual extends Expected ? true
  : "Types not equal"

/**
 * This is a type-level test to ensure that the `TypeEquals` type works as
 * expected.
 *
 * Usage:
 *
 * ```ts
 * assertTrue<TypeEquals<"green", "green">>()
 * ```
 */
export function assertTrue<T extends true>() {}

/**
 * Helper class to track outstanding timers and clear them when the object is
 * disposed.
 *
 * ```ts ignore
 * using tracer = new TimeoutTracer()
 * function infiniteTimeout() {
 *   console.log("infiniteTimeout")
 *   setTimeout(infiniteTimeout, 1000)
 * }
 * infiniteTimeout()
 * ```
 */
export class TimeoutTracer {
  outstanding = new Map<
    number,
    {
      type: "timeout" | "interval"
      cb: () => void
      stacks: Array<string | undefined>
    }
  >()

  private realSetTimeout = globalThis.setTimeout.bind(globalThis)
  private realSetInterval = globalThis.setInterval.bind(globalThis)
  private realClearTimeout = globalThis.clearTimeout.bind(globalThis)
  private realClearInterval = globalThis.clearInterval.bind(globalThis)
  private realSetImmediate = (globalThis as any).setImmediate?.bind(globalThis)

  private activeCallback: number | null = null

  private startTracking(
    id: number,
    { type, cb, stack }: {
      cb: () => void
      stack: string | undefined
      type: "interval" | "timeout"
    },
  ) {
    this.outstanding.set(id, {
      type,
      cb,
      stacks: [
        ...(this.outstanding.get(this.activeCallback as number)?.stacks ?? []),
        stack,
      ],
    })
  }

  constructor() {
    globalThis.clearTimeout = (id) => {
      this.realClearTimeout(id)
      this.outstanding.delete(id as number)
    }
    globalThis.clearInterval = globalThis.clearTimeout

    globalThis.setTimeout = (cb, ms) => {
      const newCB = () => {
        this.activeCallback = id
        ;(cb as () => void)()
        this.outstanding.delete(id)
      }
      const id = this.realSetTimeout(newCB, ms)

      this.startTracking(id, {
        type: "timeout",
        cb: newCB,
        stack: new Error().stack,
      })
      this.trace?.("setTimeout", id, ms)
      return id
    }
    globalThis.setInterval = (cb, ms) => {
      const newCB = () => {
        this.activeCallback = id
        ;(cb as () => void)()
        this.outstanding.delete(id)
      }
      const id = this.realSetInterval(newCB, ms)
      this.startTracking(id, {
        type: "interval",
        cb: newCB,
        stack: new Error().stack,
      })
      this.trace?.("setInterval", id, ms)
      return id
    }
    ;(globalThis as any).setImmediate = (cb: any) => {
      const newCB = () => {
        this.activeCallback = id
        ;(cb as () => void)()
        this.outstanding.delete(id)
      }
      const id = this.realSetTimeout(newCB, 0)
      this.startTracking(id, {
        type: "timeout",
        cb: newCB,
        stack: new Error().stack,
      })
      this.trace?.("setImmediate", id)
      return id
    }
  }

  trace?: (...args: any[]) => void;

  [Symbol.dispose]() {
    let depth = 0
    while (this.outstanding.size > 0) {
      console.warn(
        "Disposing TimeTracker:",
        this.outstanding.size,
        "timers outstanding",
        "depth",
        depth,
      )
      this.trace = console.trace
      const items = Array.from(this.outstanding.entries())
      for (const [id, { type: kind, cb, stacks }] of items) {
        console.error("Outstanding timer", id)
        for (const stack of stacks) {
          console.error(stack)
        }
        if (kind === "timeout") {
          cb()
        } else {
          clearInterval(id)
          cb()
        }
      }
      if (depth > 10) {
        for (const [id, { stacks }] of items) {
          console.error("Outstanding timer", id, stacks)
          clearTimeout(id)
        }
        break
      }
      depth++
    }
    globalThis.setTimeout = this.realSetTimeout
    globalThis.setInterval = this.realSetInterval
    globalThis.clearTimeout = this.realClearTimeout
    globalThis.clearInterval = this.realClearInterval
    ;(globalThis as any).setImmediate = this.realSetImmediate
  }
}
