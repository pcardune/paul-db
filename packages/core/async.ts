import { Promisable } from "npm:type-fest"
/**
 * Collects all items from an async iterable into an array.
 */
export async function collectAsync<T>(
  iterable: AsyncIterable<T>,
): Promise<T[]> {
  const result: T[] = []
  for await (const item of iterable) {
    result.push(item)
  }
  return result
}

/**
 * Filters an async iterable using a predicate function.
 */
export function filterAsync<T>(
  iterable: AsyncIterable<T>,
  predicate: (item: T) => boolean,
): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator]: async function* () {
      for await (const item of iterable) {
        if (predicate(item)) {
          yield item
        }
      }
    },
  }
}

export function mapAsync<T, U>(
  iterable: AsyncIterable<T>,
  mapper: (item: T) => Promisable<U>,
): AsyncIterable<U> {
  return {
    [Symbol.asyncIterator]: async function* () {
      for await (const item of iterable) {
        yield await mapper(item)
      }
    },
  }
}

export function takeAsync<T>(
  iterable: AsyncIterable<T>,
  count: number,
): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator]: async function* () {
      let i = 0
      for await (const item of iterable) {
        if (i >= count) {
          break
        }
        yield item
        i++
      }
    },
  }
}

export function skipAsync<T>(
  iterable: AsyncIterable<T>,
  count: number,
): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator]: async function* () {
      let i = 0
      for await (const item of iterable) {
        if (i >= count) {
          yield item
        }
        i++
      }
    },
  }
}

/**
 * Wraps an async iterable to provide additional functionality.
 */
export class AsyncIterableWrapper<T> {
  private iterable: AsyncIterable<T>

  constructor(
    iterable: AsyncIterable<T> | (() => AsyncGenerator<T, void, unknown>),
  ) {
    if (typeof iterable === "function") {
      this.iterable = iterable()
    } else {
      this.iterable = iterable
    }
  }

  [Symbol.asyncIterator]() {
    return this.iterable[Symbol.asyncIterator]()
  }

  toArray(): Promise<T[]> {
    return collectAsync(this.iterable)
  }

  filter(predicate: (item: T) => boolean): AsyncIterableWrapper<T> {
    return new AsyncIterableWrapper(filterAsync(this.iterable, predicate))
  }

  map<U>(mapper: (item: T) => Promisable<U>): AsyncIterableWrapper<U> {
    return new AsyncIterableWrapper(mapAsync(this.iterable, mapper))
  }

  take(count: number): AsyncIterableWrapper<T> {
    return new AsyncIterableWrapper(takeAsync(this.iterable, count))
  }

  skip(count: number): AsyncIterableWrapper<T> {
    return new AsyncIterableWrapper(skipAsync(this.iterable, count))
  }
}

/**
 * A simple mutex implementation.
 */
export class Mutex {
  private locked = false
  private queue: (() => void)[] = []

  /**
   * Acquire the lock. If the lock is already held, this will wait until it is
   * released.
   *
   * @returns A promise that resolves when the lock is acquired.
   */
  acquire(): Promise<void> {
    if (!this.locked) {
      this.locked = true
      return Promise.resolve()
    }

    return new Promise((resolve) => {
      this.queue.push(resolve)
    })
  }

  /**
   * Release the lock.
   */
  release(): void {
    if (this.queue.length > 0) {
      this.queue.shift()!()
    } else {
      this.locked = false
    }
  }

  /**
   * Whether the lock is currently held.
   */
  get isLocked(): boolean {
    return this.locked
  }
}
