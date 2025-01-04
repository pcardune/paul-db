import type { Promisable } from "type-fest"

/**
 * Collects all items from an async iterable into an array.
 */
async function collectAsync<T>(
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
function filterAsync<T>(
  iterable: AsyncIterable<T>,
  predicate: (item: T) => Promisable<boolean>,
): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator]: async function* () {
      for await (const item of iterable) {
        if (await predicate(item)) {
          yield item
        }
      }
    },
  }
}

function mapAsync<T, U>(
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

function takeAsync<T>(
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

function skipAsync<T>(
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

async function reduceAsync<T, U>(
  iterable: AsyncIterable<T>,
  reducer: (accumulator: U, item: T) => Promisable<U>,
  initialValue: U,
): Promise<U> {
  let accumulator = initialValue
  for await (const item of iterable) {
    accumulator = await reducer(accumulator, item)
  }
  return accumulator
}

/**
 * Wraps an async iterable to provide additional functionality.
 */
export class AsyncIterableWrapper<T, TNext = any> {
  private iterable: AsyncIterable<T, void, TNext>

  constructor(
    iterable:
      | Iterable<T, void, TNext>
      | AsyncIterable<T, void, TNext>
      | (() => AsyncGenerator<T, void, TNext>),
  ) {
    if (typeof iterable === "function") {
      this.iterable = iterable()
    } else if (Symbol.asyncIterator in iterable) {
      this.iterable = iterable
    } else {
      this.iterable = {
        [Symbol.asyncIterator]: async function* () {
          yield* iterable
        },
      }
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<T, void, TNext> {
    return this.iterable[Symbol.asyncIterator]()
  }

  /**
   * Collects all items from the iterable into an array.
   */
  toArray(): Promise<T[]> {
    return collectAsync(this.iterable)
  }

  async first(): Promise<T | undefined> {
    const [first] = await this.take(1).toArray()
    return first
  }

  /**
   * Filters the items in the iterable using a predicate function.
   */
  filter<S extends T>(
    predicate: (item: T) => item is S,
  ): AsyncIterableWrapper<S, TNext>
  filter(
    predicate: (item: T) => Promisable<boolean>,
  ): AsyncIterableWrapper<T, TNext>
  filter(
    predicate: (item: T) => Promisable<boolean>,
  ): AsyncIterableWrapper<T, TNext> {
    return new AsyncIterableWrapper(filterAsync(this.iterable, predicate))
  }

  /**
   * Maps the items in the iterable using a mapper function.
   */
  map<U>(mapper: (item: T) => Promisable<U>): AsyncIterableWrapper<U, TNext> {
    return new AsyncIterableWrapper(mapAsync(this.iterable, mapper))
  }

  /**
   * Takes the first `count` items from the iterable.
   */
  take(count: number): AsyncIterableWrapper<T, TNext> {
    return new AsyncIterableWrapper(takeAsync(this.iterable, count))
  }

  /**
   * Skips the first `count` items in the iterable.
   */
  skip(count: number): AsyncIterableWrapper<T, TNext> {
    return new AsyncIterableWrapper(skipAsync(this.iterable, count))
  }

  /**
   * Reduces the items in the iterable using a reducer function.
   */
  reduce<U>(
    reducer: (accumulator: U, item: T) => Promisable<U>,
    initialValue: U,
  ): Promise<U> {
    return reduceAsync(this.iterable, reducer, initialValue)
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
   * @returns A disposable object that releases the lock when disposed.
   *
   * ```ts
   * const mutex = new Mutex()
   * {
   *   using _lock = await mutex.useLock()
   *   // do something with the lock
   * }
   * ```
   */
  async useLock(): Promise<{ [Symbol.dispose]: () => void }> {
    await this.acquire()
    return {
      [Symbol.dispose]: () => this.release(),
    }
  }

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
