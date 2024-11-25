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

/**
 * Wraps an async iterable to provide additional functionality.
 */
export class AsyncIterableWrapper<T> {
  constructor(private iterable: AsyncIterable<T>) {}

  [Symbol.asyncIterator]() {
    return this.iterable[Symbol.asyncIterator]()
  }

  toArray(): Promise<T[]> {
    return collectAsync(this.iterable)
  }

  filter(predicate: (item: T) => boolean): AsyncIterableWrapper<T> {
    return new AsyncIterableWrapper(filterAsync(this.iterable, predicate))
  }
}
