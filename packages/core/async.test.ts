import { expect } from "@std/expect"
import { AsyncIterableWrapper } from "./async.ts"

function* range(start: number, end: number) {
  for (let i = start; i < end; i++) {
    yield i
  }
}

Deno.test("AsyncIterableWrapper.asyncIterator", async () => {
  const iterable = new AsyncIterableWrapper(range(1, 4))
  const result = []
  for await (const item of iterable) {
    result.push(item)
  }
  expect(result).toEqual([1, 2, 3])
})

Deno.test("AsyncIterableWrapper.map", async () => {
  const iterable = new AsyncIterableWrapper(range(1, 4))
  const result = await iterable.map((x) => x * 2).toArray()
  expect(result).toEqual([2, 4, 6])
})

Deno.test("AsyncIterableWrapper.filter", async () => {
  const iterable = new AsyncIterableWrapper(range(1, 4))
  const result = await iterable.filter((x) => x % 2 === 0).toArray()
  expect(result).toEqual([2])
})

Deno.test("AsyncIterableWrapper.take", async () => {
  const iterable = new AsyncIterableWrapper(range(1, 4))
  const result = await iterable.take(2).toArray()
  expect(result).toEqual([1, 2])
})

Deno.test("AsyncIterableWrapper.skip", async () => {
  const iterable = new AsyncIterableWrapper(range(1, 4))
  const result = await iterable.skip(2).toArray()
  expect(result).toEqual([3])
})

Deno.test("AsyncIterableWrapper.first", async () => {
  const iterable = new AsyncIterableWrapper(range(1, 4))
  const result = await iterable.first()
  expect(result).toEqual(1)
})

Deno.test("AsyncIterableWrapper.toArray", async () => {
  const iterable = new AsyncIterableWrapper(range(1, 4))
  const result = await iterable.toArray()
  expect(result).toEqual([1, 2, 3])
})

Deno.test("AsyncIterablWrapper.reduce", async () => {
  const iterable = new AsyncIterableWrapper(range(1, 4))
  const result = await iterable.reduce((acc, x) => acc + x, 0)
  expect(result).toEqual(6)
})

Deno.test("AsyncIterableWrapper.reduce with initial value", async () => {
  const iterable = new AsyncIterableWrapper(range(1, 4))
  const result = await iterable.reduce((acc, x) => acc + x, 1)
  expect(result).toEqual(7)
})

Deno.test("AsyncIterableWrapper.reduce with empty iterable", async () => {
  const iterable = new AsyncIterableWrapper(range(1, 1))
  const result = await iterable.reduce((acc, x) => acc + x, 0)
  expect(result).toEqual(0)
})
