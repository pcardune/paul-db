import { expect } from "jsr:@std/expect"
import { it } from "jsr:@std/testing/bdd"
import { randomIntegerBetween, randomSeeded } from "@std/random"
import { binarySearch } from "./binarySearch.ts"

it("Find index in sorted array", () => {
  for (let j = 1; j < 100; j++) {
    const prng = randomSeeded(0n)
    const nums = new Set<number>()
    for (let i = 0; i < j; i++) {
      nums.add(randomIntegerBetween(1, 10000, { prng }))
    }
    const arr = Array.from(nums.values()).sort((a, b) => a - b)
    for (let i = 0; i < 100; i++) {
      const n = randomIntegerBetween(1, 10000, { prng })
      const index = binarySearch(
        arr,
        n,
        (a, b) => a - b,
      )
      expect(index).toBeGreaterThanOrEqual(0)
      expect(index).toBeLessThanOrEqual(arr.length)
      if (index === arr.length) {
        expect(arr[index - 1]).toBeLessThanOrEqual(n)
        continue
      }
      expect(n).toBeLessThanOrEqual(arr[index])
    }
  }
})
