import { expect } from "jsr:@std/expect"
import { FixedWidthArray } from "./FixedWidthArray.ts"
import { describe, it } from "jsr:@std/testing/bdd"
import { FixedWidthStruct } from "./Struct.ts"

const pointType = new FixedWidthStruct<{ x: number; y: number }>({
  size: 8,
  write: (value, view) => {
    view.setInt32(0, value.x)
    view.setInt32(4, value.y)
  },
  read: (view) => ({
    x: view.getInt32(0),
    y: view.getInt32(4),
  }),
})

describe("Creating FixedWidthArray", () => {
  it("can create an empty array that can hold a given number of element", () => {
    const array = FixedWidthArray.empty({ length: 10, type: pointType })
    expect(array.length).toBe(0)
    expect(array.maxLength).toBe(10)
    expect(array.bufferSize).toBe(4 + 10 * 8)
  })

  it("can create an empty array with a given buffer size", () => {
    const array = FixedWidthArray.empty({ bufferSize: 100, type: pointType })
    expect(array.length).toBe(0)
    expect(array.maxLength).toBe(12)
    expect(array.bufferSize).toBe(100)
  })
})

describe("Reading and Writing", () => {
  it("lets you push and pop elements", () => {
    const array = FixedWidthArray.empty({ length: 10, type: pointType })
    array.push({ x: 1, y: 2 })
    array.push({ x: 3, y: 4 })
    expect(array.length).toBe(2)
    expect(array.pop()).toEqual({ x: 3, y: 4 })
    expect(array.length).toBe(1)
    expect(array.pop()).toEqual({ x: 1, y: 2 })
    expect(array.length).toBe(0)
  })

  it("lets you read and write elements", () => {
    const array = FixedWidthArray.empty({ length: 10, type: pointType })
    array.push({ x: 1, y: 2 })
    array.push({ x: 3, y: 4 })
    expect(array.get(0)).toEqual({ x: 1, y: 2 })
    expect(array.get(1)).toEqual({ x: 3, y: 4 })
    array.set(0, { x: 5, y: 6 })
    expect(array.get(0)).toEqual({ x: 5, y: 6 })
  })

  it("throws when reading or writing out of bounds", () => {
    const array = FixedWidthArray.empty({ length: 10, type: pointType })
    expect(() => array.get(10)).toThrow("Index out of bounds")
    expect(() => array.set(10, { x: 1, y: 2 })).toThrow("Index out of bounds")
  })

  it("lets you iterate over elements", () => {
    const array = FixedWidthArray.empty({ length: 10, type: pointType })
    array.push({ x: 1, y: 2 })
    array.push({ x: 3, y: 4 })
    expect([...array]).toEqual([{ x: 1, y: 2 }, { x: 3, y: 4 }])
  })
})
