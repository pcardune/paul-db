import { describe, it } from "jsr:@std/testing/bdd"
import { FixedWidthStruct, Struct, VariableWidthStruct } from "./Struct.ts"
import { expect } from "jsr:@std/expect"

const pointStruct = new FixedWidthStruct<{ x: number; y: number }>({
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

describe("FixedWidthStruct", () => {
  it("lets you read and write values that take a fixed amount of space", () => {
    const view = new DataView(new ArrayBuffer(pointStruct.size * 4))
    pointStruct.writeAt({ x: 1, y: 2 }, view, 0)
    pointStruct.writeAt({ x: 3, y: 4 }, view, pointStruct.size * 1)
    pointStruct.writeAt({ x: 5, y: 6 }, view, pointStruct.size * 2)
    pointStruct.writeAt({ x: 7, y: 8 }, view, pointStruct.size * 3)

    expect(pointStruct.readAt(view, 0)).toEqual({ x: 1, y: 2 })
    expect(pointStruct.readAt(view, pointStruct.size * 1)).toEqual({
      x: 3,
      y: 4,
    })
    expect(pointStruct.readAt(view, pointStruct.size * 2)).toEqual({
      x: 5,
      y: 6,
    })
    expect(pointStruct.readAt(view, pointStruct.size * 3)).toEqual({
      x: 7,
      y: 8,
    })
  })

  it("throws when reading out of bounds", () => {
    const view = new DataView(new ArrayBuffer(pointStruct.size * 4))
    expect(() => pointStruct.readAt(view, pointStruct.size * 4)).toThrow(
      "Reading past the end of the view",
    )
  })

  it("throws when writing out of bounds", () => {
    const view = new DataView(new ArrayBuffer(pointStruct.size * 4))
    expect(() =>
      pointStruct.writeAt({ x: 1, y: 2 }, view, pointStruct.size * 4)
    ).toThrow(
      "Writing past the end of the view",
    )
  })

  describe(".array()", () => {
    it("Creates a struct for storing an array of fixed-width values", () => {
      const pointArrayStruct = pointStruct.array()

      const points = [
        { x: 1, y: 2 },
        { x: 3, y: 4 },
        { x: 5, y: 6 },
        { x: 7, y: 8 },
      ]

      const view = new DataView(
        new ArrayBuffer(pointArrayStruct.sizeof(points)),
      )

      pointArrayStruct.writeAt(points, view, 0)

      expect(pointArrayStruct.readAt(view, 0)).toEqual([
        { x: 1, y: 2 },
        { x: 3, y: 4 },
        { x: 5, y: 6 },
        { x: 7, y: 8 },
      ])
    })
  })
})

describe("VariableWidthStruct", () => {
  const pointListStruct = new VariableWidthStruct<{ x: number; y: number }[]>({
    sizeof: (value) => pointStruct.size * value.length,
    write(value, view) {
      for (let i = 0; i < value.length; i++) {
        pointStruct.writeAt(value[i], view, i * pointStruct.size)
      }
    },
    read(view) {
      const length = view.byteLength / pointStruct.size
      const points = []
      for (let i = 0; i < length; i++) {
        points.push(pointStruct.readAt(view, i * pointStruct.size))
      }
      return points
    },
  })

  const points = [
    { x: 1, y: 2 },
    { x: 3, y: 4 },
    { x: 5, y: 6 },
    { x: 7, y: 8 },
  ]

  it("calculates its size correctly", () => {
    expect(pointListStruct.sizeof(points)).toBe(4 + points.length * 8)
  })

  it("lets you read and write values that take a variable amount of space", () => {
    const view = new DataView(new ArrayBuffer(pointListStruct.sizeof(points)))

    pointListStruct.writeAt(
      points,
      view,
      0,
    )

    expect(pointListStruct.readAt(view, 0)).toEqual([
      { x: 1, y: 2 },
      { x: 3, y: 4 },
      { x: 5, y: 6 },
      { x: 7, y: 8 },
    ])
  })
  describe(".array()", () => {
    it("Creates a struct for storing an array of variable-width values", () => {
      const pointMatrixStruct = pointListStruct.array()
      const pointMatrix = [
        [{ x: 1, y: 2 }, { x: 3, y: 4 }],
        [{ x: 5, y: 6 }, { x: 7, y: 8 }, { x: 9, y: 10 }],
      ]
      const view = new DataView(
        new ArrayBuffer(pointMatrixStruct.sizeof(pointMatrix)),
      )
      pointMatrixStruct.writeAt(pointMatrix, view, 0)
      expect(pointMatrixStruct.readAt(view, 0)).toEqual([
        [{ x: 1, y: 2 }, { x: 3, y: 4 }],
        [{ x: 5, y: 6 }, { x: 7, y: 8 }, { x: 9, y: 10 }],
      ])
    })
  })
})

describe("TupleStruct", () => {
  const headerStruct = Struct.tuple(
    Struct.uint32, // pageId
    Struct.bigUint64, // headerPageId
  )

  it("lets you read and write tuples", () => {
    const data: [number, bigint] = [1, BigInt(2)]
    expect(headerStruct.sizeof(data)).toBe(16)
    const view = new DataView(new ArrayBuffer(headerStruct.sizeof(data)))
    headerStruct.writeAt([1, BigInt(2)], view, 0)
    expect(headerStruct.readAt(view, 0)).toEqual([1, BigInt(2)])
  })
})
