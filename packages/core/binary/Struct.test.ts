import { describe, it } from "jsr:@std/testing/bdd"
import { FixedWidthStruct, Struct, VariableWidthStruct } from "./Struct.ts"
import { expect } from "jsr:@std/expect"
import { WriteableDataView } from "./dataview.ts"

const pointStruct = new FixedWidthStruct<{ x: number; y: number }>({
  toJSON: (value) => value,
  fromJSON: (json) => json as { x: number; y: number },
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
    const view = new WriteableDataView(pointStruct.size * 4)
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

  it("lets you generate a uint8array", () => {
    const bytes = pointStruct.toUint8Array({ x: 1, y: 2 })
    expect(new DataView(bytes.buffer).getUint32(0)).toBe(1)
    expect(new DataView(bytes.buffer).getUint32(4)).toBe(2)
    expect(bytes.byteLength).toBe(8)
  })

  it("throws when reading out of bounds", () => {
    const view = new WriteableDataView(new ArrayBuffer(pointStruct.size * 4))
    expect(() => pointStruct.readAt(view, pointStruct.size * 4)).toThrow(
      "Reading past the end of the view",
    )
  })

  it("throws when writing out of bounds", () => {
    const view = new WriteableDataView(new ArrayBuffer(pointStruct.size * 4))
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

      const view = new WriteableDataView(
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

  describe(".nullable()", () => {
    const nullablePointStruct = pointStruct.nullable()
    it("Takes up one more byte of space", () => {
      expect(nullablePointStruct.size).toBe(pointStruct.size + 1)
    })
    it("Creates a struct that can be null", () => {
      const view = new WriteableDataView(
        new ArrayBuffer(nullablePointStruct.size),
      )
      nullablePointStruct.writeAt(null, view, 0)
      expect(nullablePointStruct.readAt(view, 0)).toBe(null)

      nullablePointStruct.writeAt({ x: 1, y: 2 }, view, 0)
      expect(nullablePointStruct.readAt(view, 0)).toEqual({ x: 1, y: 2 })
    })
    it("serializes to JSON correctly", () => {
      expect(nullablePointStruct.toJSON(null)).toBe(null)
      expect(nullablePointStruct.toJSON({ x: 1, y: 2 })).toEqual({ x: 1, y: 2 })
    })
    it("deserializes from JSON correctly", () => {
      expect(nullablePointStruct.fromJSON(null)).toBe(null)
      expect(nullablePointStruct.fromJSON({ x: 1, y: 2 })).toEqual({
        x: 1,
        y: 2,
      })
    })
  })
})

describe("VariableWidthStruct", () => {
  const pointListStruct = new VariableWidthStruct<{ x: number; y: number }[]>({
    toJSON: (value) => value,
    fromJSON: (json) => json as { x: number; y: number }[],
    sizeof: (value) => pointStruct.size * value.length,
    emptyValue: () => [],
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
    const view = new WriteableDataView(
      new ArrayBuffer(pointListStruct.sizeof(points)),
    )

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

  it("lets you read uninitialized values", () => {
    const view = new WriteableDataView(
      new ArrayBuffer(pointListStruct.sizeof([])),
    )
    expect(pointListStruct.readAt(view, 0)).toEqual([])
  })

  describe(".array()", () => {
    it("Creates a struct for storing an array of variable-width values", () => {
      const pointMatrixStruct = pointListStruct.array()
      const pointMatrix = [
        [{ x: 1, y: 2 }, { x: 3, y: 4 }],
        [{ x: 5, y: 6 }, { x: 7, y: 8 }, { x: 9, y: 10 }],
      ]
      const view = new WriteableDataView(
        new ArrayBuffer(pointMatrixStruct.sizeof(pointMatrix)),
      )
      pointMatrixStruct.writeAt(pointMatrix, view, 0)
      expect(pointMatrixStruct.readAt(view, 0)).toEqual([
        [{ x: 1, y: 2 }, { x: 3, y: 4 }],
        [{ x: 5, y: 6 }, { x: 7, y: 8 }, { x: 9, y: 10 }],
      ])
    })
  })

  describe(".nullable()", () => {
    const nullablePointListStruct = pointListStruct.nullable()
    it("Takes up 5 additional byte", () => {
      expect(nullablePointListStruct.sizeof([{ x: 1, y: 2 }])).toBe(
        1 + 4 + pointListStruct.sizeof([{ x: 1, y: 2 }]),
      )
      expect(nullablePointListStruct.sizeof(null)).toBe(5)
    })
    it("Creates a struct that can be null", () => {
      const view = new WriteableDataView(
        new ArrayBuffer(nullablePointListStruct.sizeof(null)),
      )
      nullablePointListStruct.writeAt(null, view, 0)
      expect(nullablePointListStruct.readAt(view, 0)).toBe(null)

      const points = [{ x: 1, y: 2 }, { x: 3, y: 4 }]
      const nonNullView = new WriteableDataView(
        new ArrayBuffer(nullablePointListStruct.sizeof(points)),
      )
      nullablePointListStruct.writeAt(points, nonNullView, 0)
      expect(nullablePointListStruct.readAt(nonNullView, 0)).toEqual(points)
    })
    it("serializes to JSON correctly", () => {
      expect(nullablePointListStruct.toJSON(null)).toBe(null)
      expect(nullablePointListStruct.toJSON([{ x: 1, y: 2 }, { x: 3, y: 4 }]))
        .toEqual([{ x: 1, y: 2 }, { x: 3, y: 4 }])
    })
    it("deserializes from JSON correctly", () => {
      expect(nullablePointListStruct.fromJSON(null)).toBe(null)
      expect(nullablePointListStruct.fromJSON([{ x: 1, y: 2 }, { x: 3, y: 4 }]))
        .toEqual([{ x: 1, y: 2 }, { x: 3, y: 4 }])
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
    expect(headerStruct.sizeof(data)).toBe(12)
    const view = new WriteableDataView(
      new ArrayBuffer(headerStruct.sizeof(data)),
    )
    headerStruct.writeAt([1, BigInt(2)], view, 0)
    expect(headerStruct.readAt(view, 0)).toEqual([1, BigInt(2)])
  })
})

describe("RecordStruct", () => {
  const pointObjectStruct = Struct.record({
    x: [0, Struct.uint32],
    y: [1, Struct.uint32],
  })

  it("lets you read and write objects", () => {
    const view = new WriteableDataView(new ArrayBuffer(pointObjectStruct.size))
    pointObjectStruct.writeAt({ x: 1, y: 2 }, view, 0)
    expect(pointObjectStruct.readAt(view, 0)).toEqual({ x: 1, y: 2 })
  })
})

describe("bytes", () => {
  it("lets you read and write byte arrays", () => {
    const value = new Uint8Array([1, 2, 3, 4, 5])
    expect(Struct.bytes.sizeof(value)).toBe(4 + 5)
    const view = new WriteableDataView(Struct.bytes.sizeof(value))
    Struct.bytes.writeAt(value, view, 0)
    expect(Struct.bytes.readAt(view, 0)).toEqual(
      new Uint8Array([1, 2, 3, 4, 5]),
    )
  })

  it("serializes and deserialized to/from json correctly using base64 encoding", () => {
    const value = new Uint8Array([1, 2, 3, 4, 5])
    expect(Struct.bytes.toJSON(value)).toEqual("AQIDBAU=")
    expect(Struct.bytes.fromJSON("AQIDBAU=")).toEqual(
      new Uint8Array([1, 2, 3, 4, 5]),
    )
  })
})
