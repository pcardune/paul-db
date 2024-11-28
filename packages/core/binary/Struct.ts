import { Json } from "../types.ts"

export abstract class IStruct<ValueT> {
  abstract sizeof(value: ValueT): number
  abstract readAt(view: DataView, offset: number): ValueT
  abstract writeAt(value: ValueT, view: DataView, offset: number): void
  abstract array(): IStruct<ValueT[]>
  abstract wrap<Target>(
    toValue: (value: Target) => ValueT,
    toTarget: (value: ValueT) => Target,
  ): IStruct<Target>

  toUint8Array(value: ValueT): Uint8Array {
    const size = this.sizeof(value)
    const buffer = new ArrayBuffer(size + 4)
    const view = new DataView(buffer)
    this.writeAt(value, view, 0)
    return new Uint8Array(buffer)
  }
}
export class VariableWidthStruct<ValueT> extends IStruct<ValueT> {
  /**
   * The size of the struct in bytes.
   */
  private readonly _sizeof: (value: ValueT) => number

  /**
   * Writes a value to a DataView.
   */
  private write: (value: ValueT, view: DataView) => void

  /**
   * Reads a value from a DataView.
   */
  private read: (view: DataView) => ValueT

  constructor({ sizeof, write, read }: {
    /**
     * Calculate the size of this value in bytes.
     * @param value The javascript value to calculate the size of
     * @returns The size of the value in bytes. This is the amount of space
     * that the write and read methods will have access to.
     */
    sizeof: (value: ValueT) => number
    write: (value: ValueT, view: DataView) => void
    read: (view: DataView) => ValueT
  }) {
    super()
    this._sizeof = sizeof
    this.write = write
    this.read = read
  }

  sizeof(value: ValueT): number {
    return this._sizeof(value) + 4
  }

  /**
   * Return the size of the data at the location without
   * reading all of it.
   */
  sizeAt(view: DataView, offset: number): number {
    return view.getUint32(offset)
  }

  readAt(view: DataView, offset: number): ValueT {
    const size = view.getUint32(offset)
    return this.read(
      new DataView(view.buffer, view.byteOffset + offset + 4, size),
    )
  }

  writeAt(value: ValueT, view: DataView, offset: number): void {
    const size = this._sizeof(value)
    view.setUint32(offset, size)
    if (size > view.byteLength - offset - 4) {
      throw new Error(
        `Need to write ${size} bytes, but only ${
          view.byteLength - offset - 4
        } bytes available in this view`,
      )
    }
    this.write(
      value,
      new DataView(view.buffer, view.byteOffset + offset + 4, size),
    )
  }

  array(): VariableWidthStruct<ValueT[]> {
    return new VariableWidthStruct({
      sizeof: (value) => {
        return value.reduce((acc, val) => this.sizeof(val) + acc, 0)
      },
      write: (value, view) => {
        let offset = 0
        for (const val of value) {
          this.writeAt(val, view, offset)
          offset += this.sizeof(val)
        }
      },
      read: (view) => {
        const result = []
        let offset = 0
        while (offset < view.byteLength) {
          result.push(this.readAt(view, offset))
          offset += this.sizeof(result[result.length - 1])
        }
        return result
      },
    })
  }

  wrap<Target>(
    toValue: (value: Target) => ValueT,
    toTarget: (value: ValueT) => Target,
  ): VariableWidthStruct<Target> {
    return new VariableWidthStruct({
      sizeof: (value) => this.sizeof(toValue(value)),
      write: (value, view) => this.writeAt(toValue(value), view, 0),
      read: (view) => toTarget(this.readAt(view, 0)),
    })
  }
}

/**
 * A struct is a fixed-width binary data structure that can be read from
 * and written to using a DataView.
 */
export class FixedWidthStruct<ValueT> extends IStruct<ValueT> {
  /**
   * The size of the struct in bytes.
   */
  readonly size: number

  /**
   * Writes a value to a DataView.
   */
  private write: (value: ValueT, view: DataView) => void

  /**
   * Reads a value from a DataView.
   */
  private read: (view: DataView) => ValueT

  constructor({ size, write, read }: {
    size: number
    write: (value: ValueT, view: DataView) => void
    read: (view: DataView) => ValueT
  }) {
    super()
    this.size = size
    this.write = write
    this.read = read
  }

  sizeof(_value: ValueT): number {
    return this.size
  }

  readAt(view: DataView, offset: number): ValueT {
    if (offset + this.size > view.byteLength) {
      throw new Error("Reading past the end of the view")
    }
    return this.read(
      new DataView(view.buffer, view.byteOffset + offset, this.size),
    )
  }

  writeAt(value: ValueT, view: DataView, offset: number): void {
    if (offset + this.size > view.byteLength) {
      throw new Error("Writing past the end of the view")
    }
    this.write(
      value,
      new DataView(view.buffer, view.byteOffset + offset, this.size),
    )
  }

  array(): VariableWidthStruct<ValueT[]> {
    return new VariableWidthStruct({
      sizeof: (value) => value.length * this.size,
      write: (value, view) => {
        for (let i = 0; i < value.length; i++) {
          this.writeAt(value[i], view, i * this.size)
        }
      },
      read: (view) => {
        const length = view.byteLength / this.size
        const values = []
        for (let i = 0; i < length; i++) {
          values.push(this.readAt(view, i * this.size))
        }
        return values
      },
    })
  }

  wrap<Target>(
    toValue: (value: Target) => ValueT,
    toTarget: (value: ValueT) => Target,
  ): FixedWidthStruct<Target> {
    return new FixedWidthStruct({
      size: this.size,
      write: (value, view) => this.writeAt(toValue(value), view, 0),
      read: (view) => toTarget(this.readAt(view, 0)),
    })
  }
}

function tuple<V1>(s1: FixedWidthStruct<V1>): FixedWidthStruct<[V1]>
function tuple<V1, V2>(
  s1: FixedWidthStruct<V1>,
  s2: FixedWidthStruct<V2>,
): FixedWidthStruct<[V1, V2]>
function tuple<V1, V2, V3>(
  s1: FixedWidthStruct<V1>,
  s2: FixedWidthStruct<V2>,
  s3: FixedWidthStruct<V3>,
): FixedWidthStruct<[V1, V2, V3]>
function tuple<V1, V2, V3, V4>(
  s1: FixedWidthStruct<V1>,
  s2: FixedWidthStruct<V2>,
  s3: FixedWidthStruct<V3>,
  s4: FixedWidthStruct<V4>,
): FixedWidthStruct<[V1, V2, V3, V4]>
function tuple<V1, V2, V3, V4, V5>(
  s1: FixedWidthStruct<V1>,
  s2: FixedWidthStruct<V2>,
  s3: FixedWidthStruct<V3>,
  s4: FixedWidthStruct<V4>,
  s5: FixedWidthStruct<V5>,
): FixedWidthStruct<[V1, V2, V3, V4, V5]>
function tuple<V1, V2, V3, V4, V5, V6>(
  s1: FixedWidthStruct<V1>,
  s2: FixedWidthStruct<V2>,
  s3: FixedWidthStruct<V3>,
  s4: FixedWidthStruct<V4>,
  s5: FixedWidthStruct<V5>,
  s6: FixedWidthStruct<V6>,
): FixedWidthStruct<[V1, V2, V3, V4, V5, V6]>
function tuple<V1>(s1: IStruct<V1>): VariableWidthStruct<[V1]>
function tuple<V1, V2>(
  s1: IStruct<V1>,
  s2: IStruct<V2>,
): VariableWidthStruct<[V1, V2]>
function tuple<V1, V2, V3>(
  s1: IStruct<V1>,
  s2: IStruct<V2>,
  s3: IStruct<V3>,
): VariableWidthStruct<[V1, V2, V3]>
function tuple<V1, V2, V3, V4>(
  s1: IStruct<V1>,
  s2: IStruct<V2>,
  s3: IStruct<V3>,
  s4: IStruct<V4>,
): VariableWidthStruct<[V1, V2, V3, V4]>
function tuple<V1, V2, V3, V4, V5>(
  s1: IStruct<V1>,
  s2: IStruct<V2>,
  s3: IStruct<V3>,
  s4: IStruct<V4>,
  s5: IStruct<V5>,
): VariableWidthStruct<[V1, V2, V3, V4, V5]>
function tuple<V1, V2, V3, V4, V5, V6>(
  s1: IStruct<V1>,
  s2: IStruct<V2>,
  s3: IStruct<V3>,
  s4: IStruct<V4>,
  s5: IStruct<V5>,
  s6: IStruct<V6>,
): VariableWidthStruct<[V1, V2, V3, V4, V5, V6]>
function tuple<V1, V2, V3, V4, V5, V6, V7>(
  s1: IStruct<V1>,
  s2: IStruct<V2>,
  s3: IStruct<V3>,
  s4: IStruct<V4>,
  s5: IStruct<V5>,
  s6: IStruct<V6>,
  s7: IStruct<V7>,
): VariableWidthStruct<[V1, V2, V3, V4, V5, V6, V7]>
// deno-lint-ignore no-explicit-any
function tuple<T extends [IStruct<any>, ...IStruct<any>[]]>(...structs: T) {
  if (structs.every((s) => s instanceof FixedWidthStruct)) {
    return new FixedWidthStruct({
      size: structs.reduce(
        (acc, s) => acc + (s as FixedWidthStruct<unknown>).size,
        0,
      ),
      read: (view) => {
        const values = []
        let offset = 0
        for (const struct of structs) {
          values.push(struct.readAt(view, offset))
          offset += struct.size
        }
        return values as T
      },
      write: (value, view) => {
        let offset = 0
        for (let i = 0; i < structs.length; i++) {
          structs[i].writeAt(value[i], view, offset)
          offset += structs[i].size
        }
      },
    })
  }
  return new VariableWidthStruct<T>({
    read: (view) => {
      const values = []
      let offset = 0
      for (const struct of structs) {
        values.push(struct.readAt(view, offset))
        offset += struct.sizeof(values[values.length - 1])
      }
      return values as T
    },
    write: (value, view) => {
      let offset = 0
      for (let i = 0; i < structs.length; i++) {
        structs[i].writeAt(value[i], view, offset)
        offset += structs[i].sizeof(value[i])
      }
    },
    sizeof: (value) =>
      structs.reduce((acc, struct, i) => struct.sizeof(value[i]) + acc, 0),
  })
}

function record<V extends Record<string, any>>(
  structs: { [property in keyof V]: [number, FixedWidthStruct<V[property]>] },
): FixedWidthStruct<V>
function record<V extends Record<string, any>>(
  structs: { [property in keyof V]: [number, IStruct<V[property]>] },
): IStruct<V> {
  const asArray = Object.entries(structs).map(([key, [order, struct]]) => ({
    key,
    order,
    struct,
  })).sort((a, b) => a.order - b.order)
  const orderSet = new Set(asArray.map((x) => x.order))
  if (orderSet.size !== asArray.length) {
    throw new Error("Duplicate orders in record")
  }

  function read(view: DataView): V {
    const obj: Record<string, any> = {}
    let offset = 0
    for (const { key, struct } of asArray) {
      obj[key] = struct.readAt(view, offset)
      offset += struct.sizeof(obj[key])
    }
    return obj as V
  }
  function write(value: V, view: DataView): void {
    let offset = 0
    for (const { key, struct } of asArray) {
      struct.writeAt(value[key], view, offset)
      offset += struct.sizeof(value[key])
    }
  }

  if (asArray.every((x) => x.struct instanceof FixedWidthStruct)) {
    return new FixedWidthStruct<V>({
      size: asArray.reduce(
        (acc, { struct }) => (struct as FixedWidthStruct<unknown>).size + acc,
        0,
      ),
      read,
      write,
    })
  }

  return new VariableWidthStruct<V>({
    sizeof: (value) =>
      asArray.reduce(
        (acc, { key, struct }) => struct.sizeof(value[key]) + acc,
        0,
      ),
    read,
    write,
  })
}

const unicodeStringStruct = new VariableWidthStruct<string>({
  read: (view) => {
    const decoder = new TextDecoder()
    return decoder.decode(view)
  },
  write: (value, view) => {
    const encoder = new TextEncoder()
    const bytes = encoder.encode(value)
    new Uint8Array(view.buffer, view.byteOffset, view.byteLength).set(
      bytes,
      0,
    )
  },
  sizeof: (value) => new TextEncoder().encode(value).length,
})
const boolean = new FixedWidthStruct<boolean>({
  read: (view) => view.getUint8(0) === 1,
  write: (value, view) => view.setUint8(0, value ? 1 : 0),
  size: 1,
})
const float64 = new FixedWidthStruct<number>({
  read: (view) => view.getFloat64(0),
  write: (value, view) => view.setFloat64(0, value),
  size: 8,
})
const uint32 = new FixedWidthStruct<number>({
  read: (view) => view.getUint32(0),
  write: (value, view) => view.setUint32(0, value),
  size: 4,
})
const int32 = new FixedWidthStruct<number>({
  read: (view) => view.getInt32(0),
  write: (value, view) => view.setInt32(0, value),
  size: 4,
})
const uint8 = new FixedWidthStruct<number>({
  read: (view) => view.getUint8(0),
  write: (value, view) => view.setUint8(0, value),
  size: 1,
})
const uint16 = new FixedWidthStruct<number>({
  read: (view) => view.getUint16(0),
  write: (value, view) => view.setUint16(0, value),
  size: 2,
})
const int16 = new FixedWidthStruct<number>({
  read: (view) => view.getInt16(0),
  write: (value, view) => view.setInt16(0, value),
  size: 2,
})
const bigUint64 = new FixedWidthStruct<bigint>({
  read: (view) => view.getBigUint64(0),
  write: (value, view) => view.setBigUint64(0, value),
  size: 8,
})
const bigInt64 = new FixedWidthStruct<bigint>({
  read: (view) => view.getBigInt64(0),
  write: (value, view) => view.setBigInt64(0, value),
  size: 8,
})

const dateTuple = tuple(uint32, uint8, uint8) // year-month-day

export const Struct = {
  unicodeStringStruct,
  boolean,
  float64,
  uint8,
  uint16,
  int16,
  uint32,
  int32,
  bigUint64,
  bigInt64,
  tuple,
  record,
  date: dateTuple.wrap<Date>(
    (date) => [date.getFullYear(), date.getMonth(), date.getDate()],
    ([year, month, day]) => new Date(year, month, day),
  ),
  json: unicodeStringStruct.wrap<Json>(
    (json) => JSON.stringify(json),
    (json) => JSON.parse(json),
  ),
}
