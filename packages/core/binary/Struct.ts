import { Json, JsonRecord } from "../types.ts"
import { ReadonlyDataView, WriteableDataView } from "./dataview.ts"
import { copy } from "@std/bytes/copy"
import { decodeBase64, encodeBase64 } from "@std/encoding"
import type { UnknownRecord } from "type-fest"

export type ValueOfStruct<T extends IStruct<any>> = T extends IStruct<infer V>
  ? V
  : never

class NoSpaceError extends Error {}
export abstract class IStruct<ValueT> {
  abstract sizeof(value: ValueT): number
  abstract readAt(view: ReadonlyDataView, offset: number): ValueT
  abstract writeAt(value: ValueT, view: WriteableDataView, offset: number): void
  abstract array(): IStruct<ValueT[]>
  abstract nullable(): IStruct<ValueT | null>
  abstract wrap<Target>(
    toValue: (value: Target) => ValueT,
    toTarget: (value: ValueT) => Target,
  ): IStruct<Target>

  abstract toJSON(value: ValueT): Json
  abstract fromJSON(json: Json): ValueT
  abstract emptyValue(): ValueT

  toUint8Array(value: ValueT): Uint8Array {
    const size = this.sizeof(value)
    const buffer = new ArrayBuffer(size)
    const view = new WriteableDataView(buffer)
    this.writeAt(value, view, 0)
    return new Uint8Array(buffer)
  }

  static NoSpaceError: typeof NoSpaceError = NoSpaceError
}
export class VariableWidthStruct<ValueT> extends IStruct<ValueT> {
  /**
   * The size of the struct in bytes.
   */
  private readonly _sizeof: (value: ValueT) => number

  /**
   * Writes a value to a DataView.
   */
  private write: (value: ValueT, view: WriteableDataView) => void

  /**
   * Reads a value from a DataView.
   */
  private read: (view: ReadonlyDataView) => ValueT

  readonly toJSON: (value: ValueT) => Json
  readonly fromJSON: (json: Json) => ValueT

  readonly emptyValue: () => ValueT

  constructor({ sizeof, write, read, toJSON, fromJSON, emptyValue }: {
    /**
     * Calculate the size of this value in bytes.
     * @param value The javascript value to calculate the size of
     * @returns The size of the value in bytes. This is the amount of space
     * that the write and read methods will have access to.
     */
    sizeof: (value: ValueT) => number
    write: (value: ValueT, view: WriteableDataView) => void
    read: (view: ReadonlyDataView) => ValueT
    toJSON: (value: ValueT) => Json
    fromJSON: (json: Json) => ValueT
    emptyValue: () => ValueT
  }) {
    super()
    this._sizeof = sizeof
    this.write = write
    this.read = read
    this.toJSON = toJSON
    this.fromJSON = fromJSON
    this.emptyValue = emptyValue
  }

  sizeof(value: ValueT): number {
    return this._sizeof(value) + 4
  }

  /**
   * Return the size of the data at the location without
   * reading all of it.
   */
  sizeAt(view: ReadonlyDataView, offset: number): number {
    return view.getUint32(offset)
  }

  readAt(view: ReadonlyDataView, offset: number): ValueT {
    const size = view.getUint32(offset)
    if (size === 0) {
      return this.emptyValue()
    }
    return this.read(
      view.slice(offset + 4, size),
    )
  }

  writeAt(value: ValueT, view: WriteableDataView, offset: number): void {
    const size = this._sizeof(value)
    view.setUint32(offset, size)
    if (size > view.byteLength - offset - 4) {
      throw new IStruct.NoSpaceError(
        `Need to write ${size} bytes, but only ${
          view.byteLength - offset - 4
        } bytes available in this view`,
      )
    }
    this.write(value, view.slice(offset + 4, size))
  }

  nullable(): VariableWidthStruct<ValueT | null> {
    return new VariableWidthStruct<ValueT | null>({
      sizeof: (value) => 1 + (value === null ? 0 : this.sizeof(value)),
      emptyValue: () => null,
      toJSON: (value) => value === null ? null : this.toJSON(value),
      fromJSON: (json) => json === null ? null : this.fromJSON(json),
      read: (view) => {
        if (view.getUint8(0) === 0) {
          return null
        }
        return this.readAt(view, 1)
      },
      write: (value, view) => {
        if (value === null) {
          view.setUint8(0, 0)
        } else {
          view.setUint8(0, 1)
          this.writeAt(value, view, 1)
        }
      },
    })
  }

  array(): VariableWidthStruct<ValueT[]> {
    return new VariableWidthStruct<ValueT[]>({
      emptyValue: () => [],
      toJSON: (value) => value.map((v) => this.toJSON(v)),
      fromJSON: (json) => {
        if (!Array.isArray(json)) {
          throw new Error("Expected an array")
        }
        return json.map((v) => this.fromJSON(v))
      },
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
    {
      toJSON = ((value) => this.toJSON(toValue(value))),
      fromJSON = ((json) => toTarget(this.fromJSON(json))),
    }: {
      toJSON?: (value: Target) => Json
      fromJSON?: (json: Json) => Target
    } = {},
  ): VariableWidthStruct<Target> {
    return new VariableWidthStruct<Target>({
      toJSON,
      fromJSON,
      sizeof: (value) => this.sizeof(toValue(value)),
      write: (value, view) => this.writeAt(toValue(value), view, 0),
      read: (view) => toTarget(this.readAt(view, 0)),
      emptyValue: () => toTarget(this.emptyValue()),
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
  private write: (value: ValueT, view: WriteableDataView) => void

  /**
   * Reads a value from a DataView.
   */
  private read: (view: ReadonlyDataView) => ValueT

  readonly toJSON: (value: ValueT) => Json
  readonly fromJSON: (json: Json) => ValueT

  constructor({ size, write, read, toJSON, fromJSON }: {
    size: number
    write: (value: ValueT, view: WriteableDataView) => void
    read: (view: ReadonlyDataView) => ValueT
    toJSON: (value: ValueT) => Json
    fromJSON: (json: Json) => ValueT
  }) {
    super()
    this.size = size
    this.write = write
    this.read = read
    this.toJSON = toJSON
    this.fromJSON = fromJSON
  }

  sizeof(_value: ValueT): number {
    return this.size
  }

  override emptyValue(): ValueT {
    return this.readAt(new WriteableDataView(this.size), 0)
  }

  readAt(view: ReadonlyDataView, offset: number): ValueT {
    if (offset + this.size > view.byteLength) {
      throw new Error("Reading past the end of the view")
    }
    return this.read(
      view.slice(offset, this.size),
    )
  }

  writeAt(value: ValueT, view: WriteableDataView, offset: number): void {
    if (offset + this.size > view.byteLength) {
      throw new Error("Writing past the end of the view")
    }
    this.write(value, view.slice(offset, this.size))
  }

  nullable(): FixedWidthStruct<ValueT | null> {
    return new FixedWidthStruct<ValueT | null>({
      size: this.size + 1,
      toJSON: (value) => value === null ? null : this.toJSON(value),
      fromJSON: (json) => json === null ? null : this.fromJSON(json),
      read: (view) => {
        if (view.getUint8(0) === 0) {
          return null
        }
        return this.readAt(view, 1)
      },
      write: (value, view) => {
        if (value === null) {
          view.setUint8(0, 0)
        } else {
          view.setUint8(0, 1)
          this.writeAt(value, view, 1)
        }
      },
    })
  }

  array(): VariableWidthStruct<ValueT[]> {
    return new VariableWidthStruct<ValueT[]>({
      emptyValue: () => [],
      toJSON: (value) => value.map((v) => this.toJSON(v)),
      fromJSON: (json) => {
        if (!Array.isArray(json)) {
          throw new Error("Expected an array")
        }
        return json.map((v) => this.fromJSON(v))
      },
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
    {
      toJSON = ((value) => this.toJSON(toValue(value))),
      fromJSON = ((json) => toTarget(this.fromJSON(json))),
    }: {
      toJSON?: (value: Target) => Json
      fromJSON?: (json: Json) => Target
    } = {},
  ): FixedWidthStruct<Target> {
    return new FixedWidthStruct({
      toJSON,
      fromJSON,
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
function tuple<T extends [IStruct<unknown>, ...IStruct<unknown>[]]>(
  ...structs: T
) {
  const toJSON = (value: T) => value.map((v, i) => structs[i].toJSON(v))
  const fromJSON = (json: Json) => {
    if (!Array.isArray(json)) {
      throw new Error("Expected an array")
    }
    return json.map((v, i) => structs[i].fromJSON(v)) as T
  }
  if (structs.every((s) => s instanceof FixedWidthStruct)) {
    return new FixedWidthStruct<T>({
      toJSON,
      fromJSON,
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
    toJSON,
    fromJSON,
    emptyValue: () => structs.map((s) => s.emptyValue()) as T,
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

function record<V extends UnknownRecord>(
  structs: { [property in keyof V]: [number, FixedWidthStruct<V[property]>] },
): FixedWidthStruct<V>
function record<V extends UnknownRecord>(
  structs: { [property in keyof V]: [number, IStruct<V[property]>] },
): IStruct<V>
function record<V extends UnknownRecord>(
  structs: { [property in keyof V]: [number, IStruct<V[property]>] },
): IStruct<V> {
  const asArray = Object.entries(structs).map(
    (entry: [string, [number, IStruct<unknown>]]) => {
      const [key, [order, struct]] = entry
      return ({
        key,
        order,
        struct,
      })
    },
  ).sort((a, b) => a.order - b.order)
  const orderSet = new Set(asArray.map((x) => x.order))
  if (orderSet.size !== asArray.length) {
    throw new Error("Duplicate orders in record")
  }

  function read(view: ReadonlyDataView): V {
    const obj: UnknownRecord = {}
    let offset = 0
    for (const { key, struct } of asArray) {
      obj[key] = struct.readAt(view, offset)
      offset += struct.sizeof(obj[key])
    }
    return obj as V
  }
  function write(value: V, view: WriteableDataView): void {
    let offset = 0
    for (const { key, struct } of asArray) {
      struct.writeAt(value[key], view, offset)
      offset += struct.sizeof(value[key])
    }
  }

  function toJSON(value: V): Json {
    const obj: JsonRecord = {}
    for (const { key, struct } of asArray) {
      obj[key] = struct.toJSON(value[key])
    }
    return obj
  }

  function fromJSON(json: Json): V {
    if (typeof json !== "object" || json === null) {
      throw new Error("Expected an object")
    }
    const obj: UnknownRecord = {}
    for (const { key, struct } of asArray) {
      obj[key] = struct.fromJSON((json as JsonRecord)[key])
    }
    return obj as V
  }

  if (asArray.every((x) => x.struct instanceof FixedWidthStruct)) {
    return new FixedWidthStruct<V>({
      toJSON,
      fromJSON,
      size: asArray.reduce(
        (acc, { struct }) => (struct as FixedWidthStruct<unknown>).size + acc,
        0,
      ),
      read,
      write,
    })
  }

  return new VariableWidthStruct<V>({
    emptyValue: () => {
      return asArray.reduce((acc, { key, struct }) => {
        acc[key] = struct.emptyValue()
        return acc
      }, {} as UnknownRecord) as V
    },
    toJSON,
    fromJSON,
    sizeof: (value) =>
      asArray.reduce(
        (acc, { key, struct }) => struct.sizeof(value[key]) + acc,
        0,
      ),
    read,
    write,
  })
}

export function union<V extends Record<string, [number, IStruct<any>]>>(
  structs: V,
): IStruct<
  { [K in keyof V]: { type: K; value: ValueOfStruct<V[K][1]> } }[keyof V]
> {
  const byMarker = new Map<
    number,
    { typeName: keyof V; struct: IStruct<any> }
  >()
  for (const [typeName, [marker, struct]] of Object.entries(structs)) {
    if (byMarker.has(marker)) {
      throw new Error(`Duplicate marker ${marker}`)
    }
    byMarker.set(marker, { typeName: typeName as keyof V, struct })
  }

  return new VariableWidthStruct<
    { [K in keyof V]: { type: K; value: ValueOfStruct<V[K][1]> } }[keyof V]
  >({
    sizeof: (value) => {
      return 2 + structs[value.type][1].sizeof(value.value)
    },
    emptyValue: () => {
      return {
        type: Object.keys(structs)[0] as keyof V,
        value: structs[Object.keys(structs)[0]][1].emptyValue(),
      }
    },
    toJSON: (value) => {
      return {
        type: value.type as string,
        value: structs[value.type][1].toJSON(value.value),
      }
    },
    fromJSON: (json) => {
      if (typeof json !== "object" || json === null) {
        throw new Error("Expected an object")
      }
      if ("type" in json === false) {
        throw new Error("Expected an object with a type property")
      }
      const type = json.type
      if (typeof type !== "string") {
        throw new Error("Expected a string type")
      }
      const struct = structs[type]
      if (struct == null) {
        throw new Error(`Unknown type ${type}`)
      }
      return {
        type,
        value: struct[1].fromJSON(json.value),
      }
    },
    read: (view) => {
      const type = view.getUint16(0)
      const struct = byMarker.get(type)
      if (struct == null) {
        throw new Error(`Unknown type ${type}`)
      }
      return {
        type: struct.typeName,
        value: struct.struct.readAt(view, 2),
      }
    },
    write: (value, view) => {
      view.setUint16(0, structs[value.type][0])
      structs[value.type][1].writeAt(value.value, view, 2)
    },
  })
}

export function fixedSizeUnion<
  V extends Record<string, [number, FixedWidthStruct<any>]>,
>(
  structs: V,
): FixedWidthStruct<
  { [K in keyof V]: { type: K; value: ValueOfStruct<V[K][1]> } }[keyof V]
> {
  const byMarker = new Map<
    number,
    { typeName: keyof V; struct: IStruct<any> }
  >()
  for (const [typeName, [marker, struct]] of Object.entries(structs)) {
    if (byMarker.has(marker)) {
      throw new Error(`Duplicate marker ${marker}`)
    }
    byMarker.set(marker, { typeName: typeName as keyof V, struct })
  }

  return new FixedWidthStruct<
    { [K in keyof V]: { type: K; value: ValueOfStruct<V[K][1]> } }[keyof V]
  >({
    size: 2 + Math.max(...Object.values(structs).map((s) => s[1].size)),
    toJSON: (value) => {
      return {
        type: value.type as string,
        value: structs[value.type][1].toJSON(value.value),
      }
    },
    fromJSON: (json) => {
      if (typeof json !== "object" || json === null) {
        throw new Error("Expected an object")
      }
      if ("type" in json === false) {
        throw new Error("Expected an object with a type property")
      }
      const type = json.type
      if (typeof type !== "string") {
        throw new Error("Expected a string type")
      }
      const struct = structs[type]
      if (struct == null) {
        throw new Error(`Unknown type ${type}`)
      }
      return {
        type,
        value: struct[1].fromJSON(json.value),
      }
    },
    read: (view) => {
      const type = view.getUint16(0)
      const struct = byMarker.get(type)
      if (struct == null) {
        throw new Error(`Unknown type ${type}`)
      }
      return {
        type: struct.typeName,
        value: struct.struct.readAt(view, 2),
      }
    },
    write: (value, view) => {
      view.setUint16(0, structs[value.type][0])
      structs[value.type][1].writeAt(value.value, view, 2)
    },
  })
}

const unicodeStringStruct = new VariableWidthStruct<string>({
  emptyValue: () => "",
  toJSON: (value) => value,
  fromJSON: (json) => {
    if (typeof json !== "string") {
      throw new Error("Expected a string")
    }
    return json
  },
  read: (view) => {
    return view.decodeText()
  },
  write: (value, view) => {
    view.setFromText(value)
  },
  sizeof: (value) => new TextEncoder().encode(value).length,
})
const boolean = new FixedWidthStruct<boolean>({
  toJSON: (value) => value,
  fromJSON: (json) => {
    if (typeof json !== "boolean") {
      throw new Error("Expected a boolean")
    }
    return json
  },
  read: (view) => view.getUint8(0) === 1,
  write: (value, view) => view.setUint8(0, value ? 1 : 0),
  size: 1,
})

const bytes = new VariableWidthStruct<Uint8Array>({
  emptyValue: () => new Uint8Array(0),
  toJSON: (value) => {
    return encodeBase64(value)
  },
  fromJSON: (json) => {
    if (typeof json !== "string") {
      throw new Error("Expected a string")
    }
    return decodeBase64(json)
  },
  read: (view) => {
    const arr = new Uint8Array(view.byteLength)
    copy(view.toUint8Array(), arr)
    return arr
  },
  write: (value, view) => {
    view.setUint8Array(0, value)
  },
  sizeof: (value) => value.byteLength,
})

const jsonNumber = {
  toJSON: (value: number) => value,
  fromJSON: (json: Json) => {
    if (typeof json !== "number") {
      throw new Error("Expected a number")
    }
    return json
  },
}

const float64 = new FixedWidthStruct<number>({
  ...jsonNumber,
  read: (view) => view.getFloat64(0),
  write: (value, view) => view.setFloat64(0, value),
  size: 8,
})
const uint32 = new FixedWidthStruct<number>({
  ...jsonNumber,
  read: (view) => view.getUint32(0),
  write: (value, view) => view.setUint32(0, value),
  size: 4,
})
const int32 = new FixedWidthStruct<number>({
  ...jsonNumber,
  read: (view) => view.getInt32(0),
  write: (value, view) => view.setInt32(0, value),
  size: 4,
})
const uint8 = new FixedWidthStruct<number>({
  ...jsonNumber,
  read: (view) => view.getUint8(0),
  write: (value, view) => view.setUint8(0, value),
  size: 1,
})
const uint16 = new FixedWidthStruct<number>({
  ...jsonNumber,
  read: (view) => view.getUint16(0),
  write: (value, view) => view.setUint16(0, value),
  size: 2,
})
const int16 = new FixedWidthStruct<number>({
  ...jsonNumber,
  read: (view) => view.getInt16(0),
  write: (value, view) => view.setInt16(0, value),
  size: 2,
})

const jsonBigInt = {
  toJSON: (value: bigint) => value.toString(),
  fromJSON: (json: Json) => {
    if (typeof json !== "string") {
      throw new Error("Expected a string")
    }
    return BigInt(json)
  },
}
const bigUint64 = new FixedWidthStruct<bigint>({
  ...jsonBigInt,
  read: (view) => view.getBigUint64(0),
  write: (value, view) => view.setBigUint64(0, value),
  size: 8,
})
const bigInt64 = new FixedWidthStruct<bigint>({
  ...jsonBigInt,
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
  bytes,
  timestamp: float64.wrap<Date>(
    (date) => date.getTime(),
    (time) => new Date(time),
    {
      toJSON: (date) => date.toISOString(),
      fromJSON: (json) => {
        if (typeof json !== "string") {
          throw new Error("Expected a string")
        }
        return new Date(json)
      },
    },
  ),
  date: dateTuple.wrap<Date>(
    (date) => [date.getFullYear(), date.getMonth(), date.getDate()],
    ([year, month, day]) => new Date(year, month, day),
    {
      toJSON: (date) => date.toISOString(),
      fromJSON: (json) => {
        if (typeof json !== "string") {
          throw new Error("Expected a string")
        }
        return new Date(json)
      },
    },
  ),
  json: unicodeStringStruct.wrap<Json>(
    (json) => JSON.stringify(json),
    (json) => JSON.parse(json),
    { toJSON: (json) => json, fromJSON: (json) => json },
  ),
  union,
  fixedSizeUnion,
}
