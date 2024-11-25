export interface IStruct<ValueT> {
  sizeof(value: ValueT): number
  readAt(view: DataView, offset: number): ValueT
  writeAt(value: ValueT, view: DataView, offset: number): void
  array(): IStruct<ValueT[]>
}
export class VariableWidthStruct<ValueT> implements IStruct<ValueT> {
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
    this._sizeof = sizeof
    this.write = write
    this.read = read
  }

  sizeof(value: ValueT): number {
    return this._sizeof(value) + 4
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
}

/**
 * A struct is a fixed-width binary data structure that can be read from
 * and written to using a DataView.
 */
export class FixedWidthStruct<ValueT> implements IStruct<ValueT> {
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
}

export const Struct = {
  unicodeStringStruct: new VariableWidthStruct<string>({
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
  }),
  boolean: new FixedWidthStruct<boolean>({
    read: (view) => view.getUint8(0) === 1,
    write: (value, view) => view.setUint8(0, value ? 1 : 0),
    size: 1,
  }),
  float64: new FixedWidthStruct<number>({
    read: (view) => view.getFloat64(0),
    write: (value, view) => view.setFloat64(0, value),
    size: 8,
  }),
  uint32: new FixedWidthStruct<number>({
    read: (view) => view.getUint32(0),
    write: (value, view) => view.setUint32(0, value),
    size: 4,
  }),
  int32: new FixedWidthStruct<number>({
    read: (view) => view.getInt32(0),
    write: (value, view) => view.setInt32(0, value),
    size: 4,
  }),
  uint8: new FixedWidthStruct<number>({
    read: (view) => view.getUint8(0),
    write: (value, view) => view.setUint8(0, value),
    size: 1,
  }),
  uint16: new FixedWidthStruct<number>({
    read: (view) => view.getUint16(0),
    write: (value, view) => view.setUint16(0, value),
    size: 2,
  }),
  int16: new FixedWidthStruct<number>({
    read: (view) => view.getInt16(0),
    write: (value, view) => view.setInt16(0, value),
    size: 2,
  }),
  bigUint64: new FixedWidthStruct<bigint>({
    read: (view) => view.getBigUint64(0),
    write: (value, view) => view.setBigUint64(0, value),
    size: 8,
  }),
  bigInt64: new FixedWidthStruct<bigint>({
    read: (view) => view.getBigInt64(0),
    write: (value, view) => view.setBigInt64(0, value),
    size: 8,
  }),
  // deno-lint-ignore no-explicit-any
  tuple: <T extends any[]>(...structs: IStruct<T>[]) => {
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
  },
}
