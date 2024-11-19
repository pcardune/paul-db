/**
 * A struct is a fixed-width binary data structure that can be read from
 * and written to using a DataView.
 */
export type Struct<V> = {
  /**
   * The size of the struct in bytes.
   */
  size: number

  /**
   * Writes a value to a DataView.
   */
  write: (value: V, view: DataView) => void

  /**
   * Reads a value from a DataView.
   */
  read: (view: DataView) => V
}

/**
 * A fixed-width array that stores elements of a fixed width.
 */
export class FixedWidthArray<V> {
  private _data: Uint8Array
  private _view: DataView

  private type: Struct<V>

  constructor(
    data: Uint8Array,
    type: Struct<V>,
  ) {
    this._data = data
    this._view = new DataView(data.buffer)
    this.type = type
  }

  get length(): number {
    return this._view.getUint32(0)
  }

  private set length(value: number) {
    this._view.setUint32(0, value)
  }

  get maxLength(): number {
    return Math.floor((this._data.length - 4) / this.type.size)
  }

  private dataViewForElement(index: number): DataView {
    const offset = 4 + index * this.type.size
    return new DataView(
      this._data.buffer,
      this._data.byteOffset + offset,
      this.type.size,
    )
  }

  get bufferSize(): number {
    return this._data.length
  }

  get(index: number): V {
    if (index >= this.length) {
      throw new Error("Index out of bounds")
    }
    return this.type.read(this.dataViewForElement(index))
  }

  set(index: number, value: V): void {
    if (index >= this.length) {
      throw new Error("Index out of bounds")
    }
    this.type.write(
      value,
      this.dataViewForElement(index),
    )
  }

  push(value: V): void {
    if (this.length >= this.maxLength) {
      throw new Error("Array is full")
    }
    this.length++
    this.set(this.length - 1, value)
  }

  pop(): V {
    if (this.length === 0) {
      throw new Error("Array is empty")
    }
    const value = this.get(this.length - 1)
    this.length--
    return value
  }

  *[Symbol.iterator](): Generator<V> {
    for (let i = 0; i < this.length; i++) {
      yield this.get(i)
    }
  }

  /**
   * Creates a new empty fixed-width array.
   *
   * @param size How many
   * @param config
   * @returns
   */
  static empty<V>(
    config:
      & { type: Struct<V> }
      & ({ bufferSize: number } | { length: number }),
  ): FixedWidthArray<V> {
    let size: number
    if ("bufferSize" in config) {
      if (config.bufferSize < 4) {
        throw new Error("Byte size must be at least 4")
      }
      size = config.bufferSize
    } else {
      size = config.length * config.type.size + 4
    }
    return new FixedWidthArray(new Uint8Array(size), config.type)
  }
}
