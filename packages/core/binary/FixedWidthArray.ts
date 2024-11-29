import { ReadonlyDataView, WriteableDataView } from "./dataview.ts"
import { FixedWidthStruct } from "./Struct.ts"

/**
 * A fixed-width array that stores elements of a fixed width.
 */
export class ReadableFixedWidthArray<V> {
  constructor(
    private view: ReadonlyDataView,
    protected valueStruct: FixedWidthStruct<V>,
  ) {}

  get length(): number {
    return this.view.getUint32(0)
  }

  get maxLength(): number {
    return Math.floor((this.view.byteLength - 4) / this.valueStruct.size)
  }

  get bufferSize(): number {
    return this.view.byteLength
  }

  get(index: number): V {
    if (index >= this.length) {
      throw new Error("Index out of bounds")
    }
    return this.valueStruct.readAt(this.view, 4 + index * this.valueStruct.size)
  }

  *[Symbol.iterator](): Generator<V> {
    for (let i = 0; i < this.length; i++) {
      yield this.get(i)
    }
  }

  *enumerate(): Generator<[number, V]> {
    for (let i = 0; i < this.length; i++) {
      yield [i, this.get(i)]
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
      & { type: FixedWidthStruct<V> }
      & ({ bufferSize: number } | { length: number }),
  ): WriteableFixedWidthArray<V> {
    let size: number
    if ("bufferSize" in config) {
      if (config.bufferSize < 4) {
        throw new Error("Byte size must be at least 4")
      }
      size = config.bufferSize
    } else {
      size = config.length * config.type.size + 4
    }
    return new WriteableFixedWidthArray(
      new WriteableDataView(size),
      config.type,
    )
  }
}

export class WriteableFixedWidthArray<V> extends ReadableFixedWidthArray<V> {
  constructor(
    private writeableView: WriteableDataView,
    valueStruct: FixedWidthStruct<V>,
  ) {
    super(writeableView, valueStruct)
  }

  private setLength(value: number) {
    this.writeableView.setUint32(0, value)
  }

  set(index: number, value: V): void {
    if (index >= this.length) {
      throw new Error("Index out of bounds")
    }
    this.valueStruct.writeAt(
      value,
      this.writeableView,
      4 + index * this.valueStruct.size,
    )
  }

  push(value: V): void {
    if (this.length >= this.maxLength) {
      throw new Error("Array is full")
    }
    this.setLength(this.length + 1)
    this.set(this.length - 1, value)
  }

  pop(): V {
    if (this.length === 0) {
      throw new Error("Array is empty")
    }
    const value = this.get(this.length - 1)
    this.setLength(this.length - 1)
    return value
  }
}
