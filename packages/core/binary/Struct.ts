export interface IStruct<ValueT> {
  sizeof(value: ValueT): number
  readAt(view: DataView, offset: number): Readonly<ValueT>
  writeAt(value: ValueT, view: DataView, offset: number): void
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
  private read: (view: DataView) => Readonly<ValueT>

  constructor({ sizeof, write, read }: {
    /**
     * Calculate the size of this value in bytes.
     * @param value The javascript value to calculate the size of
     * @returns The size of the value in bytes. This is the amount of space
     * that the write and read methods will have access to.
     */
    sizeof: (value: ValueT) => number
    write: (value: ValueT, view: DataView) => void
    read: (view: DataView) => Readonly<ValueT>
  }) {
    this._sizeof = sizeof
    this.write = write
    this.read = read
  }

  sizeof(value: ValueT): number {
    return this._sizeof(value) + 4
  }

  readAt(view: DataView, offset: number): Readonly<ValueT> {
    const size = view.getUint32(offset)
    return this.read(
      new DataView(view.buffer, view.byteOffset + offset + 4, size),
    )
  }

  writeAt(value: ValueT, view: DataView, offset: number): void {
    const size = this._sizeof(value)
    view.setUint32(offset, size)
    this.write(
      value,
      new DataView(view.buffer, view.byteOffset + offset + 4, size),
    )
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
  private read: (view: DataView) => Readonly<ValueT>

  constructor({ size, write, read }: {
    size: number
    write: (value: ValueT, view: DataView) => void
    read: (view: DataView) => Readonly<ValueT>
  }) {
    this.size = size
    this.write = write
    this.read = read
  }

  sizeof(_value: ValueT): number {
    return this.size
  }

  readAt(view: DataView, offset: number): Readonly<ValueT> {
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
}
