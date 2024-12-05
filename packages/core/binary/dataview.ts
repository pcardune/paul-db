export class ReadonlyDataView {
  protected view: DataView

  constructor(
    buffer: ArrayBufferLike,
    offset: number = 0,
    length: number = buffer.byteLength,
  ) {
    this.view = new DataView(buffer, offset, length)
  }

  get byteLength(): number {
    return this.view.byteLength
  }

  toUint8Array(): Uint8Array {
    return new Uint8Array(
      this.view.buffer,
      this.view.byteOffset,
      this.view.byteLength,
    )
  }

  slice(offset: number, length: number): ReadonlyDataView {
    return new ReadonlyDataView(
      this.view.buffer,
      this.view.byteOffset + offset,
      length,
    )
  }

  getUint8(byteOffset: number): number {
    return this.view.getUint8(byteOffset)
  }

  getUint16(byteOffset: number): number {
    return this.view.getUint16(byteOffset)
  }

  getInt16(byteOffset: number): number {
    return this.view.getInt16(byteOffset)
  }

  getUint32(byteOffset: number): number {
    return this.view.getUint32(byteOffset)
  }

  getInt32(byteOffset: number): number {
    return this.view.getInt32(byteOffset)
  }

  getBigUint64(byteOffset: number): bigint {
    return this.view.getBigUint64(byteOffset)
  }

  getBigInt64(byteOffset: number): bigint {
    return this.view.getBigInt64(byteOffset)
  }

  getFloat64(byteOffset: number): number {
    return this.view.getFloat64(byteOffset)
  }

  decodeText(): string {
    return new TextDecoder().decode(this.view)
  }
}

export class WriteableDataView extends ReadonlyDataView {
  constructor(
    numBytesOrBuffer: number | ArrayBufferLike,
    offset?: number,
    length?: number,
  ) {
    if (typeof numBytesOrBuffer === "number") {
      numBytesOrBuffer = new ArrayBuffer(numBytesOrBuffer)
    }
    super(numBytesOrBuffer, offset, length)
  }

  override slice(offset: number, length: number): WriteableDataView {
    return new WriteableDataView(
      this.view.buffer,
      this.view.byteOffset + offset,
      length,
    )
  }

  setUint8(byteOffset: number, value: number): void {
    this.view.setUint8(byteOffset, value)
  }

  setUint16(byteOffset: number, value: number): void {
    this.view.setUint16(byteOffset, value)
  }

  setInt16(byteOffset: number, value: number): void {
    this.view.setInt16(byteOffset, value)
  }

  setUint32(byteOffset: number, value: number): void {
    this.view.setUint32(byteOffset, value)
  }

  setInt32(byteOffset: number, value: number): void {
    this.view.setInt32(byteOffset, value)
  }

  setBigUint64(byteOffset: number, value: bigint): void {
    this.view.setBigUint64(byteOffset, value)
  }

  setBigInt64(byteOffset: number, value: bigint): void {
    this.view.setBigInt64(byteOffset, value)
  }

  setFloat64(byteOffset: number, value: number): void {
    this.view.setFloat64(byteOffset, value)
  }

  setFromText(text: string): void {
    const encoder = new TextEncoder()
    const bytes = encoder.encode(text)
    new Uint8Array(this.view.buffer).set(bytes, this.view.byteOffset)
  }

  fill(value: number, start?: number, end?: number): void {
    new Uint8Array(this.view.buffer).fill(value, start, end)
  }

  setUint8Array(byteOffset: number, value: Uint8Array): void {
    new Uint8Array(this.view.buffer, this.view.byteOffset, this.view.byteLength)
      .set(value, byteOffset)
  }
}
