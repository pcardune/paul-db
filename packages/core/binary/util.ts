export function dumpUint32Buffer(buffer: ArrayBuffer, littleEndian?: boolean) {
  const values: number[] = []
  const view = new DataView(buffer)
  for (let i = 0; i < view.byteLength; i += 4) {
    values.push(view.getUint32(i, littleEndian))
  }
  return values
}

export function dumpUint8Buffer(buffer: ArrayBuffer): number[] {
  const values: number[] = []
  const view = new DataView(buffer)
  for (let i = 0; i < view.byteLength; i++) {
    values.push(view.getUint8(i))
  }
  return values
}
