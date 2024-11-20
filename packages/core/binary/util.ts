export function dumpUint32Buffer(buffer: ArrayBuffer, littleEndian?: boolean) {
  const values: number[] = []
  const view = new DataView(buffer)
  for (let i = 0; i < view.byteLength; i += 4) {
    values.push(view.getUint32(i, littleEndian))
  }
  console.log(values)
}
