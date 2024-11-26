/**
 * Read from the file to fill the given buffer.
 */
async function readBytesInto(file: Deno.FsFile, into: Uint8Array) {
  let bytesRead = 0
  while (bytesRead < into.length) {
    const n = await file.read(into.subarray(bytesRead))
    if (n === null) {
      throw new Error("Failed to read")
    }
    bytesRead += n
  }
  return bytesRead
}

/**
 * Read the given number of bytes from the file, returning a Uint8Array.
 */
async function readBytes(file: Deno.FsFile, numBytes: number) {
  const buffer = new Uint8Array(numBytes)
  const bytesRead = await readBytesInto(file, buffer)
  if (bytesRead !== buffer.length) {
    throw new Error(
      `Unexpected number of bytes read (${bytesRead}) wanted ${numBytes}`,
    )
  }
  return buffer
}

/**
 * Read the given number of bytes from the file at the given offset.
 */
export async function readBytesAt(
  file: Deno.FsFile,
  offset: bigint | number,
  numBytes: number,
) {
  await file.seek(offset, Deno.SeekMode.Start)
  return readBytes(file, numBytes)
}

export async function writeBytesAt(
  file: Deno.FsFile,
  offset: bigint | number,
  data: Uint8Array,
) {
  await file.seek(offset, Deno.SeekMode.Start)
  let bytesWritten = 0
  while (bytesWritten < data.length) {
    const n = await file.write(data.subarray(bytesWritten))
    if (n === null) {
      throw new Error("Failed to write")
    }
    bytesWritten += n
  }
  return bytesWritten
}
