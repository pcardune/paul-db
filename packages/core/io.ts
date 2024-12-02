import { Mutex } from "./async.ts"

export class EOFError extends Error {}

/**
 * Read from the file to fill the given buffer.
 */
async function readBytesInto(file: Deno.FsFile, into: Uint8Array) {
  let bytesRead = 0
  while (bytesRead < into.length) {
    const n = await file.read(into.subarray(bytesRead))
    if (n === null) {
      throw new EOFError(
        `Failed to read more than ${bytesRead} bytes. ${into.length} wanted.`,
      )
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

const readMutexes = new Map<Deno.FsFile, Mutex>()

const getMutex = (file: Deno.FsFile) => {
  let mutex = readMutexes.get(file)
  if (!mutex) {
    mutex = new Mutex()
    readMutexes.set(file, mutex)
  }
  return mutex
}

/**
 * Read the given number of bytes from the file at the given offset.
 */
export async function readBytesAt(
  file: Deno.FsFile,
  offset: bigint | number,
  numBytes: number,
) {
  const mutex = getMutex(file)
  await mutex.acquire()
  await file.seek(offset, Deno.SeekMode.Start)
  try {
    return await readBytes(file, numBytes)
  } finally {
    mutex.release()
    if (!mutex.isLocked) {
      readMutexes.delete(file)
    }
  }
}

export async function writeBytesAt(
  file: Deno.FsFile,
  offset: bigint | number,
  data: Uint8Array,
) {
  const mutex = getMutex(file)

  try {
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
  } finally {
    mutex.release()
    if (!mutex.isLocked) {
      readMutexes.delete(file)
    }
  }
}
