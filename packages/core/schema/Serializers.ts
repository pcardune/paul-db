export type Serializer<T> = {
  fixedLength: number | false
  serialize: (value: T) => ArrayBuffer
  deserialize: (bytes: DataView) => T
}

export const stringSerializer: Serializer<string> = {
  fixedLength: false,
  serialize: (value: string) => {
    const encoder = new TextEncoder()
    return encoder.encode(value).buffer
  },
  deserialize: (bytes: DataView) => {
    const decoder = new TextDecoder()
    return decoder.decode(bytes)
  },
}

export const floatSerializer: Serializer<number> = {
  fixedLength: 8,
  serialize: (value: number) => {
    const buffer = new ArrayBuffer(8)
    const view = new DataView(buffer)
    view.setFloat64(0, value)
    return buffer
  },
  deserialize: (bytes: DataView) => {
    return bytes.getFloat64(0)
  },
}

export const int32Serializer: Serializer<number> = {
  fixedLength: 4,
  serialize: (value: number) => {
    const buffer = new ArrayBuffer(4)
    const view = new DataView(buffer)
    view.setInt32(0, value)
    return buffer
  },
  deserialize: (bytes: DataView) => {
    return bytes.getInt32(0)
  },
}

export const uint32Serializer: Serializer<number> = {
  fixedLength: 4,
  serialize: (value: number) => {
    const buffer = new ArrayBuffer(4)
    const view = new DataView(buffer)
    view.setUint32(0, value)
    return buffer
  },
  deserialize: (bytes: DataView) => {
    return bytes.getUint32(0)
  },
}

export const int64Serializer: Serializer<bigint> = {
  fixedLength: 8,
  serialize: (value: bigint) => {
    const buffer = new ArrayBuffer(8)
    const view = new DataView(buffer)
    view.setBigInt64(0, value)
    return buffer
  },
  deserialize: (bytes: DataView) => {
    return bytes.getBigInt64(0)
  },
}

export const uint64Serializer: Serializer<bigint> = {
  fixedLength: 8,
  serialize: (value: bigint) => {
    const buffer = new ArrayBuffer(8)
    const view = new DataView(buffer)
    view.setBigUint64(0, value)
    return buffer
  },
  deserialize: (bytes: DataView) => {
    return bytes.getBigUint64(0)
  },
}

export const booleanSerializer: Serializer<boolean> = {
  fixedLength: 1,
  serialize: (value: boolean) => {
    const buffer = new ArrayBuffer(1)
    const view = new DataView(buffer)
    view.setUint8(0, value ? 1 : 0)
    return buffer
  },
  deserialize: (bytes: DataView) => {
    return bytes.getUint8(0) === 1
  },
}
