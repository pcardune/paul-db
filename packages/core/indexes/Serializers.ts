import {
  FixedWidthStruct,
  IStruct,
  VariableWidthStruct,
} from "../binary/Struct.ts"
import { PageId } from "../pages/BufferPool.ts"
import { INodeId, InternalBTreeNode, LeafBTreeNode } from "./BTreeNode.ts"

export class FileNodeId implements INodeId {
  readonly pageId: bigint
  readonly slotIndex: number
  constructor({ pageId, slotIndex }: { pageId: PageId; slotIndex: number }) {
    this.pageId = pageId
    this.slotIndex = slotIndex
  }

  equals(other: INodeId): boolean {
    return other instanceof FileNodeId && other.pageId === this.pageId &&
      other.slotIndex === this.slotIndex
  }

  serialize(): string {
    return `${this.pageId}:${this.slotIndex}`
  }

  toString(): string {
    return this.serialize()
  }
}

export const fileNodeIdStruct = new FixedWidthStruct<FileNodeId | null>({
  toJSON: (_value) => {
    throw new Error("Not Implemented")
  },
  fromJSON: (_json) => {
    throw new Error("Not Implemented")
  },
  size: 12,
  write: (value, view) => {
    if (value === null) {
      view.setBigUint64(0, 0n)
      view.setUint32(8, 0)
      return
    }
    view.setBigUint64(0, value.pageId)
    view.setUint32(8, value.slotIndex)
  },
  read: (view) => {
    const nodeId = new FileNodeId({
      pageId: view.getBigUint64(0),
      slotIndex: view.getUint32(8),
    })
    if (nodeId.pageId === 0n && nodeId.slotIndex === 0) {
      return null
    }
    return nodeId
  },
})

export function keyValsStruct<K, V>(
  keySerializer: IStruct<K>,
  valSerializer: IStruct<V>,
) {
  const valArraySerializer = valSerializer.array()
  return new VariableWidthStruct<{ key: K; vals: V[] }>({
    toJSON: (_value) => {
      throw new Error("Not Implemented")
    },
    fromJSON: (_json) => {
      throw new Error("Not Implemented")
    },
    emptyValue: () => {
      throw new Error("Not Implemented")
    },
    sizeof: (value) => {
      return keySerializer.sizeof(value.key) +
        valArraySerializer.sizeof(value.vals)
    },
    write: (value, view) => {
      // Write the key at index 0
      keySerializer.writeAt(value.key, view, 0)
      // Write the values after that
      valArraySerializer.writeAt(
        value.vals,
        view,
        keySerializer.sizeof(value.key),
      )
    },
    read: (view) => {
      // read the key at index 0
      const key = keySerializer.readAt(view, 0)
      const vals = valArraySerializer.readAt(view, keySerializer.sizeof(key))
      return { key, vals }
    },
  })
}

enum NodeType {
  LEAF = 1,
  INTERNAL = 2,
}

export class WrongNodeTypeError extends Error {
  constructor(nodeType: NodeType, expected: NodeType) {
    super(
      `Expected node type ${expected} found ${nodeType}`,
    )
  }
}

export function leafBTreeNodeStruct<K, V>(
  keySerializer: IStruct<K>,
  valSerializer: IStruct<V>,
) {
  const keyValsSerializer = keyValsStruct(keySerializer, valSerializer).array()
  return new VariableWidthStruct<
    Pick<
      LeafBTreeNode<K, V, FileNodeId>,
      "keyvals" | "nextLeafNodeId" | "prevLeafNodeId"
    >
  >({
    toJSON: (_value) => {
      throw new Error("Not Implemented")
    },
    fromJSON: (_json) => {
      throw new Error("Not Implemented")
    },
    emptyValue: () => {
      throw new Error("Not Implemented")
    },
    sizeof: (value) => {
      return 1 + fileNodeIdStruct.sizeof(value.prevLeafNodeId) +
        fileNodeIdStruct.sizeof(value.nextLeafNodeId) +
        keyValsSerializer.sizeof(value.keyvals)
    },
    write: (value, view) => {
      // Write the node type
      let offset = 0
      view.setUint8(offset, NodeType.LEAF)
      offset += 1
      // next write the prevLeafNodeId
      fileNodeIdStruct.writeAt(
        value.prevLeafNodeId,
        view,
        offset,
      )
      offset += fileNodeIdStruct.sizeof(value.prevLeafNodeId)
      // next write the nextLeafNodeId
      fileNodeIdStruct.writeAt(
        value.nextLeafNodeId,
        view,
        offset,
      )
      offset += fileNodeIdStruct.sizeof(value.prevLeafNodeId)
      // next write the keyvals
      keyValsSerializer.writeAt(value.keyvals, view, offset)
      offset += keyValsSerializer.sizeof(value.keyvals)
    },
    read: (view) => {
      // read the node type
      let offset = 0
      const nodeType = view.getUint8(offset)
      offset += 1
      if (nodeType !== NodeType.LEAF) {
        throw new WrongNodeTypeError(nodeType, NodeType.LEAF)
      }
      // read the prevLeafNodeId
      const prevLeafNodeId = fileNodeIdStruct.readAt(view, offset)
      offset += fileNodeIdStruct.sizeof(prevLeafNodeId)
      // read the nextLeafNodeId
      const nextLeafNodeId = fileNodeIdStruct.readAt(view, offset)
      offset += fileNodeIdStruct.sizeof(nextLeafNodeId)
      // read the keyvals
      const keyvals = keyValsSerializer.readAt(view, offset)
      offset += keyValsSerializer.sizeof(keyvals)
      return {
        keyvals,
        nextLeafNodeId,
        prevLeafNodeId,
      }
    },
  })
}

export function internalBTreeNodeStruct<K>(keySerializer: IStruct<K>) {
  const keyArraySerializer = keySerializer.array()
  const nodeIdArraySerializer = fileNodeIdStruct.array()
  return new VariableWidthStruct<
    Pick<InternalBTreeNode<K, FileNodeId>, "keys" | "childrenNodeIds">
  >({
    toJSON: (_value) => {
      throw new Error("Not Implemented")
    },
    fromJSON: (_json) => {
      throw new Error("Not Implemented")
    },
    emptyValue: () => {
      throw new Error("Not Implemented")
    },
    sizeof: (value) => {
      return 1 + keyArraySerializer.sizeof(value.keys) +
        nodeIdArraySerializer.sizeof(value.childrenNodeIds)
    },
    write: (value, view) => {
      // Write the node type: 1 means internal node
      let offset = 0
      view.setUint8(offset, NodeType.INTERNAL)
      offset += 1
      // next write the keys
      keyArraySerializer.writeAt(value.keys, view, offset)
      offset += keyArraySerializer.sizeof(value.keys)
      // next write the children node ids
      nodeIdArraySerializer.writeAt(
        value.childrenNodeIds,
        view,
        offset,
      )
    },
    read: (view) => {
      // read the node type
      let offset = 0
      const nodeType = view.getUint8(offset)
      offset += 1
      if (nodeType !== NodeType.INTERNAL) {
        throw new WrongNodeTypeError(nodeType, NodeType.INTERNAL)
      }
      // read the keys
      const keys = keyArraySerializer.readAt(view, offset)
      offset += keyArraySerializer.sizeof(keys)
      // read the children node ids
      const childrenNodeIds = nodeIdArraySerializer.readAt(
        view,
        offset,
      ).map((nodeId) => {
        if (nodeId == null) {
          throw new Error("Unexpected null nodeId")
        }
        return nodeId
      })
      return {
        keys,
        childrenNodeIds,
      }
    },
  })
}
