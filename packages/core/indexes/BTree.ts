import { InMemoryNodeId, InMemoryNodeList, INodeList } from "./NodeList.ts"
import { Comparator, EqualityChecker, Range } from "../types.ts"
import {
  BTreeNode,
  INodeId,
  InternalBTreeNode,
  LeafBTreeNode,
} from "./BTreeNode.ts"
import { debugJson, debugLog } from "../logging.ts"
import type { Promisable } from "type-fest"

type DumpedNode<K, V, NodeId> =
  | { type: "leaf"; nodeId: NodeId; keyvals: [K, readonly V[]][] }
  | {
    type: "internal"
    nodeId: NodeId
    keys: readonly K[]
    children: DumpedNode<K, V, NodeId>[]
  }

class LinkedList<V> {
  constructor(
    public head: V,
    public tail: LinkedList<V> | null = null,
  ) {}
}

function indexOfInSortedArray<T, V>(
  arr: readonly T[],
  value: V,
  compare: (a: V, b: T) => number,
): number | null {
  let i
  for (i = 0; i < arr.length && compare(value, arr[i]) >= 0; i++) {
    if (compare(value, arr[i]) === 0) {
      return i
    }
  }
  return null
}

export function findIndexInSortedArray<T>(
  arr: readonly T[],
  value: T,
  compare: (a: T, b: T) => number,
): number {
  let i
  for (i = 0; i < arr.length && compare(value, arr[i]) >= 0; i++) {
    // keep counting
  }
  return i
}

export type InMemoryBTreeConfig<K, V> = {
  order?: number
  compare?: Comparator<K>
  isEqual?: EqualityChecker<V>
}
export type InMemoryBTree<K, V> = ReturnType<typeof BTree.inMemory<K, V>>

/**
 * A BTree implementation that stores key-value pairs.
 *
 * See https://cs186berkeley.net/notes/note4/ for more information
 * about how a BTree works.
 */
export class BTree<
  K,
  V,
  NodeId extends INodeId,
  NodeListT extends INodeList<K, V, NodeId> = INodeList<K, V, NodeId>,
> {
  public nodes: NodeListT
  private rootNodeId: NodeId
  private __lastSavedRootNodeId: NodeId | null = null
  public readonly order: number
  public readonly compare: (a: K, b: K) => number
  public readonly isEqual: (a: V, b: V) => boolean
  private onRootNodeChanged: () => void | Promise<void>

  getRootNode(): Promise<InternalBTreeNode<K, NodeId>> {
    return this.nodes.get(this.rootNodeId) as Promise<
      InternalBTreeNode<K, NodeId>
    >
  }

  static inMemory<K, V>(
    {
      order = 2,
      compare = (a, b) => a < b ? -1 : a > b ? 1 : 0,
      isEqual = (a, b) => a === b,
    }: InMemoryBTreeConfig<K, V> = {},
  ) {
    const nodes = new InMemoryNodeList<K, V>(new Map())
    const childNode = nodes.createLeafNode({
      keyvals: [],
      nextLeafNodeId: null,
      prevLeafNodeId: null,
    })
    const rootNode = nodes.createInternalNode({
      keys: [],
      childrenNodeIds: [childNode.nodeId],
    })
    nodes.commit()
    return new BTree<K, V, InMemoryNodeId, InMemoryNodeList<K, V>>({
      order,
      compare,
      isEqual,
      nodes,
      rootNodeId: rootNode.nodeId,
      onRootNodeChanged: () => {
        // noop because this is all in memory
      },
    })
  }

  async commit() {
    if (this.__lastSavedRootNodeId !== this.rootNodeId) {
      await this.onRootNodeChanged()
      this.__lastSavedRootNodeId = this.rootNodeId
    }
    await this.nodes.commit()
  }

  getNodeWithId(nodeId: NodeId): Promisable<BTreeNode<K, V, NodeId>> {
    return this.nodes.get(nodeId)
  }

  private async _countNodes(rootNodeId: NodeId): Promise<number> {
    const node = await this.nodes.get(rootNodeId)
    if (node.type === "leaf") {
      return 1
    }
    const children = await this.childrenForNode(node)
    const childCounts = await Promise.all(
      children.map((child) => this._countNodes(child.nodeId)),
    )
    return 1 + childCounts.reduce((a, b) => a + b, 0)
  }

  countNodes(): Promise<number> {
    return this._countNodes(this.rootNodeId)
  }

  constructor(
    {
      order = 2,
      compare,
      isEqual,
      nodes,
      rootNodeId,
      onRootNodeChanged,
    }: {
      order?: number
      compare: (a: K, b: K) => number
      isEqual: (a: V, b: V) => boolean
      nodes: NodeListT
      rootNodeId: NodeId
      onRootNodeChanged: () => Promise<void> | void
    },
  ) {
    this.onRootNodeChanged = onRootNodeChanged
    this.compare = compare
    this.isEqual = isEqual
    this.nodes = nodes
    this.order = order
    this.rootNodeId = rootNodeId
  }

  childrenForNode(
    node: BTreeNode<K, V, NodeId>,
  ): Promise<BTreeNode<K, V, NodeId>[]> {
    if (node.type === "leaf") {
      return Promise.resolve([])
    }
    return Promise.all(node.childrenNodeIds.map((id) => this.nodes.get(id)))
  }

  async dumpNode(nodeId: NodeId): Promise<DumpedNode<K, V, NodeId>> {
    const node = await this.nodes.get(nodeId)
    if (node.type === "leaf") {
      return {
        type: "leaf",
        nodeId: node.nodeId,
        keyvals: node.keyvals.map((key) => [key.key, key.vals]),
      }
    }
    return {
      type: "internal",
      nodeId: node.nodeId,
      keys: node.keys,
      children: await Promise.all(
        node.childrenNodeIds.map((id) => this.dumpNode(id)),
      ),
    }
  }

  dump() {
    return this.dumpNode(this.rootNodeId)
  }

  async _get(
    nodeId: NodeId,
    key: K,
    parents: LinkedList<NodeId> | null = null,
    depth = 1,
  ): Promise<{
    nodeId: NodeId
    node: LeafBTreeNode<K, V, NodeId>
    parents: LinkedList<NodeId>
    key: K
    keyval: Readonly<{ key: K; vals: readonly V[] }> | null
    keyIndex: number
  }> {
    const node = await this.nodes.get(nodeId)
    if (node.type === "leaf") {
      if (parents == null) {
        throw new Error("all leaf nodes should have a parent")
      }
      const keyIndex = indexOfInSortedArray(
        node.keyvals,
        key,
        (a, b) => this.compare(a, b.key),
      )
      if (keyIndex == null) {
        return {
          nodeId,
          node,
          parents,
          key,
          keyIndex: -1,
          keyval: null,
        }
      }
      return {
        nodeId,
        node,
        parents,
        key,
        keyIndex,
        keyval: keyIndex < node.keyvals.length ? node.keyvals[keyIndex] : null,
      }
    }
    if (node.keys.length === 0) {
      return this._get(
        node.childrenNodeIds[0],
        key,
        new LinkedList(node.nodeId, parents),
        depth + 1,
      )
    }
    const i = findIndexInSortedArray(node.keys, key, this.compare)
    return this._get(
      node.childrenNodeIds[i],
      key,
      new LinkedList(node.nodeId, parents),
      depth + 1,
    )
  }

  /**
   * Replace one leaf node with a new leaf node by updating pointers.
   *
   * IT IS ONLY SAFE TO USE THIS IF NEXT/PREV/PARENT NODES ARE THE SAME.
   */
  private async replaceLeafNode(
    oldNode: LeafBTreeNode<K, V, NodeId>,
    newNode: LeafBTreeNode<K, V, NodeId>,
    parentNodeId: NodeId,
  ) {
    debugLog(
      `replaceLeafNode(old=${oldNode.nodeId}, new=${newNode.nodeId}, parent=${parentNodeId})`,
    )
    const parent = await this.nodes.get(parentNodeId)
    if (!(parent instanceof InternalBTreeNode)) {
      throw new Error(
        `parent of leaf node should be an internal node found ${parent} for nodeId ${parentNodeId}`,
      )
    }
    await this.nodes.deleteNode(oldNode.nodeId)
    if (oldNode.prevLeafNodeId != null) {
      const prevNode = await this.nodes.get(
        oldNode.prevLeafNodeId,
      ) as LeafBTreeNode<K, V, NodeId>
      prevNode.nextLeafNodeId = newNode.nodeId
      newNode.prevLeafNodeId = oldNode.prevLeafNodeId
    }
    if (oldNode.nextLeafNodeId != null) {
      const nextNode = await this.nodes.get(
        oldNode.nextLeafNodeId,
      ) as LeafBTreeNode<K, V, NodeId>
      nextNode.prevLeafNodeId = newNode.nodeId
      newNode.nextLeafNodeId = oldNode.nextLeafNodeId
    }
    parent.swapChildNodeId(oldNode.nodeId, newNode.nodeId)
  }

  /**
   * Replace an internal node with a new internal node by updating pointers.
   */
  private async replaceInternalNode(
    oldNode: InternalBTreeNode<K, NodeId>,
    newNode: InternalBTreeNode<K, NodeId>,
    parentNodeId: NodeId | null,
  ) {
    debugLog(
      `replaceInternalNode(old=${oldNode.nodeId}, new=${newNode.nodeId})`,
    )
    if (parentNodeId == null) {
      // TODO: The root node id actually has to be stored somewhere...
      this.rootNodeId = newNode.nodeId
    } else {
      const parent = await this.nodes.get(parentNodeId) as InternalBTreeNode<
        K,
        NodeId
      >
      parent.swapChildNodeId(oldNode.nodeId, newNode.nodeId)
    }
    await this.nodes.deleteNode(oldNode.nodeId)
  }

  async insertMany(keyvals: Iterable<[K, V]>): Promise<void> {
    for (const [key, value] of keyvals) {
      await this.insert(key, value)
    }
  }

  async insert(key: K, value: V) {
    debugLog(
      () => `\n\nBTree.insert(${debugJson(key)}, ${debugJson(value)})`,
    )
    debugLog("Root node id", this.rootNodeId)
    const found = await this._get(this.rootNodeId, key)
    debugLog("found", found)
    if (found.keyval != null) {
      const newNode = await this.nodes.createLeafNode({
        keyvals: [
          ...found.node.keyvals.slice(0, found.keyIndex),
          {
            key,
            vals: [...found.keyval.vals, value],
          },
          ...found.node.keyvals.slice(found.keyIndex + 1),
        ],
        nextLeafNodeId: found.node.nextLeafNodeId,
        prevLeafNodeId: found.node.prevLeafNodeId,
      })
      await this.replaceLeafNode(found.node, newNode, found.parents.head)
      await this.commit()
      return
    }

    const newNode = await this.nodes.createLeafNode({
      keyvals: [
        ...found.node.keyvals,
        { key, vals: [value] },
      ].sort((a, b) => this.compare(a.key, b.key)),
      nextLeafNodeId: found.node.nextLeafNodeId,
      prevLeafNodeId: found.node.prevLeafNodeId,
    })
    await this.replaceLeafNode(found.node, newNode, found.parents.head)

    if (newNode.keyvals.length <= this.order * 2) {
      await this.commit()
      return
    }
    await this.splitNode(newNode.nodeId, found.parents)
    await this.commit()
  }

  private async insertIntoParent(
    parent: InternalBTreeNode<K, NodeId>,
    grandParents: LinkedList<NodeId> | null,
    node: BTreeNode<K, V, NodeId>,
    key: K,
    depth: number,
  ) {
    debugLog(
      `insertIntoParent(parent=${parent.nodeId}, node=${node.nodeId}, ${key})`,
    )
    const newNode = await this.nodes.createInternalNode(
      parent.withInsertedNode(key, node.nodeId, this.compare),
    )
    await this.replaceInternalNode(parent, newNode, grandParents?.head ?? null)

    if (newNode.keys.length <= this.order * 2) {
      return
    }
    await this.splitInternalNode(newNode, grandParents, depth + 1)
  }

  private async splitLeafNode(
    node: LeafBTreeNode<K, V, NodeId>,
    parents: LinkedList<NodeId>,
    depth: number,
  ) {
    debugLog(`splitLeafNode(${node.nodeId}, parent=${parents.head})`)
    const L2 = await this.nodes.createLeafNode({
      keyvals: node.copyKeyvals(this.order),
      nextLeafNodeId: node.nextLeafNodeId,
      prevLeafNodeId: null,
    })
    const L1 = await this.nodes.createLeafNode({
      keyvals: node.copyKeyvals(0, this.order),
      nextLeafNodeId: L2.nodeId,
      prevLeafNodeId: node.prevLeafNodeId,
    })
    L2.prevLeafNodeId = L1.nodeId
    const parent = await this.nodes.get(parents.head)
    if (!(parent instanceof InternalBTreeNode)) {
      debugLog(`Derp: parent=${parent} is a leaf node??`)
      throw new Error(
        `parent of leaf node should be an internal node found ${parent} for nodeId ${parents.head}`,
      )
    }
    parent.swapChildNodeId(node.nodeId, L1.nodeId)
    if (node.prevLeafNodeId != null) {
      const prevNode = await this.nodes.get(
        node.prevLeafNodeId,
      ) as LeafBTreeNode<K, V, NodeId>
      prevNode.nextLeafNodeId = L1.nodeId
    }
    if (node.nextLeafNodeId != null) {
      const nextNode = await this.nodes.get(
        node.nextLeafNodeId,
      ) as LeafBTreeNode<K, V, NodeId>
      nextNode.prevLeafNodeId = L2.nodeId
    }
    await this.nodes.deleteNode(node.nodeId)
    await this.insertIntoParent(
      parent,
      parents.tail,
      L2,
      L2.keyvals[0].key,
      depth,
    )
  }

  private async splitInternalNode(
    node: InternalBTreeNode<K, NodeId>,
    parents: LinkedList<NodeId> | null,
    depth: number,
  ) {
    debugLog(`splitInternalNode(${node.nodeId}, parent=${parents?.head})`)
    let parentNode: InternalBTreeNode<K, NodeId>
    let grandParents: LinkedList<NodeId> | null
    if (parents == null) {
      // make a new parent node
      parentNode = await this.nodes.createInternalNode(
        {
          keys: [],
          childrenNodeIds: [node.nodeId],
        },
      )
      this.rootNodeId = parentNode.nodeId
      parents = new LinkedList(parentNode.nodeId)
      grandParents = null
    } else {
      parentNode = await this.nodes.get(parents.head) as InternalBTreeNode<
        K,
        NodeId
      >
      grandParents = parents.tail
    }

    const keyToMove = node.keys[this.order]
    const L2 = await this.nodes.createInternalNode(
      {
        keys: node.keys.slice(this.order + 1),
        childrenNodeIds: node.childrenNodeIds.slice(this.order + 1),
      },
    )
    const L1 = await this.nodes.createInternalNode({
      keys: node.keys.slice(0, this.order),
      childrenNodeIds: node.childrenNodeIds.slice(0, this.order + 1),
    })
    parentNode.swapChildNodeId(node.nodeId, L1.nodeId)
    await this.nodes.deleteNode(node.nodeId)

    const newParentNode = await this.nodes.createInternalNode(
      parentNode.withInsertedNode(keyToMove, L2.nodeId, this.compare),
    )
    await this.replaceInternalNode(
      parentNode,
      newParentNode,
      grandParents?.head ?? null,
    )

    if (newParentNode.keys.length > this.order * 2) {
      await this.splitNode(newParentNode.nodeId, parents.tail, depth + 1)
    }
  }

  private async splitNode(
    nodeId: NodeId,
    parents: LinkedList<NodeId> | null,
    depth = 1,
  ) {
    const node = await this.nodes.get(nodeId)
    if (node.type === "leaf") {
      if (parents == null) {
        throw new Error("all leaf nodes should have a parent")
      }
      await this.splitLeafNode(node, parents, depth)
    } else {
      await this.splitInternalNode(node, parents, depth)
    }
  }

  async removeAll(key: K) {
    const found = await this._get(this.rootNodeId, key)
    if (found.keyval == null) {
      return
    }
    const { node, keyIndex } = found
    node.removeKey(keyIndex)
    await this.commit()
  }

  async remove(key: K, value: V) {
    const found = await this._get(this.rootNodeId, key)
    if (found.keyval == null) {
      return
    }
    found.node.removeValue(found.keyIndex, value, this.isEqual)
    await this.commit()
  }

  async has(key: K): Promise<boolean> {
    const found = await this._get(this.rootNodeId, key)
    return found.keyval !== null && found.keyval.vals.length > 0
  }

  async get(key: K): Promise<readonly V[]> {
    return (await this._get(this.rootNodeId, key)).keyval?.vals ?? []
  }

  private async getMinNode(
    node?: BTreeNode<K, V, NodeId>,
  ): Promise<LeafBTreeNode<K, V, NodeId>> {
    if (node == null) {
      node = await this.getRootNode()
    }
    if (node.type === "leaf") {
      return node
    }
    return this.getMinNode(await this.nodes.get(node.childrenNodeIds[0]))
  }

  async getRange(
    { gt, gte, lte, lt }: Range<K>,
  ): Promise<{ key: K; vals: readonly V[] }[]> {
    const results: { key: K; vals: readonly V[] }[] = []

    let current: LeafBTreeNode<K, V, NodeId>
    let isGt: ((k: K) => boolean) | null = null
    let ltCheck: ((k: K) => boolean) | null = null
    if (gt !== undefined) {
      if (gte !== undefined) {
        throw new Error("Cannot have both gt and gte")
      }
      isGt = (k) => this.compare(k, gt) > 0
      current = (await this._get(this.rootNodeId, gt)).node
    } else if (gte !== undefined) {
      isGt = (k) => this.compare(k, gte) >= 0
      current = (await this._get(this.rootNodeId, gte)).node
    } else {
      current = await this.getMinNode()
    }

    if (lt !== undefined) {
      if (lte !== undefined) {
        throw new Error("Cannot have both lt and lte")
      }
      ltCheck = (k) => this.compare(k, lt) >= 0
    } else if (lte !== undefined) {
      ltCheck = (k) => this.compare(k, lte) > 0
    }

    if (isGt != null) {
      for (const keyval of current.keyvals) {
        if (!isGt(keyval.key)) {
          continue
        }
        if (ltCheck != null && ltCheck(keyval.key)) {
          return results
        }
        results.push(keyval)
      }
      if (current.nextLeafNodeId == null) return results
      current = await this.nodes.get(current.nextLeafNodeId) as LeafBTreeNode<
        K,
        V,
        NodeId
      >
    }

    if (ltCheck == null) {
      while (true) {
        results.push(...current.keyvals)
        if (current.nextLeafNodeId == null) break
        current = await this.nodes.get(current.nextLeafNodeId) as LeafBTreeNode<
          K,
          V,
          NodeId
        >
      }
      return results
    }

    while (true) {
      for (const keyval of current.keyvals) {
        if (ltCheck != null && ltCheck(keyval.key)) {
          return results
        }
        results.push(keyval)
      }
      if (current.nextLeafNodeId == null) break
      current = await this.nodes.get(current.nextLeafNodeId) as LeafBTreeNode<
        K,
        V,
        NodeId
      >
    }
    return results
  }
}
