import { Struct } from "../binary/Struct.ts"
import { debugJson, debugLog } from "../logging.ts"
import { PageSpaceAllocator } from "./HeapPageFile.ts"

/**
 * Represents a slot in a variable-length record page.
 *
 * The slot is a pair of 32-bit unsigned integers, the first representing the
 * offset of the record in the page, and the second representing the length of
 * the record.
 *
 * If a slot has a length of 0, it is considered free space.
 */
type Slot = { offset: number; length: number }

const slotStruct = Struct.record<Slot>({
  offset: [0, Struct.uint32],
  length: [4, Struct.uint32],
})

export type VariableLengthRecordPageAllocInfo = {
  freeSpace: number
  slot: Slot
  slotIndex: number
}

export class VariableLengthRecordPage {
  constructor(private view: DataView) {}

  static allocator: PageSpaceAllocator<VariableLengthRecordPageAllocInfo> = {
    allocateSpaceInPage: (pageView: DataView, numBytes: number) => {
      const recordPage = new VariableLengthRecordPage(pageView)
      const { slot, slotIndex } = recordPage.allocateSlot(numBytes)
      return {
        freeSpace: recordPage.freeSpace,
        slot,
        slotIndex,
      }
    },
  }

  /**
   * The number of slots in the page.
   */
  get slotCount(): number {
    return this.view.getUint32(this.view.byteLength - 4)
  }

  private set slotCount(value: number) {
    this.view.setUint32(this.view.byteLength - 4, value)
  }

  /**
   * The offset of the free space in the page.
   *
   * Note that this is the offset to use if and only if there is no space
   * in a previously freed slot.
   */
  get freeSpaceOffset(): number {
    return this.view.getUint32(this.view.byteLength - 8)
  }

  private set freeSpaceOffset(value: number) {
    this.view.setUint32(this.view.byteLength - 8, value)
  }

  private getSlotView(slotIndex: number): DataView {
    const slotOffset = this.view.byteLength - 8 - 8 * (slotIndex + 1)
    return new DataView(this.view.buffer, this.view.byteOffset + slotOffset, 8)
  }

  /**
   * Retrieves the slot at the given index.
   */
  getSlotEntry(slotIndex: number): Slot {
    return slotStruct.readAt(this.getSlotView(slotIndex), 0)
  }
  private setSlotEntry(slotIndex: number, slot: Slot): void {
    slotStruct.writeAt(slot, this.getSlotView(slotIndex), 0)
  }

  /**
   * Marks a slot as free for future allocations
   */
  freeSlot(slotIndex: number): void {
    debugLog(() => `freeSlot(${slotIndex}) ${debugJson(this.dumpSlots(), 2)}`)
    if (slotIndex >= this.slotCount) {
      debugLog(
        `  -> freeSlot(${slotIndex}) slot index out of bounds, already deleted?`,
      )
      return // already deleted
    }
    this.setSlotEntry(slotIndex, { offset: 0, length: 0 })

    this.freeSpaceOffset = this.iterSlots().reduce((acc, [slot]) => {
      if (slot.length === 0) return acc
      return Math.max(acc, slot.offset + slot.length)
    }, 0)

    if (slotIndex === this.slotCount - 1) {
      debugLog(`  -> freeSlot(${slotIndex}) last slot, reducing slot count`)
      // this is the last slot, lets try to reduce the slot count
      let i = this.slotCount - 1
      for (; i >= 0; i--) {
        if (this.getSlotEntry(i).length > 0) break
      }
      this.slotCount = i + 1
    }
    debugLog(
      () =>
        `  -> freeSlot(${slotIndex}) done ${debugJson(this.dumpSlots(), 2)}`,
    )
  }

  *iterSlots(): Generator<[Slot, number]> {
    for (let i = 0; i < this.slotCount; i++) {
      yield [this.getSlotEntry(i), i]
    }
  }

  getFreeSlots() {
    return this.iterSlots().filter(([slot]) => slot.length === 0).map(([, i]) =>
      i
    )
  }

  *getFreeBlocks() {
    const slots = this.iterSlots()
      .filter(([slot]) => slot.length > 0).map(([slot]) => ({
        start: slot.offset,
        end: slot.offset + slot.length,
      })).toArray().sort((a, b) => a.start - b.start)

    if (slots.length === 0) return // we're empty
    let slot = slots[0]
    if (slot.start > 0) {
      yield { offset: 0, length: slot.start }
    }
    for (let i = 1; i < slots.length; i++) {
      const nextSlot = slots[i]
      if (slot.end < nextSlot.start) {
        yield { offset: slot.end, length: nextSlot.start - slot.end }
      }
      slot = nextSlot
    }
  }

  dumpSlots(): string[] {
    return Array.from(
      this.iterSlots(),
      ([slot, i]) => `${i}: offset=${slot.offset} length=${slot.length}`,
    )
  }

  /**
   * Allocate space for `numBytes` bytes in the page and return the slot.
   * @param numBytes that are needed
   * @returns a slot with _at least_ `numBytes` of space, though may be bigger
   * if it reuses an existing slot.
   */
  allocateSlot(numBytes: number): { slot: Slot; slotIndex: number } {
    debugLog(
      () => `allocateSlot(${numBytes}): ${debugJson(this.dumpSlots(), 2)}`,
    )
    if (this.freeSpace < numBytes) {
      throw new Error("Not enough free space")
    }

    const firstFreeSlotIndex = this.getFreeSlots().next().value

    const firstFreeBlock =
      this.getFreeBlocks().filter((block) => block.length >= numBytes).next()
        .value
    let slot: Slot
    if (firstFreeBlock == null) {
      slot = { offset: this.freeSpaceOffset, length: numBytes }
      this.freeSpaceOffset += numBytes
    } else {
      slot = { offset: firstFreeBlock.offset, length: numBytes }
    }

    if (firstFreeSlotIndex == null) {
      this.setSlotEntry(this.slotCount, slot)
      this.slotCount++
      return { slot, slotIndex: this.slotCount - 1 }
    }
    this.setSlotEntry(firstFreeSlotIndex, slot)
    return { slot, slotIndex: firstFreeSlotIndex }
  }

  /**
   * The number of bytes of free space in the page.
   *
   * This takes into account the fact that an allocation _might_ need to
   * use an additional 8 bytes for the slot in the footer.
   */
  get freeSpace(): number {
    const maxFreeBlock = this.getFreeBlocks().reduce(
      (acc, block) => Math.max(acc, block.length),
      0,
    )
    const firstFreeSlotIndex = this.getFreeSlots().next().value
    const slotOverhead = firstFreeSlotIndex == null ? 8 : 0
    const finalFreeSpace = this.view.byteLength - this.footerSize -
      this.freeSpaceOffset
    return Math.max(Math.max(maxFreeBlock, finalFreeSpace) - slotOverhead, 0)
  }

  get footerSize(): number {
    return 8 + slotStruct.size * this.slotCount
  }

  get nextSlotSpace(): DataView {
    return new DataView(
      this.view.buffer,
      this.view.byteOffset + this.view.byteLength - this.footerSize -
        slotStruct.size,
      slotStruct.size,
    )
  }
}
