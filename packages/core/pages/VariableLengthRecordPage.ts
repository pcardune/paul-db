import { FixedWidthStruct } from "../binary/Struct.ts"
import { debugLog } from "../logging.ts"
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

const slotStruct = new FixedWidthStruct<Slot>({
  size: 8,
  write(value, view) {
    view.setUint32(0, value.offset)
    view.setUint32(4, value.length)
  },
  read(view) {
    return {
      offset: view.getUint32(0),
      length: view.getUint32(4),
    }
  },
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
    if (slotIndex >= this.slotCount) {
      return // already deleted
    }
    this.setSlotEntry(slotIndex, { offset: 0, length: 0 })
    if (slotIndex === this.slotCount - 1) {
      // this is the last slot, lets try to reduce the slot count
      let i = this.slotCount - 1
      for (; i >= 0; i--) {
        if (this.getSlotEntry(i).length > 0) break
      }
      this.slotCount = i + 1
      if (i === -1) {
        // all slots are free, reset the free space offset
        this.freeSpaceOffset = 0
      }
    }

    debugLog(`freeSlot(${slotIndex})`)
  }

  /**
   * Allocate space for `numBytes` bytes in the page and return the slot.
   * @param numBytes that are needed
   * @returns a slot with _at least_ `numBytes` of space, though may be bigger
   * if it reuses an existing slot.
   */
  allocateSlot(numBytes: number): { slot: Slot; slotIndex: number } {
    debugLog(
      `allocateSlot(${numBytes}):`,
      Array.from(
        Array(this.slotCount),
        (_, i) => JSON.stringify(this.getSlotEntry(i)),
      ),
    )
    if (this.freeSpace < numBytes) {
      throw new Error("Not enough free space")
    }

    // try to reuse an existing slot first
    let offset = 0
    for (let i = 0; i < this.slotCount; i++) {
      const existingSlot = this.getSlotEntry(i)
      if (existingSlot.length > 0) {
        offset = existingSlot.offset + existingSlot.length
        continue
      }
      // well, this slot is free. Let's find out how much space there is
      // until the next used slot
      let freeSpace = 0
      let j = i + 1
      for (; j < this.slotCount; j++) {
        if (this.getSlotEntry(j).length > 0) break
      }
      if (j < this.slotCount) {
        // we found a used slot before we reached the end of all the slots
        freeSpace = this.getSlotEntry(j).offset - offset
        if (freeSpace >= numBytes) {
          const slot = { offset, length: numBytes }
          this.setSlotEntry(i, slot)
          debugLog(
            `  -> reusing slot ${i} at offset ${offset} and length ${numBytes}`,
          )
          return { slot, slotIndex: i }
        } else {
          // not enough free space here, keep looking, starting
          // from the next slot
        }
      } else {
        // all the remaining slots are free. Let's use this one.
        const slot = { offset, length: numBytes }
        this.setSlotEntry(i, slot)
        this.freeSpaceOffset = offset + numBytes
        debugLog(`  -> reusing last slot ${i}`)
        return { slot, slotIndex: i }
      }
    }

    const slot = { offset: this.freeSpaceOffset, length: numBytes }
    slotStruct.writeAt(
      slot,
      this.nextSlotSpace,
      0,
    )
    this.slotCount++
    this.freeSpaceOffset += numBytes
    debugLog(`  -> allocated new slot ${this.slotCount - 1}`)
    return { slot, slotIndex: this.slotCount - 1 }
  }

  /**
   * The number of bytes of free space in the page.
   *
   * This takes into account the fact that an allocation _might_ need to
   * use an additional 8 bytes for the slot in the footer.
   */
  get freeSpace(): number {
    // Find the largest contiguous block of free space in the page.
    // let maxFreeSpace = 0
    // let offset = 0
    // for (let i = 0; i < this.slotCount; i++) {
    //   const slot = this.getSlotEntry(i)
    //   if (slot.length > 0) {
    //     offset = slot.offset + slot.length
    //     continue
    //   }

    //   let j = i + 1
    //   for (; j < this.slotCount; j++) {
    //     if (this.getSlotEntry(j).length > 0) break
    //   }
    //   if (j < this.slotCount) {
    //     const nextSlot = this.getSlotEntry(j)
    //     maxFreeSpace = Math.max(nextSlot.offset - offset, maxFreeSpace)
    //   } else {
    //     maxFreeSpace = Math.max(
    //       maxFreeSpace,
    //       this.view.byteLength - this.footerSize - offset,
    //     )
    //   }
    // }
    // return maxFreeSpace

    const freeSpace = this.view.byteLength - this.footerSize -
      this.freeSpaceOffset
    for (let i = 0; i < this.slotCount; i++) {
      if (this.getSlotEntry(i).length === 0) {
        return Math.max(freeSpace, 0)
      }
    }

    return Math.max(0, freeSpace - 8)
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
