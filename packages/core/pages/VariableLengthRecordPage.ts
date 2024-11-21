import { Struct } from "../binary/Struct.ts"

type Slot = { offset: number; length: number }

const slotStruct: Struct<Slot> = {
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
}

export class VariableLengthRecordPage {
  constructor(private view: DataView) {}

  get slotCount(): number {
    return this.view.getUint32(this.view.byteLength - 4)
  }

  set slotCount(value: number) {
    this.view.setUint32(this.view.byteLength - 4, value)
  }

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

  getSlotEntry(slotIndex: number): Slot {
    return slotStruct.read(this.getSlotView(slotIndex))
  }
  private setSlotEntry(slotIndex: number, slot: Slot): void {
    slotStruct.write(slot, this.getSlotView(slotIndex))
  }

  freeSlot(slotIndex: number): void {
    if (slotIndex >= this.slotCount) {
      throw new Error("Slot index out of bounds")
    }
    this.setSlotEntry(slotIndex, { offset: 0, length: 0 })
  }

  allocateSlot(length: number): Slot {
    if (this.freeSpace < length) {
      throw new Error("Not enough free space")
    }

    const slot = { offset: this.freeSpaceOffset, length }

    // try to reuse an existing slot first
    for (let i = 0; i < this.slotCount; i++) {
      const existingSlot = this.getSlotEntry(i)
      if (existingSlot.length === 0) {
        this.setSlotEntry(i, slot)
        this.freeSpaceOffset += length
        return slot
      }
    }

    slotStruct.write(
      slot,
      this.nextSlotSpace,
    )

    this.slotCount++
    this.freeSpaceOffset += length
    return slot
  }

  /**
   * The number of bytes of free space in the page.
   *
   * This takes into account the fact that an allocation _might_ need to
   * use an additional 8 bytes for the slot in the footer.
   */
  get freeSpace(): number {
    return this.view.byteLength - this.footerSize -
      this.freeSpaceOffset -
      slotStruct.size
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
