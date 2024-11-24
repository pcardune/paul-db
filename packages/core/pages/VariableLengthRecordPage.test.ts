import { beforeEach, describe, it } from "jsr:@std/testing/bdd"
import { VariableLengthRecordPage } from "./VariableLengthRecordPage.ts"
import { expect } from "jsr:@std/expect"

describe("VariableLengthRecordPage", () => {
  let page: VariableLengthRecordPage
  let buffer: ArrayBuffer
  const pageSize = 4 * 20

  beforeEach(() => {
    buffer = new Uint32Array(pageSize / 4).buffer
    page = new VariableLengthRecordPage(new DataView(buffer))
  })

  it("Starts out empty", () => {
    expect(page.slotCount).toBe(0)
    expect(page.freeSpaceOffset).toBe(0)

    // footer size is 8 bytes for the slot count and free space offset
    expect(page.footerSize).toBe(8)
    // free space is the entire page minus the footer, and the room it would
    // take to add a new slot
    expect(page.freeSpace).toBe(pageSize - page.footerSize - 8)
  })

  it("Can allocate a slot", () => {
    const initialFreeSpace = page.freeSpace
    const { slot, slotIndex } = page.allocateSlot(10)
    expect(slotIndex).toBe(0)
    expect(page.slotCount).toBe(1)
    expect(page.freeSpaceOffset).toBe(10)
    expect(page.getSlotEntry(0)).toEqual(slot)
    expect(slot.length).toBe(10)
    expect(slot.offset).toBe(0)
    // each allocation takes up an additional 8 bytes in the footer
    expect(page.footerSize).toBe(8 + 8)
    expect(page.freeSpace).toBe(initialFreeSpace - 10 - 8)
  })

  it("Can allocate multiple slots", () => {
    const slot1 = page.allocateSlot(10)
    const slot2 = page.allocateSlot(20)
    expect(page.slotCount).toBe(2)
    expect(page.freeSpaceOffset).toBe(30)
    expect(slot1.slotIndex).toBe(0)
    expect(slot2.slotIndex).toBe(1)
    expect(page.getSlotEntry(slot1.slotIndex)).toEqual(slot1.slot)
    expect(page.getSlotEntry(slot2.slotIndex)).toEqual(slot2.slot)
  })

  it("throws an error if there's not enough space", () => {
    expect(() => page.allocateSlot(500)).toThrow("Not enough free space")
    page.allocateSlot(30)
    expect(page.freeSpace).toBe(26)
    expect(() => page.allocateSlot(30)).toThrow("Not enough free space")
  })

  function makeSlots() {
    return [
      page.allocateSlot(10),
      page.allocateSlot(5),
      page.allocateSlot(4),
    ] as const
  }

  it("Can free a slot", () => {
    const slots = makeSlots()
    expect(page.slotCount).toBe(3)
    expect(page.freeSpaceOffset).toBe(19)
    expect(page.freeSpace).toBe(21)

    const freedSlotIndex = slots[1].slotIndex
    page.freeSlot(freedSlotIndex)
    expect(page.slotCount).toBe(3)
    expect(page.freeSpaceOffset).toBe(19)
    expect(page.freeSpace).toBe(21)
    // a freed slot is marked as having length and offset of 0
    expect(page.getSlotEntry(freedSlotIndex).length).toBe(0)
    expect(page.getSlotEntry(freedSlotIndex).offset).toBe(0)
  })

  it("Will reuse a freed slot if the new allocation fits in it", () => {
    const [firstSlot] = makeSlots()
    page.freeSlot(firstSlot.slotIndex)

    expect(page.getSlotEntry(firstSlot.slotIndex).length).toBe(0)
    const { slotIndex: reusedSlotIndex, slot: reusedSlot } = page.allocateSlot(
      firstSlot.slot.length - 2,
    )
    expect(reusedSlotIndex).toBe(firstSlot.slotIndex)
    expect(reusedSlot.offset).toBe(firstSlot.slot.offset)
    expect(reusedSlot.length).toBe(firstSlot.slot.length - 2)
  })

  it("Will allocate a new slot if the freed one is too small", () => {
    const [firstSlot] = makeSlots()
    page.freeSlot(firstSlot.slotIndex)

    const numSlotsAfterFirstFree = page.slotCount
    const newSlot = page.allocateSlot(firstSlot.slot.length + 1)
    expect(newSlot.slotIndex).not.toBe(firstSlot.slotIndex)
    expect(newSlot.slotIndex).toBe(numSlotsAfterFirstFree)
    expect(newSlot.slot.offset).not.toBe(firstSlot.slot.length)
    expect(newSlot.slot.length).toBe(firstSlot.slot.length + 1)
  })

  it("Will reuse a slot if it's the last one", () => {
    const slots = makeSlots()
    const lastSlot = slots.at(-1)!
    const secondToLastSlot = slots.at(-2)!
    page.freeSlot(lastSlot.slotIndex)
    const newSlot = page.allocateSlot(lastSlot.slot.length + 1)
    expect(newSlot.slotIndex).toBe(lastSlot.slotIndex)
    expect(newSlot.slot.offset).toBe(
      secondToLastSlot.slot.offset + secondToLastSlot.slot.length,
    )
    expect(newSlot.slot.length).toBe(lastSlot.slot.length + 1)
  })

  it.skip("Can compact the page", () => {
    // to be written
  })
})
