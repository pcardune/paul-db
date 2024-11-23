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

  it("Can delete a slot", () => {
    page.allocateSlot(10)
    page.allocateSlot(5)
    page.allocateSlot(4)
    expect(page.slotCount).toBe(3)
    expect(page.freeSpaceOffset).toBe(19)
    expect(page.freeSpace).toBe(21)

    page.freeSlot(1)
    expect(page.slotCount).toBe(3)
    expect(page.freeSpaceOffset).toBe(19)
    expect(page.freeSpace).toBe(21)

    // if we allocate again, it should reuse the slot (but not the space)
    const priorFreeSpace = page.freeSpace
    page.allocateSlot(10)
    expect(page.freeSpace).toBe(priorFreeSpace - 10)
  })

  it.skip("Can compact the page", () => {
    // to be written
  })
})
