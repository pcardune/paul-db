import { beforeEach, describe, it } from "@std/testing/bdd"
import { WriteableVariableLengthRecordPage } from "./VariableLengthRecordPage.ts"
import { expect } from "@std/expect"
import { randomIntegerBetween, randomSeeded } from "@std/random"
import { WriteableDataView } from "../binary/dataview.ts"

describe("VariableLengthRecordPage", () => {
  let page: WriteableVariableLengthRecordPage
  let buffer: ArrayBuffer
  const pageSize = 4 * 20

  function assertWellFormedPage(buffer: ArrayBuffer) {
    const page = new WriteableVariableLengthRecordPage(
      new WriteableDataView(buffer),
    )
    expect(page.freeSpaceOffset).toBeGreaterThanOrEqual(0)
    expect(page.freeSpaceOffset).toBeLessThanOrEqual(buffer.byteLength)
    expect(page.slotCount).toBeGreaterThanOrEqual(0)
    expect(page.freeSpace).toBeGreaterThanOrEqual(0)
    expect(page.footerSize).toBeGreaterThanOrEqual(0)
    expect(page.footerSize).toBeLessThanOrEqual(buffer.byteLength)

    // Now lets collect all of the non-free slots
    const slots = Array.from(
      Array(page.slotCount),
      (_, i) => page.getSlotEntry(i),
    )
    const nonEmptySlots = slots.filter((slot) => slot.length > 0).sort((a, b) =>
      a.offset - b.offset
    )

    // check that none of the slots are overlapping
    for (let i = 0; i < nonEmptySlots.length - 1; i++) {
      expect(nonEmptySlots[i].offset + nonEmptySlots[i].length)
        .toBeLessThanOrEqual(
          nonEmptySlots[i + 1].offset,
        )
    }
  }

  beforeEach(() => {
    buffer = new ArrayBuffer(pageSize)
    page = new WriteableVariableLengthRecordPage(new WriteableDataView(buffer))
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

    assertWellFormedPage(buffer)
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
    assertWellFormedPage(buffer)
  })

  it("throws an error if there's not enough space", () => {
    expect(() => page.allocateSlot(500)).toThrow("Not enough free space")
    page.allocateSlot(30)
    expect(page.freeSpace).toBe(26)
    expect(() => page.allocateSlot(30)).toThrow("Not enough free space")

    assertWellFormedPage(buffer)
  })

  function makeSlots() {
    return [
      page.allocateSlot(10),
      page.allocateSlot(5),
      page.allocateSlot(4),
    ] as const
  }

  it("Can free the first slot", () => {
    makeSlots()
    page.freeSlot(0)
    expect(Array.from(page.getFreeBlocks())).toHaveLength(1)
    expect(Array.from(page.getFreeBlocks())[0]).toEqual({
      offset: 0,
      length: 10,
    })
    expect(Array.from(page.getFreeSlots())).toEqual([0])
    expect(page.freeSpaceOffset).toBe(19) // 10 + 5 + 4 // the size of the allocated slots
    expect(page.slotCount).toBe(3)
    expect(page.footerSize).toBe(32) // 4 + 4 + 3 * 8 - 4 bytes for slot count, 4 bytes for free space offset, 8 bytes for each slot * 3 slots
    expect(page.freeSpace).toBe(80 - 32 - 19) // 29 bytes of free space
  })

  it("Can free the middle slot", () => {
    makeSlots()
    page.freeSlot(1)
    expect(Array.from(page.getFreeBlocks())).toHaveLength(1)
    expect(Array.from(page.getFreeBlocks())[0]).toEqual({
      offset: 10,
      length: 5,
    })
    expect(Array.from(page.getFreeSlots())).toEqual([1])
    expect(page.freeSpaceOffset).toBe(19) // 10 + 5 + 4 // the size of the allocated slots
    expect(page.slotCount).toBe(3)
    expect(page.footerSize).toBe(32) // 4 + 4 + 3 * 8 - 4 bytes for slot count, 4 bytes for free space offset, 8 bytes for each slot * 3 slots
    expect(page.freeSpace).toBe(80 - 32 - 19) // 29 bytes of free space
  })

  it("Can free the last slot", () => {
    makeSlots()
    page.freeSlot(2)
    expect(Array.from(page.getFreeBlocks())).toEqual([])
    expect(Array.from(page.getFreeSlots())).toEqual([])
    expect(page.freeSpaceOffset).toBe(15) // 10 + 5 + 4 // the size of the allocated slots
    expect(page.slotCount).toBe(2)
    expect(page.footerSize).toBe(4 + 4 + 2 * 8) // 24 // 4 bytes for slot count, 4 bytes for free space offset, 8 bytes for each slot * 3 slots
    expect(page.freeSpace).toBe(80 - 24 - 15 - 8) // 41 bytes of free space
  })

  it("Will reuse a freed slot if the new allocation fits in it", () => {
    const [firstSlot] = makeSlots()
    page.freeSlot(firstSlot.slotIndex)
    expect(Array.from(page.getFreeBlocks())).toHaveLength(1)

    expect(page.getSlotEntry(firstSlot.slotIndex).length).toBe(0)
    const { slotIndex: reusedSlotIndex, slot: reusedSlot } = page.allocateSlot(
      firstSlot.slot.length - 2,
    )
    expect(reusedSlotIndex).toBe(firstSlot.slotIndex)
    expect(reusedSlot.offset).toBe(firstSlot.slot.offset)
    expect(reusedSlot.length).toBe(firstSlot.slot.length - 2)

    assertWellFormedPage(buffer)
  })

  it("Will allocate a new slot if the freed one is too small", () => {
    page.allocateSlot(10)
    page.allocateSlot(5)
    page.allocateSlot(4)

    page.freeSlot(0)

    const newSlot = page.allocateSlot(11)
    expect(newSlot.slotIndex).toBe(0)
    expect(newSlot.slot.offset).not.toBe(0)
    expect(newSlot.slot.length).toBe(11)

    assertWellFormedPage(buffer)
  })

  it("Will lower the slot count if you free the last slot", () => {
    const slots = makeSlots()
    const lastSlot = slots.at(-1)!
    const secondToLastSlot = slots.at(-2)!
    const startingSlotCount = page.slotCount
    page.freeSlot(secondToLastSlot.slotIndex)
    expect(page.slotCount).toBe(startingSlotCount)
    page.freeSlot(lastSlot.slotIndex)
    expect(page.slotCount).toBe(startingSlotCount - 2) // cleans up both slots

    // const newSlot = page.allocateSlot(lastSlot.slot.length + 1)
    // expect(newSlot.slotIndex).toBe(lastSlot.slotIndex)
    // expect(newSlot.slot.offset).toBe(
    //   secondToLastSlot.slot.offset + secondToLastSlot.slot.length,
    // )
    // expect(newSlot.slot.length).toBe(lastSlot.slot.length + 1)

    assertWellFormedPage(buffer)
  })

  it("Can do a lot of random allocations and frees and maintain structural integrity", () => {
    const prng = randomSeeded(0n)
    const buffer = new ArrayBuffer(4092)
    const page = new WriteableVariableLengthRecordPage(
      new WriteableDataView(buffer),
    )
    for (let i = 0; i < 1000; i++) {
      const action = randomIntegerBetween(0, 1, { prng })
      if (action === 0 && page.freeSpace > 0) {
        // allocate!
        const size = randomIntegerBetween(1, page.freeSpace, { prng })
        page.allocateSlot(size)
      } else if (page.slotCount > 0) {
        // free!
        const freeableSlots = Array.from(
          Array(page.slotCount),
          (_, i) => ({ slotIndex: i, slot: page.getSlotEntry(i) }),
        ).filter(({ slot }) => slot.length > 0)
        const slotIndex = freeableSlots[
          randomIntegerBetween(0, freeableSlots.length - 1, { prng })
        ].slotIndex
        page.freeSlot(slotIndex)
      }
      assertWellFormedPage(buffer)
    }
  })
})
