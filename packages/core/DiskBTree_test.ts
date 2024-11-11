import { expect } from "jsr:@std/expect"
import { BTree } from "./DiskBTree.ts"

Deno.test("BTrees", () => {
  const btree = new BTree<number, { name: string }>((a, b) => a - b)

  expect(btree.has(1)).toBe(false)
  expect(btree.get(1)).toEqual([])

  btree.insert(1, { name: "Paul" })
  expect(btree.has(1)).toBe(true)
  expect(btree.get(1)).toEqual([{ name: "Paul" }])

  btree.insert(1, { name: "Meghan" })
  expect(btree.get(1)).toEqual([{ name: "Paul" }, { name: "Meghan" }])

  btree.insert(2, { name: "Mr. Blue" })
  expect(btree.get(2)).toEqual([{ name: "Mr. Blue" }])

  btree.removeAll(1)
  expect(btree.get(1)).toEqual([])
})

Deno.test("lots of nodes inserted in ascending order", () => {
  const btree = new BTree<number, string>((a, b) => a - b, { maxKeys: 3 })

  for (let i = 0; i < 40; i++) {
    btree.insert(i, `Person ${i}`)
    for (let j = 0; j <= i; j++) {
      expect({ i, j, values: btree.get(j) }).toEqual({
        i,
        j,
        values: [`Person ${j}`],
      })
    }
  }
})

// Deno.test("lots of nodes inserted in descending order", () => {
//   const btree = new BTree<number, string>((a, b) => a - b, { maxKeys: 3 })

//   const inserted: number[] = []
//   for (let i = 10; i >= 0; i--) {
//     inserted.push(i)
//     btree.insert(i, `Person ${i}`)
//     for (const j of inserted) {
//       expect({ i, j, values: btree.get(j) }).toEqual({
//         i,
//         j,
//         values: [`Person ${j}`],
//       })
//     }
//   }
// })
