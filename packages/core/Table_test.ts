import { describe, it } from "jsr:@std/testing/bdd"
import { expect } from "jsr:@std/expect"
import { Table } from "./table.ts"

describe("Table", () => {
  it("should insert and get records", () => {
    type Person = {
      name: string
      age: number
    }
    const people = Table.create<Person, Person & { lowerCaseName: string }>({
      name: {
        getValue: (r) => r.name,
      },
      age: {
        getValue: (r) => r.age,
      },
      lowerCaseName: {
        getValue: (r: Person) => r.name.toLowerCase(),
      },
    })

    people.insert({ name: "Alice", age: 12 })
    people.insert({ name: "Bob", age: 12 })
    people.insert({ name: "Charlie", age: 15 })

    expect(people.findMany("age", 12)).toEqual([
      { name: "Alice", age: 12 },
      { name: "Bob", age: 12 },
    ])
    expect(people.findMany("age", 15)).toEqual([{ name: "Charlie", age: 15 }])

    expect(people.findMany("lowerCaseName", "alice")).toEqual([
      { name: "Alice", age: 12 },
    ])
  })
})
