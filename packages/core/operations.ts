export type WriteOperation = {
  type: "insert"
  value: string
  key: string
} | {
  type: "delete"
  key: string
} | {
  type: "update"
  key: string
  newValue: string
}

export type ReadOperation = {
  type: "get"
  key: string
}
