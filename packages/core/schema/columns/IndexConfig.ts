export type ShouldIndex = {
  shouldIndex: true
  order?: number
  inMemory?: boolean
}

export type ShouldNotIndex = {
  shouldIndex: false
}

export type Config = ShouldIndex | ShouldNotIndex
