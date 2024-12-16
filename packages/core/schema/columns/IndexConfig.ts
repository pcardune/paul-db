/**
 * Type definitions for index configuration
 * @module
 */

/**
 * Represents the configuration for a column that should be indexed.
 */
export type ShouldIndex = {
  shouldIndex: true
  order?: number
  inMemory?: boolean
}

/**
 * Represents the configuration for a column that should not be indexed.
 */
export type ShouldNotIndex = {
  shouldIndex: false
}

/**
 * Represents the index configuration for a column.
 */
export type Config = ShouldIndex | ShouldNotIndex
