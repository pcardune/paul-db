/**
 * A struct is a fixed-width binary data structure that can be read from
 * and written to using a DataView.
 */
export type Struct<V> = {
  /**
   * The size of the struct in bytes.
   */
  readonly size: number

  /**
   * Writes a value to a DataView.
   */
  readonly write: (value: V, view: DataView) => void

  /**
   * Reads a value from a DataView.
   */
  readonly read: (view: DataView) => Readonly<V>
}
