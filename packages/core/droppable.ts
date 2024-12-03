import { Promisable } from "npm:type-fest"

/**
 * A droppable object can be dropped, which is a one-time operation that
 * releases any resources held by the object. After an object is dropped, it
 * cannot be used again.
 */
export interface IDroppable {
  drop(): Promisable<void>
}

export class Droppable implements IDroppable {
  private dropped = false

  constructor(private onDrop: () => Promisable<void>) {}

  drop(): Promisable<void> {
    if (this.dropped) return
    this.dropped = true
    return this.onDrop()
  }

  assertNotDropped(msg: string) {
    if (this.dropped) throw new Error(msg)
  }
}
