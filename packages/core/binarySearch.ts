/**
 * Find index in sorted array
 *
 * @param arr The sorted array
 * @param value The value to find
 * @param compare A function that compares the value with an element in the array
 * @returns
 */
export function binarySearch<T, V>(
  arr: readonly T[],
  value: V,
  compare: (a: V, b: T) => number,
): number {
  let low = 0
  let high = arr.length - 1
  while (low <= high) {
    const mid = Math.floor((low + high) / 2)
    const cmp = compare(value, arr[mid])
    if (cmp === 0) {
      return mid
    }
    if (cmp < 0) {
      high = mid - 1
    } else {
      low = mid + 1
    }
  }
  return low
}
