export const debugLogger = (cond: boolean) =>
  cond
    // deno-lint-ignore no-explicit-any
    ? (...args: any[]) => {
      if (typeof args[0] === "function") {
        console.log(args[0]())
      } else {
        console.log(...args)
      }
    }
    : () => {}

export const debugLog = debugLogger(
  globalThis.Deno?.env.get("DEBUG") === "true",
)

export function debugJson(obj: unknown, indent = 0): string {
  return JSON.stringify(obj, (_key: string, value: unknown) => {
    if (typeof value === "bigint") {
      return value.toString() + "n"
    }
    return value
  }, indent)
}
