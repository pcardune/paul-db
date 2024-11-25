export const debugLog = Deno.env.get("DEBUG") === "true"
  // deno-lint-ignore no-explicit-any
  ? (...args: any[]) => {
    if (typeof args[0] === "function") {
      console.log(args[0]())
    } else {
      console.log(...args)
    }
  }
  : () => {}
