export const debugLog = Deno.env.get("DEBUG") === "true"
  ? console.log
  : () => {}
