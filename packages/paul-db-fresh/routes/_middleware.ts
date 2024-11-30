import { FreshContext } from "$fresh/server.ts"
import { PaulDB } from "@paul-db/core"
import { getModel, Model } from "../model/mod.ts"

export interface State {
  context: Context
}

export class Context {
  private static context: Context

  public constructor(readonly db: PaulDB, readonly model: Model) {
  }

  public static async init() {
    const db = await PaulDB.open("data", { create: true })
    const model = await getModel(db)
    Context.context = new Context(db, model)
  }

  public static instance() {
    if (this.context) return this.context
    else throw new Error("Context is not initialized!")
  }
}

export async function handler(
  _req: Request,
  ctx: FreshContext<State>,
) {
  ctx.state.context = Context.instance()
  if (ctx.destination === "route") {
    // console.log("i'm logged during a request!")
    // console.log(ctx.state.context)
  }
  const resp = await ctx.next()
  return resp
}
