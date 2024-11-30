import { Handlers, PageProps } from "$fresh/server.ts"
import { State } from "./_middleware.ts"

type PageData = {
  todos: { id: string; title: string }[]
}

export const handler: Handlers<PageData, State> = {
  async GET(_req, ctx) {
    const todos = await ctx.state.context.model.todos.iterate().toArray()

    return await ctx.render({
      todos,
    })
  },
  async POST(req, ctx) {
    const form = await req.formData()
    const action = form.get("action")?.toString()
    if (action === "add") {
      const title = form.get("title")?.toString()

      await ctx.state.context.model.todos.insert({
        title: title!,
      })
    } else if (action === "delete") {
      const id = form.get("id")?.toString()
      await ctx.state.context.model.todos.removeWhere("id", id!)
    }

    // Redirect user to thank you page.
    const headers = new Headers()
    headers.set("location", "/")
    return new Response(null, {
      status: 303, // See Other
      headers,
    })
  },
}

export default function Home({ data }: PageProps<PageData>) {
  return (
    <div class="px-4 py-8 mx-auto">
      <div class="max-w-screen-md mx-auto flex flex-col items-center justify-center">
        <div class="logo-wrap">
          <img
            class="my-6 -hue-rotate-15 brightness-75 logo"
            src="/logo.svg"
            width="128"
            height="128"
            alt="the Fresh logo: a sliced lemon dripping with juice"
          />
        </div>
        <h1 class="text-4xl font-bold mb-6">Paul's To-Dos</h1>
        <form
          method="post"
          class="p-4 rounded-lg w-1/2 bg-zinc-800 mb-4 flex items-center gap-4"
        >
          <input type="hidden" name="action" value="add" />
          <input
            class="grow rounded opacity-50 hover:opacity-70 focus:opacity-100 bg-zinc-600 px-2 py-1 placeholder:text-zinc-300 focus:outline-none ring-inset focus:ring"
            type="text"
            name="title"
            value=""
            placeholder="Enter a new todo"
          />
          <button
            class="bg-zinc-600 rounded px-3 py-1 font-bold opacity-50 hover:opacity-100 focus:opacity-100 focus:outline-none ring-inset focus:ring"
            type="submit"
          >
            Add Todo
          </button>
        </form>
        <ol class="px-4 py-1 rounded-lg bg-zinc-800 w-1/2">
          {data.todos.map((todo) => (
            <li class="border-b last:border-0 border-zinc-600 py-4 px-2 flex justify items-center">
              <span>
                {todo.title}
              </span>
              <form method="post" class="ml-auto">
                <input type="hidden" name="action" value="delete" />
                <input type="hidden" name="id" value={todo.id} />
                <button
                  class="bg-zinc-600 rounded px-3 py-1 font-bold opacity-50 hover:opacity-100 focus:opacity-100 focus:outline-none ring-inset focus:ring"
                  type="submit"
                >
                  Delete
                </button>
              </form>
            </li>
          ))}
        </ol>
      </div>
    </div>
  )
}
