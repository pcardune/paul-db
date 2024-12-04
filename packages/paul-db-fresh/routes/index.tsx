import { Handlers, PageProps } from "$fresh/server.ts"
import { State } from "./_middleware.ts"
import { CheckIcon, TrashIcon } from "../components/icons.tsx"

type PageData = {
  todos: {
    id: string
    title: string
    createdAt: Date
    completedAt: Date | null
  }[]
}

export const handler: Handlers<PageData, State> = {
  async GET(_req, ctx) {
    const todos = await ctx.state.context.model.todos.iterate().toArray()
    todos.sort((a, b) => {
      if (a.completedAt && !b.completedAt) return 1
      if (!a.completedAt && b.completedAt) return -1
      if (a.completedAt && b.completedAt) {
        return b.completedAt.getTime() - a.completedAt.getTime()
      }
      return a.createdAt.getTime() - b.createdAt.getTime()
    })
    return await ctx.render({
      todos,
    })
  },
  async POST(req, ctx) {
    const form = await req.formData()
    const action = form.get("action")?.toString()
    const todos = ctx.state.context.model.todos

    const id = form.get("id")?.toString()
    if (action === "add") {
      const title = form.get("title")?.toString()
      await todos.insert({
        title: title!,
      })
    } else if (action === "delete") {
      await todos.removeWhere("id", id!)
    } else if (action === "complete") {
      await todos.updateWhere("id", id!, { completedAt: new Date() })
    } else if (action === "uncomplete") {
      await todos.updateWhere("id", id!, { completedAt: null })
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
          class="p-6 rounded-lg w-1/2 bg-zinc-800 mb-4 flex items-center gap-4"
        >
          <input
            autoFocus
            class="grow rounded opacity-50 hover:opacity-70 focus:opacity-100 bg-zinc-600 px-2 py-1 placeholder:text-zinc-300 focus:outline-none ring-inset focus:ring"
            type="text"
            name="title"
            value=""
            placeholder="Enter a new todo"
          />
          <button
            class="bg-zinc-600 rounded px-3 py-1 font-bold opacity-50 hover:opacity-100 focus:opacity-100 focus:outline-none ring-inset focus:ring"
            name="action"
            value="add"
            type="submit"
          >
            Add Todo
          </button>
        </form>
        {data.todos.length > 0 && (
          <ol class="px-4 py-1 rounded-lg bg-zinc-800 w-1/2 divide-y-2 divide-zinc-600">
            {data.todos.map((todo) => (
              <li class="border-zinc-600 py-4 px-2">
                <form
                  method="post"
                  class="flex justify-between items-center gap-2"
                >
                  <input type="hidden" name="id" value={todo.id} />
                  {todo.completedAt
                    ? (
                      <button
                        name="action"
                        value="uncomplete"
                        class="ring-zinc-600 w-6 border-zinc-600 rounded font-bold opacity-50 hover:opacity-100 focus:opacity-100 focus:outline-none ring-inset focus:ring"
                        type="submit"
                        title="Click to mark not completed"
                      >
                        <span class="stroke-2">
                          <CheckIcon />
                        </span>
                      </button>
                    )
                    : (
                      <button
                        name="action"
                        value="complete"
                        class="ring ring-zinc-600 w-6 h-6 border-zinc-600 rounded px-3 py-1 font-bold opacity-50 hover:opacity-100 focus:opacity-100 focus:outline-none ring-inset focus:ring"
                        type="submit"
                        title="Click to complete"
                      />
                    )}
                  <span class="grow">
                    {todo.title}
                  </span>

                  <button
                    name="action"
                    value="delete"
                    class="hover:bg-zinc-600 rounded px-1 py-1 font-bold opacity-50 hover:opacity-100 focus:opacity-100 focus:outline-none ring-inset focus:ring"
                    type="submit"
                  >
                    <TrashIcon />
                  </button>
                </form>
              </li>
            ))}
          </ol>
        )}
      </div>
    </div>
  )
}
