import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Stream from "effect/Stream"
import * as Argument from "effect/unstable/cli/Argument"
import * as Command from "effect/unstable/cli/Command"
import * as Flag from "effect/unstable/cli/Flag"

import * as NodeServices from "@effect/platform-node/NodeServices"

import { makeCommand } from "./command.js"
import { debouncedWatchStream } from "./watch.js"

declare const __VERSION__: string

Command.make(
  "dep-watcher",

  {
    cmd: Flag.String("cmd").pipe(
      Flag.withAlias("c"),
      Flag.atLeast(1),
      Flag.withDescription("Command to run on file changes, can provide multiple"),
    ),

    debounceDuration: Flag.Int("debounce").pipe(
      Flag.withAlias("d"),
      Flag.withDefault(500),
      Flag.withDescription("Delay in milliseconds for debouncing command runs"),
    ),

    keepOutput: Flag.Boolean("keep-output").pipe(
      Flag.withAlias("k"),
      Flag.withDefault(false),
      Flag.withDescription("Do not clear the terminal before running the command"),
    ),

    paths: Argument.String("paths").pipe(
      Argument.atLeast(1),
      Argument.withDescription("Paths or files to watch for changes, checked recursively"),
    ),
  },

  ({ cmd, debounceDuration, keepOutput, paths }) =>
    Effect.gen(function* () {
      const cmds = cmd.map((cmd) => makeCommand(cmd.trim(), keepOutput))
      const cmdStream = Stream.fromIterable(cmds)

      const fileEventStream = yield* debouncedWatchStream(paths, Duration.millis(debounceDuration))

      let fiber: Fiber.Fiber<void> | null = null

      yield* Stream.runForEach(fileEventStream, () =>
        Effect.gen(function* () {
          if (Fiber.isFiber(fiber)) {
            if (!fiber.pollUnsafe()) yield* Effect.log("Interuptting command")
            yield* Fiber.interrupt(fiber)
          }

          fiber = yield* Effect.forkDetach(
            Stream.runForEach(cmdStream, (a) => a.pipe(Effect.catch((e) => Effect.die(e.message)))),
          )
        }),
      )
    }),
).pipe(
  Command.run({
    version: __VERSION__,
  }),
  Effect.provide(NodeServices.layer),
  Effect.catchTag("PathDneError", (e) => Effect.logError("These paths DNE:", e.paths)),
  Effect.runPromise,
)
