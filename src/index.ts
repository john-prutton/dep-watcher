import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import * as Argument from "effect/unstable/cli/Argument"
import * as Command from "effect/unstable/cli/Command"
import * as Flag from "effect/unstable/cli/Flag"

import * as NodeServices from "@effect/platform-node/NodeServices"

import { runCommand } from "./command.js"
import { debouncedWatchStream } from "./watch.js"

declare const __VERSION__: string

Command.make(
  "dep-watcher",

  {
    cmd: Flag.String("cmd").pipe(
      Flag.withAlias("c"),
      Flag.withDescription("Command to run on file changes"),
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
    debouncedWatchStream(paths, Duration.millis(debounceDuration)).pipe(
      Stream.unwrap,
      Stream.runForEach(() => runCommand(cmd, keepOutput)),
    ),
).pipe(
  Command.run({
    version: __VERSION__,
  }),
  Effect.provide(NodeServices.layer),
  Effect.catchTag("PathDneError", (e) => Effect.logError("These paths DNE:", e.paths)),
  Effect.runPromise,
)
