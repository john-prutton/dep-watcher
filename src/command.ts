import * as Data from "effect/Data"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as ChildProcess from "effect/unstable/process/ChildProcess"
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner"

import { styleText } from "node:util"

const cls = ChildProcess.make("clear", { stdout: "inherit" })

export const makeCommand = (command: string, keepOutput: boolean) =>
  Effect.gen(function* () {
    const [cmd, ...args] = command.split(" ")
    const childProcess = ChildProcess.make(cmd!, args, { stdout: "inherit" })

    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner

    if (!keepOutput) yield* spawner.exitCode(cls)

    yield* Effect.log("Running command", styleText("dim", command))
    const [duration, exitCode] = yield* Effect.timed(spawner.exitCode(childProcess))

    if (exitCode !== 0) return yield* new CommandFailed({ command, duration })

    yield* Effect.log("Command finished in", Duration.format(duration))
  })

class CommandFailed extends Data.TaggedError("CommandFailed")<{
  command: string
  duration: Duration.Duration
}> {}
