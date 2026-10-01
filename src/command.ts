import * as Effect from "effect/Effect"
import * as ChildProcess from "effect/unstable/process/ChildProcess"
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner"

const cls = ChildProcess.make("clear", { stdout: "inherit" })

export const runCommand = (command: string, keepOutput: boolean) =>
  Effect.gen(function* () {
    const [cmd, ...args] = command.split(" ")
    const childProcess = ChildProcess.make(cmd!, args, { stdout: "inherit" })

    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner

    if (!keepOutput) yield* spawner.exitCode(cls)
    const exitCode = yield* spawner.exitCode(childProcess)

    yield* Effect.log("Command exited with", exitCode)
  })
