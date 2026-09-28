import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import * as Stream from "effect/Stream"

export const debouncedWatchStream = (paths: readonly string[], duration: Duration.Duration) =>
  Effect.gen(function* () {
    yield* Effect.logDebug("watching paths", paths, "with debounce duration", duration.toString())

    const { watch } = yield* FileSystem.FileSystem

    return Stream.fromIterable(paths).pipe(
      Stream.map((path) => watch(path, { recursive: true })),
      Stream.flatten,
      Stream.merge(Stream.make("init" as const)),
      Stream.debounce(duration),
    )
  })
