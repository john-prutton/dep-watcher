import * as Data from "effect/Data"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import * as Path from "effect/Path"
import * as Stream from "effect/Stream"

export const debouncedWatchStream = (paths: readonly string[], duration: Duration.Duration) =>
  Effect.gen(function* () {
    yield* Effect.logDebug("watching paths", paths, "with debounce duration", duration.toString())

    yield* ensurePathsExist(paths)

    const { files, dirs } = yield* filterPaths(paths)

    const watchStream = Stream.merge(
      yield* fileWatchStream(files),
      yield* directoryWatchStream(dirs),
    )
    const heartbeatStream = Stream.make("heart-beat" as const)

    return Stream.merge(watchStream, heartbeatStream).pipe(Stream.debounce(duration))
  })

const ensurePathsExist = Effect.fnUntraced(function* (paths: readonly string[]) {
  const { exists } = yield* FileSystem.FileSystem

  const dne = yield* Effect.filter(paths, (p) => Effect.map(exists(p), (e) => !e), {
    concurrency: "unbounded",
  })

  if (dne.length > 0) return yield* new PathDneError({ paths: dne })
})

class PathDneError extends Data.TaggedError("PathDneError")<{
  paths: string[]
}> {}

const filterPaths = Effect.fnUntraced(function* (paths: readonly string[]) {
  const { stat } = yield* FileSystem.FileSystem

  const files: string[] = [],
    dirs: string[] = [],
    unknown: string[] = []

  for (const p of paths) {
    const { type } = yield* stat(p)

    if (type === "Directory") dirs.push(p)
    else if (type === "File") files.push(p)
    else unknown.push(p)
  }

  if (unknown.length > 0) return yield* new UnknownFileType({ paths: unknown })

  return { files, dirs }
})

class UnknownFileType extends Data.TaggedError("UnknownFileType")<{
  paths: string[]
}> {}

const directoryWatchStream = Effect.fnUntraced(function* (dirs: string[]) {
  const { watch } = yield* FileSystem.FileSystem

  return Stream.fromIterable(dirs).pipe(
    Stream.map((dirPath) => watch(dirPath, { recursive: true })),
    Stream.flatten,
  )
})

const fileWatchStream = Effect.fnUntraced(function* (filePaths: string[]) {
  const dirToFiles = yield* buildMapOfDirToFiles(filePaths)

  const { watch } = yield* FileSystem.FileSystem

  return Stream.fromIterable(dirToFiles.entries()).pipe(
    Stream.map(([dir, files]) =>
      watch(dir, { recursive: false }).pipe(Stream.filter((event) => files.has(event.path))),
    ),
    Stream.flatten,
  )
})

const buildMapOfDirToFiles = Effect.fnUntraced(function* (filePaths: string[]) {
  const { dirname, basename } = yield* Path.Path

  const dirToFiles = new Map<string, Set<string>>()

  for (const filePath of filePaths) {
    const dir = dirname(filePath)

    const files = dirToFiles.getOrInsert(dir, new Set())
    files.add(basename(filePath))
  }

  return dirToFiles
})
