import * as Stream from "effect/Stream"

import * as NodeStream from "@effect/platform-node/NodeStream"

export const restartStream = NodeStream.fromReadable({
  evaluate: () => process.stdin,
  chunkSize: 1,
}).pipe(
  Stream.decodeText(),
  Stream.splitLines,
  Stream.filter((f) => f === "r"),
  Stream.map(() => "restart" as const),
)
