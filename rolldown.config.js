// @ts-check

import { defineConfig } from "rolldown"

import pkg from "./package.json" with { type: "json" }

export default defineConfig({
  input: "src/index.ts",
  output: {
    file: "dist/dep-watcher.js",
    format: "esm",
    codeSplitting: false,
    banner: "#!/usr/bin/env node",
    minify: true,
  },
  platform: "node",
  transform: {
    define: {
      __VERSION__: JSON.stringify(pkg.version),
    },
  },
})
