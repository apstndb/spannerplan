# spannerplan

[![Go Reference](https://pkg.go.dev/badge/github.com/apstndb/spannerplan.svg)](https://pkg.go.dev/github.com/apstndb/spannerplan)

Spanner QueryPlan manipulation module.

This is a v0 module, so breaking changes are possible across all packages before v1.

## Directory overview

- [`asciitable`](./asciitable): Generic ASCII table and appendix rendering helpers.
- [`cmd/lintplan`](./cmd/lintplan): CLI for printing heuristic warnings about expensive plan operators.
- [`cmd/rendertree`](./cmd/rendertree): CLI for rendering Spanner query plans and profiles as ASCII tables.
- [`examples/pgexplainjson`](./examples/pgexplainjson): Example renderer for PostgreSQL `EXPLAIN (ANALYZE, FORMAT JSON)` output.
- [`examples/wasm/render`](./examples/wasm/render): Minimal WebAssembly wrapper around the reference renderer.
- [`internal`](./internal): Internal subpackages that are not recommended for external use.
- [`lab`](./lab): Small ad hoc scripts and experiments.
- [`plantree`](./plantree): Spanner `PlanNode` tree processing and row-building primitives.
- [`plantree/reference`](./plantree/reference): High-level reference renderer API for Go, browser, and WebAssembly callers.
- [`protoyaml`](./protoyaml) (project-internal helper): YAML and JSON helpers used by this module for decoding protobuf query plan data. External use is not recommended; it is especially likely to be removed or moved behind unexported/internal helpers because it exists for internal use rather than as a supported public API.
- [`stats`](./stats): Execution statistics types and extraction helpers.
- [`treerender`](./treerender): Generic ASCII tree renderer with wrapping support.

## Browser and WASM embedding

For browser-facing renderers, use `github.com/apstndb/spannerplan/plantree/reference`
as the recommended high-level entrypoint.

- Go callers should prefer `reference.RenderTreeTableWithOptions(...)`.
- Serialized or cross-language callers such as WebAssembly or JavaScript wrappers
  should prefer `reference.RenderTreeTableWithConfig(...)` with
  `reference.RenderConfig`.

A minimal `syscall/js` wrapper lives in `examples/wasm/render`. It accepts a
Spanner query plan as JSON text or a JavaScript object containing `planNodes`,
parses `mode` and `format` strings with `reference.ParseRenderMode(...)` and
`reference.ParseFormat(...)`, decodes render settings into
`reference.RenderConfig`, and returns either `{output: string}` or
`{error: string}`.

```bash
GOOS=js GOARCH=wasm go build -o ./spannerplan.wasm ./examples/wasm/render
```

```js
const result = globalThis.spannerplanRenderTreeTable(
  queryPlanJson,
  "AUTO",
  "CURRENT",
  {wrapWidth: 80, hangingIndent: true},
)

if (result.error) {
  throw new Error(result.error)
}

console.log(result.output)
```

## Disclaimer

This module is alpha quality.
