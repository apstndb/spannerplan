# spannerplan

[![Go Reference](https://pkg.go.dev/badge/github.com/apstndb/spannerplan.svg)](https://pkg.go.dev/github.com/apstndb/spannerplan)

Spanner QueryPlan manipulation module.

This is a v0 module, so breaking changes are possible across all packages before v1.

See [ECOSYSTEM.md](ECOSYSTEM.md) and [`ecosystem/matrix.json`](ecosystem/matrix.json)
for how this module relates to spannerplan-rs, spannerplanviz, rendertree-web,
and the local unpublished diagnostics viewer.

## Directory overview

- [`asciitable`](./asciitable): Generic ASCII table, tableless row, and appendix rendering helpers.
- [`cmd/lintplan`](./cmd/lintplan): CLI for printing heuristic warnings about expensive plan operators.
- [`cmd/rendertree`](./cmd/rendertree): CLI for rendering Spanner query plans and profiles as ASCII tables.
- [`examples/pgexplainjson`](./examples/pgexplainjson): Example renderer for PostgreSQL `EXPLAIN (ANALYZE, FORMAT JSON)` output.
- [`examples/wasm/render`](./examples/wasm/render): Minimal WebAssembly wrapper around the reference renderer.
- [`internal`](./internal): Internal subpackages that are not recommended for external use.
- [`lab`](./lab): Small ad hoc scripts and experiments.
- [`plantree`](./plantree): Spanner `PlanNode` tree processing and row-building primitives, including [plantree.StructuralSignature](https://pkg.go.dev/github.com/apstndb/spannerplan/plantree#StructuralSignature) for deterministic structural comparison.
- [`plantree/reference`](./plantree/reference): High-level reference renderer API for Go, browser, and WebAssembly callers.
- [`stats`](./stats): Execution statistics types and extraction helpers.
- [`treerender`](./treerender): Generic ASCII tree renderer with wrapping support.

## Structural signatures

`plantree.StructuralSignature` produces a versioned canonical string suitable for
comparing plan shape across captures:

- Includes the operator display name, parent link types, every present metadata
  key and recursively typed value, predicates, and ordered visible child
  occurrences; this includes raw `scan_type`, `operation_type`, `scan_method`,
  and `seekable_key_size`
- Excludes plan-node IDs, ID-bearing `subquery_cluster_node` keys at any
  metadata struct depth, and execution statistics
- Reuses the Plantree traversal budgets (`MaxPlantreeDepth`,
  `MaxPlantreeOccurrences`) and cycle detection from `ProcessPlan`
- Uses a length-framed alpha encoding so included fields cannot collide through
  delimiter characters; identical operators can still collide because IDs are
  deliberately excluded, so diff/match UIs must expose ambiguity rather than
  silently pairing nodes

Equality is meaningful only for signatures made by the same alpha encoding
revision. The encoding may change during the alpha and is not yet a stable
cross-version or cross-language interchange contract. New metadata emitted by
Spanner intentionally changes the alpha signature rather than being silently
ignored.

This API is not the PlanTreeNode / ProcessPlanTree surface tracked in issue #30.
Golden fixtures live under `plantree/testdata/signature/`.

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
  {wrapWidth: 80, hangingIndent: true, layout: "tableless"},
)

if (result.error) {
  throw new Error(result.error)
}

console.log(result.output)
```

The tableless layout is compact, human-oriented text rather than an escaped
pipe-delimited interchange format. Use keyed `reference.RenderConfig` literals
in Go so additive configuration fields remain source-compatible.

## Releases

Version-specific breaking changes, migration instructions, compatibility
notes, and toolchain requirements are documented in
[GitHub Releases](https://github.com/apstndb/spannerplan/releases). Review the
notes for the target version before upgrading, especially across v0 minor
versions.

The ecosystem matrix records observed consumer pins. It does not claim that
every downstream public branch or release has adopted the latest spannerplan
version.
