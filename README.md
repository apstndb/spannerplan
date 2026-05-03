# spannerplan

Spanner QueryPlan manipulation module.

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
GOOS=js GOARCH=wasm go build -o dist/spannerplan.wasm ./examples/wasm/render
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

## Sub-projects

- [rendertree](./cmd/rendertree)

## Disclaimer

This module is alpha quality.
