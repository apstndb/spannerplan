# Spanner Query Plan Ecosystem

This document defines how the repositories that render Google Cloud Spanner
query plans relate to each other, and the rules that keep them consistent.

Machine-readable source of truth for the role and observed-pin tables:
[`ecosystem/matrix.json`](ecosystem/matrix.json). Regenerate the marked sections
with `go run ./ecosystem/cmd/render`, and verify with
`go test ./ecosystem` or `go run ./ecosystem/cmd/canary`.

## Repositories and roles

<!-- ecosystem-roles:begin -->
| Repo | Role |
|---|---|
| [spannerplan](https://github.com/apstndb/spannerplan) (Go) | Ecosystem semantic reference: plan parsing, tree building, text rendering, and golden-output generation for ports. Pure Go — no CGO/Rust/WASM requirements. |
| [spannerplan-rs](https://github.com/apstndb/spannerplan-rs) (Rust) | Distribution core for non-Go ecosystems (C FFI, browser WASM, GitHub-Release JS tarballs). Follows the Go reference via byte-for-byte parity tests and never forks rendering semantics; "canonical" claims in its docs are scoped to that repository's internals. |
| [spannerplanviz](https://github.com/apstndb/spannerplanviz) (Go) | Diagram source renderer (Mermaid, DOT via the `dot` package, and D2) plus native Graphviz rasterization for CLI use. |
| [rendertree-web](https://github.com/apstndb/rendertree-web) (Go/WASM + TypeScript) | Lightweight public renderer/playground (GitHub Pages). Thin composition over the libraries; bundle size and safe rendering of untrusted plans are first-class requirements. |
| spanner-plan-viewer (local unpublished) | Experimental interactive diagnostics workbench (local checkout only; no published GitHub remote). Not a product dependency of this release train and not a canary target. |
<!-- ecosystem-roles:end -->

## Design principle

Rendering pipelines produce text (ASCII table, Mermaid source, DOT source).
Rasterization happens at the edge: natively in CLIs, and in the browser via
JS/WASM renderers (the mermaid npm package, @hpcc-js/wasm-graphviz). Do not
embed a Graphviz runtime into WASM binaries.

`rendertree-web` is the lightweight public renderer/playground. The local
unpublished `spanner-plan-viewer` checkout is an experimental diagnostics
workbench: useful for interactive analysis, but not a published product
dependency of this release train and not part of the public canary set.

## Governance

- Any rendering-semantics change lands first in the Go reference
  (spannerplan) with regenerated goldens; the Rust port follows in the same
  release train. Parity CI must be green on both sides before either
  releases.
- Required parity CI pins the release train. Scheduled canaries against
  `latest`/`main` are non-blocking and report drift via issues.
- Versioning is v0 semver: breaking changes bump minor; everything else,
  including new public APIs, bumps patch. Rendering output changes are
  breaking events for downstream golden tests even when the API is
  unchanged.
- Stable or v1 compatibility is never implied by repository age. New
  cross-language surfaces stay v0/alpha until a maintainer explicitly
  authorizes a stable release; prerelease tags are the default place to remove
  deprecated aliases and correct accidental API boundaries.
- Structured Plantree rows used between a bundled renderer and viewer are an
  internal, co-pinned FFI detail. Public interoperability is based on Spanner
  plan input plus the reference text renderer unless a separate external
  contract is deliberately designed and released.
- Release checklist for spannerplan/spannerplanviz: before tagging, check
  downstream pins and golden suites (spanner-mycli — the direct API
  downstream; rendertree-web; spannerplan-rs). cloudspannerecosystem/spanner-cli
  has its own renderer and does not consume spannerplan; treat it as an
  output-semantics comparator, not a release blocker. Do not block releases
  on the local unpublished viewer.
- Dependency minimums: modules consumed as libraries (spannerplan,
  spannerplanviz) keep their go.mod dependency floors low for MVS
  friendliness — bumping a library's minimums raises the floor for every
  downstream. A declared `go.mod` require is a module floor, not proof that a
  newer library tag was validated. Dependency and security updates happen in
  applications (rendertree-web, spanner-mycli). Do not raise library minimums
  except when new code requires it.
- YAML handling: the Go repositories standardize on goccy/go-yaml, accessed
  through [apstndb/protoyaml](https://github.com/apstndb/protoyaml) — a
  general-purpose canonical protojson⇄YAML utility that originated in this
  ecosystem but is versioned and governed independently (it is not part of
  this release train).
- Matrix drift control: `ecosystem/matrix.json` owns the observed pins. CI
  fails if `ECOSYSTEM.md` marked tables diverge. The public pinned-ref integrity
  checker (`.github/workflows/ecosystem-canary.yml`) reads only explicit pinned
  refs for public repositories and never depends on the local viewer. It can
  detect an unreachable recorded ref or dependency content that no longer
  matches the recorded pin; it does not resolve downstream `main` or `latest`
  refs and does not attest the ref's commit identity.

## Compatibility matrix

<!-- ecosystem-matrix:begin -->
As of 2026-07-18:

Rows record observed pins and declared module floors only. A go.mod require is not a compatibility or validation claim unless an entry explicitly says so.

spannerplan tags observed while writing this matrix: latest non-prerelease v0.2.1; prerelease v0.3.0-alpha.2.

These are v0 releases and do not imply a stable compatibility contract.

| Consumer | Observed ref | Declared / recorded pins |
|---|---|---|
| spannerplanviz | `v0.10.2` | `github.com/apstndb/spannerplan v0.2.0` |
| rendertree-web | `2f260a07666b589422fafac870a050e177c02b33` | `github.com/apstndb/spannerplan v0.2.1`; `github.com/apstndb/spannerplanviz v0.10.2` |
| spanner-mycli | `v0.33.0` | `github.com/apstndb/spannerplan v0.2.0` |
| spanner-mycli | `b43f857b194751e2631073bab45ff2026e86e30c` | `github.com/apstndb/spannerplan v0.2.1` |
| spannerplan-rs | `main` | parity CI `github.com/apstndb/spannerplan/cmd/rendertree@v0.3.0-alpha.1`; fixtures synced at `v0.2.1`; latest published `v0.1.0-alpha.3` |
<!-- ecosystem-matrix:end -->

When updating pins, edit `ecosystem/matrix.json` first, then run
`go run ./ecosystem/cmd/render`. Optional live verification against the pinned
public refs. This is a pinned-ref integrity check, not a lookup of current
downstream `main` or `latest` refs:

```bash
go run ./ecosystem/cmd/canary -live
```
