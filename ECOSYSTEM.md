# Spanner Query Plan Ecosystem

This document defines how the repositories that render Google Cloud Spanner
query plans relate to each other, and the rules that keep them consistent.

## Repositories and roles

| Repo | Role |
|---|---|
| [spannerplan](https://github.com/apstndb/spannerplan) (Go) | Ecosystem semantic reference: plan parsing, tree building, text rendering. Golden-output generator for ports. Pure Go — no CGO/Rust/WASM requirements. |
| [spannerplan-rs](https://github.com/apstndb/spannerplan-rs) (Rust) | Distribution core for non-Go ecosystems (C FFI, browser WASM, GitHub-Release JS tarballs). Follows the Go reference via byte-for-byte parity tests and never forks rendering semantics; "canonical" claims in its docs are scoped to that repository's internals. |
| [spannerplanviz](https://github.com/apstndb/spannerplanviz) (Go) | Diagram source generator (Mermaid source, and DOT source via the `dot` package) plus native Graphviz rasterization for CLI use. |
| [rendertree-web](https://github.com/apstndb/rendertree-web) | Product UI (GitHub Pages). Thin composition layer over the libraries; bundle size and safe rendering of untrusted plans are first-class requirements. |

## Design principle

Rendering pipelines produce text (ASCII table, Mermaid source, DOT source).
Rasterization happens at the edge: natively in CLIs, and in the browser via
JS/WASM renderers (the mermaid npm package, @hpcc-js/wasm-graphviz). Do not
embed a Graphviz runtime into WASM binaries.

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
  output-semantics comparator, not a release blocker.
- Dependency minimums: modules consumed as libraries (spannerplan,
  spannerplanviz) keep their go.mod dependency floors low for MVS
  friendliness — bumping a library's minimums raises the floor for every
  downstream. Dependency and security updates happen in applications
  (rendertree-web, spanner-mycli). Do not raise library minimums except when
  new code requires it.
- YAML handling: the Go repositories standardize on goccy/go-yaml, accessed
  through [apstndb/protoyaml](https://github.com/apstndb/protoyaml) — a
  general-purpose canonical protojson⇄YAML utility that originated in this
  ecosystem but is versioned and governed independently (it is not part of
  this release train).

## Compatibility matrix

As of 2026-07-08:

| Consumer | Validated against |
|---|---|
| spannerplanviz v0.10.0 | spannerplan v0.2.1 |
| rendertree-web (rolling) | spannerplan v0.2.1, spannerplanviz v0.10.0 |
| spannerplan-rs (main, post-v0.1.0-alpha.1) | spannerplan `cmd/rendertree` v0.2.1 (parity CI + weekly canary vs latest) |
