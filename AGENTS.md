# Repository Guidelines

## Project Structure & Module Organization
This repository is a Go module for Cloud Spanner query plan parsing and rendering. Core package code lives at the repository root (`queryplan.go`, `extract.go`). Supporting packages are organized by responsibility: `plantree/` renders plan trees, `protoyaml/` handles YAML and JSON decoding, and `stats/` contains execution-stat helpers. CLI entry points live under `cmd/`, currently `cmd/rendertree` and `cmd/lintplan`. Keep test fixtures in package-local `testdata/` directories such as `plantree/reference/testdata/` and `cmd/rendertree/impl/testdata/`.

## Build, Test, and Development Commands
Use Go 1.24 as in CI.

- `go test -v ./...`: run the full test suite across all packages.
- `make test`: project shortcut for the same full test run.
- `go run ./cmd/rendertree < plan.yaml`: render a query plan from YAML or JSON.
- `go run ./cmd/lintplan < plan.yaml`: print heuristic warnings for expensive plan operators.
- `golangci-lint run --timeout=5m`: run the same linter family used in GitHub Actions. Run lint before creating a commit, not just before opening a PR.

## Coding Style & Naming Conventions
Follow standard Go formatting and layout: run `gofmt` on edited files, keep imports grouped by `gofmt`, and prefer small focused packages. Use tabs for indentation as produced by Go tools. Exported identifiers use `MixedCaps`; unexported helpers use lower camel case. Keep package names short and lowercase (`plantree`, `protoyaml`). Prefer table-driven tests with descriptive `name` or `desc` fields.

## Testing Guidelines
Write `_test.go` files beside the code they verify. Favor table-driven tests and `t.Run(...)`, following patterns in `queryplan_test.go` and `cmd/rendertree/impl/impl_test.go`. Store realistic input plans under `testdata/` or embed them when that keeps tests self-contained. When rendering output changes, update expected tables and predicate text together.

## Commit & Pull Request Guidelines
Recent history follows short, imperative prefixes such as `feat:`, `fix:`, `misc:`, and occasional `feature:`. Keep the subject line concise and scoped to one logical change. For pull requests, include a clear summary, mention any changed commands or fixtures, and paste representative input/output when CLI rendering changes. Before opening a PR, ensure `go test -v ./...` passes and `golangci-lint` is clean.
