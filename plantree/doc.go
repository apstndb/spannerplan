/*
Package plantree provides functionality to render PlanNode as ASCII tree (EXPERIMENTAL).

# RowWithPredicates and tree prefix

The tree prefix is exposed without tying callers to a particular struct field shape:

  - [RowWithPredicates.TreePartString] — one string (newline-separated lines), stable with historical encoding
  - [RowWithPredicates.TreePartLines] — one string per line of [RowWithPredicates.NodeText]

Prefer these methods over reading the TreePart field directly so future internal
representations can change with less churn. Typical rendering uses [RowWithPredicates.Text],
which combines tree prefix and node title.

The TreePart field remains exported for struct literals and cmp.Diff in tests; new code
should use the accessors above when not constructing rows by hand.
When tests do construct [RowWithPredicates] values directly, prefer keyed
composite literals so future exported fields do not break the call site.

Rows also expose scalar child links in original PlanNode.ChildLinks order via
[RowWithPredicates.ScalarChildLinks]. Callers should group those links at
rendering time using the parent row's [RowWithPredicates.DisplayName] together
with each [ScalarChildLink.Type], because the same child-link type can have
different meanings under different operators.
[RowWithPredicates.ChildLinks] remains available for compatibility with older
callers, but new code should prefer [RowWithPredicates.ScalarChildLinks] because
it preserves Spanner's original PlanNode.ChildLinks order after filtering to
scalar child links.
[RowWithPredicates.Keys] is also kept for compatibility and contains scalar
child descriptions grouped by child-link type; new code should use
[RowWithPredicates.ScalarChildLinks] when it needs variables, child indexes, or
stable ordering.

A []string field would avoid one strings.Join in the renderer and one strings.Split in
Text, but it is a breaking API change for modules that build [RowWithPredicates] literals
in tests (for example github.com/apstndb/spanner-mycli). Downstream production code reviewed
at the time of this change uses ProcessPlan and Text/FormatID only, not TreePart field access.

Breaking changes in this package are called out in the release / PR description when
they affect exported options or types.

# Stability

This package is still marked EXPERIMENTAL. The shape of exported row types (including
how TreePart is represented) may change in a future version if we adopt a different
internal representation—callers should prefer [RowWithPredicates.Text] and stable
[Option] entrypoints where possible, and pin module versions when upgrading.
*/
package plantree
