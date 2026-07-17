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

A []string field would avoid one strings.Join in the renderer and one strings.Split in
Text, but it is a breaking API change for modules that build [RowWithPredicates] literals
in tests (for example github.com/apstndb/spanner-mycli). Downstream production code reviewed
at the time of this change uses ProcessPlan and Text/FormatID only, not TreePart field access.

Breaking changes in this package are called out in the release / PR description when
they affect exported options or types.

# Structural signatures

[StructuralSignature] returns a deterministic, versioned canonical string for
comparing visible relational plan structure. It is intentionally separate from
any machine-readable PlanTreeNode / ProcessPlanTree API (see issue #30) and does
not expose viewer structured-row DTOs.

The signature ignores plan-node IDs and execution statistics, preserves ordered
child occurrences and parent link types, and uses the same depth / occurrence
budgets and cycle detection as [ProcessPlan]. Its metadata field set is every
present key and recursively typed value except subquery_cluster_node, whose
value is a PlanNode ID. This includes operation_type, raw scan_type, scan_method,
seekable_key_size, flags (including false), and future optimizer metadata. New
metadata therefore intentionally changes the alpha signature instead of being
silently ignored.

Equality is meaningful only for signatures made by the same alpha encoding
revision; the encoding may change during the alpha and is not a stable
interchange contract. Repeated identical operators can collide because plan-node
IDs are intentionally excluded, so comparison layers must expose matching
ambiguity. See [StructuralSignature] and plantree/testdata/signature for the
encoding and fixture corpus.

# Stability

This package is still marked EXPERIMENTAL. The shape of exported row types (including
how TreePart is represented) may change in a future version if we adopt a different
internal representation—callers should prefer [RowWithPredicates.Text] and stable
[Option] entrypoints where possible, and pin module versions when upgrading.
*/
package plantree
