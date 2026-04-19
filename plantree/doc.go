/*
Package plantree provides functionality to render PlanNode as ASCII tree (EXPERIMENTAL).

# RowWithPredicates and TreePart

RowWithPredicates.TreePart is a single string containing one line of the ASCII tree
prefix per line of NodeText, separated by newline characters (the same convention as
historical releases). Typical rendering uses [RowWithPredicates.Text], which combines
tree prefix and node title; callers rarely need to read TreePart directly except in
tests or custom formatters.

A []string shape would avoid one strings.Join in the renderer and one strings.Split
in Text, but it is a breaking API change for modules that build [RowWithPredicates]
literals in tests (for example github.com/apstndb/spanner-mycli compares full rows in
tests using string TreePart values). Downstream production code reviewed at the time of
this change uses ProcessPlan and Text/FormatID only, not TreePart field access.
Callers that want per-line prefixes can use [RowWithPredicates.TreePartLines].

Breaking changes in this package are called out in the release / PR description when
they affect exported options or types.
*/
package plantree
