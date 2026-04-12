package treerender

import (
	"slices"
	"strings"

	"github.com/apstndb/go-tabwrap"
)

type Node struct {
	Text     string
	Children []*Node
}

type Row struct {
	TreePart string
	NodeText string
}

type Style struct {
	EdgeLink      string
	EdgeMid       string
	EdgeEnd       string
	EdgeSeparator string
	IndentSize    int
}

func DefaultStyle() Style {
	return Style{
		EdgeLink:      "|",
		EdgeMid:       "+-",
		EdgeEnd:       "+-",
		EdgeSeparator: " ",
		IndentSize:    2,
	}
}

func CompactStyle() Style {
	return Style{
		EdgeLink:   "|",
		EdgeMid:    "+",
		EdgeEnd:    "+",
		IndentSize: 0,
	}
}

// Render renders a [Node] tree. It is equivalent to [RenderTree] on the same structure.
func Render(root *Node, style Style) []Row {
	return RenderTree(root, style, func(n *Node) string { return n.Text }, func(n *Node) []*Node { return n.Children })
}

// RenderTree walks an existing tree without copying it into [Node], using the supplied accessors.
func RenderTree[T any](root *T, style Style, getText func(*T) string, getChildren func(*T) []*T) []Row {
	if root == nil {
		return nil
	}

	var rows []Row
	var walk func(node *T, ancestorHasNext []bool, isLast, isRoot bool)
	walk = func(node *T, ancestorHasNext []bool, isLast, isRoot bool) {
		text := getText(node)
		rows = append(rows, Row{
			TreePart: strings.Join(prefixLines(text, ancestorHasNext, isLast, isRoot, style), "\n"),
			NodeText: text,
		})

		childAncestors := slices.Clone(ancestorHasNext)
		if !isRoot {
			childAncestors = append(childAncestors, !isLast)
		}
		children := getChildren(node)
		for i, child := range children {
			walk(child, childAncestors, i == len(children)-1, false)
		}
	}

	walk(root, nil, true, true)
	return rows
}

func prefixLines(text string, ancestorHasNext []bool, isLast, isRoot bool, style Style) []string {
	lines := strings.Split(text, "\n")
	prefixes := make([]string, len(lines))
	if isRoot {
		return prefixes
	}

	ancestorPrefix := renderAncestorPrefix(ancestorHasNext, style)
	prefixes[0] = ancestorPrefix + edgeForRow(isLast, style) + style.EdgeSeparator

	continuation := ancestorPrefix + continuationSegment(isLast, style)
	for i := 1; i < len(prefixes); i++ {
		prefixes[i] = continuation
	}

	return prefixes
}

func renderAncestorPrefix(ancestorHasNext []bool, style Style) string {
	var sb strings.Builder
	for _, hasNext := range ancestorHasNext {
		sb.WriteString(segment(hasNext, style))
	}
	return sb.String()
}

func edgeForRow(isLast bool, style Style) string {
	if isLast {
		return style.EdgeEnd
	}
	return style.EdgeMid
}

func continuationSegment(isLast bool, style Style) string {
	return segment(!isLast, style)
}

func nonNegativeIndent(style Style) int {
	return max(0, style.IndentSize)
}

func segment(hasNext bool, style Style) string {
	ind := nonNegativeIndent(style)
	wLink := tabwrap.StringWidth(style.EdgeLink)
	if hasNext {
		return style.EdgeLink + strings.Repeat(" ", ind)
	}
	return strings.Repeat(" ", ind+wLink)
}

// MaxPrefixWidthForDepth returns the width of the tree prefix on the first line of a node at the
// given depth, measured in terminal display columns (grapheme-aware via [tabwrap.StringWidth]).
// Depth is 0 for the root, 1 for its direct children, and so on. It matches [segment] and edge
// strings used by [RenderTree].
func MaxPrefixWidthForDepth(style Style, depth int) int {
	if depth <= 0 {
		return 0
	}
	ind := nonNegativeIndent(style)
	segWide := tabwrap.StringWidth(style.EdgeLink) + ind
	ancestorWide := (depth - 1) * segWide
	edgeWide := max(tabwrap.StringWidth(style.EdgeMid), tabwrap.StringWidth(style.EdgeEnd)) + tabwrap.StringWidth(style.EdgeSeparator)
	return ancestorWide + edgeWide
}
