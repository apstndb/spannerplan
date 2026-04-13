package treerender

import (
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

// styleWidths holds display widths and indent for a [Style], computed once per render.
type styleWidths struct {
	style      Style
	indent     int
	wLink      int
	wMid       int
	wEnd       int
	wSep       int
	segHasNext string
	segNoNext  string
}

func newStyleWidths(style Style) styleWidths {
	indent := max(0, style.IndentSize)
	wLink := tabwrap.StringWidth(style.EdgeLink)
	return styleWidths{
		style:      style,
		indent:     indent,
		wLink:      wLink,
		wMid:       tabwrap.StringWidth(style.EdgeMid),
		wEnd:       tabwrap.StringWidth(style.EdgeEnd),
		wSep:       tabwrap.StringWidth(style.EdgeSeparator),
		segHasNext: style.EdgeLink + strings.Repeat(" ", indent),
		segNoNext:  strings.Repeat(" ", indent+wLink),
	}
}

func (sw styleWidths) segment(hasNext bool) string {
	if hasNext {
		return sw.segHasNext
	}
	return sw.segNoNext
}

func (sw styleWidths) continuationSegment(isLast bool) string {
	return sw.segment(!isLast)
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

	sw := newStyleWidths(style)
	var rows []Row
	var walk func(node *T, ancestorPrefix string, isLast, isRoot bool)
	walk = func(node *T, ancestorPrefix string, isLast, isRoot bool) {
		if node == nil {
			return
		}
		text := getText(node)
		rows = append(rows, Row{
			TreePart: strings.Join(prefixLinesFromAncestor(ancestorPrefix, text, isLast, isRoot, sw), "\n"),
			NodeText: text,
		})

		next := ancestorPrefix
		if !isRoot {
			next = ancestorPrefix + sw.segment(!isLast)
		}
		children := getChildren(node)
		lastIdx := -1
		for i := len(children) - 1; i >= 0; i-- {
			if children[i] != nil {
				lastIdx = i
				break
			}
		}
		for i, child := range children {
			if child == nil {
				continue
			}
			walk(child, next, i == lastIdx, false)
		}
	}

	walk(root, "", true, true)
	return rows
}

func prefixLinesFromAncestor(ancestorPrefix, text string, isLast, isRoot bool, sw styleWidths) []string {
	lines := strings.Split(text, "\n")
	prefixes := make([]string, len(lines))
	if isRoot {
		return prefixes
	}

	edge := edgeForRow(isLast, sw.style)
	prefixes[0] = ancestorPrefix + edge + sw.style.EdgeSeparator

	cont := ancestorPrefix + sw.continuationSegment(isLast)
	for i := 1; i < len(prefixes); i++ {
		prefixes[i] = cont
	}

	return prefixes
}

func edgeForRow(isLast bool, style Style) string {
	if isLast {
		return style.EdgeEnd
	}
	return style.EdgeMid
}

// PrefixMetrics caches grapheme-aware display widths for a [Style] so callers that need
// prefix width at many depths (e.g. plantree wrapping) avoid recomputing [tabwrap.StringWidth]
// on every node.
type PrefixMetrics struct {
	sw styleWidths
}

// NewPrefixMetrics precomputes widths for style once; use [PrefixMetrics.MaxWidthForDepth] per level.
func NewPrefixMetrics(style Style) PrefixMetrics {
	return PrefixMetrics{sw: newStyleWidths(style)}
}

// MaxWidthForDepth returns the maximum display width of the prefix added by [RenderTree] for a node
// at the given depth. This includes the tree edges and the separator after the edge.
func (p PrefixMetrics) MaxWidthForDepth(depth int) int {
	if depth <= 0 {
		return 0
	}
	sw := p.sw
	segWide := sw.wLink + sw.indent
	ancestorWide := (depth - 1) * segWide
	firstLine := ancestorWide + max(sw.wMid, sw.wEnd) + sw.wSep
	contLine := ancestorWide + segWide
	return max(firstLine, contLine)
}

// MaxPrefixWidthForDepth returns the maximum display width of the prefix added by [RenderTree]
// for a node at the given depth. This includes the tree edges and the separator.
// For hot paths, prefer [NewPrefixMetrics] and [PrefixMetrics.MaxWidthForDepth].
func MaxPrefixWidthForDepth(style Style, depth int) int {
	return NewPrefixMetrics(style).MaxWidthForDepth(depth)
}
