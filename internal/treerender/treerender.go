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
	style  Style
	indent int
	wLink  int
	wMid   int
	wEnd   int
	wSep   int
}

func newStyleWidths(style Style) styleWidths {
	return styleWidths{
		style:  style,
		indent: max(0, style.IndentSize),
		wLink:  tabwrap.StringWidth(style.EdgeLink),
		wMid:   tabwrap.StringWidth(style.EdgeMid),
		wEnd:   tabwrap.StringWidth(style.EdgeEnd),
		wSep:   tabwrap.StringWidth(style.EdgeSeparator),
	}
}

func (sw styleWidths) segment(hasNext bool) string {
	if hasNext {
		return sw.style.EdgeLink + strings.Repeat(" ", sw.indent)
	}
	return strings.Repeat(" ", sw.indent+sw.wLink)
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
		for i, child := range children {
			walk(child, next, i == len(children)-1, false)
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

// MaxPrefixWidthForDepth returns the maximum display width of the prefix added by [RenderTree]
// for a node at the given depth. This includes the tree edges and the separator.
func MaxPrefixWidthForDepth(style Style, depth int) int {
	if depth <= 0 {
		return 0
	}
	sw := newStyleWidths(style)
	segWide := sw.wLink + sw.indent
	ancestorWide := (depth - 1) * segWide
	firstLine := ancestorWide + max(sw.wMid, sw.wEnd) + sw.wSep
	contLine := ancestorWide + segWide
	return max(firstLine, contLine)
}
