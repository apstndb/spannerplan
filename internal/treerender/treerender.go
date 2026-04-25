package treerender

import (
	"strings"
	"unicode/utf8"

	"github.com/apstndb/go-tabwrap"
)

var defaultWrapCondition = func() *tabwrap.Condition {
	cond := tabwrap.NewCondition()
	cond.TrimTrailingSpace = true
	return cond
}()

type Node struct {
	Text     string
	Children []*Node
}

type Row struct {
	// TreePart is everything rendered before NodeText on each visual line: the ASCII tree drawing
	// plus any continuation padding added by the renderer (for example, hanging-indent spacing).
	// It is joined with newlines using the same line structure as strings.Split(NodeText, "\n").
	TreePart string
	NodeText string
}

type ContinuationIndent int

const (
	ContinuationIndentTree ContinuationIndent = iota
	ContinuationIndentAnchor
)

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
	return RenderTreeWithOptions(root, style, getText, getChildren, nil, 0, nil, ContinuationIndentTree)
}

// RenderTreeWithOptions renders a tree with optional wrapping and continuation-indent behavior.
func RenderTreeWithOptions[T any](
	root *T,
	style Style,
	getText func(*T) string,
	getChildren func(*T) []*T,
	getContinuationAnchor func(*T) string,
	wrapWidth int,
	wrapCondition *tabwrap.Condition,
	continuationIndent ContinuationIndent,
) []Row {
	if root == nil {
		return nil
	}
	if wrapCondition == nil {
		wrapCondition = defaultWrapCondition
	}

	sw := newStyleWidths(style)
	var rows []Row
	var walk func(node *T, ancestorPrefix string, isLast, isRoot bool)
	walk = func(node *T, ancestorPrefix string, isLast, isRoot bool) {
		if node == nil {
			return
		}
		text := getText(node)
		anchor := ""
		if getContinuationAnchor != nil {
			anchor = getContinuationAnchor(node)
		}
		rows = append(rows, renderRow(ancestorPrefix, text, anchor, isLast, isRoot, sw, wrapWidth, wrapCondition, continuationIndent))

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

func renderRow(
	ancestorPrefix, text, anchor string,
	isLast, isRoot bool,
	sw styleWidths,
	wrapWidth int,
	wrapCondition *tabwrap.Condition,
	continuationIndent ContinuationIndent,
) Row {
	if wrapWidth <= 0 {
		return Row{
			TreePart: strings.Join(prefixLinesFromAncestor(ancestorPrefix, text, isLast, isRoot, sw), "\n"),
			NodeText: text,
		}
	}

	firstPrefix, continuationPrefix := rowPrefixes(ancestorPrefix, isLast, isRoot, sw)
	treeLines, nodeLines := wrapRowLines(text, anchor, firstPrefix, continuationPrefix, wrapWidth, wrapCondition, continuationIndent)
	return Row{
		TreePart: strings.Join(treeLines, "\n"),
		NodeText: strings.Join(nodeLines, "\n"),
	}
}

func rowPrefixes(ancestorPrefix string, isLast, isRoot bool, sw styleWidths) (first, continuation string) {
	if isRoot {
		return "", ""
	}
	first = ancestorPrefix + edgeForRow(isLast, sw.style) + sw.style.EdgeSeparator
	return first, ancestorPrefix + sw.continuationSegment(isLast)
}

func wrapRowLines(
	text, anchor, firstPrefix, continuationPrefix string,
	wrapWidth int,
	wrapCondition *tabwrap.Condition,
	continuationIndent ContinuationIndent,
) (treeLines, nodeLines []string) {
	anchorWidth := 0
	if continuationIndent == ContinuationIndentAnchor && anchor != "" && strings.HasPrefix(text, anchor) {
		anchorWidth = wrapCondition.StringWidth(anchor)
		text = strings.TrimPrefix(text, anchor)
	} else {
		anchor = ""
	}

	firstBudget := max(1, wrapWidth-wrapCondition.StringWidth(firstPrefix)-anchorWidth)
	continuationBudget := max(1, wrapWidth-wrapCondition.StringWidth(continuationPrefix)-anchorWidth)
	nodeLines = wrapChunks(text, firstBudget, continuationBudget, wrapCondition)
	if len(nodeLines) == 0 {
		nodeLines = []string{""}
	}
	nodeLines[0] = anchor + nodeLines[0]

	treeLines = make([]string, len(nodeLines))
	treeLines[0] = firstPrefix
	continuationTree := continuationPrefix
	if anchorWidth > 0 {
		continuationTree += strings.Repeat(" ", anchorWidth)
	}
	for i := 1; i < len(treeLines); i++ {
		treeLines[i] = continuationTree
	}
	return treeLines, nodeLines
}

func wrapChunks(text string, firstBudget, continuationBudget int, wrapCondition *tabwrap.Condition) []string {
	rawLines := strings.Split(text, "\n")
	lines := make([]string, 0, len(rawLines))
	budget := firstBudget
	for _, rawLine := range rawLines {
		if rawLine == "" {
			lines = append(lines, "")
			budget = continuationBudget
			continue
		}
		for rawLine != "" {
			rawChunk := wrapCondition.Truncate(rawLine, budget, "")
			if rawChunk == "" {
				_, size := utf8.DecodeRuneInString(rawLine)
				if size <= 0 {
					size = 1
				}
				rawChunk = rawLine[:size]
			}
			chunk := rawChunk
			if wrapCondition.TrimTrailingSpace {
				chunk = strings.TrimRight(chunk, " \t")
			}
			lines = append(lines, chunk)
			rawLine = rawLine[len(rawChunk):]
			budget = continuationBudget
		}
	}
	return lines
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
