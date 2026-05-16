// Package treerender renders generic ASCII operator trees with optional wrapping.
package treerender

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/apstndb/go-tabwrap"
)

var defaultWrapCondition = func() *tabwrap.Condition {
	cond := tabwrap.NewCondition()
	cond.TrimTrailingSpace = true
	return cond
}()

// Node is one vertex in a logical tree rendered as ASCII edges.
type Node struct {
	// Text is the node label rendered after the tree prefix.
	Text string
	// Children are the child nodes rendered below this node.
	Children []*Node
}

// Row is one rendered tree row: a tree prefix per visual line plus node text lines.
type Row struct {
	// TreePart is everything rendered before NodeText on each visual line: the ASCII tree drawing
	// plus any continuation padding added by the renderer (for example, hanging-indent spacing).
	// It is joined with newlines using the same line structure as strings.Split(NodeText, "\n").
	TreePart string
	// NodeText is the rendered node label, possibly split across visual lines.
	NodeText string
}

// Text returns the full rendered row text, with the tree prefix prepended to each node text line.
// If a manually constructed row has mismatched tree and node line counts, all lines are preserved.
func (r Row) Text() string {
	treeLines := r.TreePartLines()
	nodeLines := strings.Split(r.NodeText, "\n")
	var sb strings.Builder
	numLines := max(len(treeLines), len(nodeLines))
	for i := 0; i < numLines; i++ {
		if i > 0 {
			sb.WriteByte('\n')
		}
		if i < len(treeLines) {
			sb.WriteString(treeLines[i])
		}
		if i < len(nodeLines) {
			sb.WriteString(nodeLines[i])
		}
	}
	return sb.String()
}

// TreePartLines splits [Row.TreePart] into one prefix per visual line. Rows produced by
// this package align these prefixes with the lines in [Row.NodeText].
func (r Row) TreePartLines() []string {
	return strings.Split(r.TreePart, "\n")
}

// ContinuationIndent selects how wrapped continuation lines align to the tree rail.
type ContinuationIndent int

const (
	// ContinuationIndentTree keeps wrapped lines aligned only under the tree prefix.
	ContinuationIndentTree ContinuationIndent = iota
	// ContinuationIndentAnchor hangs continuation lines after a node-local prefix.
	ContinuationIndentAnchor
)

// RenderOptions configures the optional wrapping behavior of [RenderTreeWithOptions].
type RenderOptions[T any] struct {
	// GetContinuationAnchor returns the node-local prefix used for hanging continuation lines.
	GetContinuationAnchor func(*T) string
	// WrapWidth sets the maximum total rendered line width. A value of 0 disables wrapping.
	WrapWidth int
	// WrapCondition controls display-width calculation and truncation for wrapped text.
	WrapCondition *tabwrap.Condition
	// ContinuationIndent selects how wrapped continuation lines align.
	ContinuationIndent ContinuationIndent
}

// Style configures ASCII edge glyphs and indentation between rails.
type Style struct {
	// EdgeLink is the ancestor rail glyph used for rows that have following siblings.
	EdgeLink string
	// EdgeMid is the edge glyph used for non-last children.
	EdgeMid string
	// EdgeEnd is the edge glyph used for last children.
	EdgeEnd string
	// EdgeSeparator is inserted between an edge glyph and node text.
	EdgeSeparator string
	// IndentSize is the number of spaces between ancestor rails.
	IndentSize int
}

// DefaultStyle returns the default "+-" / "|" tree drawing style.
func DefaultStyle() Style {
	return Style{
		EdgeLink:      "|",
		EdgeMid:       "+-",
		EdgeEnd:       "+-",
		EdgeSeparator: " ",
		IndentSize:    2,
	}
}

// CompactStyle returns a compact tree style with minimal edge glyphs.
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

// RenderTree walks an existing tree without copying it into [Node], using accessors for *T values.
// A *Node root infers T as Node, so accessors receive *Node rather than **Node.
func RenderTree[T any](root *T, style Style, getText func(*T) string, getChildren func(*T) []*T) []Row {
	return renderTree(root, style, getText, getChildren, defaultRenderOptions[T]())
}

// RenderTreeWithOptions renders a tree with optional wrapping and continuation-indent behavior.
// It returns an error if opts contains an invalid [ContinuationIndent].
func RenderTreeWithOptions[T any](
	root *T,
	style Style,
	getText func(*T) string,
	getChildren func(*T) []*T,
	opts RenderOptions[T],
) ([]Row, error) {
	resolved, err := resolveRenderOptions(opts)
	if err != nil {
		return nil, err
	}
	return renderTree(root, style, getText, getChildren, resolved), nil
}

type resolvedRenderOptions[T any] struct {
	getContinuationAnchor func(*T) string
	wrapWidth             int
	wrapCondition         *tabwrap.Condition
	continuationIndent    ContinuationIndent
}

func defaultRenderOptions[T any]() resolvedRenderOptions[T] {
	return resolvedRenderOptions[T]{
		wrapCondition:      defaultWrapCondition,
		continuationIndent: ContinuationIndentTree,
	}
}

func resolveRenderOptions[T any](opts RenderOptions[T]) (resolvedRenderOptions[T], error) {
	resolved := defaultRenderOptions[T]()
	resolved.getContinuationAnchor = opts.GetContinuationAnchor
	resolved.wrapWidth = opts.WrapWidth
	if opts.WrapCondition != nil {
		resolved.wrapCondition = opts.WrapCondition
	}
	if err := validateContinuationIndent(opts.ContinuationIndent); err != nil {
		return resolvedRenderOptions[T]{}, err
	}
	if opts.ContinuationIndent == ContinuationIndentAnchor && opts.GetContinuationAnchor == nil {
		return resolvedRenderOptions[T]{}, fmt.Errorf("GetContinuationAnchor is required with ContinuationIndentAnchor")
	}
	resolved.continuationIndent = opts.ContinuationIndent
	return resolved, nil
}

func renderTree[T any](
	root *T,
	style Style,
	getText func(*T) string,
	getChildren func(*T) []*T,
	opts resolvedRenderOptions[T],
) []Row {
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
		children := getChildren(node)
		lastIdx := -1
		for i := len(children) - 1; i >= 0; i-- {
			if children[i] != nil {
				lastIdx = i
				break
			}
		}
		text := getText(node)
		anchor := ""
		if opts.wrapWidth > 0 && opts.continuationIndent == ContinuationIndentAnchor && opts.getContinuationAnchor != nil {
			anchor = opts.getContinuationAnchor(node)
		}
		rows = append(rows, renderRow(
			ancestorPrefix,
			text,
			anchor,
			lastIdx >= 0,
			isLast,
			isRoot,
			sw,
			opts.wrapWidth,
			opts.wrapCondition,
			opts.continuationIndent,
		))

		next := ancestorPrefix
		if !isRoot {
			next = ancestorPrefix + sw.segment(!isLast)
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
	hasChildren bool,
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
	treeLines, nodeLines := wrapRowLines(text, anchor, firstPrefix, continuationPrefix, hasChildren, sw.style.EdgeLink, wrapWidth, wrapCondition, continuationIndent)
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
	hasChildren bool,
	childGuide string,
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
		continuationTree += hangingIndentPadding(anchorWidth, hasChildren, childGuide, wrapCondition)
	}
	for i := 1; i < len(treeLines); i++ {
		treeLines[i] = continuationTree
	}
	return treeLines, nodeLines
}

func hangingIndentPadding(anchorWidth int, hasChildren bool, childGuide string, wrapCondition *tabwrap.Condition) string {
	if anchorWidth <= 0 {
		return ""
	}
	if !hasChildren || childGuide == "" {
		return strings.Repeat(" ", anchorWidth)
	}

	guide := childGuide
	guideWidth := wrapCondition.StringWidth(guide)
	if guideWidth > anchorWidth {
		guide = wrapCondition.Truncate(guide, anchorWidth, "")
		if guide == "" {
			return strings.Repeat(" ", anchorWidth)
		}
		guideWidth = wrapCondition.StringWidth(guide)
	}
	return guide + strings.Repeat(" ", max(0, anchorWidth-guideWidth))
}

func validateContinuationIndent(continuationIndent ContinuationIndent) error {
	switch continuationIndent {
	case ContinuationIndentTree, ContinuationIndentAnchor:
		return nil
	default:
		return fmt.Errorf("invalid ContinuationIndent: %d", continuationIndent)
	}
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
