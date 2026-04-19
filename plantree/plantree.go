package plantree

import (
	"fmt"
	"strconv"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/go-tabwrap"
	"github.com/apstndb/lox"
	"github.com/samber/lo"

	"github.com/apstndb/spannerplan"
	"github.com/apstndb/spannerplan/internal/treerender"
	"github.com/apstndb/spannerplan/stats"
)

var defaultWrapCondition = newDefaultWrapCondition()

func newDefaultWrapCondition() *tabwrap.Condition {
	c := tabwrap.NewCondition()
	c.TrimTrailingSpace = true // go-tabwrap v0.1.3+: stable diffs / CLI output
	return c
}

type RowWithPredicates struct {
	ID int32
	// TreePart stores the ASCII tree prefix as newline-joined lines (one per line of NodeText).
	// Prefer [RowWithPredicates.TreePartString] or [RowWithPredicates.TreePartLines] instead of
	// reading this field directly, so callers stay decoupled if the storage shape changes.
	TreePart       string
	NodeText       string
	Predicates     []string
	Keys           map[string][]string
	ExecutionStats stats.ExecutionStats
	ChildLinks     map[string][]*spannerplan.ResolvedChildLink
}

type renderedNode struct {
	ID             int32
	NodeText       string
	Predicates     []string
	ExecutionStats stats.ExecutionStats
	ChildLinks     map[string][]*spannerplan.ResolvedChildLink
	Children       []*renderedNode
}

func (n *renderedNode) lineCount() int {
	return strings.Count(n.NodeText, "\n") + 1
}

func (r RowWithPredicates) Text() string {
	treeLines := r.TreePartLines()
	nodeLines := strings.Split(r.NodeText, "\n")
	var sb strings.Builder
	for i, line := range nodeLines {
		if len(treeLines) > i {
			sb.WriteString(strings.TrimSuffix(treeLines[i], "\n"))
		}
		sb.WriteString(line)
		sb.WriteRune('\n')
	}
	return strings.TrimSuffix(sb.String(), "\n")
}

// TreePartString returns the full tree-prefix string (newline-separated lines), matching the
// historical field encoding. Use this when you need a single string; use [RowWithPredicates.TreePartLines] for per-line access.
func (r RowWithPredicates) TreePartString() string {
	return r.TreePart
}

// TreePartLines returns the tree prefix as one string per line of [RowWithPredicates.NodeText].
func (r RowWithPredicates) TreePartLines() []string {
	return strings.Split(r.TreePartString(), "\n")
}

func (r RowWithPredicates) FormatID() string {
	return lox.IfOrEmpty(len(r.Predicates) != 0, "*") + strconv.Itoa(int(r.ID))
}

type options struct {
	disallowUnknownStats bool
	queryplanOptions     []spannerplan.Option
	style                treerender.Style
	prefixMetrics        treerender.PrefixMetrics
	compact              bool
	wrapWidth            *int
	wrapper              *tabwrap.Condition
}

type Option func(*options)

func DisallowUnknownStats() Option {
	return func(o *options) {
		o.disallowUnknownStats = true
	}
}

func WithQueryPlanOptions(opts ...spannerplan.Option) Option {
	return func(o *options) {
		o.queryplanOptions = append(o.queryplanOptions, opts...)
	}
}

// WithWrapWidth sets the maximum total rendered line width, including the tree prefix.
// Node title text is wrapped to the remaining width after accounting for the tree prefix.
// A value of 0 disables wrapping (consistent with the rendertree CLI default of 0).
// Negative values make [ProcessPlan] return an error.
func WithWrapWidth(width int) Option {
	return func(o *options) {
		o.wrapWidth = &width
	}
}

// EnableCompact enables compact node title mode.
func EnableCompact() Option {
	return func(o *options) {
		o.compact = true
		o.style = treerender.CompactStyle()
		o.queryplanOptions = append(o.queryplanOptions, spannerplan.EnableCompact())
	}
}

func ProcessPlan(qp *spannerplan.QueryPlan, opts ...Option) (rows []RowWithPredicates, err error) {
	o := options{
		style:   treerender.DefaultStyle(),
		wrapper: defaultWrapCondition,
	}
	for _, opt := range opts {
		opt(&o)
	}
	if o.wrapper == nil {
		o.wrapper = defaultWrapCondition
	}
	if o.wrapWidth != nil && *o.wrapWidth < 0 {
		return nil, fmt.Errorf("wrap width cannot be negative: %d", *o.wrapWidth)
	}
	o.prefixMetrics = treerender.NewPrefixMetrics(o.style)

	root, err := buildRenderedTree(qp, nil, 0, &o)
	if err != nil {
		return nil, fmt.Errorf("failed to build rendered tree: %w", err)
	}
	if root == nil {
		return nil, nil
	}

	renderRows := treerender.RenderTree(root, o.style,
		func(n *renderedNode) string { return n.NodeText },
		func(n *renderedNode) []*renderedNode { return n.Children },
	)
	nodes := collectPreorder(root)
	if len(renderRows) != len(nodes) {
		return nil, fmt.Errorf("unexpected rendered row count: got=%d want=%d", len(renderRows), len(nodes))
	}

	result := make([]RowWithPredicates, 0, len(nodes))
	for i, node := range nodes {
		row := renderRows[i]
		gotLines := strings.Count(row.NodeText, "\n") + 1
		wantLines := node.lineCount()
		if gotLines != wantLines {
			return nil, fmt.Errorf("unexpected rendered node line count for node %d: got %d lines, want %d", node.ID, gotLines, wantLines)
		}
		result = append(result, RowWithPredicates{
			ID:             node.ID,
			Predicates:     node.Predicates,
			ChildLinks:     node.ChildLinks,
			TreePart:       row.TreePart,
			NodeText:       row.NodeText,
			ExecutionStats: node.ExecutionStats,
		})
	}

	return result, nil
}

func buildRenderedTree(qp *spannerplan.QueryPlan, link *sppb.PlanNode_ChildLink, level int, opts *options) (*renderedNode, error) {
	if !qp.IsVisible(link) {
		return nil, nil
	}

	sep := lo.Ternary(!opts.compact, " ", "")

	node := qp.GetNodeByChildLink(link)
	linkType := qp.GetLinkType(link)
	nodeText := lox.IfOrEmpty(linkType != "", "["+linkType+"]"+sep) + spannerplan.NodeTitle(node, opts.queryplanOptions...)
	// Only wrap when width is set and positive; 0 matches CLI/reference "no wrapping".
	if opts.wrapWidth != nil && *opts.wrapWidth > 0 {
		// Prefix width matches RenderTree (includes EdgeSeparator). Do not subtract sep again:
		// it is either absent from nodeText (empty linkType) or already inside the wrapped string.
		budget := *opts.wrapWidth - opts.prefixMetrics.MaxWidthForDepth(level)
		if budget < 1 {
			budget = 1
		}
		nodeText = opts.wrapper.Wrap(nodeText, budget)
	}

	var predicates []string
	for _, cl := range node.GetChildLinks() {
		if !qp.IsPredicate(cl) {
			continue
		}

		predicates = append(predicates, fmt.Sprintf("%s: %s",
			cl.GetType(),
			qp.GetNodeByChildLink(cl).GetShortRepresentation().GetDescription()))
	}

	resolvedChildLinks := lox.MapWithoutIndex(node.GetChildLinks(), qp.ResolveChildLink)

	scalarChildLinks := lox.FilterWithoutIndex(resolvedChildLinks, func(item *spannerplan.ResolvedChildLink) bool {
		return item.Child.GetKind() == sppb.PlanNode_SCALAR
	})

	childLinks := lo.GroupBy(scalarChildLinks, func(item *spannerplan.ResolvedChildLink) string {
		return item.ChildLink.GetType()
	})

	executionStats, err := stats.Extract(node, opts.disallowUnknownStats)
	if err != nil {
		return nil, err
	}

	visibleChildLinks := qp.VisibleChildLinks(node)
	rendered := &renderedNode{
		ID:             node.GetIndex(),
		NodeText:       nodeText,
		Predicates:     predicates,
		ExecutionStats: *executionStats,
		ChildLinks:     childLinks,
	}

	for _, child := range visibleChildLinks {
		renderedChild, err := buildRenderedTree(qp, child, level+1, opts)
		if err != nil {
			return nil, fmt.Errorf("buildRenderedTree failed on child link %v: %w", child, err)
		}
		if renderedChild != nil {
			rendered.Children = append(rendered.Children, renderedChild)
		}
	}
	return rendered, nil
}

func collectPreorder(root *renderedNode) []*renderedNode {
	var nodes []*renderedNode
	var walk func(*renderedNode)
	walk = func(n *renderedNode) {
		if n == nil {
			return
		}
		nodes = append(nodes, n)
		for _, child := range n.Children {
			walk(child)
		}
	}
	walk(root)
	return nodes
}
