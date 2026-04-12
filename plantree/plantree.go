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

var defaultWrapCondition = tabwrap.NewCondition()

type RowWithPredicates struct {
	ID             int32
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
	treeLines := strings.Split(r.TreePart, "\n")
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

func (r RowWithPredicates) FormatID() string {
	return lox.IfOrEmpty(len(r.Predicates) != 0, "*") + strconv.Itoa(int(r.ID))
}

type options struct {
	disallowUnknownStats bool
	queryplanOptions     []spannerplan.Option
	style                treerender.Style
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

	root, err := buildRenderedTree(qp, nil, 0, &o)
	if err != nil {
		return nil, fmt.Errorf("failed to build rendered tree: %w", err)
	}
	if root == nil {
		return nil, nil
	}

	renderRows := treerender.Render(toRenderNode(root), o.style)
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

func trimWrappedLinesRight(s string) string {
	if s == "" {
		return s
	}
	lines := strings.Split(s, "\n")
	for i := range lines {
		lines[i] = strings.TrimRight(lines[i], " \t")
	}
	return strings.Join(lines, "\n")
}

func buildRenderedTree(qp *spannerplan.QueryPlan, link *sppb.PlanNode_ChildLink, level int, opts *options) (*renderedNode, error) {
	if !qp.IsVisible(link) {
		return nil, nil
	}

	sep := lo.Ternary(!opts.compact, " ", "")

	node := qp.GetNodeByChildLink(link)
	linkType := qp.GetLinkType(link)
	nodeText := lox.IfOrEmpty(linkType != "", "["+linkType+"]"+sep) + spannerplan.NodeTitle(node, opts.queryplanOptions...)
	if opts.wrapWidth != nil {
		budget := *opts.wrapWidth - treerender.MaxPrefixWidthForDepth(opts.style, level)
		if budget < 1 {
			budget = 1
		}
		nodeText = trimWrappedLinesRight(opts.wrapper.Wrap(nodeText, budget))
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

func toRenderNode(root *renderedNode) *treerender.Node {
	if root == nil {
		return nil
	}

	node := &treerender.Node{
		Text:     root.NodeText,
		Children: make([]*treerender.Node, 0, len(root.Children)),
	}
	for _, child := range root.Children {
		node.Children = append(node.Children, toRenderNode(child))
	}
	return node
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
