package plantree

import (
	"fmt"
	"strconv"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/lox"
	"github.com/apstndb/treeprint"
	"github.com/mattn/go-runewidth"
	"github.com/samber/lo"

	"github.com/apstndb/spannerplan"
	"github.com/apstndb/spannerplan/stats"
)

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

func (n *renderedNode) String() string {
	return n.NodeText
}

func (n *renderedNode) lineCount() int {
	return len(strings.Split(n.NodeText, "\n"))
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
	treeprintOptions     []treeprint.Option
	compact              bool
	indentSize           int
	wrapWidth            *int
	wrapper              *runewidth.Condition
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

func WithTreeprintOptions(opts ...treeprint.Option) Option {
	return func(o *options) {
		o.treeprintOptions = append(o.treeprintOptions, opts...)
	}
}

func WithWrapWidth(width int) Option {
	return func(o *options) {
		o.wrapWidth = &width
	}
}

func WithWrapper(wrapper *runewidth.Condition) Option {
	return func(o *options) {
		o.wrapper = wrapper
	}
}

// EnableCompact enables compact node title mode.
func EnableCompact() Option {
	return func(o *options) {
		o.indentSize = 0
		o.compact = true
		o.queryplanOptions = append(o.queryplanOptions, spannerplan.EnableCompact())
		o.treeprintOptions = append(
			o.treeprintOptions,
			treeprint.WithEdgeTypeLink("|"),
			treeprint.WithEdgeTypeMid("+"),
			treeprint.WithEdgeTypeEnd("+"),
			treeprint.WithIndentSize(0),
			treeprint.WithEdgeSeparator(""),
		)
	}
}

func ProcessPlan(qp *spannerplan.QueryPlan, opts ...Option) (rows []RowWithPredicates, err error) {
	o := options{
		indentSize: 2,
		// default values to be override
		treeprintOptions: []treeprint.Option{
			treeprint.WithEdgeTypeLink("|"),
			treeprint.WithEdgeTypeMid("+-"),
			treeprint.WithEdgeTypeEnd("+-"),
			treeprint.WithIndentSize(2),
			treeprint.WithEdgeSeparator(" "),
		},
	}
	for _, opt := range opts {
		opt(&o)
	}

	if o.wrapper == nil {
		o.wrapper = runewidth.NewCondition()
	}

	root, err := buildRenderedTree(qp, nil, 0, &o)
	if err != nil {
		return nil, fmt.Errorf("failed to build rendered tree: %w", err)
	}

	tree := newTreeprintTree(root)
	renderedLines := splitRenderedLines(tree.StringWithOptions(o.treeprintOptions...))
	nodes := collectPreorder(root)

	var result []RowWithPredicates
	result = make([]RowWithPredicates, 0, len(nodes))
	cursor := 0
	for _, node := range nodes {
		lineCount := node.lineCount()
		if cursor+lineCount > len(renderedLines) {
			return nil, fmt.Errorf("unexpected rendered tree line count: cursor=%d, need=%d, total=%d", cursor, lineCount, len(renderedLines))
		}

		treePart, err := deriveTreePart(renderedLines[cursor:cursor+lineCount], node.NodeText)
		if err != nil {
			return nil, err
		}
		cursor += lineCount

		result = append(result, RowWithPredicates{
			ID:             node.ID,
			Predicates:     node.Predicates,
			ChildLinks:     node.ChildLinks,
			TreePart:       treePart,
			NodeText:       node.NodeText,
			ExecutionStats: node.ExecutionStats,
		})
	}

	if cursor != len(renderedLines) {
		return nil, fmt.Errorf("unexpected rendered tree line count: consumed=%d, total=%d", cursor, len(renderedLines))
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
	if opts.wrapWidth != nil {
		nodeText = opts.wrapper.Wrap(nodeText, *opts.wrapWidth-level*(opts.indentSize+1)-opts.wrapper.StringWidth(sep))
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
			return nil, fmt.Errorf("buildRenderedTree failed on link %v: %w", link, err)
		}
		if renderedChild != nil {
			rendered.Children = append(rendered.Children, renderedChild)
		}
	}
	return rendered, nil
}

func newTreeprintTree(root *renderedNode) treeprint.Tree {
	tree := treeprint.New()
	tree.SetValue(root)
	appendTreeprintChildren(tree, root.Children)
	return tree
}

func appendTreeprintChildren(parent treeprint.Tree, children []*renderedNode) {
	for _, child := range children {
		var branch treeprint.Tree
		if len(child.Children) > 0 {
			branch = parent.AddBranch(child)
		} else {
			branch = parent.AddNode(child)
		}

		if len(child.Children) > 0 {
			appendTreeprintChildren(branch, child.Children)
		}
	}
}

func collectPreorder(root *renderedNode) []*renderedNode {
	nodes := []*renderedNode{root}
	for _, child := range root.Children {
		nodes = append(nodes, collectPreorder(child)...)
	}
	return nodes
}

func splitRenderedLines(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(strings.TrimSuffix(s, "\n"), "\n")
}

func deriveTreePart(renderedLines []string, nodeText string) (string, error) {
	nodeLines := strings.Split(nodeText, "\n")
	if len(renderedLines) != len(nodeLines) {
		return "", fmt.Errorf("unexpected rendered node line count: got=%d want=%d", len(renderedLines), len(nodeLines))
	}

	treeLines := make([]string, len(renderedLines))
	for i, line := range renderedLines {
		if !strings.HasSuffix(line, nodeLines[i]) {
			return "", fmt.Errorf("unexpected rendered tree line %q for node line %q", line, nodeLines[i])
		}
		treeLines[i] = strings.TrimSuffix(line, nodeLines[i])
	}

	return strings.Join(treeLines, "\n"), nil
}
