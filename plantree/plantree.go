package plantree

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/go-tabwrap"
	"github.com/samber/lo"

	"github.com/apstndb/spannerplan"
	"github.com/apstndb/spannerplan/stats"
	"github.com/apstndb/spannerplan/treerender"
)

var defaultWrapCondition = newDefaultWrapCondition()

const (
	// MaxPlantreeDepth counts the root as depth zero. This conservative
	// first-alpha renderer budget bounds recursive tree construction, row
	// collection, and rendering passes even when a plan contains a deep DAG.
	// It may be raised non-breakingly when real capture evidence requires it.
	MaxPlantreeDepth = 256
	// MaxPlantreeOccurrences bounds visible node occurrences, rather than
	// unique PlanNode indexes, because a DAG can expand exponentially when it
	// is rendered as a tree. This conservative first-alpha renderer budget may
	// be raised non-breakingly when real capture evidence requires it.
	MaxPlantreeOccurrences = 4096
)

// ErrTraversalLimitExceeded identifies a plan whose visible rendered tree
// exceeds Plantree's fixed resource bounds.
var ErrTraversalLimitExceeded = errors.New("plantree: traversal limit exceeded")

// TraversalLimitKind identifies which rendered-tree resource bound was exceeded.
type TraversalLimitKind string

const (
	// TraversalLimitDepth reports a node below MaxPlantreeDepth.
	TraversalLimitDepth TraversalLimitKind = "depth"
	// TraversalLimitOccurrences reports more than MaxPlantreeOccurrences
	// visible node occurrences.
	TraversalLimitOccurrences TraversalLimitKind = "occurrences"
)

// TraversalLimitError describes a rendered-tree resource bound failure.
// It unwraps to ErrTraversalLimitExceeded so callers can distinguish a valid
// but too-large plan from malformed-plan and rendering failures.
type TraversalLimitError struct {
	Kind      TraversalLimitKind
	Limit     int
	Observed  int
	NodeIndex int32
}

func (e *TraversalLimitError) Error() string {
	switch e.Kind {
	case TraversalLimitDepth:
		return fmt.Sprintf(
			"plan exceeds the renderer depth budget %d at PlanNode index %d",
			e.Limit,
			e.NodeIndex,
		)
	case TraversalLimitOccurrences:
		return fmt.Sprintf(
			"plan exceeds the renderer occurrence budget %d at PlanNode index %d",
			e.Limit,
			e.NodeIndex,
		)
	default:
		return fmt.Sprintf(
			"plan exceeds the renderer %s budget %d at PlanNode index %d",
			e.Kind,
			e.Limit,
			e.NodeIndex,
		)
	}
}

// Unwrap reports the stable traversal-limit sentinel.
func (e *TraversalLimitError) Unwrap() error { return ErrTraversalLimitExceeded }

type traversalState struct {
	occurrences int
}

func newDefaultWrapCondition() *tabwrap.Condition {
	c := tabwrap.NewCondition()
	c.TrimTrailingSpace = true // go-tabwrap v0.1.3+: stable diffs / CLI output
	return c
}

// RowWithPredicates is one rendered plan row plus predicate and execution metadata.
type RowWithPredicates struct {
	// ID is the Spanner PlanNode index for this row.
	ID int32
	// TreePart stores everything rendered before NodeText on each visual line: the ASCII tree prefix
	// plus any continuation padding inserted by the renderer for wrapping / hanging indent.
	// Prefer [RowWithPredicates.TreePartString] or [RowWithPredicates.TreePartLines] instead of
	// reading this field directly, so callers stay decoupled if the storage shape changes.
	TreePart string
	// NodeText is the rendered operator title, possibly split across visual lines.
	NodeText string
	// DisplayName is the raw Spanner PlanNode display name, before metadata is folded into NodeText.
	DisplayName string
	// Predicates contains filter predicate text associated with this row.
	Predicates []string
	// ExecutionStats contains execution statistics associated with this row.
	ExecutionStats stats.ExecutionStats
	// ScalarChildLinks contains this row's scalar child links in original PlanNode.ChildLinks order.
	ScalarChildLinks []ScalarChildLink
}

// ScalarChildLink is a scalar child link attached to a rendered plan row.
//
// It keeps raw-ish child-link fields so callers can group links by the parent
// row's DisplayName and the child-link Type. The same Type can have different
// semantics under different parent operators, for example Sort Key versus
// Aggregate Key.
type ScalarChildLink struct {
	// Type is the ChildLink type, such as "Condition", "Key", or "Agg".
	Type string
	// Variable is the ChildLink variable, when Spanner provides one.
	Variable string
	// Description is the scalar child node's short representation description.
	Description string
	// DisplayName is the scalar child node's raw PlanNode display name.
	DisplayName string
	// ChildIndex is the scalar child node's PlanNode index.
	ChildIndex int32
}

type renderedNode struct {
	ID                 int32
	ContinuationAnchor string
	NodeText           string
	DisplayName        string
	Predicates         []string
	ExecutionStats     stats.ExecutionStats
	ScalarChildLinks   []ScalarChildLink
	Children           []*renderedNode
}

// Text returns the full rendered row text, with the tree prefix prepended to each node text line.
func (r RowWithPredicates) Text() string {
	return treerender.Row{TreePart: r.TreePart, NodeText: r.NodeText}.Text()
}

// TreePartString returns the full tree-prefix string (newline-separated lines), matching the
// historical field encoding. Use this when you need a single string; use [RowWithPredicates.TreePartLines] for per-line access.
func (r RowWithPredicates) TreePartString() string {
	return r.TreePart
}

// TreePartLines splits [RowWithPredicates.TreePartString] into one prefix per visual line.
func (r RowWithPredicates) TreePartLines() []string {
	return treerender.Row{TreePart: r.TreePartString()}.TreePartLines()
}

// FormatID returns the display ID, prefixed with "*" when the row has predicates.
func (r RowWithPredicates) FormatID() string {
	return lo.Ternary(len(r.Predicates) != 0, "*", "") + strconv.Itoa(int(r.ID))
}

type options struct {
	disallowUnknownStats bool
	queryplanOptions     []spannerplan.Option
	style                treerender.Style
	compact              bool
	hangingIndent        bool
	wrapWidth            *int
	wrapper              *tabwrap.Condition
}

// Option configures [ProcessPlan].
type Option func(*options)

// DisallowUnknownStats makes [ProcessPlan] fail on unknown execution-stat keys.
func DisallowUnknownStats() Option {
	return func(o *options) {
		o.disallowUnknownStats = true
	}
}

// WithQueryPlanOptions forwards node-title formatting options to the underlying query plan renderer.
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

// WithHangingIndent hangs wrapped continuation lines after a node-local prefix such as
// `[Input] ` or `[Map] ` instead of keeping the default tree-aligned indentation.
func WithHangingIndent() Option {
	return func(o *options) {
		o.hangingIndent = true
	}
}

// ProcessPlan converts a query plan into rendered tree rows with predicate and execution metadata.
func ProcessPlan(qp *spannerplan.QueryPlan, opts ...Option) (rows []RowWithPredicates, err error) {
	o := options{
		style:   treerender.DefaultStyle(),
		wrapper: defaultWrapCondition,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&o)
	}
	if o.wrapper == nil {
		o.wrapper = defaultWrapCondition
	}
	if o.wrapWidth != nil && *o.wrapWidth < 0 {
		return nil, fmt.Errorf("wrap width cannot be negative: %d", *o.wrapWidth)
	}
	root, err := buildRenderedTree(qp, nil, -1, &o, make(map[int32]struct{}), &traversalState{})
	if err != nil {
		if errors.Is(err, ErrTraversalLimitExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("failed to build rendered tree: %w", err)
	}
	if root == nil {
		return nil, nil
	}

	wrapWidth := 0
	if o.wrapWidth != nil {
		wrapWidth = *o.wrapWidth
	}
	renderRows, err := treerender.RenderTreeWithOptions(root, o.style,
		func(n *renderedNode) string { return n.NodeText },
		func(n *renderedNode) []*renderedNode { return n.Children },
		treerender.RenderOptions[renderedNode]{
			GetContinuationAnchor: func(n *renderedNode) string { return n.ContinuationAnchor },
			WrapWidth:             wrapWidth,
			WrapCondition:         o.wrapper,
			ContinuationIndent:    mapHangingIndent(o.hangingIndent),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to render tree rows: %w", err)
	}
	nodes := collectPreorder(root)
	if len(renderRows) != len(nodes) {
		return nil, fmt.Errorf("unexpected rendered row count: got=%d want=%d", len(renderRows), len(nodes))
	}

	result := make([]RowWithPredicates, 0, len(nodes))
	for i, node := range nodes {
		row := renderRows[i]
		gotLines := strings.Count(row.NodeText, "\n") + 1
		if wantTreeLines := strings.Count(row.TreePart, "\n") + 1; gotLines != wantTreeLines {
			return nil, fmt.Errorf("unexpected rendered row line count for node %d: tree=%d node=%d", node.ID, wantTreeLines, gotLines)
		}
		result = append(result, RowWithPredicates{
			ID:               node.ID,
			DisplayName:      node.DisplayName,
			Predicates:       node.Predicates,
			ScalarChildLinks: node.ScalarChildLinks,
			TreePart:         row.TreePart,
			NodeText:         row.NodeText,
			ExecutionStats:   node.ExecutionStats,
		})
	}

	return result, nil
}

func buildRenderedTree(
	qp *spannerplan.QueryPlan,
	parent *sppb.PlanNode,
	childLinkIndex int,
	opts *options,
	ancestors map[int32]struct{},
	state *traversalState,
) (*renderedNode, error) {
	var link *sppb.PlanNode_ChildLink
	if parent != nil {
		childLinks := parent.GetChildLinks()
		if childLinkIndex < 0 || childLinkIndex >= len(childLinks) {
			return nil, fmt.Errorf("child link index out of range: parent node %d childLinks[%d]", parent.GetIndex(), childLinkIndex)
		}
		link = childLinks[childLinkIndex]
	}
	if !qp.IsVisible(link) {
		return nil, nil
	}

	sep := lo.Ternary(!opts.compact, " ", "")

	node := qp.GetNodeByChildLink(link)
	if node == nil {
		// spannerplan.New rejects nil nodes and out-of-range child links; keep
		// this guard so ProcessPlan still fails cleanly if that invariant changes.
		return nil, fmt.Errorf("plan node not found for link: %v", link)
	}
	if node.GetIndex() < 0 {
		return nil, fmt.Errorf("plan node index cannot be negative: %d", node.GetIndex())
	}
	if _, ok := ancestors[node.GetIndex()]; ok {
		return nil, fmt.Errorf("cycle detected at PlanNode index %d", node.GetIndex())
	}
	depth := len(ancestors)
	if depth > MaxPlantreeDepth {
		return nil, &TraversalLimitError{
			Kind:      TraversalLimitDepth,
			Limit:     MaxPlantreeDepth,
			Observed:  depth,
			NodeIndex: node.GetIndex(),
		}
	}
	if state.occurrences >= MaxPlantreeOccurrences {
		return nil, &TraversalLimitError{
			Kind:      TraversalLimitOccurrences,
			Limit:     MaxPlantreeOccurrences,
			Observed:  state.occurrences + 1,
			NodeIndex: node.GetIndex(),
		}
	}
	state.occurrences++
	ancestors[node.GetIndex()] = struct{}{}
	defer delete(ancestors, node.GetIndex())
	linkType := qp.LinkTypeInParent(parent, childLinkIndex)
	continuationAnchor := lo.Ternary(linkType != "", "["+linkType+"]"+sep, "")
	nodeText := continuationAnchor + spannerplan.NodeTitle(node, opts.queryplanOptions...)

	var predicates []string
	for _, cl := range node.GetChildLinks() {
		if !qp.IsPredicate(cl) {
			continue
		}

		predicates = append(predicates, fmt.Sprintf("%s: %s",
			cl.GetType(),
			qp.GetNodeByChildLink(cl).GetShortRepresentation().GetDescription()))
	}

	resolvedChildLinks := lo.Map(node.GetChildLinks(), func(item *sppb.PlanNode_ChildLink, _ int) *spannerplan.ResolvedChildLink {
		return qp.ResolveChildLink(item)
	})

	scalarChildLinks := lo.Filter(resolvedChildLinks, func(item *spannerplan.ResolvedChildLink, _ int) bool {
		return item.Child.GetKind() == sppb.PlanNode_SCALAR
	})

	renderedScalarChildLinks := lo.Map(scalarChildLinks, func(item *spannerplan.ResolvedChildLink, _ int) ScalarChildLink {
		return ScalarChildLink{
			Type:        item.ChildLink.GetType(),
			Variable:    item.ChildLink.GetVariable(),
			Description: item.Child.GetShortRepresentation().GetDescription(),
			DisplayName: item.Child.GetDisplayName(),
			ChildIndex:  item.Child.GetIndex(),
		}
	})

	executionStats, err := stats.Extract(node, opts.disallowUnknownStats)
	if err != nil {
		return nil, err
	}

	rendered := &renderedNode{
		ID:                 node.GetIndex(),
		ContinuationAnchor: continuationAnchor,
		NodeText:           nodeText,
		DisplayName:        node.GetDisplayName(),
		Predicates:         predicates,
		ExecutionStats:     *executionStats,
		ScalarChildLinks:   renderedScalarChildLinks,
	}

	for childIndex, child := range node.GetChildLinks() {
		if !qp.IsVisible(child) {
			continue
		}
		renderedChild, err := buildRenderedTree(qp, node, childIndex, opts, ancestors, state)
		if err != nil {
			if errors.Is(err, ErrTraversalLimitExceeded) {
				return nil, err
			}
			return nil, fmt.Errorf("buildRenderedTree failed on child link %v: %w", child, err)
		}
		if renderedChild != nil {
			rendered.Children = append(rendered.Children, renderedChild)
		}
	}
	return rendered, nil
}

func mapHangingIndent(enabled bool) treerender.ContinuationIndent {
	if enabled {
		return treerender.ContinuationIndentAnchor
	}
	return treerender.ContinuationIndentTree
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
