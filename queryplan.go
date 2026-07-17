package spannerplan

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/samber/lo"
)

type QueryPlan struct {
	planNodes      []*sppb.PlanNode
	parentMap      map[int32]int32
	parentLinksMap map[int32][]ResolvedParentLink
}

// ErrInvalidPlan is the stable sentinel identifying any plan-validation
// failure returned by New. Every validation error wraps it, so consumers can
// detect a malformed plan with errors.Is(err, ErrInvalidPlan) regardless of
// the specific cause or the exact message wording, which is not guaranteed to
// remain stable.
var ErrInvalidPlan = errors.New("spannerplan: invalid plan")

// Category sentinels identify the specific kind of validation failure. Each is
// reported through a *ValidationError (see New) whose Kind field is set to one
// of these values and which also wraps ErrInvalidPlan. Match them with
// errors.Is; prefer ErrInvalidPlan when any validation failure should be
// treated the same way.
var (
	ErrEmptyPlanNodes           = errors.New("spannerplan: planNodes cannot be empty")
	ErrNilPlanNode              = errors.New("spannerplan: planNode cannot be nil")
	ErrPlanNodeIndexMismatch    = errors.New("spannerplan: planNode index must match slice position")
	ErrNilChildLink             = errors.New("spannerplan: childLink cannot be nil")
	ErrChildLinkIndexOutOfRange = errors.New("spannerplan: childLink childIndex out of range")
)

// ValidationError is returned by New (and any other constructor that validates
// its input) when a plan fails validation. It provides a stable, machine-
// readable identity for the failure so consumers can branch on it without
// pinning message text.
//
// Both errors.Is and errors.As work:
//
//   - errors.Is(err, spannerplan.ErrInvalidPlan) matches any validation failure.
//   - errors.Is(err, spannerplan.ErrChildLinkIndexOutOfRange) (and the other
//     category sentinels) matches a specific failure kind.
//   - errors.As(err, &verr) exposes the structured fields below.
//
// The Error string is preserved for humans but is not part of the stable API;
// use the sentinels or the fields instead.
type ValidationError struct {
	// Kind is the category sentinel for this failure, e.g.
	// ErrChildLinkIndexOutOfRange. It is always one of the exported
	// Err* sentinels; the ValidationError itself also wraps ErrInvalidPlan.
	Kind error
	// NodeIndex is the plan-node index (equivalently, slice position, which
	// New requires to match) involved in the failure, or -1 when no single
	// node applies (e.g. an empty slice).
	NodeIndex int
	// ChildIndex is the position within the parent node's ChildLinks slice
	// involved in the failure, or -1 when the failure is not about a specific
	// child link.
	ChildIndex int
	// err carries the full formatted message and wraps Kind, preserving the
	// historical error text and errors.Is behavior for the category sentinel.
	err error
}

// Error returns the full human-readable message. The exact wording is not part
// of the stable API and may change; match on ErrInvalidPlan, the category
// sentinels, or the struct fields instead.
func (e *ValidationError) Error() string { return e.err.Error() }

// Unwrap reports both ErrInvalidPlan and the wrapped category error, so that
// errors.Is matches ErrInvalidPlan and every specific sentinel.
func (e *ValidationError) Unwrap() []error { return []error{ErrInvalidPlan, e.err} }

// newValidationError builds a *ValidationError from a category sentinel and the
// fully formatted error (which itself wraps the sentinel to preserve message
// text and errors.Is behavior).
func newValidationError(kind error, nodeIndex, childIndex int, err error) *ValidationError {
	return &ValidationError{Kind: kind, NodeIndex: nodeIndex, ChildIndex: childIndex, err: err}
}

// New constructs a QueryPlan from sppb.QueryPlan.PlanNodes.
//
// The input must be the original PlanNodes slice from Cloud Spanner's
// sppb.QueryPlan. This function assumes each PlanNode.Index matches its
// position in the slice, as documented by the Spanner protobuf contract.
// Arbitrary or reordered PlanNode slices are not supported and will return
// an error.
//
// On validation failure it returns a *ValidationError that wraps ErrInvalidPlan
// and a category sentinel; see ValidationError.
func New(planNodes []*sppb.PlanNode) (*QueryPlan, error) {
	if len(planNodes) == 0 {
		return nil, newValidationError(ErrEmptyPlanNodes, -1, -1, ErrEmptyPlanNodes)
	}

	for i, planNode := range planNodes {
		if planNode == nil {
			return nil, newValidationError(ErrNilPlanNode, i, -1,
				fmt.Errorf("%w: at slice position %d", ErrNilPlanNode, i))
		}
		if planNode.GetIndex() != int32(i) {
			return nil, newValidationError(ErrPlanNodeIndexMismatch, i, -1,
				fmt.Errorf("%w: at slice position %d expected index %d, got %d", ErrPlanNodeIndexMismatch, i, i, planNode.GetIndex()))
		}
	}

	parentMap := make(map[int32]int32)
	parentLinksMap := make(map[int32][]ResolvedParentLink)
	for _, planNode := range planNodes {
		for j, childLink := range planNode.GetChildLinks() {
			if childLink == nil {
				return nil, newValidationError(ErrNilChildLink, int(planNode.GetIndex()), j,
					fmt.Errorf("%w: parent node %d childLinks[%d]", ErrNilChildLink, planNode.GetIndex(), j))
			}
			childIndex := childLink.GetChildIndex()
			if childIndex < 0 || childIndex >= int32(len(planNodes)) {
				return nil, newValidationError(ErrChildLinkIndexOutOfRange, int(planNode.GetIndex()), j,
					fmt.Errorf("%w: parent node %d childLinks[%d] has childIndex %d, len(planNodes)=%d", ErrChildLinkIndexOutOfRange, planNode.GetIndex(), j, childIndex, len(planNodes)))
			}
			parentMap[childIndex] = planNode.GetIndex()
			parentLinksMap[childIndex] = append(parentLinksMap[childIndex], ResolvedParentLink{
				Parent:    planNode,
				ChildLink: childLink,
			})
		}
	}

	return &QueryPlan{
		planNodes:      planNodes,
		parentMap:      parentMap,
		parentLinksMap: parentLinksMap,
	}, nil
}

func (qp *QueryPlan) HasStats() bool {
	return HasStats(qp.PlanNodes())
}

func (qp *QueryPlan) IsFunction(childLink *sppb.PlanNode_ChildLink) bool {
	child := qp.GetNodeByChildLink(childLink)
	return child.DisplayName == "Function"
}

func (qp *QueryPlan) IsPredicate(childLink *sppb.PlanNode_ChildLink) bool {
	// Known predicates are Search Predicate(Full Text Search), Condition(Filter, Hash Join), Seek Condition(FilterScan),
	// Residual Condition(FilterScan, Hash Join), or Split Range(Distributed Union).
	// Agg(Aggregate) is a Function but not a predicate.
	if childLink.GetType() == "Search Predicate" {
		return qp.GetNodeByChildLink(childLink).GetKind() == sppb.PlanNode_SCALAR
	}

	if !qp.IsFunction(childLink) {
		return false
	}

	if strings.HasSuffix(childLink.GetType(), "Condition") || childLink.GetType() == "Split Range" {
		return true
	}
	return false
}

func (qp *QueryPlan) PlanNodes() []*sppb.PlanNode {
	return qp.planNodes
}

func (qp *QueryPlan) GetNodeByIndex(id int32) *sppb.PlanNode {
	return qp.planNodes[id]
}

// IsVisible reports whether a child link should be rendered as part of the
// operator tree. Scalar PlanNodes are hidden unless the child link type is
// "Scalar", which represents scalar subquery-like operator subtrees.
// A nil link represents the root node.
func (qp *QueryPlan) IsVisible(link *sppb.PlanNode_ChildLink) bool {
	return qp.GetNodeByChildLink(link).GetKind() == sppb.PlanNode_RELATIONAL || link.GetType() == "Scalar"
}

func (qp *QueryPlan) VisibleChildLinks(node *sppb.PlanNode) []*sppb.PlanNode_ChildLink {
	var links []*sppb.PlanNode_ChildLink
	for _, link := range node.GetChildLinks() {
		if !qp.IsVisible(link) {
			continue
		}
		links = append(links, link)
	}
	return links
}

// GetNodeByChildLink returns PlanNode indicated by `link`.
// If `link` is nil, return the root node.
func (qp *QueryPlan) GetNodeByChildLink(link *sppb.PlanNode_ChildLink) *sppb.PlanNode {
	return qp.planNodes[link.GetChildIndex()]
}

func (qp *QueryPlan) GetParentNodeByChildIndex(index int32) *sppb.PlanNode {
	return qp.planNodes[qp.parentMap[index]]
}

func (qp *QueryPlan) GetParentNodeByChildLink(link *sppb.PlanNode_ChildLink) *sppb.PlanNode {
	return qp.GetParentNodeByChildIndex(link.GetChildIndex())
}

// ParentLinks returns all parent child links that point to childIndex.
//
// Links are returned in plan node traversal order, preserving each parent's
// ChildLinks order. The returned slice is a copy and can be modified by callers.
func (qp *QueryPlan) ParentLinks(childIndex int32) []ResolvedParentLink {
	return slices.Clone(qp.parentLinksMap[childIndex])
}

// ResolvedParentLink contains a parent PlanNode and the child link that points
// from that parent to a child node.
type ResolvedParentLink struct {
	Parent    *sppb.PlanNode
	ChildLink *sppb.PlanNode_ChildLink
}

type option struct {
	executionMethodFormat ExecutionMethodFormat
	targetMetadataFormat  TargetMetadataFormat
	knownFlagFormat       KnownFlagFormat
	compact               bool
	inlineStatsFunc       func(*sppb.PlanNode) []string
	hideMetadata          bool
}

type Option func(o *option)

type ExecutionMethodFormat int64

const (
	// ExecutionMethodFormatRaw prints execution_method metadata as is.
	ExecutionMethodFormatRaw ExecutionMethodFormat = iota

	// ExecutionMethodFormatAngle prints execution_method metadata after display_name with angle bracket like `Scan <Row>`.
	ExecutionMethodFormatAngle
)

// ParseExecutionMethodFormat parses string representation of ExecutionMethodFormat.
func ParseExecutionMethodFormat(s string) (ExecutionMethodFormat, error) {
	switch strings.ToUpper(s) {
	case "RAW":
		return ExecutionMethodFormatRaw, nil
	case "ANGLE":
		return ExecutionMethodFormatAngle, nil
	default:
		return ExecutionMethodFormatRaw, fmt.Errorf("invalid ExecutionMethodFormat, expect RAW or ANGLE: %s", s)
	}
}

// TargetMetadataFormat controls how to render target metadata.
// target metadata are scan_target, distribution_table, and table.
type TargetMetadataFormat int64

const (
	// TargetMetadataFormatRaw prints target metadata as is.
	TargetMetadataFormatRaw TargetMetadataFormat = iota

	// TargetMetadataFormatOn prints target metadata as `on <target>`.
	TargetMetadataFormatOn
)

// ParseTargetMetadataFormat parses string representation of TargetMetadataFormat.
func ParseTargetMetadataFormat(s string) (TargetMetadataFormat, error) {
	switch strings.ToUpper(s) {
	case "RAW":
		return TargetMetadataFormatRaw, nil
	case "ON":
		return TargetMetadataFormatOn, nil
	default:
		return TargetMetadataFormatRaw, fmt.Errorf("invalid TargetMetadataFormat, expect RAW or ON: %s", s)
	}
}

type KnownFlagFormat int64

const (
	// KnownFlagFormatRaw prints known boolean flag metadata as is.
	KnownFlagFormatRaw KnownFlagFormat = iota

	// KnownFlagFormatLabel prints known boolean flag metadata without value if true or omits if false.
	KnownFlagFormatLabel
)

// ParseKnownFlagFormat parses string representation of KnownFlagFormat.
func ParseKnownFlagFormat(s string) (KnownFlagFormat, error) {
	switch strings.ToUpper(s) {
	case "RAW":
		return KnownFlagFormatRaw, nil
	case "LABEL":
		return KnownFlagFormatLabel, nil
	default:
		return KnownFlagFormatRaw, fmt.Errorf("invalid KnownFlagFormat, expect RAW or LABEL: %s", s)
	}
}

func WithExecutionMethodFormat(fmt ExecutionMethodFormat) Option {
	return func(o *option) {
		o.executionMethodFormat = fmt
	}
}

func WithTargetMetadataFormat(fmt TargetMetadataFormat) Option {
	return func(o *option) {
		o.targetMetadataFormat = fmt
	}
}

func WithKnownFlagFormat(fmt KnownFlagFormat) Option {
	return func(o *option) {
		o.knownFlagFormat = fmt
	}
}

func WithInlineStatsFunc(f func(*sppb.PlanNode) []string) Option {
	return func(o *option) {
		o.inlineStatsFunc = f
	}
}

func EnableCompact() Option {
	return func(o *option) {
		o.compact = true
	}
}

// HideMetadata hides all metadata and labels even if KnownFlagFormatLabel is set.
// It is used by spannerplanviz.
func HideMetadata() Option {
	return func(o *option) {
		o.hideMetadata = true
	}
}

var (
	knownBooleanFlagKeys = []string{"Full scan", "split_ranges_aligned"}
	targetMetadataKeys   = []string{"scan_target", "distribution_table", "table"}
)

func NodeTitle(node *sppb.PlanNode, opts ...Option) string {
	var o option
	for _, opt := range opts {
		opt(&o)
	}

	sep := lo.Ternary(!o.compact, " ", "")

	metadataFields := node.GetMetadata().GetFields()

	executionMethod := metadataFields["execution_method"].GetStringValue()

	var target string
	for _, k := range targetMetadataKeys {
		if v := metadataFields[k].GetStringValue(); v != "" {
			target = v
			break
		}
	}

	operator := joinIfNotEmpty(" ",
		metadataFields["call_type"].GetStringValue(),
		metadataFields["iterator_type"].GetStringValue(),
		strings.TrimSuffix(metadataFields["scan_type"].GetStringValue(), "Scan"),
		node.GetDisplayName(),
		lo.Ternary(o.targetMetadataFormat == TargetMetadataFormatOn && len(target) > 0,
			"on "+target, ""),
	)

	executionMethodPart := lo.Ternary(
		o.executionMethodFormat == ExecutionMethodFormatAngle && len(executionMethod) > 0,
		"<"+executionMethod+">",
		"",
	)

	var labels []string
	var fields []string
	if !o.hideMetadata {
		for k, v := range metadataFields {
			if o.targetMetadataFormat != TargetMetadataFormatRaw && slices.Contains(targetMetadataKeys, k) {
				continue
			}

			switch k {
			case "call_type", "iterator_type": // Skip because it is displayed in node title
				continue
			case "scan_type": // Skip because it is combined with scan_target
				continue
			case "subquery_cluster_node": // Skip because it is useless
				continue
			case "scan_target":
				if o.targetMetadataFormat != TargetMetadataFormatRaw {
					continue
				}

				fields = append(fields, fmt.Sprintf("%s: %s",
					strings.TrimSuffix(metadataFields["scan_type"].GetStringValue(), "Scan"),
					v.GetStringValue()))
				continue
			case "execution_method":
				if o.executionMethodFormat != ExecutionMethodFormatRaw {
					continue
				}
			case "distribution_table", "table":
				if o.targetMetadataFormat != TargetMetadataFormatRaw {
					continue
				}
			}

			if o.knownFlagFormat != KnownFlagFormatRaw && slices.Contains(knownBooleanFlagKeys, k) {
				if v.GetStringValue() == "true" {
					labels = append(labels, k)
				}
				continue
			}
			fields = append(fields, fmt.Sprintf("%s:%s%s", k, sep, v.GetStringValue()))
		}
	}

	var inlineStats []string
	if o.inlineStatsFunc != nil {
		inlineStats = o.inlineStatsFunc(node)
	}

	sort.Strings(labels)
	sort.Strings(fields)

	return joinIfNotEmpty(sep, operator, executionMethodPart, encloseIfNotEmpty("(", strings.Join(slices.Concat(labels, fields, inlineStats), ","+sep), ")"))
}

func encloseIfNotEmpty(open, input, close string) string {
	if input == "" {
		return ""
	}
	return open + input + close
}

func joinIfNotEmpty(sep string, input ...string) string {
	var filtered []string
	for _, s := range input {
		if s != "" {
			filtered = append(filtered, s)
		}
	}
	return strings.Join(filtered, sep)
}

func (qp *QueryPlan) ResolveChildLink(item *sppb.PlanNode_ChildLink) *ResolvedChildLink {
	return &ResolvedChildLink{
		ChildLink: item,
		Child:     qp.GetNodeByChildLink(item),
	}
}

type ResolvedChildLink struct {
	ChildLink *sppb.PlanNode_ChildLink
	Child     *sppb.PlanNode
}

func HasStats(nodes []*sppb.PlanNode) bool {
	// hasStats returns true only if the first node has ExecutionStats.
	if len(nodes) == 0 {
		return false
	}

	return nodes[0].ExecutionStats != nil
}

// LinkTypeInParent returns the type for one child-link occurrence in parent.
//
// rawChildLinkIndex is the position in parent.ChildLinks, not the child
// PlanNode index. An explicit ChildLink.Type wins. For an untyped first child
// of an Apply operator, LinkTypeInParent returns "Input" to match the Spanner
// operator documentation. The parent and position are required because a
// PlanNode can have multiple parents and can appear more than once under the
// same parent.
//
// LinkTypeInParent returns an empty string when parent is nil or
// rawChildLinkIndex does not identify a child link. This lets callers safely
// use it while walking a plan without treating an invalid occurrence as an
// implicit Input edge.
func (qp *QueryPlan) LinkTypeInParent(parent *sppb.PlanNode, rawChildLinkIndex int) string {
	childLinks := parent.GetChildLinks()
	if rawChildLinkIndex < 0 || rawChildLinkIndex >= len(childLinks) {
		return ""
	}

	link := childLinks[rawChildLinkIndex]
	if link.GetType() != "" {
		return link.GetType()
	}

	// Treat only this raw child-link occurrence as Input. Comparing child node
	// indexes would incorrectly label every link to a shared child as Input.
	if rawChildLinkIndex == 0 && strings.HasSuffix(parent.GetDisplayName(), "Apply") {
		return "Input"
	}

	return ""
}
