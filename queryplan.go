package spannerplan

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/lox"
	"github.com/samber/lo"
)

type QueryPlan struct {
	planNodes []*sppb.PlanNode
	parentMap map[int32]int32
}

var ErrEmptyPlanNodes = errors.New("spannerplan: planNodes cannot be empty")

func New(planNodes []*sppb.PlanNode) (*QueryPlan, error) {
	if len(planNodes) == 0 {
		return nil, ErrEmptyPlanNodes
	}

	parentMap := make(map[int32]int32)
	for _, planNode := range planNodes {
		for _, childLink := range planNode.GetChildLinks() {
			parentMap[childLink.GetChildIndex()] = planNode.GetIndex()
		}
	}

	return &QueryPlan{planNodes, parentMap}, nil
}

func (qp *QueryPlan) HasStats() bool {
	return HasStats(qp.PlanNodes())
}

func (qp *QueryPlan) IsFunction(childLink *sppb.PlanNode_ChildLink) bool {
	// Known predicates are Condition(Filter, Hash Join) or Seek Condition(FilterScan) or Residual Condition(FilterScan, Hash Join) or Split Range(Distributed Union).
	// Agg(Aggregate) is a Function but not a predicate.
	child := qp.GetNodeByChildLink(childLink)
	return child.DisplayName == "Function"
}

func (qp *QueryPlan) IsPredicate(childLink *sppb.PlanNode_ChildLink) bool {
	// Known predicates are Condition(Filter, Hash Join) or Seek Condition(FilterScan) or Residual Condition(FilterScan, Hash Join) or Split Range(Distributed Union).
	// Agg(Aggregate) is a Function but not a predicate.
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
type FullScanFormat = KnownFlagFormat

const (
	// KnownFlagFormatRaw prints known boolean flag metadata as is.
	KnownFlagFormatRaw KnownFlagFormat = iota

	// KnownFlagFormatLabel prints known boolean flag metadata without value if true or omits if false.
	KnownFlagFormatLabel

	// Deprecated: FullScanFormatRaw is an alias of KnownFlagFormatRaw.
	FullScanFormatRaw = KnownFlagFormatRaw

	// Deprecated: FullScanFormatLabel is an alias of KnownFlagFormatLabel.
	FullScanFormatLabel = KnownFlagFormatLabel
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

// Deprecated: WithFullScanFormat is an alias of WithKnownFlagFormat.
func WithFullScanFormat(fmt FullScanFormat) Option {
	return WithKnownFlagFormat(fmt)
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

	sep := lox.IfOrEmpty(!o.compact, " ")

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

	executionMethodPart := lox.IfOrEmpty(o.executionMethodFormat == ExecutionMethodFormatAngle && len(executionMethod) > 0,
		"<"+executionMethod+">")

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

func (qp *QueryPlan) GetLinkType(link *sppb.PlanNode_ChildLink) string {
	if link.GetType() != "" {
		return link.GetType()
	}

	// Workaround to treat the first child of Apply as Input.
	// This is necessary because it is more consistent with the official query plan operator docs.
	// Note: Apply variants are Cross Apply, Anti Semi Apply, Semi Apply, Outer Apply, and their Distributed variants.
	if parent := qp.GetParentNodeByChildLink(link); parent != nil &&
		strings.HasSuffix(parent.GetDisplayName(), "Apply") &&
		len(parent.GetChildLinks()) > 0 && parent.GetChildLinks()[0].GetChildIndex() == link.GetChildIndex() {
		return "Input"
	}

	return ""
}
