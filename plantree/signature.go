package plantree

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

	"github.com/apstndb/spannerplan"
)

// StructuralSignatureVersion identifies the current alpha encoding revision.
// It may change in a later alpha release when the canonical encoding or its
// included fields change.
const StructuralSignatureVersion = "spannerplan.structural_signature.v1alpha1"

// signatureTargetKeys are PlanNode metadata keys treated as structural targets.
var signatureTargetKeys = []string{"scan_target", "distribution_table", "table"}

// signatureFlagKeys are boolean-ish metadata flags that affect plan shape.
var signatureFlagKeys = []string{"Full scan", "split_ranges_aligned"}

// StructuralSignature returns a deterministic, versioned canonical string that
// describes the visible relational plan tree for comparison.
//
// Included:
//   - operator identity (display name plus call_type / iterator_type / scan_type)
//   - link type in the parent for each child occurrence
//   - structural metadata targets and flags (scan_target, distribution_table,
//     table, execution_method, Full scan, split_ranges_aligned)
//   - predicate link types and short-representation descriptions, in ChildLinks order
//   - ordered visible child occurrences (DAG reuse expands like [ProcessPlan])
//
// Excluded:
//   - plan-node indexes / IDs
//   - subquery_cluster_node
//   - execution statistics and other volatile runtime metrics
//   - rendered titles, wrapping, and ASCII tree prefixes
//
// Traversal uses the same depth and occurrence budgets as [ProcessPlan]
// ([MaxPlantreeDepth], [MaxPlantreeOccurrences]) and the same cycle detection.
// Cycles return an error. Budget failures return [ErrTraversalLimitExceeded]
// via [TraversalLimitError]. Malformed plans should be rejected by
// [spannerplan.New] before calling this function; remaining link/node guards
// fail with ordinary errors.
//
// Equality is meaningful only for signatures produced by this same alpha
// encoding revision. The encoding may change during the alpha, so it is not a
// stable cross-version or cross-language interchange contract.
//
// Collision limitations: the signature deliberately omits node IDs, so repeated
// identical operators or shared DAG subtrees produce identical fragments.
// Comparison / matching layers must expose that ambiguity rather than silently
// pairing nodes. Equal signatures do not prove the plans are the same physical
// capture; unequal signatures prove a structural difference within this version's
// included fields.
//
// This API is intentionally not the machine-readable PlanTreeNode surface
// discussed in issue #30 and does not expose viewer structured-row DTOs.
func StructuralSignature(qp *spannerplan.QueryPlan) (string, error) {
	if qp == nil {
		return "", errors.New("plantree: QueryPlan is nil")
	}

	var b strings.Builder
	b.WriteString(StructuralSignatureVersion)
	b.WriteByte('\n')

	err := writeSignatureNode(qp, nil, -1, 0, make(map[int32]struct{}), &traversalState{}, &b)
	if err != nil {
		return "", err
	}
	return b.String(), nil
}

func writeSignatureNode(
	qp *spannerplan.QueryPlan,
	parent *sppb.PlanNode,
	childLinkIndex int,
	depth int,
	ancestors map[int32]struct{},
	state *traversalState,
	b *strings.Builder,
) error {
	var link *sppb.PlanNode_ChildLink
	if parent != nil {
		childLinks := parent.GetChildLinks()
		if childLinkIndex < 0 || childLinkIndex >= len(childLinks) {
			return fmt.Errorf("child link index out of range: parent node %d childLinks[%d]", parent.GetIndex(), childLinkIndex)
		}
		link = childLinks[childLinkIndex]
	}
	if !qp.IsVisible(link) {
		return nil
	}

	node := qp.GetNodeByChildLink(link)
	if node == nil {
		return fmt.Errorf("plan node not found for link: %v", link)
	}
	if node.GetIndex() < 0 {
		return fmt.Errorf("plan node index cannot be negative: %d", node.GetIndex())
	}
	if _, ok := ancestors[node.GetIndex()]; ok {
		return fmt.Errorf("cycle detected at PlanNode index %d", node.GetIndex())
	}
	if depth > MaxPlantreeDepth {
		return &TraversalLimitError{
			Kind:      TraversalLimitDepth,
			Limit:     MaxPlantreeDepth,
			Observed:  depth,
			NodeIndex: node.GetIndex(),
		}
	}
	if state.occurrences >= MaxPlantreeOccurrences {
		return &TraversalLimitError{
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
	b.WriteString("node ")
	b.WriteString(strconv.Itoa(depth))
	b.WriteByte(' ')
	appendSignatureString(b, linkType)
	b.WriteByte(' ')
	appendSignatureStrings(b, signatureOperator(node))
	b.WriteByte(' ')
	appendSignatureFields(b, signatureMetadata(node))
	b.WriteByte(' ')
	appendSignatureFields(b, signaturePredicates(qp, node))
	b.WriteByte('\n')

	for idx, child := range node.GetChildLinks() {
		if !qp.IsVisible(child) {
			continue
		}
		if err := writeSignatureNode(qp, node, idx, depth+1, ancestors, state, b); err != nil {
			if errors.Is(err, ErrTraversalLimitExceeded) {
				return err
			}
			return fmt.Errorf("structural signature failed on child link %v: %w", child, err)
		}
	}
	return nil
}

func signatureOperator(node *sppb.PlanNode) []string {
	fields := node.GetMetadata().GetFields()
	return []string{
		fields["call_type"].GetStringValue(),
		fields["iterator_type"].GetStringValue(),
		strings.TrimSuffix(fields["scan_type"].GetStringValue(), "Scan"),
		node.GetDisplayName(),
	}
}

type signatureField struct {
	key   string
	value string
}

func signatureMetadata(node *sppb.PlanNode) []signatureField {
	fields := node.GetMetadata().GetFields()
	var parts []signatureField

	if v := fields["execution_method"].GetStringValue(); v != "" {
		parts = append(parts, signatureField{key: "execution_method", value: v})
	}
	for _, key := range signatureTargetKeys {
		if v := fields[key].GetStringValue(); v != "" {
			parts = append(parts, signatureField{key: key, value: v})
		}
	}
	for _, key := range signatureFlagKeys {
		if v := fields[key].GetStringValue(); v == "true" {
			parts = append(parts, signatureField{key: key, value: "true"})
		}
	}
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].key < parts[j].key
	})
	return parts
}

func signaturePredicates(qp *spannerplan.QueryPlan, node *sppb.PlanNode) []signatureField {
	var parts []signatureField
	for _, cl := range node.GetChildLinks() {
		if !qp.IsPredicate(cl) {
			continue
		}
		desc := qp.GetNodeByChildLink(cl).GetShortRepresentation().GetDescription()
		parts = append(parts, signatureField{key: cl.GetType(), value: desc})
	}
	return parts
}

// appendSignatureString appends one byte-length-prefixed string. The trailing
// comma is part of the framing: no escaping is needed, so every included
// string remains distinguishable even when it contains delimiters or newlines.
func appendSignatureString(b *strings.Builder, s string) {
	b.WriteString(strconv.Itoa(len(s)))
	b.WriteByte(':')
	b.WriteString(s)
	b.WriteByte(',')
}

// appendSignatureStrings frames a fixed-order list of included string values.
func appendSignatureStrings(b *strings.Builder, values []string) {
	b.WriteString(strconv.Itoa(len(values)))
	b.WriteByte('[')
	for _, value := range values {
		appendSignatureString(b, value)
	}
	b.WriteByte(']')
}

// appendSignatureFields frames an ordered list of key/value pairs. Metadata is
// key-sorted before this call; predicates retain ChildLinks order.
func appendSignatureFields(b *strings.Builder, fields []signatureField) {
	b.WriteString(strconv.Itoa(len(fields)))
	b.WriteByte('[')
	for _, field := range fields {
		b.WriteByte('(')
		appendSignatureString(b, field.key)
		appendSignatureString(b, field.value)
		b.WriteByte(')')
	}
	b.WriteByte(']')
}
