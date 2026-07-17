package plantree

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

	"github.com/apstndb/spannerplan"
)

// StructuralSignatureVersion is the version prefix embedded in every signature
// string. Bump it when the canonical encoding or included fields change.
const StructuralSignatureVersion = "spannerplan.structural_signature.v1"

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
// Compatibility: exact string equality is the interchange contract for a given
// StructuralSignatureVersion. Ports must golden-test against this encoding.
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
	operator := signatureOperator(node)
	meta := signatureMetadata(node)
	preds := signaturePredicates(qp, node)

	fmt.Fprintf(b, "%d|%s|%s|%s|%s\n",
		depth,
		escapeSignatureField(linkType),
		escapeSignatureField(operator),
		escapeSignatureField(meta),
		escapeSignatureField(preds),
	)

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

func signatureOperator(node *sppb.PlanNode) string {
	fields := node.GetMetadata().GetFields()
	return joinIfNotEmpty(" ",
		fields["call_type"].GetStringValue(),
		fields["iterator_type"].GetStringValue(),
		strings.TrimSuffix(fields["scan_type"].GetStringValue(), "Scan"),
		node.GetDisplayName(),
	)
}

func signatureMetadata(node *sppb.PlanNode) string {
	fields := node.GetMetadata().GetFields()
	var parts []string

	if v := fields["execution_method"].GetStringValue(); v != "" {
		parts = append(parts, "execution_method="+v)
	}
	for _, key := range signatureTargetKeys {
		if v := fields[key].GetStringValue(); v != "" {
			parts = append(parts, key+"="+v)
		}
	}
	for _, key := range signatureFlagKeys {
		if v := fields[key].GetStringValue(); v == "true" {
			parts = append(parts, key+"=true")
		}
	}
	slices.Sort(parts)
	return strings.Join(parts, ";")
}

func signaturePredicates(qp *spannerplan.QueryPlan, node *sppb.PlanNode) string {
	var parts []string
	for _, cl := range node.GetChildLinks() {
		if !qp.IsPredicate(cl) {
			continue
		}
		desc := qp.GetNodeByChildLink(cl).GetShortRepresentation().GetDescription()
		parts = append(parts, cl.GetType()+":"+desc)
	}
	return strings.Join(parts, ";")
}

// escapeSignatureField escapes one pipe-delimited column. The signature string
// is an opaque equality contract; columns are escaped so embedded '|',
// backslashes, and newlines cannot change the line shape.
func escapeSignatureField(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '|':
			b.WriteString(`\|`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
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
