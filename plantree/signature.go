package plantree

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/apstndb/spannerplan"
)

// StructuralSignatureVersion identifies the current alpha encoding revision.
// It may change in a later alpha release when the canonical encoding or its
// included fields change.
const StructuralSignatureVersion = "spannerplan.structural_signature.v1alpha1"

// StructuralSignature returns a deterministic, versioned canonical string that
// describes the visible relational plan tree for comparison.
//
// Included:
//   - operator display name
//   - link type in the parent for each child occurrence
//   - every present metadata key, recursively preserving value type and value
//   - predicate link types and short-representation descriptions, in ChildLinks order
//   - ordered visible child occurrences (DAG reuse expands like [ProcessPlan])
//
// Excluded:
//   - plan-node indexes / IDs
//   - subquery_cluster_node keys at any metadata struct depth, because their
//     values are PlanNode IDs
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
	metadata, err := signatureMetadata(node)
	if err != nil {
		return fmt.Errorf("plan node %d metadata: %w", node.GetIndex(), err)
	}
	appendSignatureFields(b, metadata)
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
	return []string{node.GetDisplayName()}
}

type signatureField struct {
	key   string
	value string
}

func signatureMetadata(node *sppb.PlanNode) ([]signatureField, error) {
	metadata := node.GetMetadata()
	if metadata != nil && len(metadata.ProtoReflect().GetUnknown()) != 0 {
		return nil, errors.New("metadata protobuf Struct contains unknown fields")
	}
	fields := metadata.GetFields()
	parts := make([]signatureField, 0, len(fields))
	for key, value := range fields {
		if key == "subquery_cluster_node" {
			continue
		}
		canonical, err := signatureValue(value)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", key, err)
		}
		parts = append(parts, signatureField{key: key, value: canonical})
	}
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].key < parts[j].key
	})
	return parts, nil
}

// signatureValue preserves valid protobuf Struct field presence, type, and
// value. Recursively framed structs and lists make non-string and future
// metadata deterministic without silently collapsing values through
// GetStringValue. Invalid, non-JSON, or forward-unknown Value states fail
// closed because this encoding cannot canonically represent them.
func signatureValue(value *structpb.Value) (string, error) {
	var b strings.Builder
	if err := appendSignatureValue(&b, value); err != nil {
		return "", err
	}
	return b.String(), nil
}

func appendSignatureValue(b *strings.Builder, value *structpb.Value) error {
	if value == nil {
		return errors.New("nil protobuf Value")
	}
	if len(value.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("protobuf Value contains unknown fields")
	}

	switch kind := value.Kind.(type) {
	case *structpb.Value_NullValue:
		if kind == nil || kind.NullValue != structpb.NullValue_NULL_VALUE {
			return errors.New("invalid protobuf null Value")
		}
		b.WriteString("null")
	case *structpb.Value_NumberValue:
		if kind == nil {
			return errors.New("nil protobuf number Value wrapper")
		}
		if math.IsNaN(kind.NumberValue) || math.IsInf(kind.NumberValue, 0) {
			return errors.New("non-finite protobuf number Value")
		}
		b.WriteString("number ")
		b.WriteString(strconv.FormatFloat(kind.NumberValue, 'g', -1, 64))
	case *structpb.Value_StringValue:
		if kind == nil {
			return errors.New("nil protobuf string Value wrapper")
		}
		b.WriteString("string ")
		appendSignatureString(b, kind.StringValue)
	case *structpb.Value_BoolValue:
		if kind == nil {
			return errors.New("nil protobuf bool Value wrapper")
		}
		b.WriteString("bool ")
		b.WriteString(strconv.FormatBool(kind.BoolValue))
	case *structpb.Value_StructValue:
		if kind == nil || kind.StructValue == nil {
			return errors.New("nil protobuf struct Value wrapper")
		}
		return appendSignatureStruct(b, kind.StructValue)
	case *structpb.Value_ListValue:
		if kind == nil || kind.ListValue == nil {
			return errors.New("nil protobuf list Value wrapper")
		}
		return appendSignatureList(b, kind.ListValue)
	case nil:
		return errors.New("protobuf Value kind is unset")
	default:
		return fmt.Errorf("unsupported protobuf Value kind %T", kind)
	}
	return nil
}

func appendSignatureStruct(b *strings.Builder, value *structpb.Struct) error {
	if len(value.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("protobuf Struct contains unknown fields")
	}
	fields := value.GetFields()
	keys := make([]string, 0, len(fields))
	for key := range fields {
		if key == "subquery_cluster_node" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	b.WriteString("struct ")
	b.WriteString(strconv.Itoa(len(keys)))
	b.WriteByte('[')
	for _, key := range keys {
		appendSignatureString(b, key)
		canonical, err := signatureValue(fields[key])
		if err != nil {
			return fmt.Errorf("struct field %q: %w", key, err)
		}
		appendSignatureString(b, canonical)
	}
	b.WriteByte(']')
	return nil
}

func appendSignatureList(b *strings.Builder, value *structpb.ListValue) error {
	if len(value.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("protobuf ListValue contains unknown fields")
	}
	values := value.GetValues()
	b.WriteString("list ")
	b.WriteString(strconv.Itoa(len(values)))
	b.WriteByte('[')
	for i, item := range values {
		canonical, err := signatureValue(item)
		if err != nil {
			return fmt.Errorf("list item %d: %w", i, err)
		}
		appendSignatureString(b, canonical)
	}
	b.WriteByte(']')
	return nil
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
