package plantree

import (
	_ "embed"
	"strings"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/apstndb/spannerplan"
)

//go:embed reference/testdata/dca.yaml
var dcaYAML []byte

func decodeDCAPlan(t *testing.T) *spannerplan.QueryPlan {
	t.Helper()

	stats, _, err := spannerplan.ExtractQueryPlan(dcaYAML)
	if err != nil {
		t.Fatalf("ExtractQueryPlan() error = %v", err)
	}

	qp, err := spannerplan.New(stats.GetQueryPlan().GetPlanNodes())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	return qp
}

func currentOptions() []Option {
	return []Option{
		WithQueryPlanOptions(
			spannerplan.WithTargetMetadataFormat(spannerplan.TargetMetadataFormatOn),
			spannerplan.WithExecutionMethodFormat(spannerplan.ExecutionMethodFormatAngle),
			spannerplan.WithKnownFlagFormat(spannerplan.KnownFlagFormatLabel),
		),
	}
}

func rowByID(t *testing.T, rows []RowWithPredicates, id int32) RowWithPredicates {
	t.Helper()
	for _, row := range rows {
		if row.ID == id {
			return row
		}
	}
	t.Fatalf("row with ID %d not found", id)
	return RowWithPredicates{}
}

func TestProcessPlan_CurrentFormatting(t *testing.T) {
	rows, err := ProcessPlan(decodeDCAPlan(t), currentOptions()...)
	if err != nil {
		t.Fatalf("ProcessPlan() error = %v", err)
	}

	tests := map[int32]RowWithPredicates{
		0: {
			ID:         0,
			TreePart:   "",
			NodeText:   "Distributed Union on AlbumsByAlbumTitle <Row>",
			Predicates: []string{"Split Range: (STARTS_WITH($AlbumTitle, 'T') AND ($AlbumTitle LIKE 'T%e'))"},
		},
		1: {
			ID:         1,
			TreePart:   "+- ",
			NodeText:   "Distributed Cross Apply <Row>",
			Predicates: []string{"Split Range: (($SingerId' = $SingerId) AND ($AlbumId' = $AlbumId))"},
		},
		2: {ID: 2, TreePart: "   +- ", NodeText: "[Input] Create Batch <Row>"},
		3: {ID: 3, TreePart: "   |  +- ", NodeText: "Local Distributed Union <Row>"},
	}

	for id, want := range tests {
		got := rowByID(t, rows, id)
		if diff := cmp.Diff(want, RowWithPredicates{
			ID:         got.ID,
			TreePart:   got.TreePartString(),
			NodeText:   got.NodeText,
			Predicates: got.Predicates,
		}); diff != "" {
			t.Fatalf("row %d mismatch (-want +got):\n%s", id, diff)
		}
	}
}

func TestProcessPlan_WrapWidthPreservesTreeAndNodeParts(t *testing.T) {
	opts := append(currentOptions(), WithWrapWidth(40))
	rows, err := ProcessPlan(decodeDCAPlan(t), opts...)
	if err != nil {
		t.Fatalf("ProcessPlan() error = %v", err)
	}

	tests := map[int32]RowWithPredicates{
		0: {
			ID:       0,
			TreePart: "\n",
			NodeText: "Distributed Union on AlbumsByAlbumTitle\n<Row>",
		},
		5: {
			ID:       5,
			TreePart: "   |        +- \n   |           ",
			NodeText: "Filter Scan <Row> (seekab\nle_key_size: 1)",
		},
		24: {
			ID:       24,
			TreePart: "         +- \n         |  ",
			NodeText: "[Input] KeyRangeAccumulator\n<Row>",
		},
	}

	for id, want := range tests {
		got := rowByID(t, rows, id)
		if diff := cmp.Diff(want, RowWithPredicates{
			ID:       got.ID,
			TreePart: got.TreePartString(),
			NodeText: got.NodeText,
		}); diff != "" {
			t.Fatalf("wrapped row %d mismatch (-want +got):\n%s", id, diff)
		}
		if id != 0 && got.Text() == got.NodeText {
			t.Fatalf("wrapped row %d Text() = NodeText, want tree prefix contribution", id)
		}
	}
}

func nilWrapperOption() Option {
	return func(o *options) {
		o.wrapper = nil
	}
}

func TestProcessPlan_NilWrapperFallsBackToDefault(t *testing.T) {
	t.Parallel()
	opts := append(currentOptions(), nilWrapperOption(), WithWrapWidth(40))
	if _, err := ProcessPlan(decodeDCAPlan(t), opts...); err != nil {
		t.Fatalf("ProcessPlan() error = %v", err)
	}
}

func TestProcessPlan_TinyWrapWidthDoesNotPanic(t *testing.T) {
	t.Parallel()

	rows, err := ProcessPlan(decodeDCAPlan(t), append(currentOptions(), WithWrapWidth(1))...)
	if err != nil {
		t.Fatalf("ProcessPlan() error = %v", err)
	}
	if len(rows) == 0 {
		t.Fatalf("ProcessPlan() rows = %v, want non-empty", rows)
	}
}

func TestProcessPlan_WrapWidthZeroDisablesWrapping(t *testing.T) {
	t.Parallel()
	plan := decodeDCAPlan(t)
	base, err := ProcessPlan(plan, currentOptions()...)
	if err != nil {
		t.Fatalf("ProcessPlan() error = %v", err)
	}
	withZero, err := ProcessPlan(plan, append(currentOptions(), WithWrapWidth(0))...)
	if err != nil {
		t.Fatalf("ProcessPlan(WithWrapWidth(0)) error = %v", err)
	}
	if len(base) != len(withZero) {
		t.Fatalf("row count: base=%d WithWrapWidth(0)=%d", len(base), len(withZero))
	}
	for i := range base {
		if base[i].ID != withZero[i].ID {
			t.Fatalf("row %d ID: base=%d zero=%d", i, base[i].ID, withZero[i].ID)
		}
		if diff := cmp.Diff(base[i].TreePartString(), withZero[i].TreePartString()); diff != "" {
			t.Fatalf("row %d (id=%d) TreePart mismatch (-want +got):\n%s", i, base[i].ID, diff)
		}
		if base[i].NodeText != withZero[i].NodeText {
			t.Fatalf("row %d (id=%d): NodeText base=%q zero=%q", i, base[i].ID, base[i].NodeText, withZero[i].NodeText)
		}
	}
}

func TestProcessPlan_NegativeWrapWidthErrors(t *testing.T) {
	t.Parallel()
	_, err := ProcessPlan(decodeDCAPlan(t), append(currentOptions(), WithWrapWidth(-1))...)
	if err == nil {
		t.Fatal("ProcessPlan(WithWrapWidth(-1)) error = nil, want non-nil")
	}
}

func invalidContinuationIndentOption() Option {
	return func(o *options) {
		indent := ContinuationIndent(99)
		o.continuationIndent = &indent
	}
}

func TestProcessPlan_CompactFormatting(t *testing.T) {
	opts := append(currentOptions(), EnableCompact())
	rows, err := ProcessPlan(decodeDCAPlan(t), opts...)
	if err != nil {
		t.Fatalf("ProcessPlan() error = %v", err)
	}

	tests := map[int32]string{
		0: "",
		1: "+",
		2: " +",
		3: " |+",
	}

	for id, want := range tests {
		if got := rowByID(t, rows, id).TreePartString(); got != want {
			t.Fatalf("row %d TreePart = %q, want %q", id, got, want)
		}
	}
}

func TestRowWithPredicates_TreePartAccessors(t *testing.T) {
	t.Parallel()
	r := RowWithPredicates{TreePart: "  +- \n|  ", NodeText: "a\nb"}
	if got, want := r.TreePartString(), r.TreePart; got != want {
		t.Fatalf("TreePartString = %q, want %q", got, want)
	}
	if diff := cmp.Diff([]string{"  +- ", "|  "}, r.TreePartLines()); diff != "" {
		t.Fatalf("TreePartLines (-want +got):\n%s", diff)
	}
}

func TestProcessPlan_NilOptionSkipped(t *testing.T) {
	t.Parallel()
	plan := decodeDCAPlan(t)
	var nilOpt Option
	if _, err := ProcessPlan(plan, nilOpt); err != nil {
		t.Fatalf("ProcessPlan(nil Option) = %v", err)
	}
	opts := []Option{nil, nil}
	if _, err := ProcessPlan(plan, opts...); err != nil {
		t.Fatalf("ProcessPlan(slice with nil options) = %v", err)
	}
}

func TestProcessPlan_InvisibleRootReturnsEmpty(t *testing.T) {
	qp, err := spannerplan.New([]*sppb.PlanNode{{
		Index:       0,
		DisplayName: "Scalar Root",
		Kind:        sppb.PlanNode_SCALAR,
	}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	rows, err := ProcessPlan(qp)
	if err != nil {
		t.Fatalf("ProcessPlan() error = %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("ProcessPlan() rows = %v, want empty", rows)
	}
}

func hangingIndentPlan(t *testing.T) *spannerplan.QueryPlan {
	t.Helper()

	qp, err := spannerplan.New([]*sppb.PlanNode{
		{
			Index:       0,
			DisplayName: "Cross Apply",
			Kind:        sppb.PlanNode_RELATIONAL,
			ChildLinks: []*sppb.PlanNode_ChildLink{
				{ChildIndex: 1},
				{ChildIndex: 2, Type: "Map"},
			},
		},
		{
			Index:       1,
			DisplayName: "Batch Scan",
			Kind:        sppb.PlanNode_RELATIONAL,
			Metadata: &structpb.Struct{Fields: map[string]*structpb.Value{
				"execution_method": structpb.NewStringValue("Row"),
			}},
		},
		{
			Index:       2,
			DisplayName: "Serialize Result",
			Kind:        sppb.PlanNode_RELATIONAL,
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return qp
}

func TestProcessPlan_HangingIndent(t *testing.T) {
	opts := append(currentOptions(), WithWrapWidth(21), WithHangingIndent())
	rows, err := ProcessPlan(hangingIndentPlan(t), opts...)
	if err != nil {
		t.Fatalf("ProcessPlan() error = %v", err)
	}

	got := rowByID(t, rows, 1)
	want := RowWithPredicates{
		ID:       1,
		TreePart: "+- \n|          ",
		NodeText: "[Input] Batch Scan\n <Row>",
	}
	if diff := cmp.Diff(want, RowWithPredicates{
		ID:       got.ID,
		TreePart: got.TreePartString(),
		NodeText: got.NodeText,
	}); diff != "" {
		t.Fatalf("row 1 mismatch (-want +got):\n%s", diff)
	}
}

func TestProcessPlan_DeprecatedContinuationIndentNodePrefix(t *testing.T) {
	withDeprecated, err := ProcessPlan(hangingIndentPlan(t), append(currentOptions(),
		WithWrapWidth(21),
		WithContinuationIndent(ContinuationIndentNodePrefix),
	)...)
	if err != nil {
		t.Fatalf("ProcessPlan(deprecated option) error = %v", err)
	}

	withHanging, err := ProcessPlan(hangingIndentPlan(t), append(currentOptions(),
		WithWrapWidth(21),
		WithHangingIndent(),
	)...)
	if err != nil {
		t.Fatalf("ProcessPlan(WithHangingIndent) error = %v", err)
	}

	if diff := cmp.Diff(withHanging, withDeprecated); diff != "" {
		t.Fatalf("deprecated continuation-indent option mismatch (-want +got):\n%s", diff)
	}
}

func TestProcessPlan_InvalidContinuationIndentErrors(t *testing.T) {
	t.Parallel()

	_, err := ProcessPlan(hangingIndentPlan(t), append(currentOptions(), invalidContinuationIndentOption())...)
	if err == nil {
		t.Fatal("ProcessPlan(invalid continuation indent) error = nil, want non-nil")
	}
	if got := err.Error(); !strings.Contains(got, "unknown ContinuationIndent") {
		t.Fatalf("ProcessPlan() error = %q, want unknown ContinuationIndent", got)
	}
}
