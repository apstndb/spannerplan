package plantree

import (
	_ "embed"
	"strings"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"

	"github.com/apstndb/spannerplan"
)

// treePartLines splits a legacy single-string tree prefix into per-line parts (tests only).
func treePartLines(s string) []string {
	return strings.Split(s, "\n")
}

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
			TreePart:   treePartLines(""),
			NodeText:   "Distributed Union on AlbumsByAlbumTitle <Row>",
			Predicates: []string{"Split Range: (STARTS_WITH($AlbumTitle, 'T') AND ($AlbumTitle LIKE 'T%e'))"},
		},
		1: {
			ID:         1,
			TreePart:   treePartLines("+- "),
			NodeText:   "Distributed Cross Apply <Row>",
			Predicates: []string{"Split Range: (($SingerId' = $SingerId) AND ($AlbumId' = $AlbumId))"},
		},
		2: {ID: 2, TreePart: treePartLines("   +- "), NodeText: "[Input] Create Batch <Row>"},
		3: {ID: 3, TreePart: treePartLines("   |  +- "), NodeText: "Local Distributed Union <Row>"},
	}

	for id, want := range tests {
		got := rowByID(t, rows, id)
		if diff := cmp.Diff(want, RowWithPredicates{
			ID:         got.ID,
			TreePart:   got.TreePart,
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
			TreePart: treePartLines("\n"),
			NodeText: "Distributed Union on AlbumsByAlbumTitle\n<Row>",
		},
		5: {
			ID:       5,
			TreePart: treePartLines("   |        +- \n   |           "),
			NodeText: "Filter Scan <Row> (seekab\nle_key_size: 1)",
		},
		24: {
			ID:       24,
			TreePart: treePartLines("         +- \n         |  "),
			NodeText: "[Input] KeyRangeAccumulator\n<Row>",
		},
	}

	for id, want := range tests {
		got := rowByID(t, rows, id)
		if diff := cmp.Diff(want, RowWithPredicates{
			ID:       got.ID,
			TreePart: got.TreePart,
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
		if diff := cmp.Diff(base[i].TreePart, withZero[i].TreePart); diff != "" {
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

func TestProcessPlan_CompactFormatting(t *testing.T) {
	opts := append(currentOptions(), EnableCompact())
	rows, err := ProcessPlan(decodeDCAPlan(t), opts...)
	if err != nil {
		t.Fatalf("ProcessPlan() error = %v", err)
	}

	tests := map[int32][]string{
		0: {""},
		1: {"+"},
		2: {" +"},
		3: {" |+"},
	}

	for id, want := range tests {
		got := rowByID(t, rows, id).TreePart
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("row %d TreePart mismatch (-want +got):\n%s", id, diff)
		}
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
