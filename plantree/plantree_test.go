package plantree

import (
	_ "embed"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/treeprint"
	"github.com/google/go-cmp/cmp"

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
			TreePart: "\n",
			NodeText: "Distributed Union on AlbumsByAlbumTitle\n <Row>",
		},
		5: {
			ID:       5,
			TreePart: "   |        +- \n   |           ",
			NodeText: "Filter Scan <Row> (seeka\nble_key_size: 1)",
		},
		24: {
			ID:       24,
			TreePart: "         +- \n         |  ",
			NodeText: "[Input] KeyRangeAccumulator\n <Row>",
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

func TestProcessPlan_WithTreeprintOptions(t *testing.T) {
	opts := append(currentOptions(),
		WithTreeprintOptions(
			treeprint.WithEdgeTypeMid("=>"),
			treeprint.WithEdgeTypeEnd("=>"),
			treeprint.WithEdgeTypeLink(".."),
			treeprint.WithEdgeSeparator(""),
		),
	)
	rows, err := ProcessPlan(decodeDCAPlan(t), opts...)
	if err != nil {
		t.Fatalf("ProcessPlan() error = %v", err)
	}

	tests := map[int32]string{
		0: "",
		1: "=>",
		2: "   =>",
		3: "   ..  =>",
	}

	for id, want := range tests {
		if got := rowByID(t, rows, id).TreePart; got != want {
			t.Fatalf("row %d TreePart = %q, want %q", id, got, want)
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
