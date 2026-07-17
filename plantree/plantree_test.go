package plantree

import (
	_ "embed"
	"errors"
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

func TestProcessPlan_RelationalCycleReturnsError(t *testing.T) {
	qp, err := spannerplan.New([]*sppb.PlanNode{
		{
			Index:       0,
			DisplayName: "Root",
			Kind:        sppb.PlanNode_RELATIONAL,
			ChildLinks:  []*sppb.PlanNode_ChildLink{{ChildIndex: 1}},
		},
		{
			Index:       1,
			DisplayName: "Child",
			Kind:        sppb.PlanNode_RELATIONAL,
			ChildLinks:  []*sppb.PlanNode_ChildLink{{ChildIndex: 0}},
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = ProcessPlan(qp)
	if err == nil || !strings.Contains(err.Error(), "cycle detected at PlanNode index 0") {
		t.Fatalf("ProcessPlan() error = %v, want cycle error", err)
	}
	if errors.Is(err, ErrTraversalLimitExceeded) {
		t.Fatalf("ProcessPlan() error = %v, want cycle error before traversal limit", err)
	}
}

func TestProcessPlan_ChecksCycleBeforeOccurrenceBudget(t *testing.T) {
	childLinks := make([]*sppb.PlanNode_ChildLink, MaxPlantreeOccurrences)
	for i := range childLinks[:len(childLinks)-1] {
		childLinks[i] = &sppb.PlanNode_ChildLink{ChildIndex: 1}
	}
	childLinks[len(childLinks)-1] = &sppb.PlanNode_ChildLink{ChildIndex: 0}
	qp, err := spannerplan.New([]*sppb.PlanNode{
		{Index: 0, DisplayName: "Root", Kind: sppb.PlanNode_RELATIONAL, ChildLinks: childLinks},
		{Index: 1, DisplayName: "Shared Scan", Kind: sppb.PlanNode_RELATIONAL},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = ProcessPlan(qp)
	if err == nil || !strings.Contains(err.Error(), "cycle detected at PlanNode index 0") {
		t.Fatalf("ProcessPlan() error = %v, want cycle error", err)
	}
	if errors.Is(err, ErrTraversalLimitExceeded) {
		t.Fatalf("ProcessPlan() error = %v, want cycle before occurrence budget", err)
	}
}

func TestProcessPlan_SharedChildUsesOccurrenceLocalLinkType(t *testing.T) {
	tests := []struct {
		name          string
		planNodes     []*sppb.PlanNode
		wantNodeTexts []string
	}{
		{
			name: "apply parent before non apply parent",
			planNodes: []*sppb.PlanNode{
				{Index: 0, DisplayName: "Root", Kind: sppb.PlanNode_RELATIONAL, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 1}, {ChildIndex: 2}}},
				{Index: 1, DisplayName: "Cross Apply", Kind: sppb.PlanNode_RELATIONAL, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 3}}},
				{Index: 2, DisplayName: "Hash Join", Kind: sppb.PlanNode_RELATIONAL, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 3}}},
				{Index: 3, DisplayName: "Shared Scan", Kind: sppb.PlanNode_RELATIONAL},
			},
			wantNodeTexts: []string{"Root", "Cross Apply", "[Input] Shared Scan", "Hash Join", "Shared Scan"},
		},
		{
			name: "non apply parent before apply parent",
			planNodes: []*sppb.PlanNode{
				{Index: 0, DisplayName: "Root", Kind: sppb.PlanNode_RELATIONAL, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 1}, {ChildIndex: 2}}},
				{Index: 1, DisplayName: "Hash Join", Kind: sppb.PlanNode_RELATIONAL, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 3}}},
				{Index: 2, DisplayName: "Cross Apply", Kind: sppb.PlanNode_RELATIONAL, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 3}}},
				{Index: 3, DisplayName: "Shared Scan", Kind: sppb.PlanNode_RELATIONAL},
			},
			wantNodeTexts: []string{"Root", "Hash Join", "Shared Scan", "Cross Apply", "[Input] Shared Scan"},
		},
		{
			name: "duplicate links to same child only label first occurrence",
			planNodes: []*sppb.PlanNode{
				{Index: 0, DisplayName: "Cross Apply", Kind: sppb.PlanNode_RELATIONAL, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 1}, {ChildIndex: 1}}},
				{Index: 1, DisplayName: "Shared Scan", Kind: sppb.PlanNode_RELATIONAL},
			},
			wantNodeTexts: []string{"Cross Apply", "[Input] Shared Scan", "Shared Scan"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qp, err := spannerplan.New(tt.planNodes)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			rows, err := ProcessPlan(qp)
			if err != nil {
				t.Fatalf("ProcessPlan() error = %v", err)
			}
			gotNodeTexts := make([]string, len(rows))
			for i, row := range rows {
				gotNodeTexts[i] = row.NodeText
			}
			if diff := cmp.Diff(tt.wantNodeTexts, gotNodeTexts); diff != "" {
				t.Fatalf("NodeText mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestProcessPlan_TraversalLimits(t *testing.T) {
	tests := []struct {
		name        string
		planNodes   []*sppb.PlanNode
		want        TraversalLimitError
		wantMessage string
	}{
		{
			name: "visible occurrences",
			planNodes: func() []*sppb.PlanNode {
				childLinks := make([]*sppb.PlanNode_ChildLink, MaxPlantreeOccurrences)
				for i := range childLinks {
					childLinks[i] = &sppb.PlanNode_ChildLink{ChildIndex: 1}
				}
				return []*sppb.PlanNode{
					{Index: 0, DisplayName: "Root", Kind: sppb.PlanNode_RELATIONAL, ChildLinks: childLinks},
					{Index: 1, DisplayName: "Shared Scan", Kind: sppb.PlanNode_RELATIONAL},
				}
			}(),
			want: TraversalLimitError{
				Kind:      TraversalLimitOccurrences,
				Limit:     MaxPlantreeOccurrences,
				Observed:  MaxPlantreeOccurrences + 1,
				NodeIndex: 1,
			},
			wantMessage: "plan exceeds the renderer occurrence budget 4096 at PlanNode index 1",
		},
		{
			name: "visible depth",
			planNodes: func() []*sppb.PlanNode {
				nodes := make([]*sppb.PlanNode, MaxPlantreeDepth+2)
				for i := range nodes {
					nodes[i] = &sppb.PlanNode{
						Index:       int32(i),
						DisplayName: "Node",
						Kind:        sppb.PlanNode_RELATIONAL,
					}
					if i+1 < len(nodes) {
						nodes[i].ChildLinks = []*sppb.PlanNode_ChildLink{{ChildIndex: int32(i + 1)}}
					}
				}
				return nodes
			}(),
			want: TraversalLimitError{
				Kind:      TraversalLimitDepth,
				Limit:     MaxPlantreeDepth,
				Observed:  MaxPlantreeDepth + 1,
				NodeIndex: MaxPlantreeDepth + 1,
			},
			wantMessage: "plan exceeds the renderer depth budget 256 at PlanNode index 257",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qp, err := spannerplan.New(tt.planNodes)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			_, err = ProcessPlan(qp)
			if !errors.Is(err, ErrTraversalLimitExceeded) {
				t.Fatalf("ProcessPlan() error = %v, want ErrTraversalLimitExceeded", err)
			}
			if got := err.Error(); got != tt.wantMessage {
				t.Fatalf("ProcessPlan() error = %q, want %q", got, tt.wantMessage)
			}
			var got *TraversalLimitError
			if !errors.As(err, &got) {
				t.Fatalf("ProcessPlan() error = %v, want TraversalLimitError", err)
			}
			if diff := cmp.Diff(tt.want, *got); diff != "" {
				t.Fatalf("TraversalLimitError mismatch (-want +got):\n%s", diff)
			}
			if got := got.Error(); got != tt.wantMessage {
				t.Fatalf("TraversalLimitError.Error() = %q, want %q", got, tt.wantMessage)
			}
		})
	}
}

func TestProcessPlan_FullTextSearchPredicates(t *testing.T) {
	tests := []struct {
		name      string
		planNodes []*sppb.PlanNode
		want      []string
	}{
		{
			name: "simple search predicate",
			planNodes: []*sppb.PlanNode{
				{
					Index:       0,
					DisplayName: "Scan",
					Kind:        sppb.PlanNode_RELATIONAL,
					ChildLinks: []*sppb.PlanNode_ChildLink{
						{ChildIndex: 1, Type: "Search Predicate"},
					},
				},
				{
					Index:       1,
					DisplayName: "Search Predicate",
					Kind:        sppb.PlanNode_SCALAR,
					ShortRepresentation: &sppb.PlanNode_ShortRepresentation{
						Description: "SEARCH(Tokens, 'blue')",
					},
				},
			},
			want: []string{"Search Predicate: SEARCH(Tokens, 'blue')"},
		},
		{
			name: "compound search predicate function",
			planNodes: []*sppb.PlanNode{
				{
					Index:       0,
					DisplayName: "Scan",
					Kind:        sppb.PlanNode_RELATIONAL,
					ChildLinks: []*sppb.PlanNode_ChildLink{
						{ChildIndex: 1, Type: "Search Predicate"},
					},
				},
				{
					Index:       1,
					DisplayName: "Function",
					Kind:        sppb.PlanNode_SCALAR,
					ChildLinks: []*sppb.PlanNode_ChildLink{
						{ChildIndex: 2, Type: "Search Predicate"},
						{ChildIndex: 3, Type: "Search Predicate"},
					},
					ShortRepresentation: &sppb.PlanNode_ShortRepresentation{
						Description: "(SEARCH(Tokens, 'blue') AND SEARCH(Tokens, 'green'))",
					},
				},
				{
					Index:       2,
					DisplayName: "Search Predicate",
					Kind:        sppb.PlanNode_SCALAR,
					ShortRepresentation: &sppb.PlanNode_ShortRepresentation{
						Description: "SEARCH(Tokens, 'blue')",
					},
				},
				{
					Index:       3,
					DisplayName: "Search Predicate",
					Kind:        sppb.PlanNode_SCALAR,
					ShortRepresentation: &sppb.PlanNode_ShortRepresentation{
						Description: "SEARCH(Tokens, 'green')",
					},
				},
			},
			want: []string{"Search Predicate: (SEARCH(Tokens, 'blue') AND SEARCH(Tokens, 'green'))"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qp, err := spannerplan.New(tt.planNodes)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			rows, err := ProcessPlan(qp)
			if err != nil {
				t.Fatalf("ProcessPlan() error = %v", err)
			}

			got := rowByID(t, rows, 0)
			if diff := cmp.Diff(tt.want, got.Predicates); diff != "" {
				t.Fatalf("Predicates mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestProcessPlan_ScalarChildLinksPreserveParentContextAndOrder(t *testing.T) {
	qp, err := spannerplan.New([]*sppb.PlanNode{
		{
			Index:       0,
			DisplayName: "Sort",
			Kind:        sppb.PlanNode_RELATIONAL,
			ChildLinks: []*sppb.PlanNode_ChildLink{
				{ChildIndex: 1, Type: "Key", Variable: "sort_key"},
				{ChildIndex: 2, Type: "Value", Variable: "sort_value"},
				{ChildIndex: 3},
			},
		},
		{
			Index:       1,
			DisplayName: "Reference",
			Kind:        sppb.PlanNode_SCALAR,
			ShortRepresentation: &sppb.PlanNode_ShortRepresentation{
				Description: "$SongGenre",
			},
		},
		{
			Index:       2,
			DisplayName: "Reference",
			Kind:        sppb.PlanNode_SCALAR,
			ShortRepresentation: &sppb.PlanNode_ShortRepresentation{
				Description: "$SongName",
			},
		},
		{
			Index:       3,
			DisplayName: "Aggregate",
			Kind:        sppb.PlanNode_RELATIONAL,
			ChildLinks: []*sppb.PlanNode_ChildLink{
				{ChildIndex: 4, Type: "Key", Variable: "group_key"},
				{ChildIndex: 5, Type: "Agg", Variable: "song_count"},
			},
		},
		{
			Index:       4,
			DisplayName: "Reference",
			Kind:        sppb.PlanNode_SCALAR,
			ShortRepresentation: &sppb.PlanNode_ShortRepresentation{
				Description: "$SingerId",
			},
		},
		{
			Index:       5,
			DisplayName: "Function",
			Kind:        sppb.PlanNode_SCALAR,
			ShortRepresentation: &sppb.PlanNode_ShortRepresentation{
				Description: "COUNT(*)",
			},
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	rows, err := ProcessPlan(qp)
	if err != nil {
		t.Fatalf("ProcessPlan() error = %v", err)
	}

	sortRow := rowByID(t, rows, 0)
	if sortRow.DisplayName != "Sort" {
		t.Fatalf("sort row DisplayName = %q, want Sort", sortRow.DisplayName)
	}
	wantSortLinks := []ScalarChildLink{
		{Type: "Key", Variable: "sort_key", Description: "$SongGenre", DisplayName: "Reference", ChildIndex: 1},
		{Type: "Value", Variable: "sort_value", Description: "$SongName", DisplayName: "Reference", ChildIndex: 2},
	}
	if diff := cmp.Diff(wantSortLinks, sortRow.ScalarChildLinks); diff != "" {
		t.Fatalf("sort ScalarChildLinks mismatch (-want +got):\n%s", diff)
	}
	aggregateRow := rowByID(t, rows, 3)
	wantAggregateLinks := []ScalarChildLink{
		{Type: "Key", Variable: "group_key", Description: "$SingerId", DisplayName: "Reference", ChildIndex: 4},
		{Type: "Agg", Variable: "song_count", Description: "COUNT(*)", DisplayName: "Function", ChildIndex: 5},
	}
	if diff := cmp.Diff(wantAggregateLinks, aggregateRow.ScalarChildLinks); diff != "" {
		t.Fatalf("aggregate ScalarChildLinks mismatch (-want +got):\n%s", diff)
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

func hangingIndentChildGuidePlan(t *testing.T) *spannerplan.QueryPlan {
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
			ChildLinks: []*sppb.PlanNode_ChildLink{
				{ChildIndex: 3},
			},
			Metadata: &structpb.Struct{Fields: map[string]*structpb.Value{
				"execution_method": structpb.NewStringValue("Row"),
			}},
		},
		{
			Index:       2,
			DisplayName: "Serialize Result",
			Kind:        sppb.PlanNode_RELATIONAL,
		},
		{
			Index:       3,
			DisplayName: "Filter Scan",
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

func TestProcessPlan_HangingIndentKeepsChildGuide(t *testing.T) {
	opts := append(currentOptions(), WithWrapWidth(21), WithHangingIndent())
	rows, err := ProcessPlan(hangingIndentChildGuidePlan(t), opts...)
	if err != nil {
		t.Fatalf("ProcessPlan() error = %v", err)
	}

	got := rowByID(t, rows, 1)
	want := RowWithPredicates{
		ID:       1,
		TreePart: "+- \n|  |       ",
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
