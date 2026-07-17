package plantree

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/apstndb/spannerplan"
)

func TestStructuralSignature_NilQueryPlan(t *testing.T) {
	_, err := StructuralSignature(nil)
	if err == nil || !strings.Contains(err.Error(), "QueryPlan is nil") {
		t.Fatalf("StructuralSignature(nil) error = %v, want nil QueryPlan error", err)
	}
}

func TestStructuralSignature_IgnoresIDsAndExecutionStats(t *testing.T) {
	base := []*sppb.PlanNode{
		{
			Index:       0,
			DisplayName: "Filter",
			Kind:        sppb.PlanNode_RELATIONAL,
			Metadata: mustStruct(t, map[string]any{
				"execution_method": "Row",
			}),
			ChildLinks: []*sppb.PlanNode_ChildLink{
				{ChildIndex: 1, Type: "Condition"},
				{ChildIndex: 2},
			},
		},
		{
			Index:       1,
			DisplayName: "Function",
			Kind:        sppb.PlanNode_SCALAR,
			ShortRepresentation: &sppb.PlanNode_ShortRepresentation{
				Description: "$id > 1",
			},
		},
		{
			Index:       2,
			DisplayName: "Scan",
			Kind:        sppb.PlanNode_RELATIONAL,
			Metadata: mustStruct(t, map[string]any{
				"scan_type":        "IndexScan",
				"scan_target":      "AlbumsByAlbumTitle",
				"execution_method": "Row",
				"Full scan":        "true",
			}),
		},
	}

	withStats := clonePlanNodes(base)
	withStats[0].ExecutionStats = mustStruct(t, map[string]any{
		"latency":         "1.5 msecs",
		"cpu_time":        "0.8 msecs",
		"rows_total":      "12",
		"execution_count": "1",
	})
	withStats[2].ExecutionStats = mustStruct(t, map[string]any{
		"latency": "0.4 msecs",
	})
	// Different non-root indexes must not affect the signature: rebuild with the
	// same topology after swapping the scalar and relational child positions.
	renumbered := []*sppb.PlanNode{
		{
			Index:       0,
			DisplayName: "Filter",
			Kind:        sppb.PlanNode_RELATIONAL,
			Metadata: mustStruct(t, map[string]any{
				"execution_method": "Row",
			}),
			ChildLinks: []*sppb.PlanNode_ChildLink{
				{ChildIndex: 2, Type: "Condition"},
				{ChildIndex: 1},
			},
		},
		{
			Index:       1,
			DisplayName: "Scan",
			Kind:        sppb.PlanNode_RELATIONAL,
			Metadata: mustStruct(t, map[string]any{
				"scan_type":        "IndexScan",
				"scan_target":      "AlbumsByAlbumTitle",
				"execution_method": "Row",
				"Full scan":        "true",
			}),
		},
		{
			Index:       2,
			DisplayName: "Function",
			Kind:        sppb.PlanNode_SCALAR,
			ShortRepresentation: &sppb.PlanNode_ShortRepresentation{
				Description: "$id > 1",
			},
		},
	}

	gotBase := mustSignature(t, base)
	gotStats := mustSignature(t, withStats)
	gotRenumbered := mustSignature(t, renumbered)

	if !strings.HasPrefix(gotBase, StructuralSignatureVersion+"\nnode 0 ") {
		t.Fatalf("signature does not start with its alpha version and root record:\n%s", gotBase)
	}
	if gotStats != gotBase {
		t.Fatalf("signature changed when execution stats were added:\nbase:\n%s\nwith stats:\n%s", gotBase, gotStats)
	}
	if gotRenumbered != gotBase {
		t.Fatalf("signature changed for isomorphic renumbered plan:\nbase:\n%s\nrenumbered:\n%s", gotBase, gotRenumbered)
	}
	if strings.Contains(gotBase, "ExecutionStats") || strings.Contains(gotBase, "latency") {
		t.Fatalf("signature unexpectedly embeds execution stats: %q", gotBase)
	}
}

func TestStructuralSignature_PreservesOrderedChildOccurrencesAndLinkTypes(t *testing.T) {
	qp, err := spannerplan.New([]*sppb.PlanNode{
		{Index: 0, DisplayName: "Root", Kind: sppb.PlanNode_RELATIONAL, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 1}, {ChildIndex: 2}}},
		{Index: 1, DisplayName: "Cross Apply", Kind: sppb.PlanNode_RELATIONAL, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 3}}},
		{Index: 2, DisplayName: "Hash Join", Kind: sppb.PlanNode_RELATIONAL, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 3}}},
		{Index: 3, DisplayName: "Shared Scan", Kind: sppb.PlanNode_RELATIONAL},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	got, err := StructuralSignature(qp)
	if err != nil {
		t.Fatalf("StructuralSignature() error = %v", err)
	}

	if strings.Count(got, "11:Shared Scan,") != 2 {
		t.Fatalf("signature did not expand the shared child twice:\n%s", got)
	}
	if strings.Count(got, "5:Input,") != 1 {
		t.Fatalf("signature did not retain the inferred Input link type:\n%s", got)
	}
}

func TestStructuralSignature_PreservesSameParentRepeatedChildOccurrences(t *testing.T) {
	qp, err := spannerplan.New([]*sppb.PlanNode{
		{Index: 0, DisplayName: "Root", Kind: sppb.PlanNode_RELATIONAL, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 1}}},
		{
			Index:       1,
			DisplayName: "Cross Apply",
			Kind:        sppb.PlanNode_RELATIONAL,
			ChildLinks: []*sppb.PlanNode_ChildLink{
				{ChildIndex: 2},
				{ChildIndex: 2, Type: "Map"},
			},
		},
		{Index: 2, DisplayName: "Shared Scan", Kind: sppb.PlanNode_RELATIONAL},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	got, err := StructuralSignature(qp)
	if err != nil {
		t.Fatalf("StructuralSignature() error = %v", err)
	}
	if strings.Count(got, "11:Shared Scan,") != 2 {
		t.Fatalf("signature did not preserve same-parent repeated occurrences:\n%s", got)
	}
	if strings.Count(got, "5:Input,") != 1 || strings.Count(got, "3:Map,") != 1 {
		t.Fatalf("signature did not preserve the raw child-link positions:\n%s", got)
	}
}

func TestStructuralSignature_CycleAndBudgets(t *testing.T) {
	t.Run("cycle", func(t *testing.T) {
		qp, err := spannerplan.New([]*sppb.PlanNode{
			{Index: 0, DisplayName: "Root", Kind: sppb.PlanNode_RELATIONAL, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 1}}},
			{Index: 1, DisplayName: "Child", Kind: sppb.PlanNode_RELATIONAL, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 0}}},
		})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		_, err = StructuralSignature(qp)
		if err == nil || !strings.Contains(err.Error(), "cycle detected at PlanNode index 0") {
			t.Fatalf("StructuralSignature() error = %v, want cycle error", err)
		}
		if errors.Is(err, ErrTraversalLimitExceeded) {
			t.Fatalf("StructuralSignature() error = %v, want cycle before traversal limit", err)
		}
	})

	t.Run("occurrence budget", func(t *testing.T) {
		childLinks := make([]*sppb.PlanNode_ChildLink, MaxPlantreeOccurrences)
		for i := range childLinks {
			childLinks[i] = &sppb.PlanNode_ChildLink{ChildIndex: 1}
		}
		qp, err := spannerplan.New([]*sppb.PlanNode{
			{Index: 0, DisplayName: "Root", Kind: sppb.PlanNode_RELATIONAL, ChildLinks: childLinks},
			{Index: 1, DisplayName: "Shared Scan", Kind: sppb.PlanNode_RELATIONAL},
		})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		_, err = StructuralSignature(qp)
		var limitErr *TraversalLimitError
		if !errors.As(err, &limitErr) || !errors.Is(err, ErrTraversalLimitExceeded) {
			t.Fatalf("StructuralSignature() error = %v, want TraversalLimitError", err)
		}
		if limitErr.Kind != TraversalLimitOccurrences {
			t.Fatalf("Kind = %q, want %q", limitErr.Kind, TraversalLimitOccurrences)
		}
	})

	t.Run("depth budget", func(t *testing.T) {
		nodes := make([]*sppb.PlanNode, MaxPlantreeDepth+2)
		for i := range nodes {
			nodes[i] = &sppb.PlanNode{
				Index:       int32(i),
				DisplayName: "Node",
				Kind:        sppb.PlanNode_RELATIONAL,
			}
			if i < len(nodes)-1 {
				nodes[i].ChildLinks = []*sppb.PlanNode_ChildLink{{ChildIndex: int32(i + 1)}}
			}
		}
		qp, err := spannerplan.New(nodes)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		_, err = StructuralSignature(qp)
		var limitErr *TraversalLimitError
		if !errors.As(err, &limitErr) || !errors.Is(err, ErrTraversalLimitExceeded) {
			t.Fatalf("StructuralSignature() error = %v, want TraversalLimitError", err)
		}
		if limitErr.Kind != TraversalLimitDepth {
			t.Fatalf("Kind = %q, want %q", limitErr.Kind, TraversalLimitDepth)
		}
	})
}

func TestStructuralSignature_FramesSpecialCharacters(t *testing.T) {
	qp, err := spannerplan.New([]*sppb.PlanNode{
		{
			Index:       0,
			DisplayName: "Filter|Odd",
			Kind:        sppb.PlanNode_RELATIONAL,
			Metadata: mustStruct(t, map[string]any{
				"execution_method": "Row;Batch",
				"scan_target":      "A\\B",
			}),
			ChildLinks: []*sppb.PlanNode_ChildLink{
				{ChildIndex: 1, Type: "Residual Condition"},
			},
		},
		{
			Index:       1,
			DisplayName: "Function",
			Kind:        sppb.PlanNode_SCALAR,
			ShortRepresentation: &sppb.PlanNode_ShortRepresentation{
				Description: "a|b;c\nd",
			},
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	got, err := StructuralSignature(qp)
	if err != nil {
		t.Fatalf("StructuralSignature() error = %v", err)
	}
	for _, want := range []string{
		"10:Filter|Odd,",
		"9:Row;Batch,",
		"3:A\\B,",
		"7:a|b;c\nd,",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("signature does not retain framed value %q:\n%s", want, got)
		}
	}
}

func TestStructuralSignature_DistinguishesIncludedComponentCollisions(t *testing.T) {
	t.Run("metadata", func(t *testing.T) {
		withEmbeddedDelimiter := mustSignature(t, []*sppb.PlanNode{
			{
				Index:       0,
				DisplayName: "Scan",
				Kind:        sppb.PlanNode_RELATIONAL,
				Metadata: mustStruct(t, map[string]any{
					"distribution_table": "Albums;execution_method=Row",
				}),
			},
		})
		withSeparateFields := mustSignature(t, []*sppb.PlanNode{
			{
				Index:       0,
				DisplayName: "Scan",
				Kind:        sppb.PlanNode_RELATIONAL,
				Metadata: mustStruct(t, map[string]any{
					"distribution_table": "Albums",
					"execution_method":   "Row",
				}),
			},
		})
		if withEmbeddedDelimiter == withSeparateFields {
			t.Fatalf("metadata delimiter collision:\n%s", withEmbeddedDelimiter)
		}
	})

	t.Run("predicates", func(t *testing.T) {
		withEmbeddedDelimiter := mustSignature(t, []*sppb.PlanNode{
			{
				Index:       0,
				DisplayName: "Filter",
				Kind:        sppb.PlanNode_RELATIONAL,
				ChildLinks:  []*sppb.PlanNode_ChildLink{{ChildIndex: 1, Type: "Condition"}},
			},
			{
				Index:       1,
				DisplayName: "Function",
				Kind:        sppb.PlanNode_SCALAR,
				ShortRepresentation: &sppb.PlanNode_ShortRepresentation{
					Description: "a;Condition:b",
				},
			},
		})
		withSeparatePredicates := mustSignature(t, []*sppb.PlanNode{
			{
				Index:       0,
				DisplayName: "Filter",
				Kind:        sppb.PlanNode_RELATIONAL,
				ChildLinks: []*sppb.PlanNode_ChildLink{
					{ChildIndex: 1, Type: "Condition"},
					{ChildIndex: 2, Type: "Condition"},
				},
			},
			{
				Index:       1,
				DisplayName: "Function",
				Kind:        sppb.PlanNode_SCALAR,
				ShortRepresentation: &sppb.PlanNode_ShortRepresentation{
					Description: "a",
				},
			},
			{
				Index:       2,
				DisplayName: "Function",
				Kind:        sppb.PlanNode_SCALAR,
				ShortRepresentation: &sppb.PlanNode_ShortRepresentation{
					Description: "b",
				},
			},
		})
		if withEmbeddedDelimiter == withSeparatePredicates {
			t.Fatalf("predicate delimiter collision:\n%s", withEmbeddedDelimiter)
		}
	})

	t.Run("operator components", func(t *testing.T) {
		withCombinedCallType := mustSignature(t, []*sppb.PlanNode{
			{
				Index:       0,
				DisplayName: "Scan",
				Kind:        sppb.PlanNode_RELATIONAL,
				Metadata: mustStruct(t, map[string]any{
					"call_type":     "Distributed Cross",
					"iterator_type": "Apply",
				}),
			},
		})
		withSeparateCallType := mustSignature(t, []*sppb.PlanNode{
			{
				Index:       0,
				DisplayName: "Scan",
				Kind:        sppb.PlanNode_RELATIONAL,
				Metadata: mustStruct(t, map[string]any{
					"call_type":     "Distributed",
					"iterator_type": "Cross Apply",
				}),
			},
		})
		if withCombinedCallType == withSeparateCallType {
			t.Fatalf("operator component collision:\n%s", withCombinedCallType)
		}
	})
}

func TestStructuralSignature_DCAGolden(t *testing.T) {
	got, err := StructuralSignature(decodeDCAPlan(t))
	if err != nil {
		t.Fatalf("StructuralSignature() error = %v", err)
	}

	goldenPath := filepath.Join("testdata", "signature", "dca.signature.txt")
	wantBytes, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", goldenPath, err)
	}
	want := string(wantBytes)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("DCA golden mismatch (-want +got):\n%s\n\nTo update: write StructuralSignature output to %s", diff, goldenPath)
	}
}

func mustSignature(t *testing.T, nodes []*sppb.PlanNode) string {
	t.Helper()
	qp, err := spannerplan.New(nodes)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	got, err := StructuralSignature(qp)
	if err != nil {
		t.Fatalf("StructuralSignature() error = %v", err)
	}
	return got
}

func clonePlanNodes(nodes []*sppb.PlanNode) []*sppb.PlanNode {
	out := make([]*sppb.PlanNode, len(nodes))
	for i, n := range nodes {
		cp := &sppb.PlanNode{
			Index:               n.GetIndex(),
			DisplayName:         n.GetDisplayName(),
			Kind:                n.GetKind(),
			Metadata:            n.GetMetadata(),
			ShortRepresentation: n.GetShortRepresentation(),
			ExecutionStats:      n.GetExecutionStats(),
		}
		if n.ChildLinks != nil {
			cp.ChildLinks = append([]*sppb.PlanNode_ChildLink(nil), n.ChildLinks...)
		}
		out[i] = cp
	}
	return out
}

func mustStruct(t *testing.T, values map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(values)
	if err != nil {
		t.Fatalf("structpb.NewStruct() error = %v", err)
	}
	return s
}
