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
	// Different indexes must not affect the signature: rebuild with the same
	// topology but renumbered indexes via a fresh New() on an isomorphic plan.
	renumbered := []*sppb.PlanNode{
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

	gotBase := mustSignature(t, base)
	gotStats := mustSignature(t, withStats)
	gotRenumbered := mustSignature(t, renumbered)

	want := strings.Join([]string{
		StructuralSignatureVersion,
		"0||Filter|execution_method=Row|Condition:$id > 1",
		"1||Index Scan|Full scan=true;execution_method=Row;scan_target=AlbumsByAlbumTitle|",
		"",
	}, "\n")

	if diff := cmp.Diff(want, gotBase); diff != "" {
		t.Fatalf("base signature mismatch (-want +got):\n%s", diff)
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
	// Plan-node indexes must not appear as identity fields. Depth `0`/`1` are
	// structural positions, not Spanner PlanNode.Index values.
	for _, line := range strings.Split(strings.TrimSuffix(gotBase, "\n"), "\n")[1:] {
		fields := strings.SplitN(line, "|", 5)
		if len(fields) != 5 {
			t.Fatalf("unexpected line shape: %q", line)
		}
		if fields[2] == "0" || fields[2] == "2" {
			t.Fatalf("operator field looks like a plan-node id: %q", line)
		}
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

	want := strings.Join([]string{
		StructuralSignatureVersion,
		"0||Root||",
		"1||Cross Apply||",
		"2|Input|Shared Scan||",
		"1||Hash Join||",
		"2||Shared Scan||",
		"",
	}, "\n")
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("signature mismatch (-want +got):\n%s", diff)
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

func TestStructuralSignature_EscapesSpecialCharacters(t *testing.T) {
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
	want := strings.Join([]string{
		StructuralSignatureVersion,
		`0||Filter\|Odd|execution_method=Row;Batch;scan_target=A\\B|Residual Condition:a\|b;c\nd`,
		"",
	}, "\n")
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("signature mismatch (-want +got):\n%s", diff)
	}
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
