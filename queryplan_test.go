package spannerplan

import (
	"errors"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name      string
		input     []*sppb.PlanNode
		wantErr   error
		postCheck func(t *testing.T, qp *QueryPlan)
	}{
		{
			name:    "empty",
			input:   nil,
			wantErr: ErrEmptyPlanNodes,
		},
		{
			name: "nil node",
			input: []*sppb.PlanNode{
				nil,
			},
			wantErr: ErrNilPlanNode,
		},
		{
			name: "index mismatch",
			input: []*sppb.PlanNode{
				{Index: 1},
			},
			wantErr: ErrPlanNodeIndexMismatch,
		},
		{
			name: "nil child link",
			input: []*sppb.PlanNode{
				{Index: 0, ChildLinks: []*sppb.PlanNode_ChildLink{nil}},
			},
			wantErr: ErrNilChildLink,
		},
		{
			name: "child link index out of range",
			input: []*sppb.PlanNode{
				{Index: 0, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 2}}},
				{Index: 1},
			},
			wantErr: ErrChildLinkIndexOutOfRange,
		},
		{
			name: "valid query plan nodes",
			input: []*sppb.PlanNode{
				{Index: 0, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 1}}},
				{Index: 1},
			},
			postCheck: func(t *testing.T, qp *QueryPlan) {
				t.Helper()
				if got := qp.GetParentNodeByChildIndex(1).GetIndex(); got != 0 {
					t.Fatalf("GetParentNodeByChildIndex(1) = %d, want 0", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qp, err := New(tt.input)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("New() error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if qp == nil {
				t.Fatal("New() returned nil QueryPlan")
			}
			if tt.postCheck != nil {
				tt.postCheck(t, qp)
			}
		})
	}
}

func TestNewValidationError(t *testing.T) {
	tests := []struct {
		name           string
		input          []*sppb.PlanNode
		wantKind       error
		wantNodeIndex  int
		wantChildIndex int
		wantMessage    string
	}{
		{
			name:           "empty",
			input:          nil,
			wantKind:       ErrEmptyPlanNodes,
			wantNodeIndex:  -1,
			wantChildIndex: -1,
			wantMessage:    "spannerplan: planNodes cannot be empty",
		},
		{
			name:           "nil node",
			input:          []*sppb.PlanNode{nil},
			wantKind:       ErrNilPlanNode,
			wantNodeIndex:  0,
			wantChildIndex: -1,
			wantMessage:    "spannerplan: planNode cannot be nil: at slice position 0",
		},
		{
			name:           "index mismatch",
			input:          []*sppb.PlanNode{{Index: 1}},
			wantKind:       ErrPlanNodeIndexMismatch,
			wantNodeIndex:  0,
			wantChildIndex: -1,
			wantMessage:    "spannerplan: planNode index must match slice position: at slice position 0 expected index 0, got 1",
		},
		{
			name:           "nil child link",
			input:          []*sppb.PlanNode{{Index: 0, ChildLinks: []*sppb.PlanNode_ChildLink{nil}}},
			wantKind:       ErrNilChildLink,
			wantNodeIndex:  0,
			wantChildIndex: 0,
			wantMessage:    "spannerplan: childLink cannot be nil: parent node 0 childLinks[0]",
		},
		{
			name: "child link index out of range",
			input: []*sppb.PlanNode{
				{Index: 0, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 99}}},
				{Index: 1},
			},
			wantKind:       ErrChildLinkIndexOutOfRange,
			wantNodeIndex:  0,
			wantChildIndex: 0,
			// This exact text is pinned by downstream golden tests
			// (e.g. spanner-mycli). It must not change.
			wantMessage: "spannerplan: childLink childIndex out of range: parent node 0 childLinks[0] has childIndex 99, len(planNodes)=2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.input)
			if err == nil {
				t.Fatal("New() error = nil, want error")
			}

			// Umbrella sentinel matches every validation failure.
			if !errors.Is(err, ErrInvalidPlan) {
				t.Errorf("errors.Is(err, ErrInvalidPlan) = false, want true (err = %v)", err)
			}
			// The specific category sentinel matches.
			if !errors.Is(err, tt.wantKind) {
				t.Errorf("errors.Is(err, %v) = false, want true (err = %v)", tt.wantKind, err)
			}

			// The typed error exposes structured fields.
			var verr *ValidationError
			if !errors.As(err, &verr) {
				t.Fatalf("errors.As(err, *ValidationError) = false, want true (err = %v)", err)
			}
			if verr.Kind != tt.wantKind {
				t.Errorf("verr.Kind = %v, want %v", verr.Kind, tt.wantKind)
			}
			if verr.NodeIndex != tt.wantNodeIndex {
				t.Errorf("verr.NodeIndex = %d, want %d", verr.NodeIndex, tt.wantNodeIndex)
			}
			if verr.ChildIndex != tt.wantChildIndex {
				t.Errorf("verr.ChildIndex = %d, want %d", verr.ChildIndex, tt.wantChildIndex)
			}

			// Message text is preserved exactly for downstream consumers.
			if got := err.Error(); got != tt.wantMessage {
				t.Errorf("err.Error() = %q, want %q", got, tt.wantMessage)
			}
		})
	}
}

func TestHasStats(t *testing.T) {
	tests := []struct {
		name  string
		input []*sppb.PlanNode
		want  bool
	}{
		{
			"has stats",
			[]*sppb.PlanNode{{ExecutionStats: &structpb.Struct{}}},
			true,
		},
		{
			"no stats",
			[]*sppb.PlanNode{{ExecutionStats: nil}},
			false,
		},
		{
			"empty",
			nil,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HasStats(tt.input); got != tt.want {
				t.Errorf("HasStats() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParentLinks(t *testing.T) {
	firstLink := &sppb.PlanNode_ChildLink{ChildIndex: 2, Type: "Input"}
	secondLink := &sppb.PlanNode_ChildLink{ChildIndex: 2, Type: "Scalar"}
	thirdLink := &sppb.PlanNode_ChildLink{ChildIndex: 2, Type: "Condition"}

	qp, err := New([]*sppb.PlanNode{
		{
			Index:      0,
			ChildLinks: []*sppb.PlanNode_ChildLink{firstLink, secondLink},
		},
		{
			Index:      1,
			ChildLinks: []*sppb.PlanNode_ChildLink{thirdLink},
		},
		{Index: 2},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	got := qp.ParentLinks(2)
	if len(got) != 3 {
		t.Fatalf("len(ParentLinks(2)) = %d, want 3", len(got))
	}

	want := []struct {
		parentIndex int32
		childLink   *sppb.PlanNode_ChildLink
	}{
		{parentIndex: 0, childLink: firstLink},
		{parentIndex: 0, childLink: secondLink},
		{parentIndex: 1, childLink: thirdLink},
	}
	for i, wantLink := range want {
		if got[i].Parent.GetIndex() != wantLink.parentIndex {
			t.Fatalf("ParentLinks(2)[%d].Parent.Index = %d, want %d", i, got[i].Parent.GetIndex(), wantLink.parentIndex)
		}
		if got[i].ChildLink != wantLink.childLink {
			t.Fatalf("ParentLinks(2)[%d].ChildLink = %p, want %p", i, got[i].ChildLink, wantLink.childLink)
		}
	}

	if got := qp.GetParentNodeByChildIndex(2).GetIndex(); got != 1 {
		t.Fatalf("GetParentNodeByChildIndex(2) = %d, want 1", got)
	}

	got[0] = ResolvedParentLink{}
	if got := qp.ParentLinks(2)[0]; got == (ResolvedParentLink{}) {
		t.Fatal("ParentLinks(2) returned internal slice")
	}

	if got := qp.ParentLinks(0); got != nil {
		t.Fatalf("ParentLinks(0) = %v, want nil", got)
	}

	if got := qp.ParentLinks(99); got != nil {
		t.Fatalf("ParentLinks(99) = %v, want nil", got)
	}
}

func TestIsPredicate(t *testing.T) {
	tests := []struct {
		name      string
		childLink *sppb.PlanNode_ChildLink
		child     *sppb.PlanNode
		want      bool
	}{
		{
			name:      "search predicate node",
			childLink: &sppb.PlanNode_ChildLink{ChildIndex: 1, Type: "Search Predicate"},
			child: &sppb.PlanNode{
				Index:       1,
				Kind:        sppb.PlanNode_SCALAR,
				DisplayName: "Search Predicate",
			},
			want: true,
		},
		{
			name:      "compound search predicate function",
			childLink: &sppb.PlanNode_ChildLink{ChildIndex: 1, Type: "Search Predicate"},
			child: &sppb.PlanNode{
				Index:       1,
				Kind:        sppb.PlanNode_SCALAR,
				DisplayName: "Function",
			},
			want: true,
		},
		{
			name:      "search predicate link to relational node",
			childLink: &sppb.PlanNode_ChildLink{ChildIndex: 1, Type: "Search Predicate"},
			child: &sppb.PlanNode{
				Index:       1,
				Kind:        sppb.PlanNode_RELATIONAL,
				DisplayName: "Scan",
			},
			want: false,
		},
		{
			name:      "condition function remains predicate",
			childLink: &sppb.PlanNode_ChildLink{ChildIndex: 1, Type: "Seek Condition"},
			child: &sppb.PlanNode{
				Index:       1,
				Kind:        sppb.PlanNode_SCALAR,
				DisplayName: "Function",
			},
			want: true,
		},
		{
			name:      "aggregate function remains non predicate",
			childLink: &sppb.PlanNode_ChildLink{ChildIndex: 1, Type: "Agg"},
			child: &sppb.PlanNode{
				Index:       1,
				Kind:        sppb.PlanNode_SCALAR,
				DisplayName: "Function",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qp, err := New([]*sppb.PlanNode{
				{
					Index:      0,
					Kind:       sppb.PlanNode_RELATIONAL,
					ChildLinks: []*sppb.PlanNode_ChildLink{tt.childLink},
				},
				tt.child,
			})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			if got := qp.IsPredicate(tt.childLink); got != tt.want {
				t.Fatalf("IsPredicate() = %v, want %v", got, tt.want)
			}
		})
	}
}
