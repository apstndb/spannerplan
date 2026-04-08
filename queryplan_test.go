package spannerplan

import (
	"errors"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		input   []*sppb.PlanNode
		wantErr error
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
			name: "valid query plan nodes",
			input: []*sppb.PlanNode{
				{Index: 0, ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 1}}},
				{Index: 1},
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
			if got := qp.GetParentNodeByChildIndex(1).GetIndex(); got != 0 {
				t.Fatalf("GetParentNodeByChildIndex(1) = %d, want 0", got)
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
