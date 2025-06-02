package spannerplan

import (
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

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
