package spannerplan

import (
	"errors"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/goccy/go-yaml"

	"github.com/apstndb/spannerplan/protoyaml"
)

func extractQueryPlanLegacy(b []byte) (*sppb.ResultSetStats, *sppb.StructType, error) {
	var jsonObj map[string]interface{}
	err := yaml.Unmarshal(b, &jsonObj)
	if err != nil {
		return nil, nil, err
	}

	if _, ok := jsonObj["queryPlan"]; ok {
		var rss sppb.ResultSetStats
		if err := protoyaml.Unmarshal(b, &rss); err != nil {
			return nil, nil, err
		}
		return &rss, nil, nil
	} else if _, ok := jsonObj["planNodes"]; ok {
		var qp sppb.QueryPlan
		if err := protoyaml.Unmarshal(b, &qp); err != nil {
			return nil, nil, err
		}
		return &sppb.ResultSetStats{QueryPlan: &qp}, nil, nil
	} else if _, ok := jsonObj["stats"]; ok {
		var rs sppb.ResultSet
		if err := protoyaml.Unmarshal(b, &rs); err != nil {
			return nil, nil, err
		}
		return rs.GetStats(), rs.GetMetadata().GetRowType(), nil
	}
	return nil, nil, errors.New("unknown input format")
}

func TestExtractQueryPlan(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		wantNodeLen int
		wantRowType bool
		wantErr     bool
	}{
		{
			name: "result set stats",
			input: []byte(`
queryPlan:
  planNodes:
    - index: 0
      kind: RELATIONAL
      displayName: Root
`),
			wantNodeLen: 1,
		},
		{
			name: "query plan",
			input: []byte(`
planNodes:
  - index: 0
    kind: RELATIONAL
    displayName: Root
`),
			wantNodeLen: 1,
		},
		{
			name: "result set",
			input: []byte(`
metadata:
  rowType:
    fields:
      - name: SingerId
        type:
          code: INT64
stats:
  queryPlan:
    planNodes:
      - index: 0
        kind: RELATIONAL
        displayName: Root
`),
			wantNodeLen: 1,
			wantRowType: true,
		},
		{
			name: "unknown format",
			input: []byte(`
foo: bar
`),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStats, gotRowType, err := ExtractQueryPlan(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("ExtractQueryPlan() error = nil, want non-nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("ExtractQueryPlan() error = %v", err)
			}
			if gotStats == nil {
				t.Fatal("ExtractQueryPlan() stats = nil")
			}
			if got := len(gotStats.GetQueryPlan().GetPlanNodes()); got != tt.wantNodeLen {
				t.Fatalf("len(ExtractQueryPlan().GetQueryPlan().GetPlanNodes()) = %d, want %d", got, tt.wantNodeLen)
			}
			if got := gotRowType != nil; got != tt.wantRowType {
				t.Fatalf("ExtractQueryPlan() rowType presence = %v, want %v", got, tt.wantRowType)
			}
		})
	}
}

func BenchmarkExtractQueryPlan(b *testing.B) {
	inputs := []struct {
		name  string
		input []byte
	}{
		{
			name: "result_set_stats",
			input: []byte(`
queryPlan:
  planNodes:
    - index: 0
      kind: RELATIONAL
      displayName: Root
`),
		},
		{
			name: "query_plan",
			input: []byte(`
planNodes:
  - index: 0
    kind: RELATIONAL
    displayName: Root
`),
		},
		{
			name: "result_set",
			input: []byte(`
metadata:
  rowType:
    fields:
      - name: SingerId
        type:
          code: INT64
stats:
  queryPlan:
    planNodes:
      - index: 0
        kind: RELATIONAL
        displayName: Root
`),
		},
	}

	for _, tt := range inputs {
		b.Run(tt.name+"/legacy", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, _, err := extractQueryPlanLegacy(tt.input); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(tt.name+"/current", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, _, err := ExtractQueryPlan(tt.input); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
