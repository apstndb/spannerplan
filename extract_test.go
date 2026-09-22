package spannerplan

import (
	"errors"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/goccy/go-yaml"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/apstndb/protoyaml"
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

func TestExtractQueryPlan_FieldNameSpellings(t *testing.T) {
	qp := &sppb.QueryPlan{PlanNodes: []*sppb.PlanNode{{
		Kind:        sppb.PlanNode_RELATIONAL,
		DisplayName: "Scan",
	}}}
	rss := &sppb.ResultSetStats{QueryPlan: qp}
	rs := &sppb.ResultSet{
		Metadata: &sppb.ResultSetMetadata{RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{{
			Name: "SingerId",
			Type: &sppb.Type{Code: sppb.TypeCode_INT64},
		}}}},
		Stats: rss,
	}

	cases := []struct {
		name        string
		msg         proto.Message
		wantRowType bool
	}{
		{name: "bare QueryPlan", msg: qp},
		{name: "bare ResultSetStats", msg: rss},
		{name: "ResultSet", msg: rs, wantRowType: true},
	}
	spellings := []struct {
		name string
		opts protojson.MarshalOptions
	}{
		{name: "json names", opts: protojson.MarshalOptions{}},
		{name: "proto names", opts: protojson.MarshalOptions{UseProtoNames: true}},
	}

	for _, tc := range cases {
		for _, spelling := range spellings {
			t.Run(tc.name+"/"+spelling.name, func(t *testing.T) {
				input, err := spelling.opts.Marshal(tc.msg)
				if err != nil {
					t.Fatalf("protojson.Marshal() error = %v", err)
				}
				gotStats, gotRowType, err := ExtractQueryPlan(input)
				if err != nil {
					t.Fatalf("ExtractQueryPlan(%s) error = %v\ninput: %s", spelling.name, err, input)
				}
				if gotStats == nil || len(gotStats.GetQueryPlan().GetPlanNodes()) != 1 {
					t.Fatalf("plan nodes = %d, want 1", len(gotStats.GetQueryPlan().GetPlanNodes()))
				}
				if got := gotStats.GetQueryPlan().GetPlanNodes()[0].GetDisplayName(); got != "Scan" {
					t.Errorf("display name = %q, want Scan", got)
				}
				if got := gotRowType != nil; got != tc.wantRowType {
					t.Errorf("row type presence = %v, want %v", got, tc.wantRowType)
				}
			})
		}
	}
}

func TestExtractQueryPlan_DuplicateAliasAndMalformed(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name: "duplicate query plan alias",
			input: `{"queryPlan":{"planNodes":[{"kind":"RELATIONAL","displayName":"Scan"}]},` +
				`"query_plan":{"plan_nodes":[{"kind":"RELATIONAL","display_name":"Scan"}]}}`,
		},
		{
			name: "duplicate plan nodes alias",
			input: `{"planNodes":[{"kind":"RELATIONAL","displayName":"Scan"}],` +
				`"plan_nodes":[{"kind":"RELATIONAL","display_name":"Scan"}]}`,
		},
		{
			name:  "malformed json-name plan nodes",
			input: `{"planNodes":"not-an-array"}`,
		},
		{
			name:  "malformed proto-name plan nodes",
			input: `{"plan_nodes":"not-an-array"}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := ExtractQueryPlan([]byte(tc.input))
			if err == nil {
				t.Fatal("ExtractQueryPlan() error = nil, want decoder rejection")
			}
			if err.Error() == "unknown input format" {
				t.Fatalf("ExtractQueryPlan() error = %v, want the decoder to reject the payload", err)
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
			for i := 0; i < b.N; i++ {
				if _, _, err := extractQueryPlanLegacy(tt.input); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(tt.name+"/current", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, _, err := ExtractQueryPlan(tt.input); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
