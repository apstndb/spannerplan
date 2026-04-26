package reference

import (
	"os"
	"strings"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/MakeNowJust/heredoc/v2"
	queryplan "github.com/apstndb/spannerplan"
	"github.com/google/go-cmp/cmp"
)

// loadRealPlan reads the dca.yaml real plan used for integration-like tests.
func loadRealPlan(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("testdata/dca.yaml")
	if err != nil {
		t.Fatalf("failed to read testdata/dca.yaml: %v", err)
	}
	return string(b)
}

func loadWrappedPlan(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../../cmd/rendertree/impl/testdata/distributed_cross_apply.yaml")
	if err != nil {
		t.Fatalf("failed to read wrapped plan fixture: %v", err)
	}
	return string(b)
}

func Test_RenderTreeTable_withRealPlan_ALL_MODES_AND_FORMATS(t *testing.T) {
	input := loadRealPlan(t)
	tests := []struct {
		name   string
		mode   string
		format string
		want   string
	}{
		{
			name:   "AUTO CURRENT shows stats (input has stats)",
			mode:   "AUTO",
			format: "CURRENT",
			want: heredoc.Doc(`
				+-----+-----------------------------------------------------------------------------+------+-------+---------------+
				| ID  | Operator                                                                    | Rows | Exec. | Total Latency |
				+-----+-----------------------------------------------------------------------------+------+-------+---------------+
				|  *0 | Distributed Union on AlbumsByAlbumTitle <Row>                               |  386 |     1 | 12.25 msecs   |
				|  *1 | +- Distributed Cross Apply <Row>                                            |  386 |     1 | 12.14 msecs   |
				|   2 |    +- [Input] Create Batch <Row>                                            |      |       |               |
				|   3 |    |  +- Local Distributed Union <Row>                                      |  386 |     1 | 1.83 msecs    |
				|   4 |    |     +- Compute Struct <Row>                                            |  386 |     1 | 1.79 msecs    |
				|  *5 |    |        +- Filter Scan <Row> (seekable_key_size: 1)                     |  386 |     1 | 1.49 msecs    |
				|  *6 |    |           +- Index Scan on AlbumsByAlbumTitle <Row> (scan_method: Row) |  386 |     1 | 1.42 msecs    |
				|  22 |    +- [Map] Serialize Result <Row>                                          |  386 |     1 | 9.62 msecs    |
				|  23 |       +- Cross Apply <Row>                                                  |  386 |     1 | 9.45 msecs    |
				|  24 |          +- [Input] KeyRangeAccumulator <Row>                               |      |       |               |
				|  25 |          |  +- Batch Scan on $v2 <Row> (scan_method: Row)                   |      |       |               |
				|  29 |          +- [Map] Local Distributed Union <Row>                             |  386 |   386 | 9.16 msecs    |
				|  30 |             +- Filter Scan <Row> (seekable_key_size: 0)                     |      |       |               |
				| *31 |                +- Table Scan on Albums <Row> (scan_method: Row)             |  386 |   386 | 8.96 msecs    |
				+-----+-----------------------------------------------------------------------------+------+-------+---------------+
				Predicates(identified by ID):
				  0: Split Range: (STARTS_WITH($AlbumTitle, 'T') AND ($AlbumTitle LIKE 'T%e'))
				  1: Split Range: (($SingerId' = $SingerId) AND ($AlbumId' = $AlbumId))
				  5: Residual Condition: ($AlbumTitle LIKE 'T%e')
				  6: Seek Condition: STARTS_WITH($AlbumTitle, 'T')
				 31: Seek Condition: (($SingerId' = $batched_SingerId) AND ($AlbumId' = $batched_AlbumId))
			`),
		},
		{
			name:   "PLAN CURRENT hides stats",
			mode:   "PLAN",
			format: "CURRENT",
			want: heredoc.Doc(`
				+-----+-----------------------------------------------------------------------------+
				| ID  | Operator                                                                    |
				+-----+-----------------------------------------------------------------------------+
				|  *0 | Distributed Union on AlbumsByAlbumTitle <Row>                               |
				|  *1 | +- Distributed Cross Apply <Row>                                            |
				|   2 |    +- [Input] Create Batch <Row>                                            |
				|   3 |    |  +- Local Distributed Union <Row>                                      |
				|   4 |    |     +- Compute Struct <Row>                                            |
				|  *5 |    |        +- Filter Scan <Row> (seekable_key_size: 1)                     |
				|  *6 |    |           +- Index Scan on AlbumsByAlbumTitle <Row> (scan_method: Row) |
				|  22 |    +- [Map] Serialize Result <Row>                                          |
				|  23 |       +- Cross Apply <Row>                                                  |
				|  24 |          +- [Input] KeyRangeAccumulator <Row>                               |
				|  25 |          |  +- Batch Scan on $v2 <Row> (scan_method: Row)                   |
				|  29 |          +- [Map] Local Distributed Union <Row>                             |
				|  30 |             +- Filter Scan <Row> (seekable_key_size: 0)                     |
				| *31 |                +- Table Scan on Albums <Row> (scan_method: Row)             |
				+-----+-----------------------------------------------------------------------------+
				Predicates(identified by ID):
				  0: Split Range: (STARTS_WITH($AlbumTitle, 'T') AND ($AlbumTitle LIKE 'T%e'))
				  1: Split Range: (($SingerId' = $SingerId) AND ($AlbumId' = $AlbumId))
				  5: Residual Condition: ($AlbumTitle LIKE 'T%e')
				  6: Seek Condition: STARTS_WITH($AlbumTitle, 'T')
				 31: Seek Condition: (($SingerId' = $batched_SingerId) AND ($AlbumId' = $batched_AlbumId))
			`),
		},
		{
			name:   "PROFILE CURRENT shows stats",
			mode:   "PROFILE",
			format: "CURRENT",
			want: heredoc.Doc(`
				+-----+-----------------------------------------------------------------------------+------+-------+---------------+
				| ID  | Operator                                                                    | Rows | Exec. | Total Latency |
				+-----+-----------------------------------------------------------------------------+------+-------+---------------+
				|  *0 | Distributed Union on AlbumsByAlbumTitle <Row>                               |  386 |     1 | 12.25 msecs   |
				|  *1 | +- Distributed Cross Apply <Row>                                            |  386 |     1 | 12.14 msecs   |
				|   2 |    +- [Input] Create Batch <Row>                                            |      |       |               |
				|   3 |    |  +- Local Distributed Union <Row>                                      |  386 |     1 | 1.83 msecs    |
				|   4 |    |     +- Compute Struct <Row>                                            |  386 |     1 | 1.79 msecs    |
				|  *5 |    |        +- Filter Scan <Row> (seekable_key_size: 1)                     |  386 |     1 | 1.49 msecs    |
				|  *6 |    |           +- Index Scan on AlbumsByAlbumTitle <Row> (scan_method: Row) |  386 |     1 | 1.42 msecs    |
				|  22 |    +- [Map] Serialize Result <Row>                                          |  386 |     1 | 9.62 msecs    |
				|  23 |       +- Cross Apply <Row>                                                  |  386 |     1 | 9.45 msecs    |
				|  24 |          +- [Input] KeyRangeAccumulator <Row>                               |      |       |               |
				|  25 |          |  +- Batch Scan on $v2 <Row> (scan_method: Row)                   |      |       |               |
				|  29 |          +- [Map] Local Distributed Union <Row>                             |  386 |   386 | 9.16 msecs    |
				|  30 |             +- Filter Scan <Row> (seekable_key_size: 0)                     |      |       |               |
				| *31 |                +- Table Scan on Albums <Row> (scan_method: Row)             |  386 |   386 | 8.96 msecs    |
				+-----+-----------------------------------------------------------------------------+------+-------+---------------+
				Predicates(identified by ID):
				  0: Split Range: (STARTS_WITH($AlbumTitle, 'T') AND ($AlbumTitle LIKE 'T%e'))
				  1: Split Range: (($SingerId' = $SingerId) AND ($AlbumId' = $AlbumId))
				  5: Residual Condition: ($AlbumTitle LIKE 'T%e')
				  6: Seek Condition: STARTS_WITH($AlbumTitle, 'T')
				 31: Seek Condition: (($SingerId' = $batched_SingerId) AND ($AlbumId' = $batched_AlbumId))
			`),
		},
		{
			name:   "PLAN TRADITIONAL shows RAW metadata in node title",
			mode:   "PLAN",
			format: "TRADITIONAL",
			want: heredoc.Doc(`
				+-----+----------------------------------------------------------------------------------------------------------------+
				| ID  | Operator                                                                                                       |
				+-----+----------------------------------------------------------------------------------------------------------------+
				|  *0 | Distributed Union (distribution_table: AlbumsByAlbumTitle, execution_method: Row, split_ranges_aligned: false) |
				|  *1 | +- Distributed Cross Apply (execution_method: Row)                                                             |
				|   2 |    +- [Input] Create Batch (execution_method: Row)                                                             |
				|   3 |    |  +- Local Distributed Union (execution_method: Row)                                                       |
				|   4 |    |     +- Compute Struct (execution_method: Row)                                                             |
				|  *5 |    |        +- Filter Scan (execution_method: Row, seekable_key_size: 1)                                       |
				|  *6 |    |           +- Index Scan (Index: AlbumsByAlbumTitle, execution_method: Row, scan_method: Row)              |
				|  22 |    +- [Map] Serialize Result (execution_method: Row)                                                           |
				|  23 |       +- Cross Apply (execution_method: Row)                                                                   |
				|  24 |          +- [Input] KeyRangeAccumulator (execution_method: Row)                                                |
				|  25 |          |  +- Batch Scan (Batch: $v2, execution_method: Row, scan_method: Row)                                |
				|  29 |          +- [Map] Local Distributed Union (execution_method: Row)                                              |
				|  30 |             +- Filter Scan (execution_method: Row, seekable_key_size: 0)                                       |
				| *31 |                +- Table Scan (Table: Albums, execution_method: Row, scan_method: Row)                          |
				+-----+----------------------------------------------------------------------------------------------------------------+
				Predicates(identified by ID):
				  0: Split Range: (STARTS_WITH($AlbumTitle, 'T') AND ($AlbumTitle LIKE 'T%e'))
				  1: Split Range: (($SingerId' = $SingerId) AND ($AlbumId' = $AlbumId))
				  5: Residual Condition: ($AlbumTitle LIKE 'T%e')
				  6: Seek Condition: STARTS_WITH($AlbumTitle, 'T')
				 31: Seek Condition: (($SingerId' = $batched_SingerId) AND ($AlbumId' = $batched_AlbumId))
			`),
		},
		{
			name:   "PLAN COMPACT uses compact tree and titles",
			mode:   "PLAN",
			format: "COMPACT",
			want: heredoc.Doc(`
				+-----+--------------------------------------------------------------+
				| ID  | Operator                                                     |
				+-----+--------------------------------------------------------------+
				|  *0 | Distributed Union on AlbumsByAlbumTitle<Row>                 |
				|  *1 | +Distributed Cross Apply<Row>                                |
				|   2 |  +[Input]Create Batch<Row>                                   |
				|   3 |  |+Local Distributed Union<Row>                              |
				|   4 |  | +Compute Struct<Row>                                      |
				|  *5 |  |  +Filter Scan<Row>(seekable_key_size:1)                   |
				|  *6 |  |   +Index Scan on AlbumsByAlbumTitle<Row>(scan_method:Row) |
				|  22 |  +[Map]Serialize Result<Row>                                 |
				|  23 |   +Cross Apply<Row>                                          |
				|  24 |    +[Input]KeyRangeAccumulator<Row>                          |
				|  25 |    |+Batch Scan on $v2<Row>(scan_method:Row)                 |
				|  29 |    +[Map]Local Distributed Union<Row>                        |
				|  30 |     +Filter Scan<Row>(seekable_key_size:0)                   |
				| *31 |      +Table Scan on Albums<Row>(scan_method:Row)             |
				+-----+--------------------------------------------------------------+
				Predicates(identified by ID):
				  0: Split Range: (STARTS_WITH($AlbumTitle, 'T') AND ($AlbumTitle LIKE 'T%e'))
				  1: Split Range: (($SingerId' = $SingerId) AND ($AlbumId' = $AlbumId))
				  5: Residual Condition: ($AlbumTitle LIKE 'T%e')
				  6: Seek Condition: STARTS_WITH($AlbumTitle, 'T')
				 31: Seek Condition: (($SingerId' = $batched_SingerId) AND ($AlbumId' = $batched_AlbumId))
			`),
		},
		{
			name:   "PROFILE COMPACT still shows stats",
			mode:   "PROFILE",
			format: "COMPACT",
			want: heredoc.Doc(`
				+-----+--------------------------------------------------------------+------+-------+---------------+
				| ID  | Operator                                                     | Rows | Exec. | Total Latency |
				+-----+--------------------------------------------------------------+------+-------+---------------+
				|  *0 | Distributed Union on AlbumsByAlbumTitle<Row>                 |  386 |     1 | 12.25 msecs   |
				|  *1 | +Distributed Cross Apply<Row>                                |  386 |     1 | 12.14 msecs   |
				|   2 |  +[Input]Create Batch<Row>                                   |      |       |               |
				|   3 |  |+Local Distributed Union<Row>                              |  386 |     1 | 1.83 msecs    |
				|   4 |  | +Compute Struct<Row>                                      |  386 |     1 | 1.79 msecs    |
				|  *5 |  |  +Filter Scan<Row>(seekable_key_size:1)                   |  386 |     1 | 1.49 msecs    |
				|  *6 |  |   +Index Scan on AlbumsByAlbumTitle<Row>(scan_method:Row) |  386 |     1 | 1.42 msecs    |
				|  22 |  +[Map]Serialize Result<Row>                                 |  386 |     1 | 9.62 msecs    |
				|  23 |   +Cross Apply<Row>                                          |  386 |     1 | 9.45 msecs    |
				|  24 |    +[Input]KeyRangeAccumulator<Row>                          |      |       |               |
				|  25 |    |+Batch Scan on $v2<Row>(scan_method:Row)                 |      |       |               |
				|  29 |    +[Map]Local Distributed Union<Row>                        |  386 |   386 | 9.16 msecs    |
				|  30 |     +Filter Scan<Row>(seekable_key_size:0)                   |      |       |               |
				| *31 |      +Table Scan on Albums<Row>(scan_method:Row)             |  386 |   386 | 8.96 msecs    |
				+-----+--------------------------------------------------------------+------+-------+---------------+
				Predicates(identified by ID):
				  0: Split Range: (STARTS_WITH($AlbumTitle, 'T') AND ($AlbumTitle LIKE 'T%e'))
				  1: Split Range: (($SingerId' = $SingerId) AND ($AlbumId' = $AlbumId))
				  5: Residual Condition: ($AlbumTitle LIKE 'T%e')
				  6: Seek Condition: STARTS_WITH($AlbumTitle, 'T')
				 31: Seek Condition: (($SingerId' = $batched_SingerId) AND ($AlbumId' = $batched_AlbumId))
			`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Extract query plan from input
			stats, _, err := queryplan.ExtractQueryPlan([]byte(input))
			if err != nil {
				t.Fatalf("Failed to extract query plan: %v", err)
			}

			mode, err := ParseRenderMode(tc.mode)
			if err != nil {
				t.Fatalf("Failed to parse render mode: %v", err)
			}

			format, err := ParseFormat(tc.format)
			if err != nil {
				t.Fatalf("Failed to parse format: %v", err)
			}

			planNodes := stats.GetQueryPlan().GetPlanNodes()
			got, err := RenderTreeTable(planNodes, mode, format, 0)
			if err != nil {
				t.Fatalf("RenderTreeTable returned error: %v", err)
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseRenderMode(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    RenderMode
		wantErr bool
	}{
		{"valid AUTO", "AUTO", RenderModeAuto, false},
		{"valid auto lowercase", "auto", RenderModeAuto, false},
		{"valid PLAN", "PLAN", RenderModePlan, false},
		{"valid PROFILE", "PROFILE", RenderModeProfile, false},
		{"valid mixed case", "pRoFiLe", RenderModeProfile, false},
		{"invalid mode", "INVALID", "", true},
		{"empty string", "", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseRenderMode(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for input %q, got nil", tc.input)
				}
				if !strings.Contains(err.Error(), "unknown render mode") {
					t.Errorf("expected error message to contain 'unknown render mode', got: %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if got != tc.want {
					t.Errorf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestParseFormat(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Format
		wantErr bool
	}{
		{"valid TRADITIONAL", "TRADITIONAL", FormatTraditional, false},
		{"valid traditional lowercase", "traditional", FormatTraditional, false},
		{"valid CURRENT", "CURRENT", FormatCurrent, false},
		{"valid COMPACT", "COMPACT", FormatCompact, false},
		{"valid mixed case", "CoMpAcT", FormatCompact, false},
		{"invalid format", "INVALID", "", true},
		{"empty string", "", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseFormat(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for input %q, got nil", tc.input)
				}
				if !strings.Contains(err.Error(), "unknown format") {
					t.Errorf("expected error message to contain 'unknown format', got: %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if got != tc.want {
					t.Errorf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestRenderTreeTable_InvalidMode(t *testing.T) {
	// Create minimal valid plan nodes
	planNodes := []*sppb.PlanNode{{}}

	// Test with an invalid RenderMode value (bypassing ParseRenderMode)
	_, err := RenderTreeTable(planNodes, RenderMode("INVALID"), FormatCurrent, 0)
	if err == nil {
		t.Error("expected error for invalid render mode")
	}
	if !strings.Contains(err.Error(), "unknown render mode") {
		t.Errorf("expected error message to contain 'unknown render mode', got: %v", err)
	}
}

func TestRenderTreeTable_InputValidation(t *testing.T) {
	tests := []struct {
		name      string
		planNodes []*sppb.PlanNode
		wrapWidth int
		wantErr   string
	}{
		{
			name:      "empty planNodes",
			planNodes: []*sppb.PlanNode{},
			wrapWidth: 0,
			wantErr:   "planNodes cannot be empty",
		},
		{
			name:      "nil planNodes",
			planNodes: nil,
			wrapWidth: 0,
			wantErr:   "planNodes cannot be empty",
		},
		{
			name:      "negative wrapWidth",
			planNodes: []*sppb.PlanNode{{}},
			wrapWidth: -1,
			wantErr:   "wrapWidth cannot be negative",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := RenderTreeTable(tc.planNodes, RenderModeAuto, FormatCurrent, tc.wrapWidth)
			if err == nil {
				t.Error("expected error but got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("expected error to contain %q, got: %v", tc.wantErr, err)
			}
		})
	}
}

func TestRenderTreeTableWithOptions_ContinuationIndentNodePrefix(t *testing.T) {
	input := loadWrappedPlan(t)
	stats, _, err := queryplan.ExtractQueryPlan([]byte(input))
	if err != nil {
		t.Fatalf("Failed to extract query plan: %v", err)
	}

	planNodes := stats.GetQueryPlan().GetPlanNodes()
	base, err := RenderTreeTable(planNodes, RenderModePlan, FormatCurrent, 50)
	if err != nil {
		t.Fatalf("RenderTreeTable() error = %v", err)
	}

	hanging, err := RenderTreeTableWithOptions(
		planNodes,
		RenderModePlan,
		FormatCurrent,
		WithWrapWidth(50),
		WithHangingIndent(),
	)
	if err != nil {
		t.Fatalf("RenderTreeTableWithOptions() error = %v", err)
	}

	if base == hanging {
		t.Fatal("RenderTreeTableWithOptions() output = default output, want hanging indent difference")
	}

	defaultLine := lineContaining(base, "method: Row)")
	hangingLine := lineContaining(hanging, "method: Row)")
	if defaultLine == "" || hangingLine == "" {
		t.Fatalf("expected wrapped Batch Scan line in both outputs\ndefault=%q\nhanging=%q", defaultLine, hangingLine)
	}
	if !strings.Contains(defaultLine, "|  method: Row)") {
		t.Fatalf("default line = %q, want tree-aligned continuation marker", defaultLine)
	}
	if strings.Contains(hangingLine, "|  method: Row)") {
		t.Fatalf("hanging line = %q, want node-prefix hanging indent", hangingLine)
	}
}

func TestRenderTreeTableWithOptions_NilOption(t *testing.T) {
	input := loadWrappedPlan(t)
	stats, _, err := queryplan.ExtractQueryPlan([]byte(input))
	if err != nil {
		t.Fatalf("Failed to extract query plan: %v", err)
	}

	planNodes := stats.GetQueryPlan().GetPlanNodes()
	got, err := RenderTreeTableWithOptions(planNodes, RenderModePlan, FormatCurrent, nil, WithWrapWidth(50))
	if err != nil {
		t.Fatalf("RenderTreeTableWithOptions() error = %v", err)
	}

	want, err := RenderTreeTable(planNodes, RenderModePlan, FormatCurrent, 50)
	if err != nil {
		t.Fatalf("RenderTreeTable() error = %v", err)
	}

	if got != want {
		t.Fatal("RenderTreeTableWithOptions(nil, WithWrapWidth(50)) output != RenderTreeTable(..., 50)")
	}
}

func lineContaining(s, needle string) string {
	for _, line := range strings.Split(s, "\n") {
		if strings.Contains(line, needle) {
			return line
		}
	}
	return ""
}
