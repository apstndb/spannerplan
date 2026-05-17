package reference

import (
	"encoding/json"
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
	b, err := os.ReadFile("testdata/distributed_cross_apply.yaml")
	if err != nil {
		t.Fatalf("failed to read wrapped plan fixture: %v", err)
	}
	return string(b)
}

func scalarAppendixPlanNodes() []*sppb.PlanNode {
	return []*sppb.PlanNode{
		{
			Index:       0,
			DisplayName: "Sort",
			Kind:        sppb.PlanNode_RELATIONAL,
			ChildLinks: []*sppb.PlanNode_ChildLink{
				{ChildIndex: 1, Type: "Key", Variable: "sort_count"},
				{ChildIndex: 2, Type: "Key", Variable: "sort_genre"},
				{ChildIndex: 3},
			},
		},
		scalarPlanNode(1, "$SongCount (DESC)"),
		scalarPlanNode(2, "$group_SongGenre'"),
		{
			Index:       3,
			DisplayName: "Aggregate",
			Kind:        sppb.PlanNode_RELATIONAL,
			ChildLinks: []*sppb.PlanNode_ChildLink{
				{ChildIndex: 4, Type: "Key", Variable: "group_SongGenre'"},
				{ChildIndex: 5, Type: "Agg", Variable: "SongCount"},
				{ChildIndex: 6},
			},
		},
		scalarPlanNode(4, "$group_SongGenre"),
		scalarPlanNode(5, "COUNT_FINAL($v1)"),
		{
			Index:       6,
			DisplayName: "Scan",
			Kind:        sppb.PlanNode_RELATIONAL,
			ChildLinks: []*sppb.PlanNode_ChildLink{
				{ChildIndex: 7, Variable: "group_SongGenre"},
				{ChildIndex: 8, Variable: "SongGenre"},
				{ChildIndex: 9, Variable: "v1"},
			},
		},
		scalarPlanNode(7, "$SongGenre"),
		scalarPlanNode(8, "SongGenre"),
		scalarPlanNode(9, "COUNT()"),
	}
}

func scalarPlanNode(index int32, description string) *sppb.PlanNode {
	return &sppb.PlanNode{
		Index:       index,
		DisplayName: "Reference",
		Kind:        sppb.PlanNode_SCALAR,
		ShortRepresentation: &sppb.PlanNode_ShortRepresentation{
			Description: description,
		},
	}
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

func TestParsePrintSections(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    PrintSections
		wantErr string
	}{
		{
			name:  "single section",
			input: "predicates",
			want:  PrintSections{PrintPredicates},
		},
		{
			name:  "multiple sections",
			input: "predicates,ordering,aggregate",
			want:  PrintSections{PrintPredicates, PrintOrdering, PrintAggregate},
		},
		{
			name:  "case and space",
			input: " Predicates, Ordering ",
			want:  PrintSections{PrintPredicates, PrintOrdering},
		},
		{
			name:  "basic preset",
			input: "basic",
			want:  PrintSections{PrintPredicates},
		},
		{
			name:  "enhanced preset",
			input: " Enhanced ",
			want:  PrintSections{PrintPredicates, PrintOrdering, PrintAggregate},
		},
		{
			name:  "full preset",
			input: "full",
			want:  PrintSections{PrintFull},
		},
		{
			name:  "none preset",
			input: "none",
			want:  PrintSections{},
		},
		{
			name:  "empty means no sections",
			input: "",
			want:  PrintSections{},
		},
		{
			name:    "unknown",
			input:   "broken",
			wantErr: "unknown print preset or section: broken",
		},
		{
			name:    "preset cannot be combined",
			input:   "basic,ordering",
			wantErr: `print preset "basic" cannot be combined with section list`,
		},
		{
			name:    "duplicate",
			input:   "predicates,predicates",
			wantErr: "duplicate print section: predicates",
		},
		{
			name:    "raw dump cannot be combined",
			input:   "predicates,full",
			wantErr: `print section "full" cannot be combined with other sections`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePrintSections(tt.input)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("ParsePrintSections() error = nil, want non-nil")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ParsePrintSections() error = %q, want substring %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParsePrintSections() error = %v", err)
			}
			if tt.want != nil && got == nil {
				t.Fatal("ParsePrintSections() returned nil, want non-nil explicit sections")
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("ParsePrintSections() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPrintPresetSections(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  PrintSections
	}{
		{name: "basic", input: "basic", want: PrintSections{PrintPredicates}},
		{name: "enhanced", input: "enhanced", want: PrintSections{PrintPredicates, PrintOrdering, PrintAggregate}},
		{name: "full", input: "full", want: PrintSections{PrintFull}},
		{name: "none", input: "none", want: PrintSections{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			preset, err := ParsePrintPreset(tt.input)
			if err != nil {
				t.Fatalf("ParsePrintPreset() error = %v", err)
			}
			got, err := preset.Sections()
			if err != nil {
				t.Fatalf("PrintPreset.Sections() error = %v", err)
			}
			if got == nil {
				t.Fatal("PrintPreset.Sections() returned nil, want non-nil sections")
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("PrintPreset.Sections() mismatch (-want +got):\n%s", diff)
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

func TestRenderTreeTableWithOptions_PrintSections(t *testing.T) {
	got, err := RenderTreeTableWithOptions(
		scalarAppendixPlanNodes(),
		RenderModePlan,
		FormatCurrent,
		WithPrintSections(PrintOrdering, PrintAggregate),
		WithResolveScalarVarsRecursive(),
	)
	if err != nil {
		t.Fatalf("RenderTreeTableWithOptions() error = %v", err)
	}

	for _, want := range []string{
		"Ordering(identified by ID):",
		" 0: Key: COUNT_FINAL(COUNT()) DESC, SongGenre",
		"Aggregates(identified by ID):",
		" 3: Key: SongGenre",
		"    Agg: COUNT_FINAL($v1)",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("RenderTreeTableWithOptions() output does not contain %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "Predicates(identified by ID):") {
		t.Fatalf("RenderTreeTableWithOptions() output contains predicates despite explicit sections:\n%s", got)
	}
}

func TestRenderTreeTableWithConfig_PrintSections(t *testing.T) {
	got, err := RenderTreeTableWithConfig(
		scalarAppendixPlanNodes(),
		RenderModePlan,
		FormatCurrent,
		RenderConfig{
			PrintSections:     NewPrintSections(PrintOrdering, PrintAggregate),
			ShowScalarVars:    true,
			ResolveScalarVars: true,
		},
	)
	if err != nil {
		t.Fatalf("RenderTreeTableWithConfig() error = %v", err)
	}

	for _, want := range []string{
		"Ordering(identified by ID):",
		" 0: Key: $sort_count=COUNT_FINAL($v1) DESC, $sort_genre=$group_SongGenre",
		"Aggregates(identified by ID):",
		" 3: Key: $group_SongGenre'=$SongGenre",
		"    Agg: $SongCount=COUNT_FINAL($v1)",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("RenderTreeTableWithConfig() output does not contain %q:\n%s", want, got)
		}
	}
}

func TestRenderTreeTableWithOptions_RawPrintSections(t *testing.T) {
	got, err := RenderTreeTableWithOptions(
		scalarAppendixPlanNodes(),
		RenderModePlan,
		FormatCurrent,
		WithPrintSections(PrintFull),
	)
	if err != nil {
		t.Fatalf("RenderTreeTableWithOptions(PrintFull) error = %v", err)
	}
	for _, want := range []string{
		"Node Parameters(identified by ID):",
		" 0: Key: $sort_count=$SongCount (DESC), $sort_genre=$group_SongGenre'",
		" 3: Key: $group_SongGenre'=$group_SongGenre",
		"    Agg: $SongCount=COUNT_FINAL($v1)",
		" 6: $group_SongGenre=$SongGenre, $SongGenre=SongGenre, $v1=COUNT()",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("RenderTreeTableWithOptions(PrintFull) output does not contain %q:\n%s", want, got)
		}
	}

	got, err = RenderTreeTableWithOptions(
		scalarAppendixPlanNodes(),
		RenderModePlan,
		FormatCurrent,
		WithPrintSections(PrintTyped),
	)
	if err != nil {
		t.Fatalf("RenderTreeTableWithOptions(PrintTyped) error = %v", err)
	}
	if strings.Contains(got, "$group_SongGenre=$SongGenre") {
		t.Fatalf("RenderTreeTableWithOptions(PrintTyped) output contains untyped scalar link:\n%s", got)
	}
}

func TestRenderTreeTableWithOptions_PrintSectionValidation(t *testing.T) {
	_, err := RenderTreeTableWithOptions(
		scalarAppendixPlanNodes(),
		RenderModePlan,
		FormatCurrent,
		WithPrintSections(PrintPredicates, PrintFull),
	)
	if err == nil {
		t.Fatal("RenderTreeTableWithOptions() error = nil, want non-nil")
	}
	if got, want := err.Error(), `print section "full" cannot be combined with other sections`; got != want {
		t.Fatalf("RenderTreeTableWithOptions() error = %q, want %q", got, want)
	}

	got, err := RenderTreeTableWithOptions(
		scalarAppendixPlanNodes(),
		RenderModePlan,
		FormatCurrent,
		WithPrintSections(),
	)
	if err != nil {
		t.Fatalf("RenderTreeTableWithOptions(WithPrintSections()) error = %v", err)
	}
	if strings.Contains(got, "identified by ID") {
		t.Fatalf("RenderTreeTableWithOptions(WithPrintSections()) output contains appendix:\n%s", got)
	}
}

func TestRenderConfigPrintSectionsJSONRoundTripEmptySlice(t *testing.T) {
	b, err := json.Marshal(RenderConfig{PrintSections: NewPrintSections()})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if !strings.Contains(string(b), `"printSections":[]`) {
		t.Fatalf("json.Marshal() = %s, want printSections to preserve explicit empty slice", b)
	}

	var got RenderConfig
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if got.PrintSections == nil {
		t.Fatal("json round trip left PrintSections nil, want explicit empty slice")
	}
	if len(*got.PrintSections) != 0 {
		t.Fatalf("json round trip PrintSections = %#v, want empty", *got.PrintSections)
	}

	rendered, err := RenderTreeTableWithConfig(
		scalarAppendixPlanNodes(),
		RenderModePlan,
		FormatCurrent,
		got,
	)
	if err != nil {
		t.Fatalf("RenderTreeTableWithConfig() error = %v", err)
	}
	if strings.Contains(rendered, "identified by ID") {
		t.Fatalf("RenderTreeTableWithConfig() output contains appendix:\n%s", rendered)
	}
}

func TestRenderTreeTable_FullTextSearchPredicate(t *testing.T) {
	planNodes := []*sppb.PlanNode{
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
	}

	got, err := RenderTreeTable(planNodes, RenderModePlan, FormatCurrent, 0)
	if err != nil {
		t.Fatalf("RenderTreeTable() error = %v", err)
	}

	for _, want := range []string{
		"Predicates(identified by ID):",
		" 0: Search Predicate: SEARCH(Tokens, 'blue')",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("RenderTreeTable() output does not contain %q:\n%s", want, got)
		}
	}
}

func TestRenderTreeTableWithOptions_HangingIndent(t *testing.T) {
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

func TestRenderTreeTableWithOptions_HangingIndentNoopsWithoutWrapWidth(t *testing.T) {
	input := loadWrappedPlan(t)
	stats, _, err := queryplan.ExtractQueryPlan([]byte(input))
	if err != nil {
		t.Fatalf("Failed to extract query plan: %v", err)
	}

	planNodes := stats.GetQueryPlan().GetPlanNodes()
	base, err := RenderTreeTable(planNodes, RenderModePlan, FormatCurrent, 0)
	if err != nil {
		t.Fatalf("RenderTreeTable() error = %v", err)
	}

	withOption, err := RenderTreeTableWithOptions(planNodes, RenderModePlan, FormatCurrent, WithHangingIndent())
	if err != nil {
		t.Fatalf("RenderTreeTableWithOptions(WithHangingIndent) error = %v", err)
	}
	if diff := cmp.Diff(base, withOption); diff != "" {
		t.Fatalf("RenderTreeTableWithOptions(WithHangingIndent) mismatch without wrap width (-want +got):\n%s", diff)
	}

	withConfig, err := RenderTreeTableWithConfig(
		planNodes,
		RenderModePlan,
		FormatCurrent,
		RenderConfig{HangingIndent: true},
	)
	if err != nil {
		t.Fatalf("RenderTreeTableWithConfig(HangingIndent) error = %v", err)
	}
	if diff := cmp.Diff(base, withConfig); diff != "" {
		t.Fatalf("RenderTreeTableWithConfig(HangingIndent) mismatch without wrap width (-want +got):\n%s", diff)
	}
}

func TestRenderTreeTableWithConfig(t *testing.T) {
	input := loadWrappedPlan(t)
	stats, _, err := queryplan.ExtractQueryPlan([]byte(input))
	if err != nil {
		t.Fatalf("Failed to extract query plan: %v", err)
	}
	planNodes := stats.GetQueryPlan().GetPlanNodes()

	tests := []struct {
		name   string
		config RenderConfig
		opts   []Option
	}{
		{
			name:   "zero value matches no options",
			config: RenderConfig{},
		},
		{
			name:   "wrap width matches WithWrapWidth",
			config: RenderConfig{WrapWidth: 50},
			opts:   []Option{WithWrapWidth(50)},
		},
		{
			name:   "hanging indent matches WithHangingIndent",
			config: RenderConfig{HangingIndent: true},
			opts:   []Option{WithHangingIndent()},
		},
		{
			name:   "wrap width and hanging indent match options",
			config: RenderConfig{WrapWidth: 50, HangingIndent: true},
			opts:   []Option{WithWrapWidth(50), WithHangingIndent()},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RenderTreeTableWithConfig(planNodes, RenderModePlan, FormatCurrent, tc.config)
			if err != nil {
				t.Fatalf("RenderTreeTableWithConfig() error = %v", err)
			}

			want, err := RenderTreeTableWithOptions(planNodes, RenderModePlan, FormatCurrent, tc.opts...)
			if err != nil {
				t.Fatalf("RenderTreeTableWithOptions() error = %v", err)
			}

			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("RenderTreeTableWithConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRenderTreeTableWithConfig_NegativeWrapWidth(t *testing.T) {
	_, err := RenderTreeTableWithConfig(
		[]*sppb.PlanNode{{}},
		RenderModeAuto,
		FormatCurrent,
		RenderConfig{WrapWidth: -1},
	)
	if err == nil {
		t.Fatal("RenderTreeTableWithConfig() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "wrapWidth cannot be negative") {
		t.Fatalf("RenderTreeTableWithConfig() error = %q, want wrapWidth cannot be negative", err)
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
