package impl

import (
	"bytes"
	_ "embed"
	"errors"
	"strings"
	"testing"

	heredoc "github.com/MakeNowJust/heredoc/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/samber/lo"

	"github.com/apstndb/spannerplan"
	"github.com/apstndb/spannerplan/plantree"
)

func sliceOf[T any](vs ...T) []T {
	return vs
}

func Test_customFileToTableRenderDef(t *testing.T) {
	yamlContent := `
- name: ID
  template: '{{.FormatID}}'
  alignment: RIGHT
`

	trd, err := customFileToTableRenderDef([]byte(yamlContent))
	if err != nil {
		t.Fatal(err)
	}

	if v := len(trd.Columns); v != 1 {
		t.Fatalf("unexpected value: %v", v)
	}
	if v := trd.Columns[0]; v.Alignment != tw.AlignRight {
		t.Fatalf("unexpected value: %v", v)
	}
}

func Test_customColumnListToTableRenderDef(t *testing.T) {
	trd, err := customColumnListToTableRenderDef([]string{
		`{"name":"CPU,Time","template":"{{printf \"%s:%s,%s\" \"a\" \"b\" \"c\"}}","alignment":"RIGHT","inline":"ALWAYS"}`,
		`{name: Operator, template: "{{.Text}}"}`,
	})
	if err != nil {
		t.Fatal(err)
	}

	if got, want := len(trd.Columns), 2; got != want {
		t.Fatalf("column count = %d, want %d", got, want)
	}
	if got, want := trd.Columns[0].Name, "CPU,Time"; got != want {
		t.Fatalf("name = %q, want %q", got, want)
	}
	if got, want := trd.Columns[0].Alignment, tw.AlignRight; got != want {
		t.Fatalf("alignment = %v, want %v", got, want)
	}
	if got, want := trd.Columns[0].Inline, inlineTypeAlways; got != want {
		t.Fatalf("inline = %q, want %q", got, want)
	}
}

//go:embed testdata/distributed_cross_apply.yaml
var dcaYAML []byte

//go:embed testdata/distributed_cross_apply_profile.yaml
var dcaProfileYAML []byte

//go:embed testdata/delete.yaml
var deleteYAML []byte

func TestRenderTree(t *testing.T) {
	tests := []struct {
		desc      string
		input     []byte
		renderDef tableRenderDef
		inline    bool
		opts      []plantree.Option
		want      string
	}{
		{
			desc:      "PLAN",
			input:     dcaYAML,
			renderDef: withStatsToRenderDefMap[false],
			want: heredoc.Doc(`
+-----+-------------------------------------------------------------------------------------------+
| ID  | Operator                                                                                  |
+-----+-------------------------------------------------------------------------------------------+
|   0 | Distributed Union on AlbumsByAlbumTitle <Row>                                             |
|  *1 | +- Distributed Cross Apply <Row>                                                          |
|   2 |    +- [Input] Create Batch <Row>                                                          |
|   3 |    |  +- Local Distributed Union <Row>                                                    |
|   4 |    |     +- Compute Struct <Row>                                                          |
|   5 |    |        +- Index Scan on AlbumsByAlbumTitle <Row> (Full scan, scan_method: Automatic) |
|  11 |    +- [Map] Serialize Result <Row>                                                        |
|  12 |       +- Cross Apply <Row>                                                                |
|  13 |          +- [Input] Batch Scan on $v2 <Row> (scan_method: Row)                            |
|  16 |          +- [Map] Local Distributed Union <Row>                                           |
| *17 |             +- Filter Scan <Row> (seekable_key_size: 0)                                   |
|  18 |                +- Index Scan on SongsBySongGenre <Row> (Full scan, scan_method: Row)      |
+-----+-------------------------------------------------------------------------------------------+
Predicates(identified by ID):
  1: Split Range: ($AlbumId = $AlbumId_1)
 17: Residual Condition: ($AlbumId = $batched_AlbumId_1)
`),
		},
		{
			desc:      "compact PLAN",
			input:     dcaYAML,
			renderDef: withStatsToRenderDefMap[false],
			opts:      sliceOf(plantree.EnableCompact()),
			want: heredoc.Doc(`
+-----+-----------------------------------------------------------------------------+
| ID  | Operator                                                                    |
+-----+-----------------------------------------------------------------------------+
|   0 | Distributed Union on AlbumsByAlbumTitle<Row>                                |
|  *1 | +Distributed Cross Apply<Row>                                               |
|   2 |  +[Input]Create Batch<Row>                                                  |
|   3 |  |+Local Distributed Union<Row>                                             |
|   4 |  | +Compute Struct<Row>                                                     |
|   5 |  |  +Index Scan on AlbumsByAlbumTitle<Row>(Full scan,scan_method:Automatic) |
|  11 |  +[Map]Serialize Result<Row>                                                |
|  12 |   +Cross Apply<Row>                                                         |
|  13 |    +[Input]Batch Scan on $v2<Row>(scan_method:Row)                          |
|  16 |    +[Map]Local Distributed Union<Row>                                       |
| *17 |     +Filter Scan<Row>(seekable_key_size:0)                                  |
|  18 |      +Index Scan on SongsBySongGenre<Row>(Full scan,scan_method:Row)        |
+-----+-----------------------------------------------------------------------------+
Predicates(identified by ID):
  1: Split Range: ($AlbumId = $AlbumId_1)
 17: Residual Condition: ($AlbumId = $batched_AlbumId_1)
`),
		},
		{
			desc:      "wrapped compact PLAN",
			input:     dcaYAML,
			renderDef: withStatsToRenderDefMap[false],
			opts:      sliceOf(plantree.EnableCompact(), plantree.WithWrapWidth(40)),
			want: heredoc.Doc(`
+-----+------------------------------------------+
| ID  | Operator                                 |
+-----+------------------------------------------+
|   0 | Distributed Union on AlbumsByAlbumTitle< |
|     | Row>                                     |
|  *1 | +Distributed Cross Apply<Row>            |
|   2 |  +[Input]Create Batch<Row>               |
|   3 |  |+Local Distributed Union<Row>          |
|   4 |  | +Compute Struct<Row>                  |
|   5 |  |  +Index Scan on AlbumsByAlbumTitle<Ro |
|     |  |   w>(Full scan,scan_method:Automatic) |
|  11 |  +[Map]Serialize Result<Row>             |
|  12 |   +Cross Apply<Row>                      |
|  13 |    +[Input]Batch Scan on $v2<Row>(scan_m |
|     |    |ethod:Row)                           |
|  16 |    +[Map]Local Distributed Union<Row>    |
| *17 |     +Filter Scan<Row>(seekable_key_size: |
|     |      0)                                  |
|  18 |      +Index Scan on SongsBySongGenre<Row |
|     |       >(Full scan,scan_method:Row)       |
+-----+------------------------------------------+
Predicates(identified by ID):
  1: Split Range: ($AlbumId = $AlbumId_1)
 17: Residual Condition: ($AlbumId = $batched_AlbumId_1)
`),
		},
		{
			desc:      "wrapped PLAN",
			input:     dcaYAML,
			renderDef: withStatsToRenderDefMap[false],
			opts:      sliceOf(plantree.WithWrapWidth(50)),
			want: heredoc.Doc(`
+-----+----------------------------------------------------+
| ID  | Operator                                           |
+-----+----------------------------------------------------+
|   0 | Distributed Union on AlbumsByAlbumTitle <Row>      |
|  *1 | +- Distributed Cross Apply <Row>                   |
|   2 |    +- [Input] Create Batch <Row>                   |
|   3 |    |  +- Local Distributed Union <Row>             |
|   4 |    |     +- Compute Struct <Row>                   |
|   5 |    |        +- Index Scan on AlbumsByAlbumTitle <R |
|     |    |           ow> (Full scan, scan_method: Automa |
|     |    |           tic)                                |
|  11 |    +- [Map] Serialize Result <Row>                 |
|  12 |       +- Cross Apply <Row>                         |
|  13 |          +- [Input] Batch Scan on $v2 <Row> (scan_ |
|     |          |  method: Row)                           |
|  16 |          +- [Map] Local Distributed Union <Row>    |
| *17 |             +- Filter Scan <Row> (seekable_key_siz |
|     |                e: 0)                               |
|  18 |                +- Index Scan on SongsBySongGenre < |
|     |                   Row> (Full scan, scan_method: Ro |
|     |                   w)                               |
+-----+----------------------------------------------------+
Predicates(identified by ID):
  1: Split Range: ($AlbumId = $AlbumId_1)
 17: Residual Condition: ($AlbumId = $batched_AlbumId_1)
`),
		},
		{
			desc:      "PROFILE",
			input:     dcaProfileYAML,
			renderDef: withStatsToRenderDefMap[true],
			want: heredoc.Doc(`
+-----+-------------------------------------------------------------------------------------------+------+-------+---------+
| ID  | Operator                                                                                  | Rows | Exec. | Latency |
+-----+-------------------------------------------------------------------------------------------+------+-------+---------+
|   0 | Distributed Union on AlbumsByAlbumTitle <Row>                                             |   33 |     1 | 1.92 ms |
|  *1 | +- Distributed Cross Apply <Row>                                                          |   33 |     1 |  1.9 ms |
|   2 |    +- [Input] Create Batch <Row>                                                          |      |       |         |
|   3 |    |  +- Local Distributed Union <Row>                                                    |    7 |     1 | 0.95 ms |
|   4 |    |     +- Compute Struct <Row>                                                          |    7 |     1 | 0.94 ms |
|   5 |    |        +- Index Scan on AlbumsByAlbumTitle <Row> (Full scan, scan_method: Automatic) |    7 |     1 | 0.93 ms |
|  11 |    +- [Map] Serialize Result <Row>                                                        |   33 |     1 | 0.88 ms |
|  12 |       +- Cross Apply <Row>                                                                |   33 |     1 | 0.87 ms |
|  13 |          +- [Input] Batch Scan on $v2 <Row> (scan_method: Row)                            |    7 |     1 | 0.01 ms |
|  16 |          +- [Map] Local Distributed Union <Row>                                           |   33 |     7 | 0.85 ms |
| *17 |             +- Filter Scan <Row> (seekable_key_size: 0)                                   |      |       |         |
|  18 |                +- Index Scan on SongsBySongGenre <Row> (Full scan, scan_method: Row)      |   33 |     7 | 0.84 ms |
+-----+-------------------------------------------------------------------------------------------+------+-------+---------+
Predicates(identified by ID):
  1: Split Range: ($AlbumId = $AlbumId_1)
 17: Residual Condition: ($AlbumId = $batched_AlbumId_1)
`),
		},
		{
			desc:  "PROFILE with custom",
			input: dcaProfileYAML,
			renderDef: lo.Must(customFileToTableRenderDef([]byte(
				heredoc.Doc(`
- name: ID
  template: '{{.FormatID}}'
  alignment: RIGHT
- name: Operator
  template: '{{.Text}}'
  alignment: LEFT
- name: Rows
  template: '{{.ExecutionStats.Rows.Total}}'
  alignment: RIGHT
- name: Scanned
  template: '{{.ExecutionStats.ScannedRows.Total}}'
  alignment: RIGHT
- name: Filtered
  template: '{{.ExecutionStats.FilteredRows.Total}}'
  alignment: RIGHT
`)))),
			want: heredoc.Doc(`
+-----+-------------------------------------------------------------------------------------------+------+---------+----------+
| ID  | Operator                                                                                  | Rows | Scanned | Filtered |
+-----+-------------------------------------------------------------------------------------------+------+---------+----------+
|   0 | Distributed Union on AlbumsByAlbumTitle <Row>                                             |   33 |         |          |
|  *1 | +- Distributed Cross Apply <Row>                                                          |   33 |         |          |
|   2 |    +- [Input] Create Batch <Row>                                                          |      |         |          |
|   3 |    |  +- Local Distributed Union <Row>                                                    |    7 |         |          |
|   4 |    |     +- Compute Struct <Row>                                                          |    7 |         |          |
|   5 |    |        +- Index Scan on AlbumsByAlbumTitle <Row> (Full scan, scan_method: Automatic) |    7 |       7 |        0 |
|  11 |    +- [Map] Serialize Result <Row>                                                        |   33 |         |          |
|  12 |       +- Cross Apply <Row>                                                                |   33 |         |          |
|  13 |          +- [Input] Batch Scan on $v2 <Row> (scan_method: Row)                            |    7 |         |          |
|  16 |          +- [Map] Local Distributed Union <Row>                                           |   33 |         |          |
| *17 |             +- Filter Scan <Row> (seekable_key_size: 0)                                   |      |         |          |
|  18 |                +- Index Scan on SongsBySongGenre <Row> (Full scan, scan_method: Row)      |   33 |      63 |       30 |
+-----+-------------------------------------------------------------------------------------------+------+---------+----------+
Predicates(identified by ID):
  1: Split Range: ($AlbumId = $AlbumId_1)
 17: Residual Condition: ($AlbumId = $batched_AlbumId_1)
`),
		},
		{
			desc:  "PROFILE with custom list",
			input: dcaProfileYAML,
			renderDef: lo.Must(customListToTableRenderDef([]string{
				`ID:{{.FormatID}}:RIGHT`,
				`Operator:{{.Text}}`,
				`Rows:{{.ExecutionStats.Rows.Total}}:RIGHT`,
				`Scanned:{{.ExecutionStats.ScannedRows.Total}}:RIGHT`,
				`Filtered:{{.ExecutionStats.FilteredRows.Total}}:RIGHT`,
			})),
			want: heredoc.Doc(`
+-----+-------------------------------------------------------------------------------------------+------+---------+----------+
| ID  | Operator                                                                                  | Rows | Scanned | Filtered |
+-----+-------------------------------------------------------------------------------------------+------+---------+----------+
|   0 | Distributed Union on AlbumsByAlbumTitle <Row>                                             |   33 |         |          |
|  *1 | +- Distributed Cross Apply <Row>                                                          |   33 |         |          |
|   2 |    +- [Input] Create Batch <Row>                                                          |      |         |          |
|   3 |    |  +- Local Distributed Union <Row>                                                    |    7 |         |          |
|   4 |    |     +- Compute Struct <Row>                                                          |    7 |         |          |
|   5 |    |        +- Index Scan on AlbumsByAlbumTitle <Row> (Full scan, scan_method: Automatic) |    7 |       7 |        0 |
|  11 |    +- [Map] Serialize Result <Row>                                                        |   33 |         |          |
|  12 |       +- Cross Apply <Row>                                                                |   33 |         |          |
|  13 |          +- [Input] Batch Scan on $v2 <Row> (scan_method: Row)                            |    7 |         |          |
|  16 |          +- [Map] Local Distributed Union <Row>                                           |   33 |         |          |
| *17 |             +- Filter Scan <Row> (seekable_key_size: 0)                                   |      |         |          |
|  18 |                +- Index Scan on SongsBySongGenre <Row> (Full scan, scan_method: Row)      |   33 |      63 |       30 |
+-----+-------------------------------------------------------------------------------------------+------+---------+----------+
Predicates(identified by ID):
  1: Split Range: ($AlbumId = $AlbumId_1)
 17: Residual Condition: ($AlbumId = $batched_AlbumId_1)
`),
		},
		{
			desc:  "PROFILE with custom list, inline",
			input: dcaProfileYAML,
			renderDef: lo.Must(customListToTableRenderDef([]string{
				`ID:{{.FormatID}}:RIGHT:NEVER`,
				`Operator:{{.Text}}::NEVER`,
				`Rows:{{.ExecutionStats.Rows.Total}}:RIGHT:NEVER`,
				`Scanned:{{.ExecutionStats.ScannedRows.Total}}:RIGHT`,
				`Filtered:{{.ExecutionStats.FilteredRows.Total}}:RIGHT`,
			})),
			inline: true,
			opts:   sliceOf(plantree.WithWrapWidth(60)),
			want: heredoc.Doc(`
+-----+--------------------------------------------------------------+------+
| ID  | Operator                                                     | Rows |
+-----+--------------------------------------------------------------+------+
|   0 | Distributed Union on AlbumsByAlbumTitle <Row>                |   33 |
|  *1 | +- Distributed Cross Apply <Row>                             |   33 |
|   2 |    +- [Input] Create Batch <Row>                             |      |
|   3 |    |  +- Local Distributed Union <Row>                       |    7 |
|   4 |    |     +- Compute Struct <Row>                             |    7 |
|   5 |    |        +- Index Scan on AlbumsByAlbumTitle <Row> (Full  |    7 |
|     |    |           scan, scan_method: Automatic, Scanned=7, Filt |      |
|     |    |           ered=0)                                       |      |
|  11 |    +- [Map] Serialize Result <Row>                           |   33 |
|  12 |       +- Cross Apply <Row>                                   |   33 |
|  13 |          +- [Input] Batch Scan on $v2 <Row> (scan_method: Ro |    7 |
|     |          |  w)                                               |      |
|  16 |          +- [Map] Local Distributed Union <Row>              |   33 |
| *17 |             +- Filter Scan <Row> (seekable_key_size: 0)      |      |
|  18 |                +- Index Scan on SongsBySongGenre <Row> (Full |   33 |
|     |                    scan, scan_method: Row, Scanned=63, Filte |      |
|     |                   red=30)                                    |      |
+-----+--------------------------------------------------------------+------+
Predicates(identified by ID):
  1: Split Range: ($AlbumId = $AlbumId_1)
 17: Residual Condition: ($AlbumId = $batched_AlbumId_1)
`),
		},
		{
			desc:      "DELETE PLAN",
			input:     deleteYAML,
			renderDef: withStatsToRenderDefMap[false],
			want: heredoc.Doc(`
+----+----------------------------------------------------------------------------------+
| ID | Operator                                                                         |
+----+----------------------------------------------------------------------------------+
|  0 | Apply Mutations on MutationTest <Row> (operation_type: DELETE)                   |
|  1 | +- Distributed Union on MutationTest <Row>                                       |
|  2 |    +- Local Distributed Union <Row>                                              |
|  3 |       +- Serialize Result <Row>                                                  |
|  4 |          +- Table Scan on MutationTest <Row> (Full scan, scan_method: Automatic) |
+----+----------------------------------------------------------------------------------+
`),
		},
		{
			desc:      "DELETE PLAN traditional",
			input:     deleteYAML,
			renderDef: withStatsToRenderDefMap[false],
			opts: sliceOf(plantree.WithQueryPlanOptions(
				spannerplan.WithKnownFlagFormat(spannerplan.KnownFlagFormatRaw),
				spannerplan.WithExecutionMethodFormat(spannerplan.ExecutionMethodFormatRaw),
				spannerplan.WithTargetMetadataFormat(spannerplan.TargetMetadataFormatRaw),
			)),
			want: heredoc.Doc(`
+----+--------------------------------------------------------------------------------------------------------------+
| ID | Operator                                                                                                     |
+----+--------------------------------------------------------------------------------------------------------------+
|  0 | Apply Mutations (execution_method: Row, operation_type: DELETE, table: MutationTest)                         |
|  1 | +- Distributed Union (distribution_table: MutationTest, execution_method: Row, split_ranges_aligned: false)  |
|  2 |    +- Local Distributed Union (execution_method: Row)                                                        |
|  3 |       +- Serialize Result (execution_method: Row)                                                            |
|  4 |          +- Table Scan (Full scan: true, Table: MutationTest, execution_method: Row, scan_method: Automatic) |
+----+--------------------------------------------------------------------------------------------------------------+
`),
		},
	}

	for _, tcase := range tests {
		t.Run(tcase.desc, func(t *testing.T) {
			stats, _, err := spannerplan.ExtractQueryPlan(tcase.input)
			if err != nil {
				t.Fatalf("invalid input at protoyaml.Unmarshal:\nerror: %v", err)
			}

			opts := []plantree.Option{plantree.WithQueryPlanOptions(
				spannerplan.WithTargetMetadataFormat(spannerplan.TargetMetadataFormatOn),
				spannerplan.WithExecutionMethodFormat(spannerplan.ExecutionMethodFormatAngle),
				spannerplan.WithKnownFlagFormat(spannerplan.KnownFlagFormatLabel),
			)}

			opts = append(opts, tcase.opts...)

			got, err := renderTreeImpl(stats.GetQueryPlan().GetPlanNodes(), tcase.renderDef, PrintPredicates, true, tcase.inline, opts)
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(tcase.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestShouldRenderWithStats(t *testing.T) {
	decodeQueryPlan := func(b []byte) (*spannerplan.QueryPlan, error) {
		stats, _, err := spannerplan.ExtractQueryPlan(b)
		if err != nil {
			return nil, err
		}
		return spannerplan.New(stats.GetQueryPlan().GetPlanNodes())
	}

	tests := []struct {
		desc       string
		qp         *spannerplan.QueryPlan
		parsedMode explainMode
		want       bool
	}{
		{
			"PLAN mode, no stats",
			lo.Must(decodeQueryPlan(dcaYAML)),
			explainModePlan,
			false,
		},
		{
			"PLAN mode, with stats",
			lo.Must(decodeQueryPlan(dcaProfileYAML)),
			explainModePlan,
			false,
		},
		{
			"PROFILE mode, no stats",
			lo.Must(decodeQueryPlan(dcaYAML)),
			explainModeProfile,
			true,
		},
		{
			"PROFILE mode, with stats",
			lo.Must(decodeQueryPlan(dcaProfileYAML)),
			explainModeProfile,
			true,
		},
		{
			"AUTO mode, no stats",
			lo.Must(decodeQueryPlan(dcaYAML)),
			explainModeAuto,
			false,
		},
		{
			"AUTO mode, with stats",
			lo.Must(decodeQueryPlan(dcaProfileYAML)),
			explainModeAuto,
			true,
		},
	}

	for _, tcase := range tests {
		t.Run(tcase.desc, func(t *testing.T) {
			got := shouldRenderWithStats(tcase.qp.PlanNodes(), tcase.parsedMode)
			if got != tcase.want {
				t.Errorf("shouldRenderWithStats got %v, but want %v", got, tcase.want)
			}
		})
	}
}

func TestRun_UsageErrors(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		wantErrText string
		postCheck   func(t *testing.T, stderr string, err error)
	}{
		{
			name:        "invalid mode",
			args:        []string{"-mode", "broken"},
			wantErrText: "invalid input: broken",
			postCheck: func(t *testing.T, stderr string, err error) {
				t.Helper()
				if !strings.Contains(stderr, "Invalid value for -mode flag:") {
					t.Fatalf("stderr = %q, want invalid mode message", stderr)
				}
			},
		},
		{
			name:        "invalid print",
			args:        []string{"-print", "broken"},
			wantErrText: "unknown PrintMode: broken",
			postCheck: func(t *testing.T, stderr string, err error) {
				t.Helper()
				if !strings.Contains(stderr, "Invalid value for -print flag:") {
					t.Fatalf("stderr = %q, want invalid print message", stderr)
				}
				if !strings.Contains(stderr, "Usage of rendertree:") {
					t.Fatalf("stderr = %q, want usage output", stderr)
				}
			},
		},
		{
			name:        "unknown flag",
			args:        []string{"-unknown"},
			wantErrText: "flag provided but not defined: -unknown",
			postCheck: func(t *testing.T, stderr string, err error) {
				t.Helper()
				if !strings.Contains(stderr, "flag provided but not defined: -unknown") {
					t.Fatalf("stderr = %q, want unknown flag message", stderr)
				}
				if !strings.Contains(stderr, "Usage of rendertree:") {
					t.Fatalf("stderr = %q, want usage output", stderr)
				}
			},
		},
		{
			name:        "full-scan and known-flag are mutually exclusive",
			args:        []string{"-full-scan", "raw", "-known-flag", "label"},
			wantErrText: "--full-scan and --known-flag are mutually exclusive",
			postCheck: func(t *testing.T, stderr string, err error) {
				t.Helper()
				if !strings.Contains(stderr, "--full-scan and --known-flag are mutually exclusive") {
					t.Fatalf("stderr = %q, want mutual exclusion message", stderr)
				}
			},
		},
		{
			name:        "custom and custom-file are mutually exclusive",
			args:        []string{"-custom", "ID:{{.FormatID}}", "-custom-file", "custom.yaml"},
			wantErrText: "--custom and --custom-file are mutually exclusive",
			postCheck: func(t *testing.T, stderr string, err error) {
				t.Helper()
				if !strings.Contains(stderr, "--custom and --custom-file are mutually exclusive") {
					t.Fatalf("stderr = %q, want mutual exclusion message", stderr)
				}
			},
		},
		{
			name:        "custom-column and custom-file are mutually exclusive",
			args:        []string{"-custom-column", `{"name":"ID","template":"{{.FormatID}}"}`, "-custom-file", "custom.yaml"},
			wantErrText: "--custom-column and --custom-file are mutually exclusive",
			postCheck: func(t *testing.T, stderr string, err error) {
				t.Helper()
				if !strings.Contains(stderr, "--custom-column and --custom-file are mutually exclusive") {
					t.Fatalf("stderr = %q, want mutual exclusion message", stderr)
				}
			},
		},
		{
			name:        "custom and custom-column are mutually exclusive",
			args:        []string{"-custom", "ID:{{.FormatID}}", "-custom-column", `{"name":"Operator","template":"{{.Text}}"}`},
			wantErrText: "--custom and --custom-column are mutually exclusive",
			postCheck: func(t *testing.T, stderr string, err error) {
				t.Helper()
				if !strings.Contains(stderr, "--custom and --custom-column are mutually exclusive") {
					t.Fatalf("stderr = %q, want mutual exclusion message", stderr)
				}
			},
		},
		{
			name:        "invalid hanging-indent",
			args:        []string{"-hanging-indent=broken"},
			wantErrText: "invalid boolean value \"broken\" for -hanging-indent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer

			err := run(tt.args, strings.NewReader(""), &stdout, &stderr)
			if err == nil {
				t.Fatal("run() error = nil, want non-nil")
			}

			var usageErr *usageError
			if !errors.As(err, &usageErr) {
				t.Fatalf("run() error = %T, want *usageError", err)
			}
			if !strings.Contains(err.Error(), tt.wantErrText) {
				t.Fatalf("run() error = %q, want substring %q", err.Error(), tt.wantErrText)
			}
			if tt.postCheck != nil {
				tt.postCheck(t, stderr.String(), err)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
		})
	}
}

func TestRun_DeprecatedFullScanAlias(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-full-scan", "label", "-mode", "plan"}, bytes.NewReader(deleteYAML), &stdout, &stderr)
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if !strings.Contains(stderr.String(), "--full-scan is deprecated. You must migrate to --known-flag.") {
		t.Fatalf("stderr = %q, want deprecation warning", stderr.String())
	}
	if !strings.Contains(stdout.String(), "Apply Mutations on MutationTest <Row>") {
		t.Fatalf("stdout = %q, want rendered table output", stdout.String())
	}
}

func TestRun_DeprecatedCustomAlias(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-custom", "ID:{{.FormatID}}:RIGHT", "-mode", "plan"}, bytes.NewReader(deleteYAML), &stdout, &stderr)
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if !strings.Contains(stderr.String(), "--custom is deprecated. You must migrate to --custom-column or --custom-file.") {
		t.Fatalf("stderr = %q, want deprecation warning", stderr.String())
	}
	if !strings.Contains(stdout.String(), "| ID |") {
		t.Fatalf("stdout = %q, want custom output", stdout.String())
	}
}

func TestRun_HelpReturnsNil(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-h"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("run() error = %v, want nil", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if !strings.Contains(stderr.String(), "-mode") {
		t.Fatalf("stderr = %q, want usage output", stderr.String())
	}
}

func TestRun_HangingIndent(t *testing.T) {
	t.Parallel()

	var defaultStdout bytes.Buffer
	var defaultStderr bytes.Buffer
	if err := run([]string{"-mode", "plan", "-wrap-width", "50"}, bytes.NewReader(dcaYAML), &defaultStdout, &defaultStderr); err != nil {
		t.Fatalf("run(default) error = %v", err)
	}

	var hangingStdout bytes.Buffer
	var hangingStderr bytes.Buffer
	if err := run([]string{"-mode", "plan", "-wrap-width", "50", "-hanging-indent"}, bytes.NewReader(dcaYAML), &hangingStdout, &hangingStderr); err != nil {
		t.Fatalf("run(-hanging-indent) error = %v", err)
	}

	defaultLine := lineContaining(defaultStdout.String(), "method: Row)")
	hangingLine := lineContaining(hangingStdout.String(), "method: Row)")
	if defaultLine == "" || hangingLine == "" {
		t.Fatalf("expected wrapped Batch Scan continuation line\ndefault=%q\nhanging=%q", defaultLine, hangingLine)
	}
	if defaultLine == hangingLine {
		t.Fatalf("continuation line unchanged:\n%s", defaultLine)
	}
	if !strings.Contains(defaultLine, "|  method: Row)") {
		t.Fatalf("default line = %q, want tree-aligned continuation marker", defaultLine)
	}
	if strings.Contains(hangingLine, "|  method: Row)") {
		t.Fatalf("hanging line = %q, want node-prefix hanging indent", hangingLine)
	}
}

func TestRun_CustomColumn(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run([]string{
		"-mode", "plan",
		"-custom-column", `{"name":"Literal,Value","template":"{{printf \"%s:%s,%s\" \"a\" \"b\" \"c\"}}"}`,
		"-custom-column", `{"name":"Operator","template":"{{.Text}}"}`,
	}, bytes.NewReader(dcaYAML), &stdout, &stderr)
	if err != nil {
		t.Fatalf("run(-custom-column) error = %v", err)
	}

	if got := stderr.String(); got != "" {
		t.Fatalf("stderr = %q, want empty", got)
	}
	if !strings.Contains(stdout.String(), "Literal,Value") {
		t.Fatalf("stdout = %q, want custom column header", stdout.String())
	}
	if !strings.Contains(stdout.String(), "a:b,c") {
		t.Fatalf("stdout = %q, want literal colon/comma content", stdout.String())
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
