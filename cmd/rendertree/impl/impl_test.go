package impl

import (
	_ "embed"
	"testing"

	"github.com/MakeNowJust/heredoc/v2"
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
		opts      []plantree.Option
		want      string
	}{
		{
			"PLAN",
			dcaYAML,
			withStatsToRenderDefMap[false],
			nil,
			heredoc.Doc(`
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
			"compact PLAN",
			dcaYAML,
			withStatsToRenderDefMap[false],
			sliceOf(plantree.EnableCompact()),
			heredoc.Doc(`
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
			"wrapped compact PLAN",
			dcaYAML,
			withStatsToRenderDefMap[false],
			sliceOf(plantree.EnableCompact(), plantree.WithWrapWidth(40)),
			heredoc.Doc(`
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
			"wrapped PLAN",
			dcaYAML,
			withStatsToRenderDefMap[false],
			sliceOf(plantree.WithWrapWidth(50)),
			heredoc.Doc(`
+-----+---------------------------------------------------+
| ID  | Operator                                          |
+-----+---------------------------------------------------+
|   0 | Distributed Union on AlbumsByAlbumTitle <Row>     |
|  *1 | +- Distributed Cross Apply <Row>                  |
|   2 |    +- [Input] Create Batch <Row>                  |
|   3 |    |  +- Local Distributed Union <Row>            |
|   4 |    |     +- Compute Struct <Row>                  |
|   5 |    |        +- Index Scan on AlbumsByAlbumTitle < |
|     |    |           Row> (Full scan, scan_method: Auto |
|     |    |           matic)                             |
|  11 |    +- [Map] Serialize Result <Row>                |
|  12 |       +- Cross Apply <Row>                        |
|  13 |          +- [Input] Batch Scan on $v2 <Row> (scan |
|     |          |  _method: Row)                         |
|  16 |          +- [Map] Local Distributed Union <Row>   |
| *17 |             +- Filter Scan <Row> (seekable_key_si |
|     |                ze: 0)                             |
|  18 |                +- Index Scan on SongsBySongGenre  |
|     |                   <Row> (Full scan, scan_method:  |
|     |                   Row)                            |
+-----+---------------------------------------------------+
Predicates(identified by ID):
  1: Split Range: ($AlbumId = $AlbumId_1)
 17: Residual Condition: ($AlbumId = $batched_AlbumId_1)
`),
		},
		{
			"PROFILE",
			dcaProfileYAML,
			withStatsToRenderDefMap[true],
			nil,
			heredoc.Doc(`
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
			"PROFILE with custom",
			dcaProfileYAML,
			lo.Must(customFileToTableRenderDef([]byte(
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
			nil,
			heredoc.Doc(`
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
			"PROFILE with custom list",
			dcaProfileYAML,
			lo.Must(customListToTableRenderDef([]string{
				`ID:{{.FormatID}}:RIGHT`,
				`Operator:{{.Text}}`,
				`Rows:{{.ExecutionStats.Rows.Total}}:RIGHT`,
				`Scanned:{{.ExecutionStats.ScannedRows.Total}}:RIGHT`,
				`Filtered:{{.ExecutionStats.FilteredRows.Total}}:RIGHT`,
			})),
			nil,
			heredoc.Doc(`
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
			"DELETE PLAN",
			deleteYAML,
			withStatsToRenderDefMap[false],
			nil,
			heredoc.Doc(`
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
			"DELETE PLAN traditional",
			deleteYAML,
			withStatsToRenderDefMap[false],
			sliceOf(plantree.WithQueryPlanOptions(
				spannerplan.WithKnownFlagFormat(spannerplan.KnownFlagFormatRaw),
				spannerplan.WithExecutionMethodFormat(spannerplan.ExecutionMethodFormatRaw),
				spannerplan.WithTargetMetadataFormat(spannerplan.TargetMetadataFormatRaw),
			)),
			heredoc.Doc(`
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

			qp, err := spannerplan.New(stats.GetQueryPlan().GetPlanNodes())
			if err != nil {
				t.Fatal(err)
			}

			rows, err := plantree.ProcessPlan(qp, opts...)
			if err != nil {
				t.Fatal(err)
			}

			got, err := printResult(tcase.renderDef, rows, PrintPredicates)
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(tcase.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
