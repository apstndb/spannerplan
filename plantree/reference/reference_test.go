package reference

import (
	"os"
	"testing"

	"github.com/MakeNowJust/heredoc/v2"
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

func Test_RenderASCIIImpl_withRealPlan_ALL_MODES_AND_FORMATS(t *testing.T) {
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
			got, err := renderASCIIImpl(input, tc.mode, tc.format, 0)
			if err != nil {
				t.Fatalf("renderASCIIImpl returned error: %v", err)
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
