This tool render YAML or JSON representation of Cloud Spanner query plan as ascii format.

It can read various types.
* [QueryPlan](https://cloud.google.com/spanner/docs/reference/rest/v1/ResultSetStats?hl=en#QueryPlan)
  * Can get easily by client libraries
    * [AnalyzeQuery()](https://pkg.go.dev/cloud.google.com/go/spanner#ReadOnlyTransaction.AnalyzeQuery)
    * [RowIterator.QueryPlan](https://pkg.go.dev/cloud.google.com/go/spanner#RowIterator)
* [ResultSetStats](https://cloud.google.com/spanner/docs/reference/rest/v1/ResultSetStats?hl=en)
  * Output of DOWNLOAD JSON in [the official query plan visualizer](https://cloud.google.com/spanner/docs/tune-query-with-visualizer?hl=en)
* [ResultSet](https://cloud.google.com/spanner/docs/reference/rest/v1/ResultSet?hl=en)
  * Output of `gcloud spanner databases execute-sql` and [execspansql](https://github.com/apstndb/execspansql)

It can render both PLAN or PROFILE.

```
# from file
$ cat queryplan.yaml | rendertree --mode=PLAN
+----+-----------------------------------------+
| ID | Operator                                |
+----+-----------------------------------------+
| *0 | Distributed Union                       |
|  1 | +- Local Distributed Union              |
|  2 |    +- Serialize Result                  |
| *3 |       +- FilterScan                     |
|  4 |          +- Table Scan (Table: Singers) |
+----+-----------------------------------------+
Predicates(identified by ID):
 0: Split Range: ($SingerId = 1)
 3: Seek Condition: ($SingerId = 1)

# with gcloud spanner databases execute-sql
$ gcloud spanner databases execute-sql ${DATABASE_ID} --sql="SELECT * FROM Singers" --format=json --query-mode=PROFILE |
    rendertree --mode=PROFILE
+----+-------------------------------------------------------+------+-------+---------+
| ID | Operator                                              | Rows | Exec. | Latency |
+----+-------------------------------------------------------+------+-------+---------+
|  0 | Distributed Union                                     | 1000 |     1 | 6.29 ms |
|  1 | +- Local Distributed Union                            | 1000 |     1 | 6.21 ms |
|  2 |    +- Serialize Result                                | 1000 |     1 | 6.16 ms |
|  3 |       +- Table Scan (Full scan: true, Table: Singers) | 1000 |     1 | 5.78 ms |
+----+-------------------------------------------------------+------+-------+---------+
```

Note: `--mode=PLAN` and `--mode=PROFILE` can be omitted because the default `--mode=AUTO` can detect whether the input has execution statistics or not.

Rendered stats columns are customizable using `--custom-file`.

```
$ cat custom.yaml
- name: ID
  template: '{{.FormatID}}'
  alignment: RIGHT
- name: Operator
  template: '{{.Text}}'
  alignment: LEFT
- name: Rows
  template: '{{.ExecutionStats.Rows.Total}}'
  alignment: RIGHT
  inline: NEVER
- name: Scanned
  template: '{{.ExecutionStats.ScannedRows.Total}}'
  alignment: RIGHT
  inline: CAN
- name: remote_calls
  template: '{{.ExecutionStats.RemoteCalls.Total}}'
  alignment: RIGHT
  inline: ALWAYS
```

`inline` field in the custom configuration and the `--inline-stats` command-line flag together control how execution statistics are rendered.
Inline stats are particularly useful for displaying *sparse* statistics (those that only appear on a few operators) without adding many empty columns to the main table, thus improving readability.

The following table shows how the `inline` field setting for a specific statistic interacts with the `--inline-stats` flag to determine its display location:

| `inline`/`--inline-stats` | true     | false    |
|---------------------------|----------|----------|
| `NEVER`                   | in table | in table |
| `CAN`      or unspecified | inline   | in table |
| `ALWAYS`                  | inline   | inline   |

In summary, the `--inline-stats` flag enables inline display for statistics marked as `CAN` or unspecified.
`ALWAYS` forces inline display regardless of the flag, and `NEVER` always keeps them in a separate column.

Note: `ID` and `Operator` columns are treated as `ALWAYS` if `inline` is not specified.

```
$ rendertree --custom-file custom.example.yaml < profile.yaml
+-----+--------------------------------------------------------------------------+------+---------+
| ID  | Operator                                                                 | Rows | scanned |
+-----+--------------------------------------------------------------------------+------+---------+
|  *0 | Distributed Union on SongsBySongName <Row> (remote_calls=0)              | 3069 |         |
|  *1 | +- Distributed Cross Apply <Row> (remote_calls=0)                        | 3069 |         |
|   2 |    +- [Input] Create Batch <Row>                                         |      |         |
|   3 |    |  +- Local Distributed Union <Row> (remote_calls=0)                  | 3069 |         |
|   4 |    |     +- Compute Struct <Row>                                         | 3069 |         |
|  *5 |    |        +- Filter Scan <Row> (seekable_key_size: 1)                  | 3069 |         |
|  *6 |    |           +- Index Scan on SongsBySongName <Row> (scan_method: Row) | 3069 |   14212 |
|  24 |    +- [Map] Serialize Result <Row>                                       | 3069 |         |
|  25 |       +- Cross Apply <Row>                                               | 3069 |         |
|  26 |          +- [Input] KeyRangeAccumulator <Row>                            |      |         |
|  27 |          |  +- Batch Scan on $v2 <Row> (scan_method: Row)                |      |         |
|  32 |          +- [Map] Local Distributed Union <Row> (remote_calls=0)         | 3069 |         |
|  33 |             +- Filter Scan <Row> (seekable_key_size: 0)                  |      |         |
| *34 |                +- Table Scan on Songs <Row> (scan_method: Row)           | 3069 |    3069 |
+-----+--------------------------------------------------------------------------+------+---------+
Predicates(identified by ID):
  0: Split Range: (STARTS_WITH($SongName, 'Th') AND ($SongName LIKE 'Th%e'))
  1: Split Range: (($SingerId' = $SingerId) AND ($AlbumId' = $AlbumId) AND ($TrackId' = $TrackId))
  5: Residual Condition: ($SongName LIKE 'Th%e')
  6: Seek Condition: STARTS_WITH($SongName, 'Th')
 34: Seek Condition: (($SingerId' = $batched_SingerId) AND ($AlbumId' = $batched_AlbumId) AND ($TrackId' = $batched_TrackId))
```

```
$ rendertree --inline-stats --custom-file custom.example.yaml < profile.yaml
+-----+-----------------------------------------------------------------------------------------+------+
| ID  | Operator                                                                                | Rows |
+-----+-----------------------------------------------------------------------------------------+------+
|  *0 | Distributed Union on SongsBySongName <Row> (remote_calls=0)                             | 3069 |
|  *1 | +- Distributed Cross Apply <Row> (remote_calls=0)                                       | 3069 |
|   2 |    +- [Input] Create Batch <Row>                                                        |      |
|   3 |    |  +- Local Distributed Union <Row> (remote_calls=0)                                 | 3069 |
|   4 |    |     +- Compute Struct <Row>                                                        | 3069 |
|  *5 |    |        +- Filter Scan <Row> (seekable_key_size: 1)                                 | 3069 |
|  *6 |    |           +- Index Scan on SongsBySongName <Row> (scan_method: Row, scanned=14212) | 3069 |
|  24 |    +- [Map] Serialize Result <Row>                                                      | 3069 |
|  25 |       +- Cross Apply <Row>                                                              | 3069 |
|  26 |          +- [Input] KeyRangeAccumulator <Row>                                           |      |
|  27 |          |  +- Batch Scan on $v2 <Row> (scan_method: Row)                               |      |
|  32 |          +- [Map] Local Distributed Union <Row> (remote_calls=0)                        | 3069 |
|  33 |             +- Filter Scan <Row> (seekable_key_size: 0)                                 |      |
| *34 |                +- Table Scan on Songs <Row> (scan_method: Row, scanned=3069)            | 3069 |
+-----+-----------------------------------------------------------------------------------------+------+
Predicates(identified by ID):
  0: Split Range: (STARTS_WITH($SongName, 'Th') AND ($SongName LIKE 'Th%e'))
  1: Split Range: (($SingerId' = $SingerId) AND ($AlbumId' = $AlbumId) AND ($TrackId' = $TrackId))
  5: Residual Condition: ($SongName LIKE 'Th%e')
  6: Seek Condition: STARTS_WITH($SongName, 'Th')
 34: Seek Condition: (($SingerId' = $batched_SingerId) AND ($AlbumId' = $batched_AlbumId) AND ($TrackId' = $batched_TrackId))
```

You can also use `--custom=<name>:<template>[:<align>[:<inline_type>]]`.

```
$ cat distributed_cross_apply_profile.yaml | rendertree --custom "ID:{{.FormatID}}:RIGHT,Operator:{{.Text}},CPU Time:{{.ExecutionStats.CpuTime | secsToS}},remote_calls:{{.ExecutionStats.RemoteCalls.Total}}::ALWAYS" 
+-----+-------------------------------------------------------------------------------------------+----------+
| ID  | Operator                                                                                  | CPU Time |
+-----+-------------------------------------------------------------------------------------------+----------+
|   0 | Distributed Union on AlbumsByAlbumTitle <Row> (remote_calls=0)                            | 0.59 ms  |
|  *1 | +- Distributed Cross Apply <Row> (remote_calls=0)                                         | 0.57 ms  |
|   2 |    +- [Input] Create Batch <Row>                                                          |          |
|   3 |    |  +- Local Distributed Union <Row> (remote_calls=0)                                   | 0.28 ms  |
|   4 |    |     +- Compute Struct <Row>                                                          | 0.27 ms  |
|   5 |    |        +- Index Scan on AlbumsByAlbumTitle <Row> (Full scan, scan_method: Automatic) | 0.26 ms  |
|  11 |    +- [Map] Serialize Result <Row>                                                        | 0.22 ms  |
|  12 |       +- Cross Apply <Row>                                                                | 0.2 ms   |
|  13 |          +- [Input] Batch Scan on $v2 <Row> (scan_method: Row)                            | 0.01 ms  |
|  16 |          +- [Map] Local Distributed Union <Row> (remote_calls=0)                          | 0.19 ms  |
| *17 |             +- Filter Scan <Row> (seekable_key_size: 0)                                   |          |
|  18 |                +- Index Scan on SongsBySongGenre <Row> (Full scan, scan_method: Row)      | 0.18 ms  |
+-----+-------------------------------------------------------------------------------------------+----------+
Predicates(identified by ID):
  1: Split Range: ($AlbumId = $AlbumId_1)
 17: Residual Condition: ($AlbumId = $batched_AlbumId_1)
```

## Options for narrower width

rendertree supports a compact format and wrapping for limited width environment.

- `--compact` enables the compact format:
  - Each level of depth in the Query Plan tree adds only one character to its indentation.
  - Whitespaces are not inserted for operator and metadata display unless it causes ambiguity.
- `--wrap-width` specifies the number of characters at which to wrap the content of the Operator column.
  - The tree won't be broken even when lines are wrapped.

```
$ rendertree --compact --wrap-width=60 < testdata/distributed_cross_apply.yaml 
+-----+--------------------------------------------------------------+
| ID  | Operator                                                     |
+-----+--------------------------------------------------------------+
|   0 | Distributed Union on AlbumsByAlbumTitle<Row>                 |
|  *1 | +Distributed Cross Apply<Row>                                |
|   2 |  +[Input]Create Batch<Row>                                   |
|   3 |  |+Local Distributed Union<Row>                              |
|   4 |  | +Compute Struct<Row>                                      |
|   5 |  |  +Index Scan on AlbumsByAlbumTitle<Row>(Full scan,scan_me |
|     |  |   thod:Automatic)                                         |
|  11 |  +[Map]Serialize Result<Row>                                 |
|  12 |   +Cross Apply<Row>                                          |
|  13 |    +[Input]Batch Scan on $v2<Row>(scan_method:Row)           |
|  16 |    +[Map]Local Distributed Union<Row>                        |
| *17 |     +Filter Scan<Row>(seekable_key_size:0)                   |
|  18 |      +Index Scan on SongsBySongGenre<Row>(Full scan,scan_met |
|     |       hod:Row)                                               |
+-----+--------------------------------------------------------------+
Predicates(identified by ID):
  1: Split Range: ($AlbumId = $AlbumId_1)
 17: Residual Condition: ($AlbumId = $batched_AlbumId_1)
```
