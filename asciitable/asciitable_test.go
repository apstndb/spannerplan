package asciitable_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/MakeNowJust/heredoc/v2"
	"github.com/google/go-cmp/cmp"

	"github.com/apstndb/spannerplan/asciitable"
)

type testRow struct {
	id         uint
	idText     string
	text       string
	rows       string
	predicates []string
}

func TestRenderTable(t *testing.T) {
	rows := []testRow{
		{id: 1, idText: "1", text: "Root", rows: "10"},
		{id: 2, idText: "2", text: "+- Child", rows: "3"},
	}
	spec := asciitable.TableSpec[testRow]{
		Columns: []asciitable.Column[testRow]{
			idColumn(),
			operatorColumn(),
			{
				Header:    "Rows",
				Alignment: asciitable.AlignRight,
				Cell: func(row testRow, _ int) string {
					return row.rows
				},
			},
		},
	}

	got, err := asciitable.RenderTable(rows, spec)
	if err != nil {
		t.Fatalf("RenderTable() error = %v", err)
	}
	want := heredoc.Doc(`
		+----+----------+------+
		| ID | Operator | Rows |
		+----+----------+------+
		|  1 | Root     |   10 |
		|  2 | +- Child |    3 |
		+----+----------+------+
	`)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("RenderTable() mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderTable_RowIndex(t *testing.T) {
	rows := []testRow{
		{id: 1, idText: "1", text: "Root"},
		{id: 2, idText: "2", text: "Child"},
	}
	spec := asciitable.TableSpec[testRow]{
		Columns: []asciitable.Column[testRow]{
			idColumn(),
			{
				Header:    "Index",
				Alignment: asciitable.AlignRight,
				Cell: func(_ testRow, index int) string {
					return strconv.Itoa(index)
				},
			},
		},
	}

	got, err := asciitable.RenderTable(rows, spec)
	if err != nil {
		t.Fatalf("RenderTable() error = %v", err)
	}
	want := heredoc.Doc(`
		+----+-------+
		| ID | Index |
		+----+-------+
		|  1 |     0 |
		|  2 |     1 |
		+----+-------+
	`)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("RenderTable() mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderTableless(t *testing.T) {
	rows := []testRow{
		{id: 1, idText: "1", text: "Root", rows: "10"},
		{id: 12, idText: "*12", text: "+- Child", rows: "3"},
	}
	spec := asciitable.TableSpec[testRow]{
		Columns: []asciitable.Column[testRow]{
			idColumn(),
			operatorColumn(),
			{
				Header:    "Rows",
				Alignment: asciitable.AlignRight,
				Cell: func(row testRow, _ int) string {
					return row.rows
				},
			},
		},
	}

	got, err := asciitable.RenderTableless(rows, spec)
	if err != nil {
		t.Fatalf("RenderTableless() error = %v", err)
	}
	want := heredoc.Doc(`
		  1|Root|10
		*12|+- Child| 3
	`)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("RenderTableless() mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderTableless_PreservesMultilineCells(t *testing.T) {
	rows := []testRow{
		{id: 1, idText: "1", text: "Root\n+- Child"},
	}
	spec := asciitable.TableSpec[testRow]{
		Columns: []asciitable.Column[testRow]{
			idColumn(),
			operatorColumn(),
			{
				Header:    "Rows",
				Alignment: asciitable.AlignRight,
				Cell: func(row testRow, _ int) string {
					return row.rows
				},
			},
		},
	}

	got, err := asciitable.RenderTableless(rows, spec)
	if err != nil {
		t.Fatalf("RenderTableless() error = %v", err)
	}
	want := heredoc.Doc(`
		1|Root
		 |+- Child
	`)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("RenderTableless() mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderTableless_PreservesEmptyPhysicalLinesAndRows(t *testing.T) {
	rows := []testRow{
		{idText: "12", text: "Root"},
		{idText: "", text: "\nChild\n"},
		{},
		{idText: "3", text: "Done"},
	}
	spec := asciitable.TableSpec[testRow]{
		Columns: []asciitable.Column[testRow]{
			idColumn(),
			operatorColumn(),
		},
	}

	got, err := asciitable.RenderTableless(rows, spec)
	if err != nil {
		t.Fatalf("RenderTableless() error = %v", err)
	}
	want := "12|Root\n\n  |Child\n\n\n 3|Done\n"
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("RenderTableless() mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderTableless_CenterAlignmentIsUnpadded(t *testing.T) {
	rows := []testRow{{text: "long"}, {text: "x"}}
	spec := asciitable.TableSpec[testRow]{
		Columns: []asciitable.Column[testRow]{
			{
				Header:    "Operator",
				Alignment: asciitable.AlignCenter,
				Cell: func(row testRow, _ int) string {
					return row.text
				},
			},
		},
	}

	got, err := asciitable.RenderTableless(rows, spec)
	if err != nil {
		t.Fatalf("RenderTableless() error = %v", err)
	}
	want := "long\nx\n"
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("RenderTableless() mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderTable_PreservesMultilineCells(t *testing.T) {
	rows := []testRow{
		{id: 1, idText: "1", text: "Root\n+- Child"},
	}
	spec := asciitable.TableSpec[testRow]{
		Columns: []asciitable.Column[testRow]{
			idColumn(),
			operatorColumn(),
		},
	}

	got, err := asciitable.RenderTable(rows, spec)
	if err != nil {
		t.Fatalf("RenderTable() error = %v", err)
	}
	want := heredoc.Doc(`
		+----+----------+
		| ID | Operator |
		+----+----------+
		|  1 | Root     |
		|    | +- Child |
		+----+----------+
	`)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("RenderTable() mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderTable_InvalidSpec(t *testing.T) {
	_, err := asciitable.RenderTable[testRow](nil, asciitable.TableSpec[testRow]{})
	if err == nil {
		t.Fatal("RenderTable() error = nil, want non-nil")
	}

	_, err = asciitable.RenderTable(nil, asciitable.TableSpec[testRow]{
		Columns: []asciitable.Column[testRow]{
			{
				Header:    "bad",
				Alignment: asciitable.Alignment("diagonal"),
				Cell: func(testRow, int) string {
					return ""
				},
			},
		},
	})
	if err == nil {
		t.Fatal("RenderTable() invalid alignment error = nil, want non-nil")
	}

	_, err = asciitable.RenderTable(nil, asciitable.TableSpec[testRow]{
		Columns: []asciitable.Column[testRow]{{Header: "bad"}},
	})
	if err == nil {
		t.Fatal("RenderTable() nil Cell error = nil, want non-nil")
	}
}

func TestRenderTableless_InvalidSpec(t *testing.T) {
	tests := []struct {
		name string
		spec asciitable.TableSpec[testRow]
	}{
		{
			name: "no columns",
		},
		{
			name: "invalid alignment",
			spec: asciitable.TableSpec[testRow]{
				Columns: []asciitable.Column[testRow]{
					{
						Header:    "bad",
						Alignment: asciitable.Alignment("diagonal"),
						Cell: func(testRow, int) string {
							return ""
						},
					},
				},
			},
		},
		{
			name: "nil Cell",
			spec: asciitable.TableSpec[testRow]{
				Columns: []asciitable.Column[testRow]{{Header: "bad"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := asciitable.RenderTableless[testRow](nil, tt.spec); err == nil {
				t.Fatal("RenderTableless() error = nil, want non-nil")
			}
		})
	}
}

func BenchmarkRenderTableless_SparseMultilineRow(b *testing.B) {
	const (
		columnCount = 1000
		lineCount   = 1000
	)

	row := make([]string, columnCount)
	row[0] = strings.Repeat("x\n", lineCount-1) + "x"
	spec := asciitable.TableSpec[[]string]{
		Columns: make([]asciitable.Column[[]string], columnCount),
	}
	for i := range spec.Columns {
		columnIndex := i
		spec.Columns[i] = asciitable.Column[[]string]{
			Cell: func(row []string, _ int) string {
				return row[columnIndex]
			},
		}
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := asciitable.RenderTableless([][]string{row}, spec); err != nil {
			b.Fatal(err)
		}
	}
}

func TestRenderAppendix(t *testing.T) {
	rows := []testRow{
		{id: 3, text: "Filter", predicates: []string{"Filter: a = 1", "Expression: b"}},
		{id: 12, text: "Scan", predicates: []string{"Seek Condition: k = 1"}},
	}

	got, err := asciitable.RenderAppendix(rows, testAppendixSpec("Predicates(identified by ID):"))
	if err != nil {
		t.Fatalf("RenderAppendix() error = %v", err)
	}
	want := heredoc.Doc(`
		Predicates(identified by ID):
		  3: Filter: a = 1
		     Expression: b
		 12: Seek Condition: k = 1
	`)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("RenderAppendix() mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderAppendix_CustomTitle(t *testing.T) {
	rows := []testRow{
		{id: 3, text: "Filter", predicates: []string{"Filter: a = 1"}},
	}

	got, err := asciitable.RenderAppendix(rows, testAppendixSpec("Filters:"))
	if err != nil {
		t.Fatalf("RenderAppendix() error = %v", err)
	}
	want := heredoc.Doc(`
		Filters:
		 3: Filter: a = 1
	`)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("RenderAppendix() mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderAppendix_MultiDigitIDs(t *testing.T) {
	rows := []testRow{
		{id: 3, text: "Filter", predicates: []string{"Filter: a = 1", "Expression: b"}},
		{id: 120, text: "Scan", predicates: []string{"Seek Condition: k = 1"}},
	}

	got, err := asciitable.RenderAppendix(rows, testAppendixSpec("Predicates(identified by ID):"))
	if err != nil {
		t.Fatalf("RenderAppendix() error = %v", err)
	}
	want := heredoc.Doc(`
		Predicates(identified by ID):
		   3: Filter: a = 1
		      Expression: b
		 120: Seek Condition: k = 1
	`)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("RenderAppendix() mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderAppendix_None(t *testing.T) {
	got, err := asciitable.RenderAppendix([]testRow{{id: 1, text: "Root"}}, testAppendixSpec("Predicates(identified by ID):"))
	if err != nil {
		t.Fatalf("RenderAppendix() error = %v", err)
	}
	if got != "" {
		t.Fatalf("RenderAppendix() = %q, want empty", got)
	}
}

func TestRenderAppendix_ReadsEachRowOnce(t *testing.T) {
	rows := []testRow{
		{id: 1, text: "Root"},
		{id: 2, text: "Filter", predicates: []string{"Filter: true"}},
	}
	var idCalls int
	var itemsCalls int
	spec := asciitable.AppendixSpec[testRow]{
		Title: "Predicates(identified by ID):",
		ID: func(row testRow) uint {
			idCalls++
			return row.id
		},
		Items: func(row testRow) []string {
			itemsCalls++
			return row.predicates
		},
	}

	_, err := asciitable.RenderAppendix(rows, spec)
	if err != nil {
		t.Fatalf("RenderAppendix() error = %v", err)
	}
	if idCalls != len(rows) {
		t.Fatalf("ID calls = %d, want %d", idCalls, len(rows))
	}
	if itemsCalls != len(rows) {
		t.Fatalf("Items calls = %d, want %d", itemsCalls, len(rows))
	}
}

func TestRenderAppendix_InvalidSpec(t *testing.T) {
	rows := []testRow{
		{id: 1, text: "Root", predicates: []string{"Filter: true"}},
	}

	_, err := asciitable.RenderAppendix(rows, asciitable.AppendixSpec[testRow]{})
	if err == nil {
		t.Fatal("RenderAppendix() error = nil, want non-nil")
	}
}

func idColumn() asciitable.Column[testRow] {
	return asciitable.Column[testRow]{
		Header:    "ID",
		Alignment: asciitable.AlignRight,
		Cell: func(row testRow, _ int) string {
			return row.idText
		},
	}
}

func operatorColumn() asciitable.Column[testRow] {
	return asciitable.Column[testRow]{
		Header:    "Operator",
		Alignment: asciitable.AlignLeft,
		Cell: func(row testRow, _ int) string {
			return row.text
		},
	}
}

func testAppendixSpec(title string) asciitable.AppendixSpec[testRow] {
	return asciitable.AppendixSpec[testRow]{
		Title: title,
		ID: func(row testRow) uint {
			return row.id
		},
		Items: func(row testRow) []string {
			return row.predicates
		},
	}
}
