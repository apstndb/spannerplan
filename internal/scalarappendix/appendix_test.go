package scalarappendix

import (
	"strings"
	"testing"

	heredoc "github.com/MakeNowJust/heredoc/v2"
	"github.com/google/go-cmp/cmp"

	"github.com/apstndb/spannerplan/plantree"
)

func TestParseSections(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Sections
		wantErr string
	}{
		{
			name:  "single section",
			input: "predicates",
			want:  Sections{SectionPredicates},
		},
		{
			name:  "multiple sections",
			input: " Predicates, Ordering, aggregate ",
			want:  Sections{SectionPredicates, SectionOrdering, SectionAggregate},
		},
		{
			name:  "basic preset",
			input: "basic",
			want:  Sections{SectionPredicates},
		},
		{
			name:  "enhanced preset",
			input: " Enhanced ",
			want:  Sections{SectionPredicates, SectionOrdering, SectionAggregate},
		},
		{
			name:  "full preset",
			input: "full",
			want:  Sections{SectionFull},
		},
		{
			name:  "none preset",
			input: "none",
			want:  Sections{},
		},
		{
			name:  "empty means no sections",
			input: "",
			want:  Sections{},
		},
		{
			name:  "blank means no sections",
			input: " \t ",
			want:  Sections{},
		},
		{
			name:    "empty element",
			input:   "predicates,",
			wantErr: "print section must not be empty",
		},
		{
			name:    "unknown",
			input:   "broken",
			wantErr: "unknown print preset or section: broken",
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
			got, err := ParseSections(tt.input)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("ParseSections() error = nil, want non-nil")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ParseSections() error = %q, want substring %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseSections() error = %v", err)
			}
			if tt.want != nil && got == nil {
				t.Fatal("ParseSections() returned nil, want non-nil explicit sections")
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("ParseSections() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRender(t *testing.T) {
	rows := scalarAppendixRows()

	sections := Sections{SectionPredicates, SectionOrdering, SectionAggregate}
	got, err := Render(rows, Options{Sections: &sections})
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	want := heredoc.Doc(`
Predicates(identified by ID):
 2: Condition: ($SingerId = $SingerId_1)

Ordering(identified by ID):
 0: Key: $SongCount DESC, $group_SongGenre'

Aggregates(identified by ID):
 1: Key: $group_SongGenre
    Agg: COUNT_FINAL($v1)
`)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Render() mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderDefaultAndEmptySections(t *testing.T) {
	rows := scalarAppendixRows()

	got, err := Render(rows, Options{})
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	want := heredoc.Doc(`
Predicates(identified by ID):
 2: Condition: ($SingerId = $SingerId_1)
`)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Render() default sections mismatch (-want +got):\n%s", diff)
	}

	sections := Sections{}
	got, err = Render(rows, Options{Sections: &sections})
	if err != nil {
		t.Fatalf("Render(empty sections) error = %v", err)
	}
	if got != "" {
		t.Fatalf("Render(empty sections) = %q, want empty", got)
	}
}

func TestRenderResolveScalarVars(t *testing.T) {
	rows := scalarAppendixRows()
	sections := Sections{SectionOrdering, SectionAggregate}

	got, err := Render(rows, Options{
		Sections:                   &sections,
		ShowScalarVars:             true,
		ResolveScalarVarsRecursive: true,
	})
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	want := heredoc.Doc(`
Ordering(identified by ID):
 0: Key: $sort_count=COUNT_FINAL(COUNT()) DESC, $sort_genre=SongGenre

Aggregates(identified by ID):
 1: Key: $group_SongGenre'=SongGenre
    Agg: $SongCount=COUNT_FINAL($v1)
`)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Render() mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderRawSections(t *testing.T) {
	rows := scalarAppendixRows()

	sections := Sections{SectionTyped}
	got, err := Render(rows, Options{Sections: &sections})
	if err != nil {
		t.Fatalf("Render(typed) error = %v", err)
	}
	want := heredoc.Doc(`
Node Parameters(identified by ID):
 0: Key: $sort_count=$SongCount (DESC), $sort_genre=$group_SongGenre'
 1: Key: $group_SongGenre'=$group_SongGenre
    Agg: $SongCount=COUNT_FINAL($v1)
 2: Condition: ($SingerId = $SingerId_1)
`)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Render(typed) mismatch (-want +got):\n%s", diff)
	}

	sections = Sections{SectionFull}
	got, err = Render(rows, Options{Sections: &sections})
	if err != nil {
		t.Fatalf("Render(full) error = %v", err)
	}
	want = heredoc.Doc(`
Node Parameters(identified by ID):
 0: Key: $sort_count=$SongCount (DESC), $sort_genre=$group_SongGenre'
 1: Key: $group_SongGenre'=$group_SongGenre
    Agg: $SongCount=COUNT_FINAL($v1)
 2: Condition: ($SingerId = $SingerId_1)
 3: $group_SongGenre=$SongGenre, $SongGenre=SongGenre, $v1=COUNT()
`)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Render(full) mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderUnsupportedSection(t *testing.T) {
	sections := Sections{"broken"}
	_, err := Render(nil, Options{Sections: &sections})
	if err == nil {
		t.Fatal("Render() error = nil, want non-nil")
	}
	if got, want := err.Error(), "unsupported print section: broken"; got != want {
		t.Fatalf("Render() error = %q, want %q", got, want)
	}
}

func scalarAppendixRows() []plantree.RowWithPredicates {
	return []plantree.RowWithPredicates{
		{
			ID:          0,
			DisplayName: "Sort",
			NodeText:    "Sort",
			ScalarChildLinks: []plantree.ScalarChildLink{
				{Type: "Key", Variable: "sort_count", Description: "$SongCount (DESC)"},
				{Type: "Key", Variable: "sort_genre", Description: "$group_SongGenre'"},
			},
		},
		{
			ID:          1,
			DisplayName: "Aggregate",
			NodeText:    "Aggregate",
			ScalarChildLinks: []plantree.ScalarChildLink{
				{Type: "Key", Variable: "group_SongGenre'", Description: "$group_SongGenre"},
				{Type: "Agg", Variable: "SongCount", Description: "COUNT_FINAL($v1)"},
			},
		},
		{
			ID:          2,
			DisplayName: "Filter",
			NodeText:    "Filter",
			Predicates:  []string{"Condition: ($SingerId = $SingerId_1)"},
			ScalarChildLinks: []plantree.ScalarChildLink{
				{Type: "Condition", Description: "($SingerId = $SingerId_1)"},
			},
		},
		{
			ID:          3,
			DisplayName: "Scan",
			NodeText:    "Scan",
			ScalarChildLinks: []plantree.ScalarChildLink{
				{Variable: "group_SongGenre", Description: "$SongGenre"},
				{Variable: "SongGenre", Description: "SongGenre"},
				{Variable: "v1", Description: "COUNT()"},
			},
		},
	}
}
