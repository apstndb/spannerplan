// Package asciitable renders caller-owned rows as ASCII tables.
//
// It is intentionally independent of Spanner plan protobufs and plantree types:
// callers build rows from their own plan model, then provide column definitions
// and predicate accessors for the cells they want to show.
package asciitable

import (
	"fmt"
	"strings"

	"github.com/apstndb/go-tabwrap"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/renderer"
	"github.com/olekukonko/tablewriter/tw"
)

// Alignment controls horizontal alignment for a rendered column. The zero value means [AlignLeft].
type Alignment string

const (
	// AlignLeft left-aligns cell text. It is also the default when [Column.Alignment] is empty.
	AlignLeft Alignment = "left"
	// AlignRight right-aligns cell text.
	AlignRight Alignment = "right"
	// AlignCenter centers cell text.
	AlignCenter Alignment = "center"
)

// CellFunc returns one table cell for row at index.
type CellFunc[T any] func(row T, index int) string

// Column defines one rendered table column.
type Column[T any] struct {
	Header string
	// Alignment controls the cell alignment. The zero value uses [AlignLeft].
	Alignment Alignment
	Cell      CellFunc[T]
}

// TableSpec defines the columns of an ASCII table.
type TableSpec[T any] struct {
	Columns []Column[T]
}

// PredicateSpec defines how predicate appendices read row IDs and predicate lines.
type PredicateSpec[T any] struct {
	// ID returns the non-negative display ID used in the predicate appendix.
	ID         func(row T) uint
	Predicates func(row T) []string
}

type predicateRows struct {
	rows          []predicateRow
	hasPredicates bool
	maxIDLength   int
}

type predicateRow struct {
	id         uint
	predicates []string
}

// RenderTable renders rows using spec.
func RenderTable[T any](rows []T, spec TableSpec[T]) (string, error) {
	if len(spec.Columns) == 0 {
		return "", fmt.Errorf("table spec must contain at least one column")
	}

	headers := make([]string, 0, len(spec.Columns))
	alignments := make([]tw.Align, 0, len(spec.Columns))
	for i, col := range spec.Columns {
		if col.Cell == nil {
			return "", fmt.Errorf("table spec column %d (%q) has nil Cell", i, col.Header)
		}
		alignment, err := mapAlignment(col.Alignment)
		if err != nil {
			return "", fmt.Errorf("table spec column %d (%q): %w", i, col.Header, err)
		}
		headers = append(headers, col.Header)
		alignments = append(alignments, alignment)
	}

	var sb strings.Builder
	table := tablewriter.NewTable(&sb,
		tablewriter.WithRenderer(
			renderer.NewBlueprint(tw.Rendition{Symbols: tw.NewSymbols(tw.StyleASCII)}),
		),
		tablewriter.WithTrimSpace(tw.Off),
		tablewriter.WithHeaderAutoFormat(tw.Off),
		tablewriter.WithHeaderAlignment(tw.AlignLeft),
		tablewriter.WithRowAlignmentConfig(tw.CellAlignment{PerColumn: alignments}),
	)
	table.Header(headers)

	for i, row := range rows {
		rowData := make([]string, 0, len(spec.Columns))
		for _, col := range spec.Columns {
			rowData = append(rowData, col.Cell(row, i))
		}

		if err := table.Append(rowData); err != nil {
			return "", fmt.Errorf("failed to append row at index %d: %w", i, err)
		}
	}

	if err := table.Render(); err != nil {
		return "", fmt.Errorf("failed to render table: %w", err)
	}
	return sb.String(), nil
}

// RenderPredicates formats the predicate appendix for rows with predicates.
func RenderPredicates[T any](rows []T, spec PredicateSpec[T]) (string, error) {
	resolved, err := collectPredicateRows(rows, spec)
	if err != nil {
		return "", err
	}
	if !resolved.hasPredicates {
		return "", nil
	}

	var sb strings.Builder
	_, _ = fmt.Fprintln(&sb, "Predicates(identified by ID):")
	for _, row := range resolved.rows {
		for i, predicate := range row.predicates {
			idPartStr := ""
			if i == 0 {
				idPartStr = fmt.Sprint(row.id) + ":"
			}

			prefix := tabwrap.FillLeft(idPartStr, resolved.maxIDLength+1)
			_, _ = fmt.Fprintf(&sb, " %s %s\n", prefix, predicate)
		}
	}
	return sb.String(), nil
}

func collectPredicateRows[T any](rows []T, spec PredicateSpec[T]) (predicateRows, error) {
	if spec.ID == nil {
		return predicateRows{}, fmt.Errorf("predicate spec has nil ID")
	}
	if spec.Predicates == nil {
		return predicateRows{}, fmt.Errorf("predicate spec has nil Predicates")
	}

	resolved := predicateRows{
		rows: make([]predicateRow, 0, len(rows)),
	}
	for _, row := range rows {
		id := spec.ID(row)
		predicates := spec.Predicates(row)
		if idLength := len(fmt.Sprint(id)); idLength > resolved.maxIDLength {
			resolved.maxIDLength = idLength
		}
		if len(predicates) > 0 {
			resolved.hasPredicates = true
		}
		resolved.rows = append(resolved.rows, predicateRow{
			id:         id,
			predicates: predicates,
		})
	}
	return resolved, nil
}

func mapAlignment(alignment Alignment) (tw.Align, error) {
	switch alignment {
	case "", AlignLeft:
		return tw.AlignLeft, nil
	case AlignRight:
		return tw.AlignRight, nil
	case AlignCenter:
		return tw.AlignCenter, nil
	default:
		return tw.AlignDefault, fmt.Errorf("unknown alignment %q", alignment)
	}
}
