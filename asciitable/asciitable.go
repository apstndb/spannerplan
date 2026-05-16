// Package asciitable renders caller-owned rows as ASCII tables.
//
// It is intentionally independent of Spanner plan protobufs and plantree types:
// callers build rows from their own plan model, then provide column definitions
// and predicate accessors for the cells they want to show.
package asciitable

import (
	"fmt"
	"strconv"
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
	// Header is the column header text.
	Header string
	// Alignment controls the cell alignment. The zero value uses [AlignLeft].
	Alignment Alignment
	// Cell returns the rendered cell text for each row.
	Cell CellFunc[T]
}

// TableSpec defines the columns of an ASCII table.
type TableSpec[T any] struct {
	// Columns is the ordered list of table columns.
	Columns []Column[T]
}

// AppendixSpec defines how appendices read row IDs and item lines.
type AppendixSpec[T any] struct {
	// Title is printed before item lines. It must be non-empty.
	Title string
	// ID returns the non-negative display ID used in the appendix.
	ID func(row T) uint
	// Items returns the item lines associated with the row.
	Items func(row T) []string
}

type appendixRows struct {
	rows        []appendixRow
	hasItems    bool
	maxIDLength int
}

type appendixRow struct {
	id    uint
	items []string
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
		tablewriter.WithRowAutoWrap(tw.WrapNone),
	)
	table.Header(headers)

	for i, row := range rows {
		rowData := make([]string, len(spec.Columns))
		for j, col := range spec.Columns {
			rowData[j] = col.Cell(row, i)
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

// RenderAppendix formats an appendix for rows with associated items.
func RenderAppendix[T any](rows []T, spec AppendixSpec[T]) (string, error) {
	resolved, err := collectAppendixRows(rows, spec)
	if err != nil {
		return "", err
	}
	if !resolved.hasItems {
		return "", nil
	}

	var sb strings.Builder
	_, _ = fmt.Fprintln(&sb, spec.Title)
	for _, row := range resolved.rows {
		for i, item := range row.items {
			idPartStr := ""
			if i == 0 {
				idPartStr = strconv.FormatUint(uint64(row.id), 10) + ":"
			}

			prefix := tabwrap.FillLeft(idPartStr, resolved.maxIDLength+1)
			_, _ = fmt.Fprintf(&sb, " %s %s\n", prefix, item)
		}
	}
	return sb.String(), nil
}

func collectAppendixRows[T any](rows []T, spec AppendixSpec[T]) (appendixRows, error) {
	if spec.Title == "" {
		return appendixRows{}, fmt.Errorf("appendix spec has empty Title")
	}
	if spec.ID == nil {
		return appendixRows{}, fmt.Errorf("appendix spec has nil ID")
	}
	if spec.Items == nil {
		return appendixRows{}, fmt.Errorf("appendix spec has nil Items")
	}

	var resolved appendixRows
	for _, row := range rows {
		id := spec.ID(row)
		items := spec.Items(row)
		if idLength := len(strconv.FormatUint(uint64(id), 10)); idLength > resolved.maxIDLength {
			resolved.maxIDLength = idLength
		}
		if len(items) > 0 {
			resolved.hasItems = true
			resolved.rows = append(resolved.rows, appendixRow{
				id:    id,
				items: items,
			})
		}
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
