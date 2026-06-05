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
	tableRows, headers, alignments, err := collectTableRows(rows, spec)
	if err != nil {
		return "", err
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

	for i, rowData := range tableRows {
		if err := table.Append(rowData); err != nil {
			return "", fmt.Errorf("failed to append row at index %d: %w", i, err)
		}
	}

	if err := table.Render(); err != nil {
		return "", fmt.Errorf("failed to render table: %w", err)
	}
	return sb.String(), nil
}

// RenderTableless renders rows without a table grid, using "|" as a one-character column separator.
// Right-aligned columns are left-padded to their own maximum content width; left-aligned columns
// are not padded, so wide text columns do not reintroduce table-grid width. Center alignment is
// not supported by this layout and is rendered as unpadded text.
func RenderTableless[T any](rows []T, spec TableSpec[T]) (string, error) {
	tableRows, _, alignments, err := collectTableRows(rows, spec)
	if err != nil {
		return "", err
	}

	columnWidths := maxTableColumnLineWidths(tableRows, len(alignments))

	var sb strings.Builder
	for _, row := range tableRows {
		lines := splitTableRowLines(row)
		for _, line := range lines {
			for len(line) > 0 && line[len(line)-1] == "" {
				line = line[:len(line)-1]
			}
			if len(line) == 0 {
				continue
			}
			for i := range line {
				line[i] = alignTablelessCell(line[i], columnWidths[i], alignments[i])
			}
			sb.WriteString(strings.Join(line, "|"))
			sb.WriteByte('\n')
		}
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

func collectTableRows[T any](rows []T, spec TableSpec[T]) ([][]string, []string, []tw.Align, error) {
	if len(spec.Columns) == 0 {
		return nil, nil, nil, fmt.Errorf("table spec must contain at least one column")
	}

	headers := make([]string, 0, len(spec.Columns))
	alignments := make([]tw.Align, 0, len(spec.Columns))
	for i, col := range spec.Columns {
		if col.Cell == nil {
			return nil, nil, nil, fmt.Errorf("table spec column %d (%q) has nil Cell", i, col.Header)
		}
		alignment, err := mapAlignment(col.Alignment)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("table spec column %d (%q): %w", i, col.Header, err)
		}
		headers = append(headers, col.Header)
		alignments = append(alignments, alignment)
	}

	tableRows := make([][]string, 0, len(rows))
	for i, row := range rows {
		rowData := make([]string, len(spec.Columns))
		for j, col := range spec.Columns {
			rowData[j] = col.Cell(row, i)
		}
		tableRows = append(tableRows, rowData)
	}
	return tableRows, headers, alignments, nil
}

func maxTableColumnLineWidths(rows [][]string, columns int) []int {
	widths := make([]int, columns)
	for _, row := range rows {
		for i, cell := range row {
			if i >= len(widths) {
				break
			}
			for _, line := range strings.Split(cell, "\n") {
				widths[i] = max(widths[i], tabwrap.StringWidth(line))
			}
		}
	}
	return widths
}

func splitTableRowLines(row []string) [][]string {
	height := 1
	splitCells := make([][]string, 0, len(row))
	for _, cell := range row {
		lines := strings.Split(cell, "\n")
		height = max(height, len(lines))
		splitCells = append(splitCells, lines)
	}

	result := make([][]string, 0, height)
	for i := range height {
		line := make([]string, len(splitCells))
		for j, cellLines := range splitCells {
			if i < len(cellLines) {
				line[j] = cellLines[i]
			}
		}
		result = append(result, line)
	}
	return result
}

func alignTablelessCell(s string, width int, alignment tw.Align) string {
	if alignment == tw.AlignRight {
		return leftPadDisplay(s, width)
	}
	// Tableless output intentionally supports only right-padding behavior; center
	// alignment would require adding both-side padding and reintroduce grid width.
	return s
}

func leftPadDisplay(s string, width int) string {
	currentWidth := tabwrap.StringWidth(s)
	if currentWidth >= width {
		return s
	}
	return strings.Repeat(" ", width-currentWidth) + s
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
