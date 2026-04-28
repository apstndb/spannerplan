// Package reference provides a reference implementation for rendering
// Spanner query plans as ASCII tables with various formatting options.
package reference

import (
	"fmt"
	"slices"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/go-tabwrap"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/renderer"
	"github.com/olekukonko/tablewriter/tw"

	queryplan "github.com/apstndb/spannerplan"
	"github.com/apstndb/spannerplan/plantree"
)

// RenderMode specifies how to render the query plan output.
type RenderMode string

const (
	// RenderModeAuto automatically determines whether to show statistics based on availability.
	RenderModeAuto RenderMode = "AUTO"
	// RenderModePlan shows only the query plan without statistics.
	RenderModePlan RenderMode = "PLAN"
	// RenderModeProfile shows the query plan with execution statistics.
	RenderModeProfile RenderMode = "PROFILE"
)

type options struct {
	wrapWidth     int
	hangingIndent bool
}

// Option configures optional rendering behavior for [RenderTreeTableWithOptions].
type Option func(*options)

// WithWrapWidth sets the maximum total rendered line width, including the tree prefix.
// A value of 0 disables wrapping. Negative values make [RenderTreeTableWithOptions] return an error.
func WithWrapWidth(width int) Option {
	return func(o *options) {
		o.wrapWidth = width
	}
}

// WithHangingIndent hangs wrapped continuation lines after node-local prefixes such as
// `[Input] ` and `[Map] ` instead of keeping the default tree-aligned indentation.
func WithHangingIndent() Option {
	return func(o *options) {
		o.hangingIndent = true
	}
}

// ParseRenderMode parses a string into a RenderMode.
// Valid values are "AUTO", "PLAN", and "PROFILE" (case-insensitive).
func ParseRenderMode(s string) (RenderMode, error) {
	switch strings.ToUpper(s) {
	case "AUTO":
		return RenderModeAuto, nil
	case "PLAN":
		return RenderModePlan, nil
	case "PROFILE":
		return RenderModeProfile, nil
	default:
		return "", fmt.Errorf("unknown render mode: %s", s)
	}
}

// RenderTreeTable renders Spanner plan nodes as an ASCII table.
// It supports different render modes (AUTO, PLAN, PROFILE) and formats (TRADITIONAL, CURRENT, COMPACT).
// The wrapWidth parameter controls text wrapping (0 disables wrapping).
func RenderTreeTable(planNodes []*sppb.PlanNode, mode RenderMode, format Format, wrapWidth int) (string, error) {
	return RenderTreeTableWithOptions(planNodes, mode, format, WithWrapWidth(wrapWidth))
}

// RenderTreeTableWithOptions renders Spanner plan nodes as an ASCII table with optional rendering configuration.
func RenderTreeTableWithOptions(planNodes []*sppb.PlanNode, mode RenderMode, format Format, opts ...Option) (string, error) {
	o := options{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&o)
	}

	// Validate input parameters
	if len(planNodes) == 0 {
		return "", fmt.Errorf("planNodes cannot be empty")
	}
	if o.wrapWidth < 0 {
		return "", fmt.Errorf("wrapWidth cannot be negative: %d", o.wrapWidth)
	}

	var withStats bool
	switch mode {
	case RenderModeAuto:
		withStats = queryplan.HasStats(planNodes)
	case RenderModePlan:
		withStats = false
	case RenderModeProfile:
		withStats = true
	default:
		return "", fmt.Errorf("unknown render mode: %s", mode)
	}

	rendered, err := processTree(planNodes, format, o)
	if err != nil {
		return "", err
	}

	tablePart, err := renderTablePart(rendered, withStats)
	if err != nil {
		return "", err
	}

	predPart := renderPredicatesPart(rendered)

	return tablePart + predPart, nil
}

// hasPredicates checks if any row contains predicates.
func hasPredicates(rows []plantree.RowWithPredicates) bool {
	for _, row := range rows {
		if len(row.Predicates) > 0 {
			return true
		}
	}
	return false
}

// renderPredicatesPart formats predicates section for rows with associated predicates.
func renderPredicatesPart(rendered []plantree.RowWithPredicates) string {
	// Early return if no predicates exist
	if !hasPredicates(rendered) {
		return ""
	}

	// Find the maximum ID value first, then format it once to get the width
	var maxID int32
	for _, row := range rendered {
		if row.ID > maxID {
			maxID = row.ID
		}
	}
	maxIDLength := len(fmt.Sprint(maxID))

	var sb strings.Builder
	_, _ = fmt.Fprintln(&sb, "Predicates(identified by ID):")
	for _, row := range rendered {
		for i, predicate := range row.Predicates {
			var idPartStr string
			if i == 0 {
				idPartStr = fmt.Sprint(row.ID) + ":"
			}

			// +1 is for the colon after ID
			prefix := tabwrap.FillLeft(idPartStr, maxIDLength+1)
			_, _ = fmt.Fprintf(&sb, " %s %s\n", prefix, predicate)
		}
	}
	return sb.String()
}

// getColumnAlignments returns the column alignment configuration based on whether stats are shown.
func getColumnAlignments(withStats bool) []tw.Align {
	if withStats {
		// ID, Operator, Rows, Exec., Total Latency
		return []tw.Align{tw.AlignRight, tw.AlignLeft, tw.AlignRight, tw.AlignRight, tw.AlignLeft}
	}
	// ID, Operator
	return []tw.Align{tw.AlignRight, tw.AlignLeft}
}

// renderTablePart renders the main table showing the query plan tree structure.
func renderTablePart(rendered []plantree.RowWithPredicates, withStats bool) (string, error) {
	var sb strings.Builder
	table := tablewriter.NewTable(&sb,
		tablewriter.WithRenderer(
			renderer.NewBlueprint(tw.Rendition{Symbols: tw.NewSymbols(tw.StyleASCII)}),
		),
		tablewriter.WithTrimSpace(tw.Off),
		tablewriter.WithHeaderAutoFormat(tw.Off),
		tablewriter.WithHeaderAlignment(tw.AlignLeft),
		tablewriter.WithRowAlignmentConfig(tw.CellAlignment{PerColumn: getColumnAlignments(withStats)}),
	)

	var header []string
	if withStats {
		header = []string{"ID", "Operator", "Rows", "Exec.", "Total Latency"}
	} else {
		header = []string{"ID", "Operator"}
	}
	table.Header(header)

	for _, n := range rendered {
		rowData := []string{n.FormatID(), n.Text()}
		if withStats {
			rowData = append(rowData, n.ExecutionStats.Rows.Total, n.ExecutionStats.ExecutionSummary.NumExecutions, n.ExecutionStats.Latency.String())
		}

		if err := table.Append(rowData); err != nil {
			return "", fmt.Errorf("failed to append row %d: %w", n.ID, err)
		}
	}

	if err := table.Render(); err != nil {
		return "", fmt.Errorf("failed to render table: %w", err)
	}
	return sb.String(), nil
}

// Format specifies the formatting style for the query plan output.
type Format string

const (
	// FormatTraditional uses raw metadata format in node titles.
	FormatTraditional Format = "TRADITIONAL"
	// FormatCurrent uses modern formatting with labels and angle brackets.
	FormatCurrent Format = "CURRENT"
	// FormatCompact uses compact tree rendering with minimal spacing.
	FormatCompact Format = "COMPACT"
)

// ParseFormat parses a string into a Format.
// Valid values are "TRADITIONAL", "CURRENT", and "COMPACT" (case-insensitive).
func ParseFormat(str string) (Format, error) {
	switch strings.ToUpper(str) {
	case "TRADITIONAL":
		return FormatTraditional, nil
	case "CURRENT":
		return FormatCurrent, nil
	case "COMPACT":
		return FormatCompact, nil
	default:
		return "", fmt.Errorf("unknown format: %s", str)
	}
}

// processTree converts Spanner plan nodes into a structured tree representation.
func processTree(planNodes []*sppb.PlanNode, format Format, opts options) ([]plantree.RowWithPredicates, error) {
	qp, err := queryplan.New(planNodes)
	if err != nil {
		return nil, fmt.Errorf("failed to create query plan: %w", err)
	}

	plantreeOpts := optsForFormat(format)
	if opts.wrapWidth > 0 {
		plantreeOpts = append(plantreeOpts, plantree.WithWrapWidth(opts.wrapWidth))
	}
	if opts.hangingIndent {
		plantreeOpts = append(plantreeOpts, plantree.WithHangingIndent())
	}

	return plantree.ProcessPlan(qp, plantreeOpts...)
}

// optsForFormat returns the appropriate rendering options for the given format.
func optsForFormat(format Format) []plantree.Option {
	currentOpts := []plantree.Option{
		plantree.WithQueryPlanOptions(
			queryplan.WithKnownFlagFormat(queryplan.KnownFlagFormatLabel),
			queryplan.WithExecutionMethodFormat(queryplan.ExecutionMethodFormatAngle),
			queryplan.WithTargetMetadataFormat(queryplan.TargetMetadataFormatOn),
		),
	}

	switch format {
	case FormatTraditional:
		return nil
	case FormatCurrent:
		return currentOpts
	case FormatCompact:
		return slices.Concat(currentOpts,
			[]plantree.Option{plantree.EnableCompact()})
	default:
		// This should never happen as Format is constrained by type
		panic(fmt.Sprintf("unexpected format: %v", format))
	}
}
