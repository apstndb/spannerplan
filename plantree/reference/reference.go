package reference

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/mattn/go-runewidth"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/renderer"
	"github.com/olekukonko/tablewriter/tw"

	queryplan "github.com/apstndb/spannerplan"
	"github.com/apstndb/spannerplan/plantree"
)

type RenderMode string

const (
	RenderModeAuto    RenderMode = "AUTO"
	RenderModePlan    RenderMode = "PLAN"
	RenderModeProfile RenderMode = "PROFILE"
)

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

func RenderTreeTable(planNodes []*sppb.PlanNode, mode RenderMode, format Format, wrapWidth int) (string, error) {
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

	rendered, err := ProcessTree(planNodes, format, wrapWidth)
	if err != nil {
		return "", err
	}

	tablePart, err := renderTablePart(rendered, withStats)
	if err != nil {
		return "", err
	}

	predPart, err := renderPredicatesPart(rendered)
	if err != nil {
		return "", err
	}

	return tablePart + predPart, nil

}

func renderPredicatesPart(rendered []plantree.RowWithPredicates) (string, error) {
	var maxIDLength int
	for _, row := range rendered {
		if l := len(fmt.Sprint(row.ID)); l > maxIDLength {
			maxIDLength = l
		}
	}

	var predicates []string
	for _, row := range rendered {
		for i, predicate := range row.Predicates {
			var idPartStr string
			if i == 0 {
				idPartStr = fmt.Sprint(row.ID) + ":"
			}

			prefix := runewidth.FillLeft(idPartStr, maxIDLength+1)
			predicates = append(predicates, fmt.Sprintf("%s %s", prefix, predicate))
		}
	}

	var sb strings.Builder
	if len(predicates) > 0 {
		fmt.Fprintln(&sb, "Predicates(identified by ID):")
		for _, s := range predicates {
			fmt.Fprintln(&sb, " "+s)
		}
	}
	return sb.String(), nil
}

func renderTablePart(rendered []plantree.RowWithPredicates, withStats bool) (string, error) {
	var sb strings.Builder
	table := tablewriter.NewTable(&sb,
		tablewriter.WithRenderer(
			renderer.NewBlueprint(tw.Rendition{Symbols: tw.NewSymbols(tw.StyleASCII)}),
		),
		tablewriter.WithTrimSpace(tw.Off),
		tablewriter.WithHeaderAutoFormat(tw.Off),
		tablewriter.WithHeaderAlignment(tw.AlignLeft),
	)
	table.Configure(func(config *tablewriter.Config) {
		config.Header.Formatting.AutoFormat = tw.Off
		// Per-column alignment: ID right, Operator left, optional stats columns
		if withStats {
			config.Row.Alignment.PerColumn = []tw.Align{tw.AlignRight, tw.AlignLeft, tw.AlignRight, tw.AlignRight, tw.AlignLeft}
		} else {
			config.Row.Alignment.PerColumn = []tw.Align{tw.AlignRight, tw.AlignLeft}
		}
	})

	header := []string{"ID", "Operator"}
	if withStats {
		header = append(header, "Rows", "Exec.", "Total Latency")
	}
	table.Header(header)

	for _, n := range rendered {
		rowData := []string{n.FormatID(), n.Text()}
		if withStats {
			rowData = append(rowData, n.ExecutionStats.Rows.Total, n.ExecutionStats.ExecutionSummary.NumExecutions, n.ExecutionStats.Latency.String())
		}

		if err := table.Append(rowData); err != nil {
			return "", err
		}
	}

	if err := table.Render(); err != nil {
		return "", err
	}
	return sb.String(), nil
}

type params struct {
	Input     string `json:"input"`
	Mode      string `json:"mode"`
	Format    string `json:"format"`
	WrapWidth int    `json:"wrapWidth"`
}

// Response represents the structured response from WASM
type Response struct {
	Success bool   `json:"success"`
	Result  string `json:"result,omitempty"`
	Error   *Error `json:"error,omitempty"`
}

// Error represents detailed error information
type Error struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

// Error types for better error handling
const (
	ErrorTypeParseError           = "PARSE_ERROR"
	ErrorTypeInvalidSpannerFormat = "INVALID_SPANNER_FORMAT"
	ErrorTypeRenderError          = "RENDER_ERROR"
	ErrorTypeInvalidParameters    = "INVALID_PARAMETERS"
)

// Custom error types for better classification
// These correspond to WasmErrorType constants in TypeScript

// ParseError represents JSON/YAML parsing failures
type ParseError struct {
	msg string
}

func (e ParseError) Error() string {
	return e.msg
}

// InvalidSpannerFormatError represents invalid Spanner query plan format or structure
type InvalidSpannerFormatError struct {
	msg string
}

func (e InvalidSpannerFormatError) Error() string {
	return e.msg
}

// RenderError represents general rendering failures
type RenderError struct {
	msg string
}

func (e RenderError) Error() string {
	return e.msg
}

// InvalidParametersError represents invalid function parameters
type InvalidParametersError struct {
	msg string
}

func (e InvalidParametersError) Error() string {
	return e.msg
}

// classifyError determines the error type using errors.As for type-safe classification
func classifyError(err error) string {
	// Check for custom error types first
	var parseErr ParseError
	if errors.As(err, &parseErr) {
		return ErrorTypeParseError
	}

	var spannerErr InvalidSpannerFormatError
	if errors.As(err, &spannerErr) {
		return ErrorTypeInvalidSpannerFormat
	}

	var renderErr RenderError
	if errors.As(err, &renderErr) {
		return ErrorTypeRenderError
	}

	var paramErr InvalidParametersError
	if errors.As(err, &paramErr) {
		return ErrorTypeInvalidParameters
	}

	// Default to render error for unknown error types
	return ErrorTypeRenderError
}

// RenderASCII renders an ASCII table for the given Spanner plan input (JSON or YAML)
// using the specified mode (AUTO|PLAN|PROFILE), format (TRADITIONAL|CURRENT|COMPACT),
// and optional wrap width. It is a thin wrapper around the internal implementation
// for use by tools.
func RenderASCII(j string, modeStr string, formatStr string, wrapWidth int) (string, error) {
	return renderASCIIImpl(j, modeStr, formatStr, wrapWidth)
}

// renderASCIIImpl implements the core rendering logic
// Validates parameters, extracts query plan, and renders ASCII output
func renderASCIIImpl(j string, modeStr string, formatStr string, wrapWidth int) (string, error) {
	stats, _, err := queryplan.ExtractQueryPlan([]byte(j))
	if err != nil {
		// Wrap external parsing errors in our custom type
		return "", ParseError{msg: fmt.Sprintf("Failed to extract query plan: %v", err)}
	}

	mode, err := ParseRenderMode(modeStr)
	if err != nil {
		return "", InvalidParametersError{msg: fmt.Sprintf("Invalid render mode: %v", err)}
	}

	format, err := ParseFormat(formatStr)
	if err != nil {
		return "", InvalidParametersError{msg: fmt.Sprintf("Invalid format type: %v", err)}
	}

	// Validate Spanner query plan structure
	queryPlan := stats.GetQueryPlan()
	if queryPlan == nil {
		return "", InvalidSpannerFormatError{msg: "Query plan is missing from input"}
	}

	planNodes := queryPlan.GetPlanNodes()
	if len(planNodes) == 0 {
		return "", InvalidSpannerFormatError{msg: "Plan nodes are missing from query plan"}
	}

	s, err := RenderTreeTable(planNodes, mode, format, wrapWidth)
	if err != nil {
		return "", RenderError{msg: fmt.Sprintf("Failed to render tree table: %v", err)}
	}
	return s, nil
}

type Format string

const (
	formatTraditional Format = "TRADITIONAL"
	formatCurrent     Format = "CURRENT"
	formatCompact     Format = "COMPACT"
)

func ParseFormat(str string) (Format, error) {
	switch strings.ToUpper(str) {
	case "TRADITIONAL":
		return formatTraditional, nil
	case "CURRENT":
		return formatCurrent, nil
	case "COMPACT":
		return formatCompact, nil
	default:
		return "", fmt.Errorf("unknown Format: %s", str)
	}

}

func ProcessTree(planNodes []*sppb.PlanNode, format Format, wrapWidth int) ([]plantree.RowWithPredicates, error) {
	qp, err := queryplan.New(planNodes)
	if err != nil {
		return nil, err
	}

	var opts []plantree.Option
	opts = append(opts, optsForFormat(format)...)

	if wrapWidth > 0 {
		opts = append(opts, plantree.WithWrapWidth(wrapWidth))
	}

	return plantree.ProcessPlan(qp, opts...)
}

func optsForFormat(format Format) []plantree.Option {
	currentOpts := []plantree.Option{
		plantree.WithQueryPlanOptions(
			queryplan.WithKnownFlagFormat(queryplan.KnownFlagFormatLabel),
			queryplan.WithExecutionMethodFormat(queryplan.ExecutionMethodFormatAngle),
			queryplan.WithTargetMetadataFormat(queryplan.TargetMetadataFormatOn),
		),
	}

	switch format {
	case formatTraditional:
		return nil
	case formatCurrent:
		return currentOpts
	case formatCompact:
		return slices.Concat(currentOpts,
			[]plantree.Option{plantree.EnableCompact()})
	default:
		return nil
	}
}
