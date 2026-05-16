// Package reference provides a reference implementation for rendering
// Spanner query plans as ASCII tables with various formatting options.
//
// Go callers should prefer [RenderTreeTableWithOptions] with functional options.
// Cross-language integrations, such as WebAssembly or JavaScript wrappers that
// start from JSON-like configuration, can use [RenderTreeTableWithConfig] and
// [RenderConfig].
//
// For browser or WebAssembly embeddings, this package is the recommended
// high-level renderer entrypoint: decode serialized query plan JSON into
// spannerpb.QueryPlan with protojson, parse string inputs with [ParseRenderMode]
// and [ParseFormat], then call [RenderTreeTableWithConfig] with
// plan.GetPlanNodes().
// The repository's examples/wasm/render example shows a small syscall/js wrapper
// with that flow.
package reference

import (
	"fmt"
	"slices"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

	queryplan "github.com/apstndb/spannerplan"
	"github.com/apstndb/spannerplan/asciitable"
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

// RenderConfig configures optional rendering behavior using serialization-friendly fields.
//
// Use this type for cross-language integrations that start from JSON-like configuration,
// such as WebAssembly or JavaScript callers. Go callers can continue to use functional
// [Option] values with [RenderTreeTableWithOptions].
type RenderConfig struct {
	// WrapWidth sets the maximum total rendered line width, including the tree prefix.
	// A value of 0 disables wrapping. Negative values make [RenderTreeTableWithConfig] return an error.
	WrapWidth int `json:"wrapWidth,omitempty"`

	// HangingIndent hangs wrapped continuation lines after node-local prefixes such as
	// `[Input] ` and `[Map] ` instead of keeping the default tree-aligned indentation.
	HangingIndent bool `json:"hangingIndent,omitempty"`
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

// RenderTreeTableWithConfig renders Spanner plan nodes as an ASCII table using
// serialization-friendly rendering configuration.
func RenderTreeTableWithConfig(planNodes []*sppb.PlanNode, mode RenderMode, format Format, config RenderConfig) (string, error) {
	return renderTreeTable(planNodes, mode, format, optionsFromConfig(config))
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

	return renderTreeTable(planNodes, mode, format, o)
}

func optionsFromConfig(config RenderConfig) options {
	return options{
		wrapWidth:     config.WrapWidth,
		hangingIndent: config.HangingIndent,
	}
}

func renderTreeTable(planNodes []*sppb.PlanNode, mode RenderMode, format Format, o options) (string, error) {
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

	predPart, err := renderPredicatesPart(rendered)
	if err != nil {
		return "", err
	}

	return tablePart + predPart, nil
}

func renderPredicatesPart(rendered []plantree.RowWithPredicates) (string, error) {
	return asciitable.RenderPredicates(rendered, predicateSpec())
}

func renderTablePart(rendered []plantree.RowWithPredicates, withStats bool) (string, error) {
	return asciitable.RenderTable(rendered, spannerTableSpec(withStats))
}

func spannerTableSpec(withStats bool) asciitable.TableSpec[plantree.RowWithPredicates] {
	spec := asciitable.TableSpec[plantree.RowWithPredicates]{
		Columns: []asciitable.Column[plantree.RowWithPredicates]{
			{
				Header:    "ID",
				Alignment: asciitable.AlignRight,
				Cell: func(row plantree.RowWithPredicates, _ int) string {
					return row.FormatID()
				},
			},
			{
				Header:    "Operator",
				Alignment: asciitable.AlignLeft,
				Cell: func(row plantree.RowWithPredicates, _ int) string {
					return row.Text()
				},
			},
		},
	}
	if !withStats {
		return spec
	}

	spec.Columns = append(spec.Columns,
		asciitable.Column[plantree.RowWithPredicates]{
			Header:    "Rows",
			Alignment: asciitable.AlignRight,
			Cell: func(row plantree.RowWithPredicates, _ int) string {
				return row.ExecutionStats.Rows.Total
			},
		},
		asciitable.Column[plantree.RowWithPredicates]{
			Header:    "Exec.",
			Alignment: asciitable.AlignRight,
			Cell: func(row plantree.RowWithPredicates, _ int) string {
				return row.ExecutionStats.ExecutionSummary.NumExecutions
			},
		},
		asciitable.Column[plantree.RowWithPredicates]{
			Header:    "Total Latency",
			Alignment: asciitable.AlignLeft,
			Cell: func(row plantree.RowWithPredicates, _ int) string {
				return row.ExecutionStats.Latency.String()
			},
		},
	)
	return spec
}

func predicateSpec() asciitable.PredicateSpec[plantree.RowWithPredicates] {
	return asciitable.PredicateSpec[plantree.RowWithPredicates]{
		ID: func(row plantree.RowWithPredicates) uint {
			// Spanner PlanNode indexes are zero-based node positions, so they are
			// non-negative when used as predicate appendix display IDs.
			return uint(row.ID)
		},
		Predicates: func(row plantree.RowWithPredicates) []string {
			return row.Predicates
		},
	}
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
