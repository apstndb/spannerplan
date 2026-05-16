package impl

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"regexp"
	"slices"
	"strings"
	"text/template"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/goccy/go-yaml"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/samber/lo"

	"github.com/apstndb/spannerplan"
	"github.com/apstndb/spannerplan/asciitable"
	"github.com/apstndb/spannerplan/plantree"
	"github.com/apstndb/spannerplan/stats"
)

var customDecodeOptions = []yaml.DecodeOption{yaml.CustomUnmarshaler(unmarshalAlign)}

// Main is the entry point of this command.
// It is also used by github.com/apstndb/spannerplanviz/cmd/rendertree
func Main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		var usageErr *usageError
		if errors.As(err, &usageErr) {
			os.Exit(2)
		}
		log.Fatal(err)
	}
}

type usageError struct {
	err error
}

func (e *usageError) Error() string {
	return e.err.Error()
}

func (e *usageError) Unwrap() error {
	return e.err
}

type tableRenderDef struct {
	Columns []columnRenderDef
}

func (tdef tableRenderDef) ColumnMapFunc(row plantree.RowWithPredicates) ([]string, error) {
	var columns []string
	for _, s := range tdef.Columns {
		v, err := s.MapFunc(row)
		if err != nil {
			return nil, err
		}
		columns = append(columns, v)
	}
	return columns, nil
}

func parseAlignment(s string) (tw.Align, error) {
	switch strings.TrimPrefix(s, "ALIGN_") {
	case "RIGHT":
		return tw.AlignRight, nil
	case "LEFT":
		return tw.AlignLeft, nil
	case "CENTER":
		return tw.AlignCenter, nil
	case "DEFAULT":
		return tw.AlignDefault, nil
	case "NONE":
		return tw.AlignNone, nil
	default:
		return tw.AlignNone, fmt.Errorf("unknown Alignment: %s", s)
	}
}

type inlineType string

func (i *inlineType) UnmarshalYAML(b []byte) error {
	var s string
	err := yaml.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	inline, err := parseInlineType(s)
	if err != nil {
		return err
	}

	*i = inline
	return nil
}

const (
	inlineTypeUnspecified inlineType = ""
	inlineTypeNever       inlineType = "NEVER"
	inlineTypeAlways      inlineType = "ALWAYS"
	inlineTypeCan         inlineType = "CAN"
)

var _ yaml.BytesUnmarshaler = (*inlineType)(nil)

type plainColumnRenderDef struct {
	Template  string     `json:"template"`
	Name      string     `json:"name"`
	Alignment tw.Align   `json:"alignment"`
	Inline    inlineType `json:"inline"`
}

type columnRenderDef struct {
	MapFunc   func(row plantree.RowWithPredicates) (string, error)
	Name      string
	Alignment tw.Align
	Inline    inlineType
}

func (d columnRenderDef) shouldInline(inline bool) bool {
	switch d.Inline {
	case inlineTypeAlways:
		return true
	case inlineTypeCan:
		return inline
	case inlineTypeUnspecified:
		return inline && !slices.Contains([]string{"ID", "Operator"}, d.Name)
	default:
		return false
	}
}

func templateMapFunc(tmplName, tmplText string) (func(row plantree.RowWithPredicates) (string, error), error) {
	tmpl, err := template.New(tmplName).Funcs(map[string]any{
		"secsToS": secsToS,
	}).Parse(tmplText)
	if err != nil {
		return nil, err
	}

	return func(row plantree.RowWithPredicates) (string, error) {
		var sb strings.Builder
		if err = tmpl.Execute(&sb, row); err != nil {
			return "", err
		}

		return sb.String(), nil
	}, nil
}

var (
	idRenderDef = columnRenderDef{
		Name:      "ID",
		Alignment: tw.AlignRight,
		MapFunc: func(row plantree.RowWithPredicates) (string, error) {
			return row.FormatID(), nil
		},
		Inline: inlineTypeNever,
	}
	operatorRenderDef = columnRenderDef{
		Name:      "Operator",
		Alignment: tw.AlignLeft,
		MapFunc: func(row plantree.RowWithPredicates) (string, error) {
			return row.Text(), nil
		},
		Inline: inlineTypeNever,
	}
)

var secsRe = regexp.MustCompile(`secs$`)

func secsToS(v any) string {
	return secsRe.ReplaceAllString(fmt.Sprint(v), "s")
}

var (
	withStatsToRenderDefMap = map[bool]tableRenderDef{
		false: {
			Columns: []columnRenderDef{idRenderDef, operatorRenderDef},
		},
		true: {
			Columns: []columnRenderDef{
				idRenderDef,
				operatorRenderDef,
				{
					MapFunc: func(row plantree.RowWithPredicates) (string, error) {
						return row.ExecutionStats.Rows.Total, nil
					},
					Name:      "Rows",
					Alignment: tw.AlignRight,
				},
				{
					MapFunc: func(row plantree.RowWithPredicates) (string, error) {
						return row.ExecutionStats.ExecutionSummary.NumExecutions, nil
					},
					Name:      "Exec.",
					Alignment: tw.AlignRight,
				},
				{
					MapFunc: func(row plantree.RowWithPredicates) (string, error) {
						return secsToS(row.ExecutionStats.Latency), nil
					},
					Name:      "Latency",
					Alignment: tw.AlignRight,
				},
			},
		},
	}
)

type stringList []string

func (s *stringList) String() string {
	return fmt.Sprint([]string(*s))
}

func (s *stringList) Set(s2 string) error {
	*s = append(*s, strings.Split(s2, ",")...)
	return nil
}

type repeatableStringList []string

func (s *repeatableStringList) String() string {
	return fmt.Sprint([]string(*s))
}

func (s *repeatableStringList) Set(s2 string) error {
	*s = append(*s, s2)
	return nil
}

const jsonSnippetLen = 140

// PrintSection selects one appendix section printed after the rendered tree.
type PrintSection string

const (
	// PrintPredicates prints predicate-like scalar links.
	PrintPredicates PrintSection = "predicates"
	// PrintOrdering prints ordering scalar links for sort operators.
	PrintOrdering PrintSection = "ordering"
	// PrintAggregate prints grouping and aggregate scalar links for aggregate operators.
	PrintAggregate PrintSection = "aggregate"
	// PrintTyped prints all typed scalar links as a raw debug dump.
	PrintTyped PrintSection = "typed"
	// PrintFull prints all scalar links, including unnamed links, as a raw debug dump.
	PrintFull PrintSection = "full"
)

// PrintSections is the ordered list of appendix sections requested by the CLI.
type PrintSections []PrintSection

func parsePrintSections(s string) (PrintSections, error) {
	var sections PrintSections
	seen := map[PrintSection]bool{}
	for _, raw := range strings.Split(s, ",") {
		part := strings.TrimSpace(strings.ToLower(raw))
		if part == "" {
			return nil, fmt.Errorf("print section must not be empty")
		}

		var section PrintSection
		switch part {
		case string(PrintPredicates):
			section = PrintPredicates
		case string(PrintOrdering):
			section = PrintOrdering
		case string(PrintAggregate):
			section = PrintAggregate
		case string(PrintTyped):
			section = PrintTyped
		case string(PrintFull):
			section = PrintFull
		default:
			return nil, fmt.Errorf("unknown print section: %s", raw)
		}

		if seen[section] {
			return nil, fmt.Errorf("duplicate print section: %s", section)
		}
		seen[section] = true
		sections = append(sections, section)
	}

	if len(sections) == 0 {
		return nil, fmt.Errorf("print section must not be empty")
	}

	for _, section := range sections {
		if (section == PrintTyped || section == PrintFull) && len(sections) > 1 {
			return nil, fmt.Errorf("print section %q cannot be combined with other sections", section)
		}
	}
	return sections, nil
}

type explainMode string

const (
	explainModePlan    explainMode = "PLAN"
	explainModeProfile explainMode = "PROFILE"
	explainModeAuto    explainMode = "AUTO"
)

func parseExplainMode(s string) (explainMode, error) {
	switch strings.ToUpper(s) {
	case "PLAN":
		return explainModePlan, nil
	case "PROFILE":
		return explainModeProfile, nil
	case "AUTO":
		return explainModeAuto, nil
	default:
		return "", fmt.Errorf("invalid input: %s. Must be one of AUTO, PLAN, PROFILE (case-insensitive)", s)
	}
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	flagSet := flag.NewFlagSet("rendertree", flag.ContinueOnError)
	flagSet.SetOutput(stderr)

	customFile := flagSet.String("custom-file", "", "Read custom table column definitions from a YAML file (mutually exclusive with --custom and --custom-column)")
	mode := flagSet.String("mode", "AUTO", "PROFILE, PLAN, AUTO(ignore case)")
	printSectionsStr := flagSet.String("print", "predicates", "print appendix sections: predicates, ordering, aggregate, typed, full (comma-separated; typed/full are raw debug dumps)")
	disallowUnknownStats := flagSet.Bool("disallow-unknown-stats", false, "error on unknown stats field")
	executionMethod := flagSet.String("execution-method", "angle", "Format execution method metadata: 'angle' or 'raw' (default: angle)")
	targetMetadata := flagSet.String("target-metadata", "on", "Format target metadata: 'on' or 'raw' (default: on)")
	fullscan := flagSet.String("full-scan", "", "Deprecated alias for --known-flag.")
	knownFlag := flagSet.String("known-flag", "", "Format known flags: 'label' or 'raw' (default: label)")
	compact := flagSet.Bool("compact", false, "Enable compact format")
	inlineStats := flagSet.Bool("inline-stats", false, "Enable inline stats")
	wrapWidth := flagSet.Int("wrap-width", 0, "Number of characters at which to wrap the Operator column content. 0 means no wrapping.")
	hangingIndent := flagSet.Bool("hanging-indent", false, "Enable hanging indent for wrapped lines after node-local prefixes such as [Input] and [Map]")

	var custom stringList
	var customColumn repeatableStringList
	flagSet.Var(&custom, "custom", "DEPRECATED: add a custom table column definition in NAME:TEMPLATE[:ALIGNMENT[:INLINE_TYPE]] form (mutually exclusive with --custom-file and --custom-column)")
	flagSet.Var(&customColumn, "custom-column", "Add one custom table column definition as a YAML/JSON object (repeatable, mutually exclusive with --custom and --custom-file)")
	if err := flagSet.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return &usageError{err: err}
	}

	// These are semantic flag-combination checks that run after Parse succeeds.
	// flag.ContinueOnError only covers parse-time failures, so we still print usage here.
	if *fullscan != "" {
		if *knownFlag != "" {
			const msg = "--full-scan and --known-flag are mutually exclusive"
			_, _ = fmt.Fprintln(stderr, msg)
			flagSet.Usage()
			return &usageError{err: errors.New(msg)}
		}

		_, _ = fmt.Fprintln(stderr, "--full-scan is deprecated. You must migrate to --known-flag.")

		*knownFlag = *fullscan
	}

	if len(custom) > 0 && *customFile != "" {
		const msg = "--custom and --custom-file are mutually exclusive"
		_, _ = fmt.Fprintln(stderr, msg)
		flagSet.Usage()
		return &usageError{err: errors.New(msg)}
	}
	if len(customColumn) > 0 && *customFile != "" {
		const msg = "--custom-column and --custom-file are mutually exclusive"
		_, _ = fmt.Fprintln(stderr, msg)
		flagSet.Usage()
		return &usageError{err: errors.New(msg)}
	}
	if len(custom) > 0 && len(customColumn) > 0 {
		const msg = "--custom and --custom-column are mutually exclusive"
		_, _ = fmt.Fprintln(stderr, msg)
		flagSet.Usage()
		return &usageError{err: errors.New(msg)}
	}
	if len(custom) > 0 {
		_, _ = fmt.Fprintln(stderr, "--custom is deprecated. You must migrate to --custom-column or --custom-file.")
	}

	printSections, err := parsePrintSections(*printSectionsStr)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "Invalid value for -print flag: %v\n", err)
		flagSet.Usage()
		return &usageError{err: err}
	}

	parsedMode, err := parseExplainMode(*mode)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "Invalid value for -mode flag: %v\n", err)
		flagSet.Usage()
		return &usageError{err: err}
	}

	var opts []plantree.Option
	if *disallowUnknownStats {
		opts = append(opts, plantree.DisallowUnknownStats())
	}

	if *compact {
		opts = append(opts, plantree.EnableCompact())
	}

	em := spannerplan.ExecutionMethodFormatAngle
	if *executionMethod != "" {
		em, err = spannerplan.ParseExecutionMethodFormat(*executionMethod)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "Invalid value for -execution-method flag: %v.\n", err)
			flagSet.Usage()
			return &usageError{err: err}
		}
	}
	opts = append(opts, plantree.WithQueryPlanOptions(spannerplan.WithExecutionMethodFormat(em)))

	tm := spannerplan.TargetMetadataFormatOn
	if *targetMetadata != "" {
		tm, err = spannerplan.ParseTargetMetadataFormat(*targetMetadata)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "Invalid value for -target-metadata flag: %v.\n", err)
			flagSet.Usage()
			return &usageError{err: err}
		}
	}
	opts = append(opts, plantree.WithQueryPlanOptions(spannerplan.WithTargetMetadataFormat(tm)))

	kf := spannerplan.KnownFlagFormatLabel
	if *knownFlag != "" {
		kf, err = spannerplan.ParseKnownFlagFormat(*knownFlag)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "Invalid value for -known-flag: %v.\n", err)
			flagSet.Usage()
			return &usageError{err: err}
		}
	}
	opts = append(opts, plantree.WithQueryPlanOptions(spannerplan.WithKnownFlagFormat(kf)))

	if *wrapWidth > 0 {
		opts = append(opts, plantree.WithWrapWidth(*wrapWidth))
	}
	if *hangingIndent {
		opts = append(opts, plantree.WithHangingIndent())
	}

	b, err := io.ReadAll(stdin)
	if err != nil {
		return err
	}

	qs, _, err := spannerplan.ExtractQueryPlan(b)
	if err != nil {
		var collapsedStr string
		if len(b) > jsonSnippetLen {
			collapsedStr = "(collapsed)"
		}
		return fmt.Errorf("invalid input at protoyaml.Unmarshal:\nerror: %w\ninput: %.*s%s", err, jsonSnippetLen, strings.TrimSpace(string(b)), collapsedStr)
	}

	planNodes := qs.GetQueryPlan().GetPlanNodes()

	var renderDef tableRenderDef
	if len(customColumn) > 0 {
		renderDef, err = customColumnListToTableRenderDef(customColumn)
		if err != nil {
			return err
		}
	} else if len(custom) > 0 {
		renderDef, err = customListToTableRenderDef(custom)
		if err != nil {
			return err
		}
	} else if *customFile != "" {
		b, err := os.ReadFile(*customFile)
		if err != nil {
			return err
		}
		renderDef, err = customFileToTableRenderDef(b)
		if err != nil {
			return err
		}
	} else {
		withStats := shouldRenderWithStats(planNodes, parsedMode)
		renderDef = withStatsToRenderDefMap[withStats]
	}

	s, err := renderTreeImpl(planNodes, renderDef, printSections, *disallowUnknownStats, *inlineStats, opts)
	if err != nil {
		return err
	}

	_, err = io.WriteString(stdout, s)
	return err
}

func renderTreeImpl(planNodes []*sppb.PlanNode, renderDef tableRenderDef, printSections PrintSections, disallowUnknownStats bool, inline bool, opts []plantree.Option) (string, error) {
	opts = append(opts,
		plantree.WithQueryPlanOptions(
			spannerplan.WithInlineStatsFunc(inlineStatsFuncFromTableRenderDef(disallowUnknownStats, renderDef, inline)),
		))

	qp, err := spannerplan.New(planNodes)
	if err != nil {
		return "", err
	}

	rows, err := plantree.ProcessPlan(qp, opts...)
	if err != nil {
		return "", err
	}

	s, err := printResult(
		tableRenderDef{
			Columns: lo.Filter(renderDef.Columns, func(def columnRenderDef, index int) bool {
				return !def.shouldInline(inline)
			}),
		},
		rows, printSections)
	if err != nil {
		return "", err
	}

	return s, nil
}

func inlineStatsFuncFromTableRenderDef(disallowUnknownStats bool, renderDef tableRenderDef, inlineStats bool) func(node *sppb.PlanNode) []string {
	return func(node *sppb.PlanNode) []string {
		executionStats, err := stats.Extract(node, disallowUnknownStats)
		if err != nil {
			slog.Warn("failed to extract execution stats", "node_id", node.GetIndex(), "err", err)
			return nil
		}

		row := plantree.RowWithPredicates{ExecutionStats: *executionStats}

		var result []string
		for _, def := range renderDef.Columns {
			if !def.shouldInline(inlineStats) {
				continue
			}

			v, err := def.MapFunc(row)
			if err != nil {
				slog.Warn("failed to execute map function for inline stat", "node_id", node.GetIndex(), "name", def.Name, "err", err)
				continue
			}

			if v != "" {
				result = append(result, fmt.Sprintf("%v=%v", def.Name, v))
			}
		}
		return result
	}
}

func shouldRenderWithStats(qp []*sppb.PlanNode, parsedMode explainMode) bool {
	switch parsedMode {
	case explainModePlan:
		return false
	case explainModeProfile:
		return true
	default:
		return spannerplan.HasStats(qp)
	}
}

func unmarshalAlign(t *tw.Align, bytes []byte) error {
	var s string
	if err := yaml.Unmarshal(bytes, &s); err != nil {
		return err
	}

	align, err := parseAlignment(s)
	if err != nil {
		return err
	}

	*t = align
	return nil
}

func plainColumnRenderDefsToTableRenderDef(defs []plainColumnRenderDef) (tableRenderDef, error) {
	tdef := tableRenderDef{Columns: make([]columnRenderDef, 0, len(defs))}
	for _, def := range defs {
		mapFunc, err := templateMapFunc(def.Name, def.Template)
		if err != nil {
			return tableRenderDef{}, err
		}
		tdef.Columns = append(tdef.Columns, columnRenderDef{
			MapFunc:   mapFunc,
			Name:      def.Name,
			Alignment: def.Alignment,
			Inline:    def.Inline,
		})
	}
	return tdef, nil
}

func customFileToTableRenderDef(b []byte) (tableRenderDef, error) {
	var defs []plainColumnRenderDef
	if err := yaml.UnmarshalWithOptions(b, &defs, customDecodeOptions...); err != nil {
		return tableRenderDef{}, err
	}

	return plainColumnRenderDefsToTableRenderDef(defs)
}

func customColumnListToTableRenderDef(customColumns []string) (tableRenderDef, error) {
	defs := make([]plainColumnRenderDef, 0, len(customColumns))
	for i, raw := range customColumns {
		var def plainColumnRenderDef
		if err := yaml.UnmarshalWithOptions([]byte(raw), &def, customDecodeOptions...); err != nil {
			return tableRenderDef{}, fmt.Errorf("failed to parse --custom-column[%d]: %w", i, err)
		}
		defs = append(defs, def)
	}

	return plainColumnRenderDefsToTableRenderDef(defs)
}

func parseInlineType(s string) (inlineType, error) {
	switch i := inlineType(strings.ToUpper(s)); i {
	case inlineTypeNever, inlineTypeCan, inlineTypeAlways:
		return i, nil
	default:
		return "", fmt.Errorf("inlineType must be one of ALWAYS, CAN, NEVER, but: %v", s)
	}
}

func customListToTableRenderDef(custom []string) (tableRenderDef, error) {
	var columns []columnRenderDef
	for _, s := range custom {
		split := strings.SplitN(s, ":", 4)

		var err error
		if len(split) < 2 || len(split) > 4 {
			return tableRenderDef{}, fmt.Errorf(`invalid format: must be "<name>:<template>[:<alignment>[:<inline_type>]]", but: %v`, s)
		}

		inline := inlineTypeUnspecified
		if len(split) == 4 {
			if inlineStr := split[3]; inlineStr != "" {
				inline, err = parseInlineType(inlineStr)
				if err != nil {
					return tableRenderDef{}, fmt.Errorf("failed on parse inline_type: %w", err)
				}
			}
		}

		align := tw.AlignNone
		if len(split) >= 3 {
			if alignStr := split[2]; alignStr != "" {
				align, err = parseAlignment(alignStr)
				if err != nil {
					return tableRenderDef{}, fmt.Errorf("failed on parse alignment: %w", err)
				}
			}
		}

		name, templateStr := split[0], split[1]
		mapFunc, err := templateMapFunc(name, templateStr)
		if err != nil {
			return tableRenderDef{}, err
		}

		columns = append(columns, columnRenderDef{
			MapFunc:   mapFunc,
			Name:      name,
			Alignment: align,
			Inline:    inline,
		})
	}
	return tableRenderDef{Columns: columns}, nil
}

func printResult(renderDef tableRenderDef, rows []plantree.RowWithPredicates, printSections PrintSections) (string, error) {
	var b strings.Builder

	if len(rows) > 0 && len(renderDef.Columns) > 0 {
		tablePart, err := renderTablePart(renderDef, rows)
		if err != nil {
			return "", err
		}
		b.WriteString(tablePart)
	}

	for _, section := range printSections {
		var (
			part string
			err  error
		)
		switch section {
		case PrintFull, PrintTyped:
			part, err = asciitable.RenderAppendix(rows, scalarAppendixSpec(
				"Node Parameters(identified by ID):",
				func(row plantree.RowWithPredicates) []string {
					return scalarLinkLines(row, func(_ plantree.RowWithPredicates, link plantree.ScalarChildLink) bool {
						return section == PrintFull || link.Type != ""
					})
				},
			))
		case PrintPredicates:
			part, err = asciitable.RenderAppendix(rows, scalarAppendixSpec(
				"Predicates(identified by ID):",
				func(row plantree.RowWithPredicates) []string {
					return row.Predicates
				},
			))
		case PrintOrdering:
			part, err = asciitable.RenderAppendix(rows, scalarAppendixSpec(
				"Ordering(identified by ID):",
				func(row plantree.RowWithPredicates) []string {
					return scalarLinkLines(row, isOrderingScalarLink)
				},
			))
		case PrintAggregate:
			part, err = asciitable.RenderAppendix(rows, scalarAppendixSpec(
				"Aggregates(identified by ID):",
				func(row plantree.RowWithPredicates) []string {
					return scalarLinkLines(row, isAggregateScalarLink)
				},
			))
		}
		if err != nil {
			return "", err
		}
		b.WriteString(part)
	}
	return b.String(), nil
}

func scalarAppendixSpec(title string, items func(row plantree.RowWithPredicates) []string) asciitable.AppendixSpec[plantree.RowWithPredicates] {
	return asciitable.AppendixSpec[plantree.RowWithPredicates]{
		Title: title,
		ID: func(row plantree.RowWithPredicates) uint {
			// Spanner PlanNode indexes are zero-based node positions, so they are
			// non-negative when used as appendix display IDs.
			return uint(row.ID)
		},
		Items: items,
	}
}

type scalarLinkGroup struct {
	typ    string
	values []string
}

func scalarLinkLines(row plantree.RowWithPredicates, include func(plantree.RowWithPredicates, plantree.ScalarChildLink) bool) []string {
	groupByType := map[string]int{}
	var groups []scalarLinkGroup

	for _, link := range row.ScalarChildLinks {
		if !include(row, link) {
			continue
		}

		groupIndex, ok := groupByType[link.Type]
		if !ok {
			groupIndex = len(groups)
			groupByType[link.Type] = groupIndex
			groups = append(groups, scalarLinkGroup{typ: link.Type})
		}
		groups[groupIndex].values = append(groups[groupIndex].values, formatScalarLink(link))
	}

	lines := make([]string, 0, len(groups))
	for _, group := range groups {
		joined := strings.Join(group.values, ", ")
		if joined == "" {
			continue
		}
		typePartStr := lo.Ternary(group.typ != "", group.typ+": ", "")
		lines = append(lines, typePartStr+joined)
	}
	return lines
}

func formatScalarLink(link plantree.ScalarChildLink) string {
	if link.Variable != "" {
		return fmt.Sprintf("$%s=%s", link.Variable, link.Description)
	}
	return link.Description
}

func isOrderingScalarLink(row plantree.RowWithPredicates, link plantree.ScalarChildLink) bool {
	switch row.DisplayName {
	case "Sort", "Sort Limit":
		return link.Type == "Key"
	case "Minor Sort", "Minor Sort Limit":
		return link.Type == "MajorKey" || link.Type == "MinorKey"
	default:
		return false
	}
}

func isAggregateScalarLink(row plantree.RowWithPredicates, link plantree.ScalarChildLink) bool {
	return row.DisplayName == "Aggregate" && (link.Type == "Key" || link.Type == "Agg")
}

type renderedTableRow []string

func renderTablePart(renderDef tableRenderDef, rows []plantree.RowWithPredicates) (string, error) {
	tableRows := make([]renderedTableRow, 0, len(rows))
	for _, row := range rows {
		values, err := renderDef.ColumnMapFunc(row)
		if err != nil {
			return "", err
		}
		tableRows = append(tableRows, renderedTableRow(values))
	}

	spec := asciitable.TableSpec[renderedTableRow]{
		Columns: make([]asciitable.Column[renderedTableRow], 0, len(renderDef.Columns)),
	}
	for i, col := range renderDef.Columns {
		alignment, err := tableAlignment(col.Alignment)
		if err != nil {
			return "", fmt.Errorf("column %d (%q): %w", i, col.Name, err)
		}
		index := i
		spec.Columns = append(spec.Columns, asciitable.Column[renderedTableRow]{
			Header:    col.Name,
			Alignment: alignment,
			Cell: func(row renderedTableRow, _ int) string {
				if index >= len(row) {
					return ""
				}
				return row[index]
			},
		})
	}
	return asciitable.RenderTable(tableRows, spec)
}

func tableAlignment(alignment tw.Align) (asciitable.Alignment, error) {
	switch alignment {
	case tw.AlignRight:
		return asciitable.AlignRight, nil
	case tw.AlignCenter:
		return asciitable.AlignCenter, nil
	case "", tw.AlignLeft, tw.AlignNone:
		// tw.AlignDefault is an alias of tw.AlignLeft in tablewriter v1.
		return asciitable.AlignLeft, nil
	default:
		return "", fmt.Errorf("unsupported alignment %v", alignment)
	}
}
