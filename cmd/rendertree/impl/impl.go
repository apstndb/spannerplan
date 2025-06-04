package impl

import (
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
	"github.com/apstndb/lox"
	"github.com/goccy/go-yaml"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/renderer"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/samber/lo"

	"github.com/apstndb/spannerplan"
	"github.com/apstndb/spannerplan/plantree"
	"github.com/apstndb/spannerplan/stats"
)

// Main is the entry point of this command.
// It is also used by github.com/apstndb/spannerplanviz/cmd/rendertree
func Main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

type tableRenderDef struct {
	Columns []columnRenderDef
}

func (tdef tableRenderDef) ColumnNames() []string {
	var columnNames []string
	for _, s := range tdef.Columns {
		columnNames = append(columnNames, s.Name)
	}
	return columnNames
}

func (tdef tableRenderDef) ColumnAlignments() []tw.Align {
	var alignments []tw.Align
	for _, s := range tdef.Columns {
		alignments = append(alignments, s.Alignment)
	}
	return alignments
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

const jsonSnippetLen = 140

type PrintMode int

const (
	PrintPredicates PrintMode = iota
	PrintTyped
	PrintFull
)

func parsePrintMode(s string) (PrintMode, error) {
	switch strings.ToLower(s) {
	case "predicates":
		return PrintPredicates, nil
	case "typed":
		return PrintTyped, nil
	case "full":
		return PrintFull, nil
	default:
		return 0, fmt.Errorf("unknown PrintMode: %s", s)
	}
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

func run() error {
	customFile := flag.String("custom-file", "", "")
	mode := flag.String("mode", "AUTO", "PROFILE, PLAN, AUTO(ignore case)")
	printModeStr := flag.String("print", "predicates", "print node parameters(EXPERIMENTAL)")
	disallowUnknownStats := flag.Bool("disallow-unknown-stats", false, "error on unknown stats field")
	executionMethod := flag.String("execution-method", "angle", "Format execution method metadata: 'angle' or 'raw' (default: angle)")
	targetMetadata := flag.String("target-metadata", "on", "Format target metadata: 'on' or 'raw' (default: on)")
	fullscan := flag.String("full-scan", "", "Deprecated alias for --known-flag.")
	knownFlag := flag.String("known-flag", "", "Format known flags: 'label' or 'raw' (default: label)")
	compact := flag.Bool("compact", false, "Enable compact format")
	inlineStats := flag.Bool("inline-stats", false, "Enable inline stats")
	wrapWidth := flag.Int("wrap-width", 0, "Number of characters at which to wrap the Operator column content. 0 means no wrapping.")

	var custom stringList
	flag.Var(&custom, "custom", "")
	flag.Parse()

	if *fullscan != "" {
		if *knownFlag != "" {
			fmt.Fprintln(os.Stderr, "--full-scan and --known-flag are mutually exclusive.")
			flag.Usage()
			os.Exit(1)
		}

		fmt.Fprintln(os.Stderr, "--full-scan is deprecated. you must migrate to --known-flag.")

		*knownFlag = *fullscan
	}

	printMode, err := parsePrintMode(*printModeStr)
	if err != nil {
		return err
	}

	parsedMode, err := parseExplainMode(*mode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid value for -mode flag: %v\n", err)
		flag.Usage()
		os.Exit(1)
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
			fmt.Fprintf(os.Stderr, "Invalid value for -execution-method flag: %v.\n", err)
			flag.Usage()
			os.Exit(1)
		}
	}
	opts = append(opts, plantree.WithQueryPlanOptions(spannerplan.WithExecutionMethodFormat(em)))

	tm := spannerplan.TargetMetadataFormatOn
	if *targetMetadata != "" {
		tm, err = spannerplan.ParseTargetMetadataFormat(*targetMetadata)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid value for -target-metadata flag: %v.\n", err)
			flag.Usage()
			os.Exit(1)
		}
	}
	opts = append(opts, plantree.WithQueryPlanOptions(spannerplan.WithTargetMetadataFormat(tm)))

	kf := spannerplan.KnownFlagFormatLabel
	if *knownFlag != "" {
		kf, err = spannerplan.ParseKnownFlagFormat(*knownFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid value for -known-flag flag: %v.\n", err)
			flag.Usage()
			os.Exit(1)
		}
	}
	opts = append(opts, plantree.WithQueryPlanOptions(spannerplan.WithKnownFlagFormat(kf)))

	if *wrapWidth > 0 {
		opts = append(opts, plantree.WithWrapWidth(*wrapWidth))
	}

	b, err := io.ReadAll(os.Stdin)
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
	if len(custom) > 0 {
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

	s, err := renderTreeImpl(planNodes, renderDef, printMode, *disallowUnknownStats, *inlineStats, opts)
	if err != nil {
		return err
	}

	_, err = os.Stdout.WriteString(s)
	return err
}

func renderTreeImpl(planNodes []*sppb.PlanNode, renderDef tableRenderDef, printMode PrintMode, disallowUnknownStats bool, inline bool, opts []plantree.Option) (string, error) {
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
		rows, printMode)
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

func customFileToTableRenderDef(b []byte) (tableRenderDef, error) {
	decodeOpts := []yaml.DecodeOption{yaml.CustomUnmarshaler(unmarshalAlign)}

	var defs []plainColumnRenderDef
	if err := yaml.UnmarshalWithOptions(b, &defs, decodeOpts...); err != nil {
		return tableRenderDef{}, err
	}

	var tdef tableRenderDef
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

func printResult(renderDef tableRenderDef, rows []plantree.RowWithPredicates, printMode PrintMode) (string, error) {
	var b strings.Builder
	table := tablewriter.NewTable(&b,
		tablewriter.WithRenderer(
			renderer.NewBlueprint(tw.Rendition{Symbols: tw.NewSymbols(tw.StyleASCII)})),
		tablewriter.WithHeaderAlignment(tw.AlignLeft),
		tablewriter.WithTrimSpace(tw.Off),
	)

	// Some config can't be correctly configured by tablewriter.Option.
	table.Configure(func(config *tablewriter.Config) {
		config.Row.ColumnAligns = renderDef.ColumnAlignments()
		config.Row.Formatting.AutoWrap = tw.WrapNone
		config.Header.Formatting.AutoFormat = false
	})

	table.Header(renderDef.ColumnNames())

	for _, row := range rows {
		values, err := renderDef.ColumnMapFunc(row)
		if err != nil {
			return "", err
		}
		if err = table.Append(values); err != nil {
			return "", err
		}
	}

	if len(rows) > 0 {
		if err := table.Render(); err != nil {
			return "", err
		}
	}

	var maxIDLength int
	for _, row := range rows {
		if length := len(fmt.Sprint(row.ID)); length > maxIDLength {
			maxIDLength = length
		}
	}

	var predicates []string
	var parameters []string
	for _, row := range rows {
		var prefix string
		for i, predicate := range row.Predicates {
			if i == 0 {
				prefix = fmt.Sprintf("%*d:", maxIDLength, row.ID)
			} else {
				prefix = strings.Repeat(" ", maxIDLength+1)
			}
			predicates = append(predicates, fmt.Sprintf("%s %s", prefix, predicate))
		}

		i := 0
		for _, t := range lox.EntriesSortedByKey(row.ChildLinks) {
			typ, childLinks := t.Key, t.Value
			if printMode != PrintFull && typ == "" {
				continue
			}

			if i == 0 {
				prefix = fmt.Sprintf("%*d:", maxIDLength, row.ID)
			} else {
				prefix = strings.Repeat(" ", maxIDLength+1)
			}

			join := strings.Join(lo.Map(childLinks, func(item *spannerplan.ResolvedChildLink, index int) string {
				if varName := item.ChildLink.GetVariable(); varName != "" {
					return fmt.Sprintf("$%s=%s", item.ChildLink.GetVariable(), item.Child.GetShortRepresentation().GetDescription())
				} else {
					return item.Child.GetShortRepresentation().GetDescription()
				}
			}), ", ")
			if join == "" {
				continue
			}
			i++
			typePartStr := lo.Ternary(typ != "", typ+": ", "")
			parameters = append(parameters, fmt.Sprintf("%s %s%s", prefix, typePartStr, join))
		}
	}

	switch printMode {
	case PrintFull, PrintTyped:
		if len(parameters) > 0 {
			fmt.Fprintln(&b, "Node Parameters(identified by ID):")
			for _, s := range parameters {
				fmt.Fprintf(&b, " %s\n", s)
			}
		}
	case PrintPredicates:
		if len(predicates) > 0 {
			fmt.Fprintln(&b, "Predicates(identified by ID):")
			for _, s := range predicates {
				fmt.Fprintf(&b, " %s\n", s)
			}
		}
	}
	return b.String(), nil
}
