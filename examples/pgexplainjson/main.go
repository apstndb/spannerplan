package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/apstndb/spannerplan/asciitable"
	"github.com/apstndb/spannerplan/treerender"
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		var usageErr *usageError
		if errors.As(err, &usageErr) {
			os.Exit(2)
		}
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
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

type explainResult struct {
	Plan *planNode `json:"Plan"`
}

type planNode struct {
	NodeType           string      `json:"Node Type"`
	ParentRelationship string      `json:"Parent Relationship"`
	RelationName       string      `json:"Relation Name"`
	Alias              string      `json:"Alias"`
	IndexName          string      `json:"Index Name"`
	JoinType           string      `json:"Join Type"`
	Strategy           string      `json:"Strategy"`
	SortKey            []string    `json:"Sort Key"`
	GroupKey           []string    `json:"Group Key"`
	StartupCost        *float64    `json:"Startup Cost"`
	TotalCost          *float64    `json:"Total Cost"`
	PlanRows           *float64    `json:"Plan Rows"`
	ActualStartupTime  *float64    `json:"Actual Startup Time"`
	ActualTotalTime    *float64    `json:"Actual Total Time"`
	ActualRows         *float64    `json:"Actual Rows"`
	ActualLoops        *float64    `json:"Actual Loops"`
	Filter             string      `json:"Filter"`
	IndexCond          string      `json:"Index Cond"`
	RecheckCond        string      `json:"Recheck Cond"`
	HashCond           string      `json:"Hash Cond"`
	MergeCond          string      `json:"Merge Cond"`
	JoinFilter         string      `json:"Join Filter"`
	Plans              []*planNode `json:"Plans"`
}

type renderNode struct {
	id       uint
	plan     *planNode
	children []*renderNode
}

type renderedRow struct {
	id         uint
	treeRow    treerender.Row
	predicates []string
	plan       *planNode
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	flagSet := flag.NewFlagSet("pgexplainjson", flag.ContinueOnError)
	flagSet.SetOutput(stderr)
	compact := flagSet.Bool("compact", false, "render a compact tree")
	wrapWidth := flagSet.Int("wrap-width", 0, "maximum rendered line width; 0 disables wrapping")
	if err := flagSet.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		// flag.Parse has already printed the parse error and usage to stderr
		// because the flag set output is stderr.
		return &usageError{err: err}
	}
	if flagSet.NArg() != 0 {
		err := fmt.Errorf("unexpected positional arguments: %s", strings.Join(flagSet.Args(), " "))
		_, _ = fmt.Fprintln(stderr, err)
		flagSet.Usage()
		return &usageError{err: err}
	}

	root, err := decodeRoot(stdin)
	if err != nil {
		return err
	}

	out, err := renderPlan(root, renderOptions{
		compact:   *compact,
		wrapWidth: *wrapWidth,
	})
	if err != nil {
		return err
	}
	_, err = io.WriteString(stdout, out)
	return err
}

type renderOptions struct {
	compact   bool
	wrapWidth int
}

func decodeRoot(r io.Reader) (*planNode, error) {
	dec := json.NewDecoder(r)
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to decode PostgreSQL EXPLAIN JSON: %w", err)
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '[' {
		return nil, fmt.Errorf("PostgreSQL EXPLAIN JSON must be an array")
	}
	if !dec.More() {
		return nil, fmt.Errorf("PostgreSQL EXPLAIN JSON contains no results")
	}

	var result explainResult
	if err := dec.Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode PostgreSQL EXPLAIN JSON result 0: %w", err)
	}
	if result.Plan == nil {
		return nil, fmt.Errorf("PostgreSQL EXPLAIN JSON result 0 has no Plan")
	}
	if dec.More() {
		return nil, fmt.Errorf("PostgreSQL EXPLAIN JSON contains multiple results")
	}
	tok, err = dec.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to decode PostgreSQL EXPLAIN JSON array end: %w", err)
	}
	delim, ok = tok.(json.Delim)
	if !ok || delim != ']' {
		return nil, fmt.Errorf("PostgreSQL EXPLAIN JSON array is not closed")
	}
	if tok, err = dec.Token(); err == nil {
		return nil, fmt.Errorf("PostgreSQL EXPLAIN JSON contains trailing data after array: %v", tok)
	} else if !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to decode PostgreSQL EXPLAIN JSON trailing data: %w", err)
	}
	return result.Plan, nil
}

func renderPlan(rootPlan *planNode, opts renderOptions) (string, error) {
	if rootPlan == nil {
		return "", nil
	}
	nextID := uint(0)
	root := buildRenderTree(rootPlan, &nextID)

	style := treerender.DefaultStyle()
	if opts.compact {
		style = treerender.CompactStyle()
	}

	treeRows, err := treerender.RenderTreeWithOptions(
		root,
		style,
		func(n *renderNode) string { return n.text() },
		func(n *renderNode) []*renderNode { return n.children },
		treerender.RenderOptions[renderNode]{
			WrapWidth: opts.wrapWidth,
		},
	)
	if err != nil {
		return "", err
	}

	nodes := collectRenderNodes(root)
	rows := make([]renderedRow, 0, len(nodes))
	for i, node := range nodes {
		rows = append(rows, renderedRow{
			id:         node.id,
			treeRow:    treeRows[i],
			predicates: node.predicates(),
			plan:       node.plan,
		})
	}

	tablePart, err := asciitable.RenderTable(rows, tableSpec())
	if err != nil {
		return "", err
	}
	predicatePart, err := asciitable.RenderAppendix(rows, predicateAppendixSpec())
	if err != nil {
		return "", err
	}
	return tablePart + predicatePart, nil
}

func buildRenderTree(plan *planNode, nextID *uint) *renderNode {
	node := &renderNode{
		id:   *nextID,
		plan: plan,
	}
	*nextID = *nextID + 1
	node.children = make([]*renderNode, 0, len(plan.Plans))
	for _, childPlan := range plan.Plans {
		if childPlan == nil {
			continue
		}
		node.children = append(node.children, buildRenderTree(childPlan, nextID))
	}
	return node
}

func collectRenderNodes(root *renderNode) []renderNode {
	var nodes []renderNode
	var walk func(*renderNode)
	walk = func(n *renderNode) {
		if n == nil {
			return
		}
		node := *n
		node.children = nil
		nodes = append(nodes, node)
		for _, child := range n.children {
			walk(child)
		}
	}
	walk(root)
	return nodes
}

func (n *renderNode) text() string {
	if n.plan == nil {
		return "(nil)"
	}
	p := n.plan
	nodeType := p.NodeType
	if nodeType == "" {
		nodeType = "(unknown)"
	}
	text := nodeType
	switch {
	case p.RelationName != "" && p.Alias != "" && p.Alias != p.RelationName:
		text += " on " + p.RelationName + " " + p.Alias
	case p.RelationName != "":
		text += " on " + p.RelationName
	}
	if p.IndexName != "" {
		text += " using " + p.IndexName
	}

	var attrs []string
	if p.JoinType != "" {
		attrs = append(attrs, p.JoinType)
	}
	if p.Strategy != "" {
		attrs = append(attrs, p.Strategy)
	}
	if len(p.SortKey) > 0 {
		attrs = append(attrs, "Sort Key: "+strings.Join(p.SortKey, ", "))
	}
	if len(p.GroupKey) > 0 {
		attrs = append(attrs, "Group Key: "+strings.Join(p.GroupKey, ", "))
	}
	if len(attrs) > 0 {
		text += " (" + strings.Join(attrs, ", ") + ")"
	}
	return text
}

func (n *renderNode) predicates() []string {
	if n.plan == nil {
		return nil
	}
	p := n.plan
	var predicates []string
	appendPredicate := func(label, value string) {
		if value != "" {
			predicates = append(predicates, label+": "+value)
		}
	}
	appendPredicate("Hash Cond", p.HashCond)
	appendPredicate("Merge Cond", p.MergeCond)
	appendPredicate("Join Filter", p.JoinFilter)
	appendPredicate("Filter", p.Filter)
	appendPredicate("Index Cond", p.IndexCond)
	appendPredicate("Recheck Cond", p.RecheckCond)
	return predicates
}

func tableSpec() asciitable.TableSpec[renderedRow] {
	return asciitable.TableSpec[renderedRow]{
		Columns: []asciitable.Column[renderedRow]{
			{
				Header:    "ID",
				Alignment: asciitable.AlignRight,
				Cell: func(row renderedRow, _ int) string {
					return row.formatID()
				},
			},
			{
				Header: "Operator",
				Cell: func(row renderedRow, _ int) string {
					return row.text()
				},
			},
			{
				Header:    "Rows",
				Alignment: asciitable.AlignRight,
				Cell: func(row renderedRow, _ int) string {
					return formatNumber(row.plan.ActualRows)
				},
			},
			{
				Header:    "Loops",
				Alignment: asciitable.AlignRight,
				Cell: func(row renderedRow, _ int) string {
					return formatNumber(row.plan.ActualLoops)
				},
			},
			{
				Header:    "Time",
				Alignment: asciitable.AlignRight,
				Cell: func(row renderedRow, _ int) string {
					return formatRange(row.plan.ActualStartupTime, row.plan.ActualTotalTime)
				},
			},
			{
				Header:    "Cost",
				Alignment: asciitable.AlignRight,
				Cell: func(row renderedRow, _ int) string {
					return formatRange(row.plan.StartupCost, row.plan.TotalCost)
				},
			},
			{
				Header:    "Plan Rows",
				Alignment: asciitable.AlignRight,
				Cell: func(row renderedRow, _ int) string {
					return formatNumber(row.plan.PlanRows)
				},
			},
		},
	}
}

func predicateAppendixSpec() asciitable.AppendixSpec[renderedRow] {
	return asciitable.AppendixSpec[renderedRow]{
		Title: "Predicates(identified by ID):",
		ID: func(row renderedRow) uint {
			return row.id
		},
		Items: func(row renderedRow) []string {
			return row.predicates
		},
	}
}

func (r renderedRow) formatID() string {
	id := strconv.FormatUint(uint64(r.id), 10)
	if len(r.predicates) > 0 {
		return "*" + id
	}
	return id
}

func (r renderedRow) text() string {
	return r.treeRow.Text()
}

// PostgreSQL EXPLAIN ANALYZE emits startup and total values as pairs. If a future input
// has only one side, keep the missing side blank rather than inventing a value.
func formatRange(start, end *float64) string {
	if start == nil && end == nil {
		return ""
	}
	return formatNumber(start) + ".." + formatNumber(end)
}

func formatNumber(v *float64) string {
	if v == nil {
		return ""
	}
	s := strconv.FormatFloat(*v, 'f', 3, 64)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	if s == "-0" {
		return "0"
	}
	return s
}
