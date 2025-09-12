package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

	queryplan "github.com/apstndb/spannerplan"
	"github.com/apstndb/spannerplan/plantree/reference"
)

// renderWithParams renders a query plan with the given parameters
func renderWithParams(planNodes []*sppb.PlanNode, modeStr, formatStr string, wrap int) (string, error) {
	mode, err := reference.ParseRenderMode(modeStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse mode: %w", err)
	}
	format, err := reference.ParseFormat(formatStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse format: %w", err)
	}
	return reference.RenderTreeTable(planNodes, mode, format, wrap)
}

func main() {
	var (
		inputPath string
		mode      string
		format    string
		wrap      int
		all       bool
	)

	flag.StringVar(&inputPath, "input", "testdata/dca.yaml", "Path to input JSON/YAML containing Spanner query plan")
	flag.StringVar(&mode, "mode", "AUTO", "Render mode: AUTO|PLAN|PROFILE")
	flag.StringVar(&format, "format", "CURRENT", "Format: TRADITIONAL|CURRENT|COMPACT")
	flag.IntVar(&wrap, "wrap", 0, "Wrap width (0 disables wrapping)")
	flag.BoolVar(&all, "all", false, "Render all common combinations (ignores -mode and -format)")
	flag.Parse()

	b, err := os.ReadFile(inputPath)
	if err != nil {
		log.Fatalf("failed to read %s: %v", inputPath, err)
	}
	input := string(b)

	// Extract query plan once
	stats, _, err := queryplan.ExtractQueryPlan([]byte(input))
	if err != nil {
		log.Fatalf("failed to extract query plan: %v", err)
	}
	queryPlan := stats.GetQueryPlan()
	if queryPlan == nil {
		log.Fatal("query plan is nil")
	}
	planNodes := queryPlan.GetPlanNodes()

	if all {
		cases := []struct {
			name   string
			mode   string
			format string
		}{
			{"AUTO CURRENT", "AUTO", "CURRENT"},
			{"PLAN CURRENT", "PLAN", "CURRENT"},
			{"PROFILE CURRENT", "PROFILE", "CURRENT"},
			{"PLAN TRADITIONAL", "PLAN", "TRADITIONAL"},
			{"PLAN COMPACT", "PLAN", "COMPACT"},
			{"PROFILE COMPACT", "PROFILE", "COMPACT"},
		}

		for _, tc := range cases {
			out, err := renderWithParams(planNodes, tc.mode, tc.format, wrap)
			if err != nil {
				log.Fatalf("render error for %s: %v", tc.name, err)
			}
			if _, err := fmt.Fprintf(os.Stdout, "===== %s =====\n%s\n", tc.name, out); err != nil {
				log.Fatal(err)
			}
		}
		return
	}

	out, err := renderWithParams(planNodes, mode, format, wrap)
	if err != nil {
		log.Fatalf("render error: %v", err)
	}
	if _, err := fmt.Fprintln(os.Stdout, out); err != nil {
		log.Fatal(err)
	}
}
