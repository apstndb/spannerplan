package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	reference "github.com/apstndb/spannerplan/plantree/reference"
)

func main() {
	var (
		inputPath string
		mode     string
		format   string
		wrap     int
		all      bool
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
			out, err := reference.RenderASCII(input, tc.mode, tc.format, wrap)
			if err != nil {
				log.Fatalf("render error for %s: %v", tc.name, err)
			}
			fmt.Fprintf(os.Stdout, "===== %s =====\n%s\n", tc.name, out)
		}
		return
	}

	out, err := reference.RenderASCII(input, mode, format, wrap)
	if err != nil {
		log.Fatalf("render error: %v", err)
	}
	fmt.Fprintln(os.Stdout, out)
}
