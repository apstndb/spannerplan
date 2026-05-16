package reference

import (
	"github.com/apstndb/spannerplan/internal/scalarappendix"
	"github.com/apstndb/spannerplan/plantree"
)

// PrintSection selects one appendix section printed after the rendered tree table.
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

// PrintSections is an ordered list of appendix sections.
type PrintSections []PrintSection

// ParsePrintSection parses one print-section name.
// Valid values are "predicates", "ordering", "aggregate", "typed", and "full" (case-insensitive).
func ParsePrintSection(s string) (PrintSection, error) {
	section, err := scalarappendix.ParseSection(s)
	return PrintSection(section), err
}

// ParsePrintSections parses a comma-separated print-section list.
func ParsePrintSections(s string) (PrintSections, error) {
	sections, err := scalarappendix.ParseSections(s)
	if err != nil {
		return nil, err
	}
	return printSectionsFromScalarAppendix(sections), nil
}

func printOptionsFromOptions(o options) scalarappendix.Options {
	var sections *scalarappendix.Sections
	if o.printSections != nil {
		converted := scalarAppendixSections(*o.printSections)
		sections = &converted
	}
	return scalarappendix.Options{
		Sections:                   sections,
		ShowScalarVars:             o.showScalarVars,
		ResolveScalarVars:          o.resolveScalarVars,
		ResolveScalarVarsRecursive: o.resolveScalarVarsRecursive,
	}
}

func renderAppendices(rows []plantree.RowWithPredicates, printOpts scalarappendix.Options) (string, error) {
	return scalarappendix.Render(rows, printOpts)
}

func scalarAppendixSections(sections PrintSections) scalarappendix.Sections {
	converted := make(scalarappendix.Sections, 0, len(sections))
	for _, section := range sections {
		converted = append(converted, scalarappendix.Section(section))
	}
	return converted
}

func printSectionsFromScalarAppendix(sections scalarappendix.Sections) PrintSections {
	converted := make(PrintSections, 0, len(sections))
	for _, section := range sections {
		converted = append(converted, PrintSection(section))
	}
	return converted
}
