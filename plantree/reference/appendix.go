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

// Appendix is a nonempty, titled section built from processed plan rows.
// Lines have aligned node-ID prefixes but no outer indentation or trailing
// newline. Input item text, including embedded newlines, is preserved.
// The returned slice and Lines slices are caller-owned.
type Appendix struct {
	Section PrintSection
	Title   string
	Lines   []string
}

// BuildAppendices builds scalar appendices without rendering a tree or table.
// Omitted print sections select [PrintPredicates]; [WithPrintSections] with no
// arguments suppresses all sections. Empty sections are omitted, and selected
// sections retain their requested order. Print-section and scalar-variable
// options apply; layout, wrapping, and hanging-indent options do not.
// It uses the same builder as [RenderTreeTableWithOptions].
func BuildAppendices(rows []plantree.RowWithPredicates, opts ...Option) ([]Appendix, error) {
	o := options{}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	parts, err := scalarappendix.Build(rows, printOptionsFromOptions(o))
	if err != nil {
		return nil, err
	}
	var result []Appendix
	for _, part := range parts {
		result = append(result, Appendix{Section: PrintSection(part.Section), Title: part.Title, Lines: part.Lines})
	}
	return result, nil
}

// PrintPreset selects an intent-based appendix section set.
type PrintPreset string

const (
	// PrintPresetBasic prints predicate-like scalar links.
	PrintPresetBasic PrintPreset = "basic"
	// PrintPresetEnhanced prints predicate, ordering, and aggregate sections.
	PrintPresetEnhanced PrintPreset = "enhanced"
	// PrintPresetFull prints all scalar links, including unnamed links.
	PrintPresetFull PrintPreset = "full"
	// PrintPresetNone suppresses appendix sections.
	PrintPresetNone PrintPreset = "none"
)

// NewPrintSections returns a config-ready print section pointer.
// Passing no sections returns an explicit empty section list that suppresses appendices.
// Use nil to keep the default [PrintPresetBasic] behavior in [RenderConfig].
func NewPrintSections(sections ...PrintSection) *PrintSections {
	copied := append(PrintSections{}, sections...)
	return &copied
}

// ParsePrintPreset parses one print preset name.
// Valid values are "basic", "enhanced", "full", and "none" (case-insensitive).
func ParsePrintPreset(s string) (PrintPreset, error) {
	preset, err := scalarappendix.ParsePreset(s)
	return PrintPreset(preset), err
}

// Sections returns the appendix sections for p.
func (p PrintPreset) Sections() (PrintSections, error) {
	sections, err := scalarappendix.Preset(p).Sections()
	if err != nil {
		return nil, err
	}
	return printSectionsFromScalarAppendix(sections), nil
}

// ParsePrintSection parses one print-section name.
// Valid values are "predicates", "ordering", "aggregate", "typed", and "full" (case-insensitive).
func ParsePrintSection(s string) (PrintSection, error) {
	section, err := scalarappendix.ParseSection(s)
	return PrintSection(section), err
}

// ParsePrintSections parses a named preset or a comma-separated print-section list.
// Empty or blank input returns a non-nil empty list that suppresses appendices when used explicitly.
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
