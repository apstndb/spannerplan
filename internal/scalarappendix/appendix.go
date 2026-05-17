package scalarappendix

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/apstndb/spannerplan/asciitable"
	"github.com/apstndb/spannerplan/plantree"
)

var (
	scalarVariableReferenceRe   = regexp.MustCompile(`\$[A-Za-z0-9_']+(?:\.[A-Za-z0-9_']+)*`)
	scalarVariableDescriptionRe = regexp.MustCompile(`^\$[A-Za-z0-9_']+(?:\.[A-Za-z0-9_']+)*$`)
)

// Section selects one appendix section printed after a rendered tree table.
type Section string

const (
	// SectionPredicates prints predicate-like scalar links.
	SectionPredicates Section = "predicates"
	// SectionOrdering prints ordering scalar links for sort operators.
	SectionOrdering Section = "ordering"
	// SectionAggregate prints grouping and aggregate scalar links for aggregate operators.
	SectionAggregate Section = "aggregate"
	// SectionTyped prints all typed scalar links as a raw debug dump.
	SectionTyped Section = "typed"
	// SectionFull prints all scalar links, including unnamed links, as a raw debug dump.
	SectionFull Section = "full"
)

// Sections is an ordered list of appendix sections.
type Sections []Section

// Preset selects an intent-based appendix section set.
type Preset string

const (
	// PresetBasic prints predicate-like scalar links.
	PresetBasic Preset = "basic"
	// PresetEnhanced prints predicate, ordering, and aggregate sections.
	PresetEnhanced Preset = "enhanced"
	// PresetFull prints all scalar links, including unnamed links.
	PresetFull Preset = "full"
	// PresetNone suppresses appendix sections.
	PresetNone Preset = "none"
)

// Options configures appendix rendering.
type Options struct {
	// Sections selects appendix sections.
	// A nil value uses the default SectionPredicates section.
	// An explicitly empty value renders no appendix sections.
	Sections *Sections

	// ShowScalarVars prints scalar assignment variable names in semantic appendix sections.
	ShowScalarVars bool

	// ResolveScalarVars replaces direct scalar variable aliases in semantic appendix sections.
	ResolveScalarVars bool

	// ResolveScalarVarsRecursive recursively resolves scalar variable aliases in semantic appendix sections.
	ResolveScalarVarsRecursive bool
}

// ParsePreset parses one print preset name.
// Valid values are "basic", "enhanced", "full", and "none" (case-insensitive).
func ParsePreset(s string) (Preset, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case string(PresetBasic):
		return PresetBasic, nil
	case string(PresetEnhanced):
		return PresetEnhanced, nil
	case string(PresetFull):
		return PresetFull, nil
	case string(PresetNone):
		return PresetNone, nil
	default:
		return "", fmt.Errorf("unknown print preset: %s", s)
	}
}

// Sections returns the appendix sections for p.
func (p Preset) Sections() (Sections, error) {
	switch p {
	case PresetBasic:
		return Sections{SectionPredicates}, nil
	case PresetEnhanced:
		return Sections{SectionPredicates, SectionOrdering, SectionAggregate}, nil
	case PresetFull:
		return Sections{SectionFull}, nil
	case PresetNone:
		return Sections{}, nil
	default:
		return nil, fmt.Errorf("unsupported print preset: %s", p)
	}
}

// ParseSection parses one print-section name.
// Valid values are "predicates", "ordering", "aggregate", "typed", and "full" (case-insensitive).
func ParseSection(s string) (Section, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case string(SectionPredicates):
		return SectionPredicates, nil
	case string(SectionOrdering):
		return SectionOrdering, nil
	case string(SectionAggregate):
		return SectionAggregate, nil
	case string(SectionTyped):
		return SectionTyped, nil
	case string(SectionFull):
		return SectionFull, nil
	default:
		return "", fmt.Errorf("unknown print section: %s", s)
	}
}

// ParseSections parses a named preset or a comma-separated print-section list.
// An empty string returns a non-nil empty list, which renders no appendix sections.
func ParseSections(s string) (Sections, error) {
	s = strings.TrimSpace(s)
	var sections Sections
	if s == "" {
		sections = Sections{}
	} else if !strings.Contains(s, ",") {
		preset, presetErr := ParsePreset(s)
		if presetErr == nil {
			var err error
			sections, err = preset.Sections()
			if err != nil {
				return nil, err
			}
		} else {
			section, sectionErr := ParseSection(s)
			if sectionErr != nil {
				return nil, fmt.Errorf("unknown print preset or section: %s", s)
			}
			sections = Sections{section}
		}
	} else {
		for _, raw := range strings.Split(s, ",") {
			token := strings.TrimSpace(raw)
			if token == "" {
				return nil, fmt.Errorf("print section must not be empty")
			}

			section, err := ParseSection(token)
			if err != nil {
				if _, presetErr := ParsePreset(token); presetErr == nil {
					return nil, fmt.Errorf("print preset %q cannot be combined with section list", token)
				}
				return nil, err
			}
			sections = append(sections, section)
		}
	}

	if err := ValidateSections(sections); err != nil {
		return nil, err
	}
	return sections, nil
}

// ValidateSections validates an ordered print-section list.
func ValidateSections(sections Sections) error {
	seen := map[Section]bool{}
	for _, section := range sections {
		switch section {
		case SectionPredicates, SectionOrdering, SectionAggregate, SectionTyped, SectionFull:
		default:
			return fmt.Errorf("unsupported print section: %s", section)
		}

		if seen[section] {
			return fmt.Errorf("duplicate print section: %s", section)
		}
		seen[section] = true
	}

	if len(sections) > 1 {
		for _, section := range sections {
			if section == SectionTyped || section == SectionFull {
				return fmt.Errorf("print section %q cannot be combined with other sections", section)
			}
		}
	}
	return nil
}

// Render renders the configured scalar appendices without a leading separator.
func Render(rows []plantree.RowWithPredicates, opts Options) (string, error) {
	sections, err := resolvedSections(opts.Sections)
	if err != nil {
		return "", err
	}

	resolveVars := opts.ResolveScalarVars || opts.ResolveScalarVarsRecursive
	var resolver scalarLinkResolver
	if resolveVars && needsScalarLinkResolver(sections) {
		resolver = newScalarLinkResolver(rows)
	}

	var b strings.Builder
	for _, section := range sections {
		var (
			part string
			err  error
		)
		switch section {
		case SectionFull, SectionTyped:
			part, err = asciitable.RenderAppendix(rows, scalarAppendixSpec(
				"Node Parameters(identified by ID):",
				func(row plantree.RowWithPredicates) []string {
					return scalarLinkLines(row, func(_ plantree.RowWithPredicates, link plantree.ScalarChildLink) bool {
						return section == SectionFull || link.Type != ""
					}, formatRawScalarLink)
				},
			))
		case SectionPredicates:
			part, err = asciitable.RenderAppendix(rows, scalarAppendixSpec(
				"Predicates(identified by ID):",
				func(row plantree.RowWithPredicates) []string {
					return row.Predicates
				},
			))
		case SectionOrdering:
			format := semanticScalarLinkFormatter(opts.ShowScalarVars, keyScalarLinkDescription)
			if resolveVars {
				format = semanticScalarLinkFormatter(opts.ShowScalarVars, func(link plantree.ScalarChildLink) string {
					return resolver.formatKeyScalarLink(link, opts.ResolveScalarVarsRecursive)
				})
			}
			part, err = asciitable.RenderAppendix(rows, scalarAppendixSpec(
				"Ordering(identified by ID):",
				func(row plantree.RowWithPredicates) []string {
					return scalarLinkLines(row, isOrderingScalarLink, format)
				},
			))
		case SectionAggregate:
			format := semanticScalarLinkFormatter(opts.ShowScalarVars, scalarLinkDescription)
			if resolveVars {
				format = semanticScalarLinkFormatter(opts.ShowScalarVars, func(link plantree.ScalarChildLink) string {
					return resolver.formatAggregateScalarLink(link, opts.ResolveScalarVarsRecursive)
				})
			}
			part, err = asciitable.RenderAppendix(rows, scalarAppendixSpec(
				"Aggregates(identified by ID):",
				func(row plantree.RowWithPredicates) []string {
					return scalarLinkLines(row, isAggregateScalarLink, format)
				},
			))
		default:
			return "", fmt.Errorf("unsupported print section: %s", section)
		}
		if err != nil {
			return "", err
		}
		if part != "" {
			if b.Len() > 0 {
				b.WriteString("\n")
			}
			b.WriteString(part)
		}
	}
	return b.String(), nil
}

func resolvedSections(sections *Sections) (Sections, error) {
	if sections == nil {
		return Sections{SectionPredicates}, nil
	}
	resolved := append(Sections{}, (*sections)...)
	return resolved, ValidateSections(resolved)
}

func needsScalarLinkResolver(sections Sections) bool {
	return slices.Contains(sections, SectionOrdering) || slices.Contains(sections, SectionAggregate)
}

func scalarAppendixSpec(
	title string,
	items func(row plantree.RowWithPredicates) []string,
) asciitable.AppendixSpec[plantree.RowWithPredicates] {
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

func scalarLinkLines(
	row plantree.RowWithPredicates,
	include func(plantree.RowWithPredicates, plantree.ScalarChildLink) bool,
	format func(plantree.ScalarChildLink) string,
) []string {
	groupByType := map[string]int{}
	groups := []scalarLinkGroup{}

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
		groups[groupIndex].values = append(groups[groupIndex].values, format(link))
	}

	lines := make([]string, 0, len(groups))
	for _, group := range groups {
		joined := strings.Join(group.values, ", ")
		if joined == "" {
			continue
		}

		typePart := ""
		if group.typ != "" {
			typePart = group.typ + ": "
		}
		lines = append(lines, typePart+joined)
	}
	return lines
}

func formatRawScalarLink(link plantree.ScalarChildLink) string {
	if link.Variable != "" {
		return fmt.Sprintf("$%s=%s", link.Variable, link.Description)
	}
	return link.Description
}

func scalarLinkDescription(link plantree.ScalarChildLink) string {
	return link.Description
}

func keyScalarLinkDescription(link plantree.ScalarChildLink) string {
	return normalizeKeyOrderSuffix(link.Description)
}

func semanticScalarLinkFormatter(
	showVars bool,
	description func(plantree.ScalarChildLink) string,
) func(plantree.ScalarChildLink) string {
	return func(link plantree.ScalarChildLink) string {
		desc := description(link)
		if showVars && link.Variable != "" {
			return fmt.Sprintf("$%s=%s", link.Variable, desc)
		}
		return desc
	}
}

type scalarLinkResolver struct {
	variableToDescription map[string]string
}

func newScalarLinkResolver(rows []plantree.RowWithPredicates) scalarLinkResolver {
	variableToDescription := map[string]string{}
	for _, row := range rows {
		for _, link := range row.ScalarChildLinks {
			if link.Variable == "" {
				continue
			}
			variableToDescription[link.Variable] = link.Description
		}
	}
	return scalarLinkResolver{variableToDescription: variableToDescription}
}

func (r scalarLinkResolver) formatKeyScalarLink(link plantree.ScalarChildLink, recursive bool) string {
	return r.resolveKeyDescription(link.Description, recursive)
}

func (r scalarLinkResolver) formatAggregateScalarLink(link plantree.ScalarChildLink, recursive bool) string {
	if link.Type == "Key" {
		return r.resolveKeyDescription(link.Description, recursive)
	}
	return link.Description
}

func (r scalarLinkResolver) resolveKeyDescription(desc string, recursive bool) string {
	if !recursive {
		return normalizeKeyOrderSuffix(r.resolveDirectDescriptionVariables(desc))
	}
	return normalizeKeyOrderSuffix(r.resolveDescriptionVariables(desc, map[string]bool{}))
}

func (r scalarLinkResolver) resolveDirectDescriptionVariables(desc string) string {
	return scalarVariableReferenceRe.ReplaceAllStringFunc(desc, func(ref string) string {
		resolved, ok := r.variableToDescription[strings.TrimPrefix(ref, "$")]
		if !ok {
			return ref
		}
		return strings.TrimSpace(resolved)
	})
}

func (r scalarLinkResolver) resolveDescriptionVariables(desc string, seen map[string]bool) string {
	return scalarVariableReferenceRe.ReplaceAllStringFunc(desc, func(ref string) string {
		return r.lookupVarRecursive(ref, seen)
	})
}

func (r scalarLinkResolver) lookupVarRecursive(ref string, seen map[string]bool) string {
	if !strings.HasPrefix(ref, "$") {
		return ref
	}

	varName := strings.TrimPrefix(ref, "$")
	if seen[varName] {
		return ref
	}

	desc, ok := r.variableToDescription[varName]
	if !ok {
		return ref
	}

	seen[varName] = true
	defer delete(seen, varName)

	desc = strings.TrimSpace(desc)
	if scalarVariableDescriptionRe.MatchString(desc) {
		return r.lookupVarRecursive(desc, seen)
	}
	return r.resolveDescriptionVariables(desc, seen)
}

func normalizeKeyOrderSuffix(s string) string {
	s = strings.TrimSpace(s)
	for _, suffix := range []string{"(ASC)", "(DESC)"} {
		if strings.HasSuffix(s, " "+suffix) {
			return strings.TrimSuffix(s, " "+suffix) + " " + strings.Trim(suffix, "()")
		}
	}
	return s
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
