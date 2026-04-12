package treerender

import "strings"

type Node struct {
	Text     string
	Children []*Node
}

type Row struct {
	TreePart string
	NodeText string
}

type Style struct {
	EdgeLink      string
	EdgeMid       string
	EdgeEnd       string
	EdgeSeparator string
	IndentSize    int
}

func DefaultStyle() Style {
	return Style{
		EdgeLink:      "|",
		EdgeMid:       "+-",
		EdgeEnd:       "+-",
		EdgeSeparator: " ",
		IndentSize:    2,
	}
}

func CompactStyle() Style {
	return Style{
		EdgeLink:   "|",
		EdgeMid:    "+",
		EdgeEnd:    "+",
		IndentSize: 0,
	}
}

func Render(root *Node, style Style) []Row {
	if root == nil {
		return nil
	}

	var rows []Row
	var walk func(*Node, []bool, bool, bool)
	walk = func(node *Node, ancestorHasNext []bool, isLast, isRoot bool) {
		rows = append(rows, Row{
			TreePart: strings.Join(prefixLines(node.Text, ancestorHasNext, isLast, isRoot, style), "\n"),
			NodeText: node.Text,
		})

		childAncestors := slicesClone(ancestorHasNext)
		if !isRoot {
			childAncestors = append(childAncestors, !isLast)
		}
		for i, child := range node.Children {
			walk(child, childAncestors, i == len(node.Children)-1, false)
		}
	}

	walk(root, nil, true, true)
	return rows
}

func prefixLines(text string, ancestorHasNext []bool, isLast, isRoot bool, style Style) []string {
	lines := strings.Split(text, "\n")
	prefixes := make([]string, len(lines))
	if isRoot {
		return prefixes
	}

	ancestorPrefix := renderAncestorPrefix(ancestorHasNext, style)
	prefixes[0] = ancestorPrefix + edgeForRow(isLast, style) + style.EdgeSeparator

	continuation := ancestorPrefix + continuationSegment(isLast, style)
	for i := 1; i < len(prefixes); i++ {
		prefixes[i] = continuation
	}

	return prefixes
}

func renderAncestorPrefix(ancestorHasNext []bool, style Style) string {
	var sb strings.Builder
	for _, hasNext := range ancestorHasNext {
		sb.WriteString(segment(hasNext, style))
	}
	return sb.String()
}

func edgeForRow(isLast bool, style Style) string {
	if isLast {
		return style.EdgeEnd
	}
	return style.EdgeMid
}

func continuationSegment(isLast bool, style Style) string {
	return segment(!isLast, style)
}

func segment(hasNext bool, style Style) string {
	if hasNext {
		return style.EdgeLink + strings.Repeat(" ", style.IndentSize)
	}
	return strings.Repeat(" ", style.IndentSize+1)
}

func slicesClone(items []bool) []bool {
	if len(items) == 0 {
		return nil
	}

	out := make([]bool, len(items))
	copy(out, items)
	return out
}
