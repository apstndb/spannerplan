package treerender

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/apstndb/go-tabwrap"
	"github.com/google/go-cmp/cmp"
)

func sampleTree() *Node {
	return &Node{
		Text: "root",
		Children: []*Node{
			{
				Text: "left\ncont",
				Children: []*Node{
					{Text: "leaf-a"},
					{Text: "leaf-b"},
				},
			},
			{
				Text: "right",
			},
		},
	}
}

func defaultStyleSampleExpectedRows() []Row {
	return []Row{
		{TreePart: "", NodeText: "root"},
		{TreePart: "+- \n|  ", NodeText: "left\ncont"},
		{TreePart: "|  +- ", NodeText: "leaf-a"},
		{TreePart: "|  +- ", NodeText: "leaf-b"},
		{TreePart: "+- ", NodeText: "right"},
	}
}

func TestRenderTree_DefaultStyle(t *testing.T) {
	got := RenderTree(sampleTree(), DefaultStyle(), func(n *Node) string { return n.Text }, func(n *Node) []*Node { return n.Children })
	want := defaultStyleSampleExpectedRows()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("RenderTree() mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderTree_NilRoot(t *testing.T) {
	var root *Node
	if got := RenderTree(root, DefaultStyle(), func(n *Node) string { return n.Text }, func(n *Node) []*Node { return n.Children }); got != nil {
		t.Fatalf("RenderTree(nil) = %#v, want nil", got)
	}
}

func TestRenderTree_SkipsNilChildPointers(t *testing.T) {
	root := &Node{
		Text: "root",
		Children: []*Node{
			{Text: "only"},
			nil,
		},
	}
	want := RenderTree(&Node{
		Text:     "root",
		Children: []*Node{{Text: "only"}},
	}, DefaultStyle(), func(n *Node) string { return n.Text }, func(n *Node) []*Node { return n.Children })
	got := RenderTree(root, DefaultStyle(), func(n *Node) string { return n.Text }, func(n *Node) []*Node { return n.Children })
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("nil child should be ignored (-want +got):\n%s", diff)
	}
}

func TestRender_DefaultStyle(t *testing.T) {
	got := Render(sampleTree(), DefaultStyle())
	want := defaultStyleSampleExpectedRows()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Render() mismatch (-want +got):\n%s", diff)
	}
}

func TestRender_CompactStyle(t *testing.T) {
	got := Render(sampleTree(), CompactStyle())
	want := []Row{
		{TreePart: "", NodeText: "root"},
		{TreePart: "+\n|", NodeText: "left\ncont"},
		{TreePart: "|+", NodeText: "leaf-a"},
		{TreePart: "|+", NodeText: "leaf-b"},
		{TreePart: "+", NodeText: "right"},
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Render() mismatch (-want +got):\n%s", diff)
	}
}

func TestRender_NegativeIndentDoesNotPanic(t *testing.T) {
	style := DefaultStyle()
	style.IndentSize = -1
	_ = Render(sampleTree(), style)
}

func TestMaxPrefixWidthForDepth_DefaultStyle(t *testing.T) {
	style := DefaultStyle()
	tests := []struct {
		depth int
		want  int
	}{
		{0, 0},
		{1, 3}, // "+- "
		{2, 6}, // "   +- "
		{3, 9}, // "   |  +- "
	}
	for _, tc := range tests {
		if got := MaxPrefixWidthForDepth(style, tc.depth); got != tc.want {
			t.Fatalf("MaxPrefixWidthForDepth(DefaultStyle(), %d) = %d, want %d", tc.depth, got, tc.want)
		}
	}
}

func TestRender_CustomStyle(t *testing.T) {
	style := Style{
		EdgeLink:      "..",
		EdgeMid:       "=>",
		EdgeEnd:       "--",
		EdgeSeparator: "",
		IndentSize:    1,
	}

	got := Render(sampleTree(), style)
	want := []Row{
		{TreePart: "", NodeText: "root"},
		{TreePart: "=>\n.. ", NodeText: "left\ncont"},
		{TreePart: ".. =>", NodeText: "leaf-a"},
		{TreePart: ".. --", NodeText: "leaf-b"},
		{TreePart: "--", NodeText: "right"},
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Render() mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderTreeWithOptions_HangingIndentAnchor(t *testing.T) {
	root := &Node{
		Text: "root",
		Children: []*Node{
			{Text: "[Input] Batch Scan <Row>"},
			{Text: "tail"},
		},
	}

	got := RenderTreeWithOptions(
		root,
		DefaultStyle(),
		func(n *Node) string { return n.Text },
		func(n *Node) []*Node { return n.Children },
		func(n *Node) string {
			if strings.HasPrefix(n.Text, "[Input] ") {
				return "[Input] "
			}
			return ""
		},
		21,
		tabwrap.NewCondition(),
		ContinuationIndentAnchor,
	)

	want := []Row{
		{TreePart: "", NodeText: "root"},
		{TreePart: "+- \n|          ", NodeText: "[Input] Batch Scan\n <Row>"},
		{TreePart: "+- ", NodeText: "tail"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("RenderTreeWithOptions() mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderTreeWithOptions_TinyBudgetKeepsUTF8Valid(t *testing.T) {
	root := &Node{
		Text: "root",
		Children: []*Node{
			{Text: "あい"},
		},
	}

	got := RenderTreeWithOptions(
		root,
		DefaultStyle(),
		func(n *Node) string { return n.Text },
		func(n *Node) []*Node { return n.Children },
		nil,
		4, // child budget becomes 1 after "+- "
		nil,
		ContinuationIndentTree,
	)

	if len(got) != 2 {
		t.Fatalf("RenderTreeWithOptions() rows = %d, want 2", len(got))
	}
	if !utf8.ValidString(got[1].NodeText) {
		t.Fatalf("wrapped NodeText = %q, want valid UTF-8", got[1].NodeText)
	}
	if diff := cmp.Diff("あ\nい", got[1].NodeText); diff != "" {
		t.Fatalf("wrapped NodeText mismatch (-want +got):\n%s", diff)
	}
}
