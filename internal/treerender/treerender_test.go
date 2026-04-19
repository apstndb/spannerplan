package treerender

import (
	"testing"

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
