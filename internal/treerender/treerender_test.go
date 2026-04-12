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

func TestRender_DefaultStyle(t *testing.T) {
	got := Render(sampleTree(), DefaultStyle())
	want := []Row{
		{TreePart: "", NodeText: "root"},
		{TreePart: "+- \n|  ", NodeText: "left\ncont"},
		{TreePart: "|  +- ", NodeText: "leaf-a"},
		{TreePart: "|  +- ", NodeText: "leaf-b"},
		{TreePart: "+- ", NodeText: "right"},
	}

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
