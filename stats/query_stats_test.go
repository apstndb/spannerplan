package stats_test

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/apstndb/spannerplan/stats"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestQueryStatsEntries(t *testing.T) {
	t.Parallel()

	value := structpb.NewStringValue("12.5 msecs")
	input := &structpb.Struct{Fields: map[string]*structpb.Value{
		"elapsed_time": value,
		"z":            structpb.NewNullValue(),
		"query_text":   structpb.NewStringValue("SELECT 1"),
		"CPU":          structpb.NewNumberValue(15),
	}}
	entries := stats.QueryStatsEntries(input, false)
	if got, want := statKeys(entries), []string{"CPU", "elapsed_time", "z"}; !slices.Equal(got, want) {
		t.Fatalf("keys = %v, want %v", got, want)
	}
	if entries[1].Value != value || len(input.Fields) != 4 {
		t.Fatal("entries should borrow values and preserve the input map")
	}
	if got, want := statKeys(stats.QueryStatsEntries(input, true)), []string{"CPU", "elapsed_time", "query_text", "z"}; !slices.Equal(got, want) {
		t.Fatalf("keys with query_text = %v, want %v", got, want)
	}
	for _, empty := range []*structpb.Struct{nil, {}} {
		text, err := stats.FormatQueryStats(stats.QueryStatsEntries(empty, false))
		if err != nil || text != "" {
			t.Fatalf("empty stats = %q, %v", text, err)
		}
	}
}

func statKeys(entries []stats.QueryStat) []string {
	keys := make([]string, len(entries))
	for i, entry := range entries {
		keys[i] = entry.Key
	}
	return keys
}

func TestFormatQueryStats(t *testing.T) {
	t.Parallel()

	input := &structpb.Struct{Fields: map[string]*structpb.Value{
		"string": structpb.NewStringValue(`x < y & z > a\b`),
		"null":   structpb.NewNullValue(),
		"bool":   structpb.NewBoolValue(true),
		"number": structpb.NewNumberValue(1.25),
		"list": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
			structpb.NewNullValue(), structpb.NewNumberValue(2),
		}}),
		"object": structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			"z": structpb.NewNumberValue(math.Copysign(0, -1)),
			"a": structpb.NewStringValue("<>&\u0085\u202e"),
		}}),
	}}
	want := `bool: true
list: [null,2]
null: null
number: 1.25
object: {"a":"<>&\u0085\u202e","z":-0}
string: x < y & z > a\b`
	for range 25 {
		got, err := stats.FormatQueryStats(stats.QueryStatsEntries(input, false))
		if err != nil || got != want {
			t.Fatalf("FormatQueryStats = %q, %v; want %q", got, err, want)
		}
	}
}

func TestFormatQueryStatControls(t *testing.T) {
	t.Parallel()

	const key = "line\nkey\t\u2028"
	const value = "a\r\nb\x1b\x7f\u0085\u2029<>&"
	entry := stats.QueryStat{Key: key, Value: structpb.NewStringValue(value)}
	got, err := stats.FormatQueryStats([]stats.QueryStat{entry})
	if err != nil {
		t.Fatal(err)
	}
	want := `"line\nkey\t\u2028": "a\r\nb\u001b\u007f\u0085\u2029<>&"`
	if got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
	formatted, err := entry.FormatValue()
	if err != nil {
		t.Fatal(err)
	}
	var decoded string
	if err := json.Unmarshal([]byte(formatted), &decoded); err != nil || decoded != value {
		t.Fatalf("quoted value decodes to %q, %v; want %q", decoded, err, value)
	}
}

func TestFormatQueryStatsUnicodeSeparators(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		char       string
		jsonEscape string
	}{
		{"\u2028", `\u2028`},
		{"\u2029", `\u2029`},
		{"\u202e", `\u202e`},
	} {
		entry := stats.QueryStat{
			Key:   "key" + tc.char,
			Value: structpb.NewStringValue("value" + tc.char),
		}
		got, err := stats.FormatQueryStats([]stats.QueryStat{entry})
		want := fmt.Sprintf(`"key%s": "value%s"`, tc.jsonEscape, tc.jsonEscape)
		if err != nil || got != want {
			t.Fatalf("FormatQueryStats for %q = %q, %v; want %q", tc.char, got, err, want)
		}
	}
}

func TestFormatQueryStatsInvalidValues(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		value *structpb.Value
	}{
		{"nil", nil},
		{"unset", &structpb.Value{}},
		{"nan", structpb.NewNumberValue(math.NaN())},
		{"nested infinity", structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
			structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
				"value": structpb.NewNumberValue(math.Inf(1)),
			}}),
		}})},
		{"nested unset", structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"unset": {}}})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			text, err := stats.FormatQueryStats([]stats.QueryStat{
				{Key: "valid", Value: structpb.NewStringValue("a")},
				{Key: "invalid", Value: tc.value},
			})
			if err == nil || !strings.Contains(err.Error(), `query stat "invalid"`) || text != "" {
				t.Fatalf("FormatQueryStats = %q, %v; want an error naming the invalid key", text, err)
			}
		})
	}
}

func ExampleFormatQueryStats() {
	input := &structpb.Struct{Fields: map[string]*structpb.Value{
		"rows_scanned": structpb.NewNumberValue(42),
		"query_text":   structpb.NewStringValue("SELECT * FROM Albums"),
		"elapsed_time": structpb.NewStringValue("12 msecs"),
	}}
	text, err := stats.FormatQueryStats(stats.QueryStatsEntries(input, false))
	if err != nil {
		panic(err)
	}
	fmt.Println(text)
	// Output:
	// elapsed_time: 12 msecs
	// rows_scanned: 42
}
