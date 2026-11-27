package stats

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode"

	"google.golang.org/protobuf/types/known/structpb"
)

// QueryStat is one entry in ResultSetStats.query_stats. [QueryStatsEntries]
// borrows Value from the input. Keep the value unchanged while using the entry.
type QueryStat struct {
	Key   string
	Value *structpb.Value
}

// QueryStatsEntries returns the entries in bytewise key order. Set includeQueryText to
// include the query_text entry, which can contain the original SQL.
func QueryStatsEntries(queryStats *structpb.Struct, includeQueryText bool) []QueryStat {
	var entries []QueryStat
	for key, value := range queryStats.GetFields() {
		if key == "query_text" && !includeQueryText {
			continue
		}
		entries = append(entries, QueryStat{Key: key, Value: value})
	}
	slices.SortFunc(entries, func(a, b QueryStat) int {
		return strings.Compare(a.Key, b.Key)
	})
	return entries
}

// FormatValue formats an entry for single-line display. It quotes strings
// containing control characters, line separators, or bidirectional controls.
// Other protobuf values are rendered as compact JSON. Non-finite numbers and
// unset values return an error. Distinct protobuf types can display the same
// text.
func (entry QueryStat) FormatValue() (string, error) {
	if text, ok := entry.Value.GetKind().(*structpb.Value_StringValue); ok {
		return queryStatLine(text.StringValue), nil
	}
	value, err := queryStatJSONValue(entry.Value)
	if err != nil {
		return "", fmt.Errorf("format query stat %q: %w", entry.Key, err)
	}
	text, err := encodeQueryStatJSON(value)
	if err != nil {
		return "", fmt.Errorf("format query stat %q: %w", entry.Key, err)
	}
	return text, nil
}

// FormatQueryStats formats the supplied entries in order for display, with one
// key-value pair per line and no trailing newline.
func FormatQueryStats(entries []QueryStat) (string, error) {
	lines := make([]string, 0, len(entries))
	for _, entry := range entries {
		text, err := entry.FormatValue()
		if err != nil {
			return "", err
		}
		lines = append(lines, queryStatLine(entry.Key)+": "+text)
	}
	return strings.Join(lines, "\n"), nil
}

func queryStatLine(text string) string {
	if strings.ContainsFunc(text, queryStatEscape) {
		quoted, _ := encodeQueryStatJSON(text) // Encoding a plain string cannot fail.
		return quoted
	}
	return text
}

func queryStatEscape(r rune) bool {
	return unicode.IsControl(r) || unicode.Is(unicode.Bidi_Control, r) || r == '\u2028' || r == '\u2029'
}

func encodeQueryStatJSON(value any) (string, error) {
	var out strings.Builder
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return "", err
	}
	text := strings.TrimSuffix(out.String(), "\n")
	// JSON permits DEL, C1, and bidirectional controls inside strings.
	if !strings.ContainsFunc(text, queryStatEscape) {
		return text, nil
	}
	var escaped strings.Builder
	for _, r := range text {
		if queryStatEscape(r) {
			fmt.Fprintf(&escaped, `\u%04x`, r)
		} else {
			escaped.WriteRune(r)
		}
	}
	return escaped.String(), nil
}

// Keep numbers as float64 so encoding/json rejects non-finite numbers,
// including in lists and objects. AsInterface turns them into strings.
func queryStatJSONValue(value *structpb.Value) (any, error) {
	switch kind := value.GetKind().(type) {
	case *structpb.Value_NumberValue:
		return kind.NumberValue, nil
	case *structpb.Value_ListValue:
		values := kind.ListValue.GetValues()
		list := make([]any, len(values))
		for i, element := range values {
			decoded, err := queryStatJSONValue(element)
			if err != nil {
				return nil, err
			}
			list[i] = decoded
		}
		return list, nil
	case *structpb.Value_StructValue:
		fields := kind.StructValue.GetFields()
		object := make(map[string]any, len(fields))
		for key, field := range fields {
			decoded, err := queryStatJSONValue(field)
			if err != nil {
				return nil, err
			}
			object[key] = decoded
		}
		return object, nil
	case nil:
		return nil, errors.New("unset protobuf value")
	default:
		return value.AsInterface(), nil
	}
}
