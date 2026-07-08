// Package protoyaml is a frozen, internal helper for decoding protobuf query
// plan data as YAML/JSON. It is retained only for downstream compatibility:
// spanner-mycli pins the exact bytes produced by Marshal, so the output format
// must not change.
//
// The Marshal output format was never a designed artifact. It is an accident of
// reflection-based encoding (goccy/go-yaml over the protobuf message via
// UseJSONMarshaler) rather than a deliberate, canonical mapping. New code should
// use github.com/apstndb/protoyaml, which implements the canonical
// protojson-over-YAML mapping.
//
// This package will be removed in v0.3.0 once spanner-mycli has migrated.
//
// Deprecated: Use github.com/apstndb/protoyaml instead. This package is frozen
// for downstream compatibility and will be removed in v0.3.0.
package protoyaml

import (
	"encoding/json"
	"slices"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/goccy/go-yaml"
)

var jsonpb = protojson.UnmarshalOptions{
	DiscardUnknown: true,
}

// Unmarshal parses YAML bytes into a protobuf message.
//
// Deprecated: Use github.com/apstndb/protoyaml.Unmarshal instead. This package
// is frozen for downstream compatibility and will be removed in v0.3.0.
func Unmarshal(b []byte, result proto.Message) error {
	j, err := YAMLToJSON(b)
	if err != nil {
		return err
	}
	return UnmarshalJSON(j, result)
}

// UnmarshalJSON unmarshals JSON bytes into a protobuf message with this package's decode options.
//
// Deprecated: Use github.com/apstndb/protoyaml.UnmarshalJSON instead. This
// package is frozen for downstream compatibility and will be removed in v0.3.0.
func UnmarshalJSON(j []byte, result proto.Message) error {
	return jsonpb.Unmarshal(j, result)
}

// Marshal encodes a protobuf message as YAML using reflection-based encoding.
//
// Deprecated: The output format is an accident of reflection-based encoding, not
// a designed artifact. This package is frozen for downstream compatibility
// (spanner-mycli pins its exact output) and will be removed in v0.3.0. New code
// should use github.com/apstndb/protoyaml.Marshal, which produces canonical
// protojson-over-YAML output.
func Marshal(m proto.Message, opts ...yaml.EncodeOption) ([]byte, error) {
	opts = slices.Clone(opts)
	opts = append(opts, yaml.UseJSONMarshaler())
	return yaml.MarshalWithOptions(m, opts...)
}

// YAMLToJSON converts YAML bytes into JSON bytes for protojson.Unmarshal.
//
// Deprecated: Use github.com/apstndb/protoyaml.YAMLToJSON instead. This package
// is frozen for downstream compatibility and will be removed in v0.3.0.
func YAMLToJSON(y []byte) ([]byte, error) {
	var i interface{}
	err := yaml.Unmarshal(y, &i)
	if err != nil {
		return nil, err
	}
	return json.Marshal(i)
}
