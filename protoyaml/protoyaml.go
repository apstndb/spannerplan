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

func Unmarshal(b []byte, result proto.Message) error {
	j, err := YAMLToJSON(b)
	if err != nil {
		return err
	}
	return UnmarshalJSON(j, result)
}

// UnmarshalJSON unmarshals JSON bytes into a protobuf message with this package's decode options.
func UnmarshalJSON(j []byte, result proto.Message) error {
	return jsonpb.Unmarshal(j, result)
}

func Marshal(m proto.Message, opts ...yaml.EncodeOption) ([]byte, error) {
	opts = slices.Clone(opts)
	opts = append(opts, yaml.UseJSONMarshaler())
	return yaml.MarshalWithOptions(m, opts...)
}

// YAMLToJSON converts YAML bytes into JSON bytes for protojson.Unmarshal.
func YAMLToJSON(y []byte) ([]byte, error) {
	var i interface{}
	err := yaml.Unmarshal(y, &i)
	if err != nil {
		return nil, err
	}
	return json.Marshal(i)
}
